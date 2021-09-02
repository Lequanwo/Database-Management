
#include "src/include/qe.h"

namespace PeterDB {

    //<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Filter >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>//

    Filter::Filter(Iterator *input, const Condition &condition) {
        this->input = input;
        this->condition = condition;
    }

    RC Filter::getNextTuple(void *data) {
        do {
            if (input->getNextTuple(data) == QE_EOF) {
                return QE_EOF;
            }
        } while(!isSatisfied(data));

        // get the next tuple
        return 0;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        input->getAttributes(attrs);
        return 0;
    }

    bool Filter::isSatisfied(void *data) {
        if (condition.bRhsIsAttr) {
            // should be value, instead of a attribute
            return -1;
        }

        std::vector<Attribute> lhsAttrs;
        input->getAttributes(lhsAttrs);

        // get the attrType
        attrType = condition.rhsValue.type;

        void *selData = malloc(PAGE_SIZE);
        std::vector<std::string> selAttrs;
        selAttrs.push_back(condition.lhsAttr);

        extractFromReturnedData(lhsAttrs, selAttrs, data, selData);

        int leftDataOffset = ceil(double(selAttrs.size())/CHAR_BIT);
        RC rc = compLeftRightVal(this->attrType, this->condition, selData, condition.rhsValue.data, leftDataOffset, 0);

        free(selData);

        return rc;
    }

    //<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Project >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>//

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
        this->input = input;
        this->attrNames = attrNames;

        // get all attributes in input
        input->getAttributes(allAttrs);

        // find the matching attributes
        for(const auto & attrName : attrNames){
            for(auto & allAttr : allAttrs){
                if(allAttr.name == attrName){
                    projectAttrs.emplace_back(allAttr);
                    // to the next attr name
                    break;
                }
            }
        }
    }

    RC Project::getNextTuple(void *data) {
        // first, we get the whole tuple.
        void *tuple = malloc(PAGE_SIZE);
        memset(tuple, 0, PAGE_SIZE);

        if(input->getNextTuple(tuple) != 0)
        {
            free(tuple);
            return QE_EOF;
        }

        // the filter by the names
        extractFromReturnedData(this->allAttrs, this->attrNames, tuple, data);
        free(tuple);

        return 0;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = projectAttrs;

        return 0;
    }

    //<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Block Nest-Loop Join >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>//

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->leftIn->getAttributes(lhsAttributes);
        this->rightIn->getAttributes(rhsAttributes);

        this->condition = condition;
        this->numPageinBlock = numPages;
        this->lhsTupleData = nullptr;
        this->rhsTupleData = nullptr;
        this->leftTableisOver = false;
        this->isFirstTime = true;
        this->restart = false;
        this->lhsOffset = 0;
        this->rhsOffset = 0;
    }

    RC BNLJoin::getNextTuple(void *data) {
        if (!condition.bRhsIsAttr) {
            // should be attribute, instead of value
            return -1;
        }

        // like filter, should load data firstly
        if (isFirstTime) {
            // initialize
            isFirstTime = false;

            rhsTupleData = malloc(PAGE_SIZE);
            if (rightIn->getNextTuple(rhsTupleData) == QE_EOF) {
                return -1;
            }

            rm.printTuple(rhsAttributes, rhsTupleData, std::cout);

            rhsOffset = ceil((double(rhsAttributes.size()) / CHAR_BIT));
            for (auto &attr : rhsAttributes) {
                int stepLen = 0;
                if (attr.type == TypeVarChar) {
                    // we need to add the varchar length to the offset
                    int varCharLen = 0;
                    memcpy(&varCharLen, (char *) rhsTupleData + rhsOffset, 4);
                    stepLen += varCharLen;
                }

                stepLen += 4;

                if (attr.name == condition.rhsAttr) {
                    joinTargetType = attr.type;
                    break;
                }

                rhsOffset += stepLen;
            }

            loadBlockBuffer(true);
        }

        // do the comparison and check status
        while (!isBNLJoinSatisfied()) {
            if (joinTargetType == TypeInt) {
                // first, we need to check whether the hashmap is empty or full
                if (intCurrentPos == intBlockBuffer.end() && !leftTableisOver) {
                    // we have reached the last element of hashmap and we should clean and reload.
                    loadBlockBuffer(false);
                    continue;
                }

                if (intCurrentPos == intBlockBuffer.end() && leftTableisOver) {
                    // Finish the first round, we need to update both the left and right.
                    restart = true;
                    it = leftTable.begin();

                    loadBlockBuffer(false);

                    memset(rhsTupleData, 0, PAGE_SIZE);
                    if (rightIn->getNextTuple(rhsTupleData) == QE_EOF) {
                        if (leftTableisOver) {
                            // finish the join
                            return -1;
                        }
                    }
                    continue;
                }

                intCurrentPos++;
            } else if (joinTargetType == TypeReal) {
                // first, we need to check whether the hashmap is empty or full
                if (floatCurrentPos == floatBlockBuffer.end() && !leftTableisOver) {
                    // we have reached the last element of hashmap and we should clean and reload.
                    loadBlockBuffer(false);
                    continue;
                }

                if (floatCurrentPos == floatBlockBuffer.end() && leftTableisOver) {
                    // Finish the first round, we need to update both the left and right.
                    restart = true;
                    it = leftTable.begin();
                    loadBlockBuffer(false);
                    memset(rhsTupleData, 0, PAGE_SIZE);
                    if (rightIn->getNextTuple(rhsTupleData) == QE_EOF) {
                        // finish the join.
                        return -1;
                    }

                    continue;
                }

                floatCurrentPos++;
            } else {
                // first, we need to check whether the hashmap is empty or full
                if (varcharCurrentPos == varcharBlockBuffer.end() && !leftTableisOver) {
                    // we have reached the last element of hashmap and we should clean and reload.
                    loadBlockBuffer(false);
                    continue;
                }

                if (varcharCurrentPos == varcharBlockBuffer.end() && leftTableisOver) {
                    // Finish the first round, we need to update both the left and right.
                    restart = true;
                    it = leftTable.begin();
                    loadBlockBuffer(false);
                    memset(rhsTupleData, 0, PAGE_SIZE);
                    if (rightIn->getNextTuple(rhsTupleData) == QE_EOF) {
                        // finish the join.
                        return -1;
                    }

                    continue;
                }

                varcharCurrentPos++;
            }
        }

        // current satisfy the join, we concatenate them and output.
        std::vector<Attribute> allAttrs;
        this->getAttributes(allAttrs);

        if (joinTargetType == TypeInt) {
            concatenateData(allAttrs, lhsAttributes, rhsAttributes, intCurrentPos->second, rhsTupleData, data);
            intCurrentPos++;
        } else if (joinTargetType == TypeReal) {
            concatenateData(allAttrs, lhsAttributes, rhsAttributes, floatCurrentPos->second, rhsTupleData, data);
            floatCurrentPos++;
        } else {
            // varchar
            concatenateData(allAttrs, lhsAttributes, rhsAttributes, varcharCurrentPos->second, rhsTupleData, data);
            varcharCurrentPos++;
        }

        return 0;
    }

    RC BNLJoin::loadBlockBuffer(bool isFirst) {

        // clean first.
        cleanBlockBuffer();

        // call the left.getNextTuple to load tuples into the map until the buffer is full.
        int totalTupleLen = 0;

        if(isFirst){
            // we need to calculate countIndex and offsetIndex
            lhsTupleData = malloc(PAGE_SIZE);
            memset(lhsTupleData, 0, PAGE_SIZE);
            if(leftIn->getNextTuple(lhsTupleData) == QE_EOF){
                // the scan of left table is over
                leftTableisOver = true;
                free(lhsTupleData);
                // std::cout << "[Error] No data in the left table. " << std::endl;
                return -1;
            }

            /* for debug */
            rm.printTuple(lhsAttributes, lhsTupleData, std::cout);

            // copy the tuple to our vector for later use.
            leftTable.emplace_back(lhsTupleData);

            // Find the attribute's offset of this tuple
            lhsOffset = ceil((double(lhsAttributes.size())/CHAR_BIT));
            for(auto &attr : lhsAttributes){
                int stepLen = 0;
                if(attr.type == TypeVarChar){
                    // we need to add the varchar length to the offset
                    int varCharLen = 0;
                    memcpy(&varCharLen, (char*)lhsTupleData + lhsOffset, 4);
                    stepLen += varCharLen;
                }

                stepLen += 4;

                if(attr.name == condition.lhsAttr){
                    // matched
                    if(attr.type != joinTargetType){
                        return -1;
                    }
                    break;
                }

                lhsOffset += stepLen;
            }

            // get the joinValue and load this tuple.
            if(joinTargetType == TypeInt){
                int joinVal = 0;
                memcpy(&joinVal, (char*)lhsTupleData + lhsOffset, sizeof(int));
                intBlockBuffer.insert(std::pair<int, void*>(joinVal, lhsTupleData));
            }
            else if(joinTargetType == TypeReal){
                float joinVal;
                memcpy(&joinVal, (char*)lhsTupleData + lhsOffset, sizeof(float));
                floatBlockBuffer.insert(std::pair<float, void*>(joinVal, lhsTupleData));
            }
            else{
                // varchar
                int varcharLen = 0;
                char* temp;
                memcpy(&varcharLen, (char*)lhsTupleData + lhsOffset, 4);
                memcpy(&temp, (char*)lhsTupleData + lhsOffset + 4, varcharLen);
                std::string joinVal(temp);
                varcharBlockBuffer.insert(std::pair<std::string, void*>(joinVal, lhsTupleData));
            }

            totalTupleLen += getDataLength(lhsAttributes, lhsTupleData);
        }

        while(totalTupleLen <= numPageinBlock * PAGE_SIZE){
            // clear the tuple to getNext
            lhsTupleData = malloc(PAGE_SIZE);
            memset(lhsTupleData, 0, PAGE_SIZE);

            if(!restart){
                if(leftIn->getNextTuple(lhsTupleData) == QE_EOF){
                    // scan over
                    leftTableisOver = true;
                    break;
                }

                // copy the tuple to our vector for later use.
                leftTable.emplace_back(lhsTupleData);
            }
            else{
                if(leftTable.empty()){
                    return -1;
                }

                if(it == leftTable.end()){
                    leftTableisOver = true;
                    break;
                }

                memcpy(lhsTupleData, *it, PAGE_SIZE);
                it++;
            }

            /* for debug */
            rm.printTuple(lhsAttributes, lhsTupleData, std::cout);

            // load this tuple.
            if(joinTargetType == TypeInt){
                int joinVal = 0;
                memcpy(&joinVal, (char*)lhsTupleData + lhsOffset, sizeof(int));
                intBlockBuffer.insert(std::pair<int, void*>(joinVal, lhsTupleData));
            }
            else if(joinTargetType == TypeReal){
                float joinVal;
                memcpy(&joinVal, (char*)lhsTupleData + lhsOffset, sizeof(float));
                floatBlockBuffer.insert(std::pair<float, void*>(joinVal, lhsTupleData));
            }
            else{
                // varchar
                int varcharLen = 0;
                char* temp;
                memcpy(&varcharLen, (char*)lhsTupleData + lhsOffset, 4);
                memcpy(&temp, (char*)lhsTupleData + lhsOffset + 4, varcharLen);
                std::string joinVal(temp);
                varcharBlockBuffer.insert(std::pair<std::string, void*>(joinVal, lhsTupleData));
            }

            totalTupleLen += getDataLength(lhsAttributes, lhsTupleData);
        }

        // initialize the currentPos ptr
        if(joinTargetType == TypeInt){
            intCurrentPos = intBlockBuffer.begin();
        }
        else if(joinTargetType == TypeReal){
            floatCurrentPos = floatBlockBuffer.begin();
        }
        else{
            // varchar
            varcharCurrentPos = varcharBlockBuffer.begin();
        }

        // free(lhsTupleData);

        return 0;
    }

    RC BNLJoin::cleanBlockBuffer() {

        if(joinTargetType == TypeInt){
            if(intBlockBuffer.empty())
                return 0;

            intBlockBuffer.clear();
        }
        else if(joinTargetType == TypeReal){
            if(floatBlockBuffer.empty())
                return 0;

            floatBlockBuffer.clear();
        }
        else{
            // varchar
            if(varcharBlockBuffer.empty())
                return 0;

            varcharBlockBuffer.clear();
        }

        return 0;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        // add lhs and rhs
        for(auto& lhsAttr : lhsAttributes){
            attrs.emplace_back(lhsAttr);
        }

        for(auto& rhsAttr : rhsAttributes){
            attrs.emplace_back(rhsAttr);
        }

        return 0;
    }

    bool BNLJoin::isBNLJoinSatisfied() {
        // check status
        if(joinTargetType == TypeInt){
            // first, we need to check whether the hashmap is empty or full
            if(intCurrentPos == intBlockBuffer.end())
                return false;

            int rhsJoinVal = 0;
            memcpy(&rhsJoinVal, (char*)rhsTupleData + rhsOffset, sizeof(int));
            return intCurrentPos->first == rhsJoinVal;
        }
        else if(joinTargetType == TypeReal){
            // first, we need to check whether the hashmap is empty or full
            if(floatCurrentPos == floatBlockBuffer.end())
                return false;

            float rhsJoinVal;
            memcpy(&rhsJoinVal, (char*)rhsTupleData + rhsOffset, sizeof(float));
            return floatCurrentPos->first == rhsJoinVal;
        }
        else{
            // first, we need to check whether the hashmap is empty or full
            if(varcharCurrentPos == varcharBlockBuffer.end())
                return false;

            char* temp;
            int varCharLen = 0;
            memcpy(&varCharLen, (char*)rhsTupleData + rhsOffset, 4);
            memcpy(&temp, (char*)rhsTupleData + rhsOffset + 4, varCharLen);
            std::string rhsJoinVal(temp);

            return varcharCurrentPos->first == rhsJoinVal;
        }
    }

    //<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Index Nest-Loop Join >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>//

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->condition = condition;

        this->isNewLeftNeeded = true;
        this->isNewRight = true;

        this->leftIn->getAttributes(this->leftInAttrs);
        this->rightIn->getAttributes(this->rightInAttrs);

        this->leftAttrName.push_back(this->condition.lhsAttr);
        this->rightAttrName.push_back(this->condition.rhsAttr);

        leftValue = malloc(PAGE_SIZE);
        rightValue = malloc(PAGE_SIZE);
        leftAttrValue = malloc(PAGE_SIZE);
        rightAttrValue = malloc(PAGE_SIZE);
        keyValue = malloc(PAGE_SIZE);

        for(Attribute & attr : this->leftInAttrs){
            this->allAttrs.push_back(attr);
        }
        for(Attribute & attr : this->rightInAttrs){
            this->allAttrs.push_back(attr);
        }
    }

    INLJoin::~INLJoin(){
        free(leftValue);
        free(rightValue);
        free(leftAttrValue);
        free(rightAttrValue);
        free(keyValue);
    }

    RC INLJoin::getNextTuple(void *data) {
        while(true){

            if(isNewLeftNeeded){
                while(leftIn->getNextTuple(leftValue) != QE_EOF){
                    extractFromReturnedData(leftInAttrs, leftAttrName, leftValue, leftAttrValue);
                    if (((char *) leftAttrValue)[0] & (unsigned) 1 << (unsigned) 7){
                        // null
                        continue;
                    }
                    // left value
                    int nullIndicatorSize = ceil(double(leftAttrName.size())/CHAR_BIT);
                    isNewLeftNeeded = false;
                    memcpy(keyValue, (char *)leftAttrValue + nullIndicatorSize, PAGE_SIZE - nullIndicatorSize);

//                    float keyv;
//                    memcpy(&keyv, keyValue, sizeof(float));
//                    std::cout << "left value: " << keyv << std::endl;

                    break;
                }
                if(isNewLeftNeeded){
                    // get the end of left
                    return QE_EOF;
                }
            }

            if(isNewRight){
                rightIn->setIterator(keyValue, keyValue, true, true);
            }
            while(true){
                RC rc = rightIn->getNextTuple(rightValue);
                if(rc == IX_EOF){
                    isNewLeftNeeded = true;
                    isNewRight = true;
                    break;
                    // leftIn should get a new value
                }
                else{
                    isNewRight = false;
                    concatenateData(allAttrs, leftInAttrs, rightInAttrs, leftValue, rightValue, data);
                    return 0;
                }
            }
        }

        return -1;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        for(auto& attr1 : leftInAttrs){
            attrs.emplace_back(attr1);
        }
        for(auto& attr2 : rightInAttrs){
            attrs.emplace_back(attr2);
        }
        return 0;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    //<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Aggregate >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>//

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {
        this->input = input;
        this->input->getAttributes(this->allAttrs);
        this->aggAttr = aggAttr;
        this->op = op;
        this->opDone = false;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {
        // extra credits
    }

    RC Aggregate::getNextTuple(void *data) {
        if (this->opDone){
            return QE_EOF;
        }

        void *selData = malloc(PAGE_SIZE);
        void *tuple = malloc(PAGE_SIZE);
        std::vector<std::string> selAttrNames;
        selAttrNames.emplace_back(this->aggAttr.name);
        int nullIndicatorSize = ceil(double(selAttrNames.size())/CHAR_BIT);
        int count = 0;

        int aggInt;
        float aggFloat;
        if (this->op == MIN) {
            aggInt = std::numeric_limits<int>::max();
            aggFloat = std::numeric_limits<float>::max();
        } else {
            aggInt = 0;
            aggFloat = 0;
        }

        while (input->getNextTuple(tuple) == 0) {
            extractFromReturnedData(this->allAttrs, selAttrNames, tuple, selData);

            if (this->aggAttr.type == TypeInt) {
                doIntOp(selData, nullIndicatorSize, aggInt);
            } else {
                doFloatOp(selData, nullIndicatorSize, aggFloat);
            }
            count++;
        }

        this->opDone = true;

        // because the final aggregation result should be in float
        float aggResult;

        if (this->aggAttr.type == TypeInt) {
            aggResult = (float)aggInt;
        } else {
            aggResult = aggFloat;
        }

        if (this->op == AVG) {
            aggResult /= count;
        }
        if (this->op == COUNT) {
            aggResult = float(count);
        }

        memcpy(data, selData, nullIndicatorSize);
        free(selData);
        free(tuple);

        memcpy((char *)data+nullIndicatorSize, &aggResult, sizeof(float));

        return 0;
    }

    RC Aggregate::doIntOp(void *data, int nullIndicatorSize, int &aggInt) {
        int tmp;
        memcpy(&tmp, (char *)data+nullIndicatorSize, 4);

        switch (this->op) {
            case MIN: {
                if(tmp < aggInt)
                    aggInt = tmp;
                break;
            }
            case MAX: {
                if(tmp > aggInt)
                    aggInt = tmp;
                break;
            }
            case SUM: {
                aggInt += tmp;
                break;
            }
            case AVG: {
                aggInt += tmp;
                break;
            }
            case COUNT:
                break;
            default:
                break;
        }
        return 0;
    }

    RC Aggregate::doFloatOp(void *data, int nullIndicatorSize, float &aggFloat) {
        float tmp;
        memcpy(&tmp, (char *)data+nullIndicatorSize, 4);

        switch (this->op) {
            case MIN: {
                if(tmp < aggFloat)
                    aggFloat = tmp;
                break;
            }
            case MAX: {
                if(tmp > aggFloat)
                    aggFloat = tmp;
                break;
            }
            case SUM: {
                aggFloat += tmp;
                break;
            }
            case AVG: {
                aggFloat += tmp;
                break;
            }
            case COUNT:
                break;
            default:
                break;
        }
        return 0;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        std::string opAndAttr;

        switch (this->op) {
            case MIN:{
                opAndAttr = "MIN(";
                break;
            }
            case MAX:{
                opAndAttr = "MAX(";
                break;
            }
            case COUNT:{
                opAndAttr = "COUNT(";
                break;
            }
            case SUM:{
                opAndAttr = "SUM(";
                break;
            }
            case AVG:{
                opAndAttr = "AVG(";
                break;
            }
            default: {
                break;
            }
        }

        attrs.clear();
        opAndAttr += this->aggAttr.name;
        opAndAttr += ")";

        Attribute tmpAttr = this->aggAttr;
        tmpAttr.name = opAndAttr;
        tmpAttr.type = TypeReal;
        attrs.emplace_back(tmpAttr);

        return 0;
    }

    //<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Helper Function >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>//

    RC extractFromReturnedData(const std::vector<Attribute> &attrs, const std::vector<std::string> &selAttrNames, const void *data, void *selData) {

        int offsetData = 0;
        int offsetSelData = 0;

        int fieldLength = attrs.size();
        int selAttrsLength = selAttrNames.size();

        int nullIndicatorForSelAttrsSize = ceil((double(selAttrsLength) / CHAR_BIT));
        auto *nullIndicatorForSelAttrs = (unsigned char *) malloc(nullIndicatorForSelAttrsSize);
        // must set the memory to 0
        memset(nullIndicatorForSelAttrs, 0, nullIndicatorForSelAttrsSize);
        offsetSelData += nullIndicatorForSelAttrsSize;

        // 3. NullIndicator
        int nullIndicatorSize = ceil((double(fieldLength) / CHAR_BIT));
        auto *nullsIndicator = (unsigned char *) malloc(nullIndicatorSize);
        memcpy((char *) nullsIndicator, (char *) data, nullIndicatorSize);

        // 4. iterate over attributes
        int nullByteIndex, bitIndex, fieldIndex;
        int varCharLen = 0;

        // 5, iterate over selected attributes
        int selAttrsIndex;

        for (selAttrsIndex = 0; selAttrsIndex < selAttrsLength; selAttrsIndex++) {
            offsetData = nullIndicatorSize;
            for (nullByteIndex = 0; nullByteIndex < nullIndicatorSize; nullByteIndex++) {
                for (bitIndex = 0; bitIndex < 8; bitIndex++) {
                    fieldIndex = nullByteIndex * 8 + bitIndex;

                    if (fieldIndex == fieldLength)
                        break;

                    // deal with null field
                    if (nullsIndicator[nullByteIndex] & (unsigned) 1 << (unsigned) (7 - bitIndex)) {
                        if (attrs[fieldIndex].name == selAttrNames[selAttrsIndex]) {
                            nullIndicatorForSelAttrs[selAttrsIndex / CHAR_BIT] |= ((unsigned char) 1
                                    << (unsigned) (7 - selAttrsIndex % CHAR_BIT));
                            selAttrsIndex++;
                        }
                        continue;
                    }

                    switch (attrs[fieldIndex].type) {
                        case TypeVarChar:
                            if (attrs[fieldIndex].name == selAttrNames[selAttrsIndex]) {
                                memcpy(&varCharLen, (char *) data + offsetData, 4);
                                memcpy((char *) selData + offsetSelData, (char *) data + offsetData, 4 + varCharLen);
                                offsetSelData += (4 + varCharLen);
                            }
                            offsetData += (4 + varCharLen);
                            break;
                        case TypeInt:
                            if (attrs[fieldIndex].name == selAttrNames[selAttrsIndex]) {
                                /* for debug
                                int intData;
                                memcpy(&intData, (char *)data + offsetData, sizeof(intData));
                                std::cout << "right.D:  " << intData << std::endl;
                                 */

                                memcpy((char *) selData + offsetSelData, (char *) data + offsetData, 4);
                                offsetSelData += 4;
                            }
                            offsetData += 4;
                            break;
                        case TypeReal:
                            if (attrs[fieldIndex].name == selAttrNames[selAttrsIndex]) {
                                /* for debug
                                float floatData;
                                memcpy(&floatData, (char *)data + offsetData, sizeof(floatData));
                                std::cout << "right.C:  " << floatData << std::endl;
                                 */

                                memcpy((char *) selData + offsetSelData, (char *) data + offsetData, 4);
                                offsetSelData += 4;
                            }
                            offsetData += 4;
                            break;
                        default:
                            break;
                    }

                }
            }
        }
        // write the nullsIndicatorForAttributes back to data
        memcpy(selData, nullIndicatorForSelAttrs, nullIndicatorForSelAttrsSize);
        free(nullsIndicator);
        free(nullIndicatorForSelAttrs);
        return 0;
    }

    bool compLeftRightVal(AttrType attrType, Condition condition, const void *leftData, const void *rightData, int leftOffset, int rightOffset) {
        if(attrType == TypeInt){
            int leftVal, rightVal = 0;
            memcpy(&leftVal, (char*)leftData + leftOffset, sizeof(int));
            memcpy(&rightVal, (char *)rightData + rightOffset, sizeof(int));

            switch (condition.op){
                case EQ_OP:{
                    return leftVal == rightVal;
                }
                case LT_OP:{
                    return leftVal < rightVal;
                }
                case LE_OP:{
                    return leftVal <= rightVal;
                }
                case GT_OP:{
                    return leftVal > rightVal;
                }
                case GE_OP:{
                    return leftVal >= rightVal;
                };
                case NE_OP:{
                    return leftVal != rightVal;
                }
                case NO_OP:{
                    return true;
                }
                default:
                    break;
            }

            return -1;

        } else if (attrType == TypeReal){
            float leftVal, rightVal = 0;
            memcpy(&leftVal, (char*)leftData + leftOffset, sizeof(float));
            memcpy(&rightVal, (char *)rightData + rightOffset, sizeof(float));

            switch (condition.op){
                case EQ_OP:{
                    return leftVal == rightVal;
                }
                case LT_OP:{
                    return leftVal < rightVal;
                }
                case LE_OP:{
                    return leftVal <= rightVal;
                }
                case GT_OP:{
                    return leftVal > rightVal;
                }
                case GE_OP:{
                    return leftVal >= rightVal;
                };
                case NE_OP:{
                    return leftVal != rightVal;
                }
                case NO_OP:{
                    return true;
                }
                default:
                    break;
            }

            return -1;

        } else if (attrType == TypeVarChar){
            bool res;
            int varCharLen;

            memcpy(&varCharLen, (char*)leftData + leftOffset, 4);
            void* leftVal = malloc(varCharLen + 1);
            memcpy(leftVal, (char*)leftData + leftOffset + 4, varCharLen);
            ((char*)leftVal)[varCharLen] = '\0';

            memcpy(&varCharLen, (char *)rightData + rightOffset, 4);
            void* rightVal = malloc(varCharLen + 1);
            memcpy(rightVal, (char*)rightData + rightOffset + 4, varCharLen);
            ((char*)rightVal)[varCharLen] = '\0';

            switch (condition.op){
                case EQ_OP:{
                    res = ( strcmp((char*)leftVal, (char*)rightVal) == 0 );
                    free(leftVal);
                    free(rightVal);
                    return res;
                }
                case LT_OP:{
                    res = ( strcmp((char*)leftVal, (char*)rightVal) < 0 );
                    free(leftVal);
                    free(rightVal);
                    return res;
                }
                case LE_OP:{
                    res = ( strcmp((char*)leftVal, (char*)rightVal) <= 0 );
                    free(leftVal);
                    free(rightVal);
                    return res;
                }
                case GT_OP:{
                    res = ( strcmp((char*)leftVal, (char*)rightVal) > 0 );
                    free(leftVal);
                    free(rightVal);
                    return res;
                }
                case GE_OP:{
                    res = ( strcmp((char*)leftVal, (char*)rightVal) >= 0 );
                    free(leftVal);
                    free(rightVal);
                    return res;
                };
                case NE_OP:{
                    res = ( strcmp((char*)leftVal, (char*)rightVal) != 0 );
                    free(leftVal);
                    free(rightVal);
                    return res;
                }
                case NO_OP:{
                    return true;
                }
                default:
                    break;
            }

            return -1;
        }
        else{
            return -1;
        }
    }

    int getDataLength(const std::vector<Attribute> &attrs, const void *data){
        // fieldNumber, offset, length, fieldlength, fieldoffset -> 2 Byte -> char16_t

        int offset = 0;
        int numAttrs = attrs.size();
        int nullIndicatorSize = ceil(double(attrs.size())/CHAR_BIT);
        char *nullIndicator = (char *)malloc(nullIndicatorSize);
        memcpy(nullIndicator, data, nullIndicatorSize);

        offset += nullIndicatorSize;
        int fieldIndex;
        for(int nullByteIndex = 0; nullByteIndex < nullIndicatorSize; nullByteIndex++){
            for(int bitIndex = 0; bitIndex < 8; bitIndex++){
                fieldIndex = nullByteIndex * 8 + bitIndex;

                // eof
                if(fieldIndex == numAttrs)
                    break;

                // if the corresponding field is null
                if(nullIndicator[nullByteIndex] & (unsigned) 1 << (unsigned) (7-bitIndex)){
                    continue;
                }
                switch(attrs[fieldIndex].type){
                    case TypeInt:
                        offset += 4;
                        break;
                    case TypeReal:
                        offset += 4;
                        break;
                    case TypeVarChar:
                        int varCharLen;
                        memcpy(&varCharLen, (char *)data+offset, sizeof(varCharLen));
                        offset += 4;    // use two byte to store length, use two byte to store offset
                        offset += varCharLen;
                        break;
                    default:
                        break;
                }
            }
        }
        free(nullIndicator);
        return offset;
    }

    RC concatenateData(std::vector<Attribute> allAttributes, std::vector<Attribute> lhsAttributes,
                       std::vector<Attribute> rhsAttributes, void *lhsTupleData, void *rhsTupleData, void *data){
        int nullIndicatorSize = ceil(double(allAttributes.size())/CHAR_BIT);
        int nullIndicatorSizeLeft = ceil(double(lhsAttributes.size())/CHAR_BIT);
        int nullIndicatorSizeRight = ceil(double(rhsAttributes.size())/CHAR_BIT);

        auto *nullIndicator = (char *)malloc(nullIndicatorSize);
        memset(nullIndicator, 0, nullIndicatorSize);
        auto *nullIndicatorLeft = (char *)malloc(nullIndicatorSizeLeft);
        memcpy(nullIndicatorLeft, lhsTupleData, nullIndicatorSizeLeft);
        auto *nullIndicatorRight = (char *)malloc(nullIndicatorSizeRight);
        memcpy(nullIndicatorRight, rhsTupleData, nullIndicatorSizeRight);

        int lhsTupleLen = getDataLength(lhsAttributes, lhsTupleData);
        int rhsTupleLen = getDataLength(rhsAttributes, rhsTupleData);

        int lhsDataNum = lhsAttributes.size();
        int rhsDataNum = rhsAttributes.size();

        int fieldIndex;
        int leftBitIndex = 0;
        int rightBitIndex = 0;

        for(int byteIndex = 0; byteIndex < nullIndicatorSize; byteIndex++){
            for(int bitIndex= 0; bitIndex < 8; bitIndex++){
                fieldIndex = byteIndex * 8 + bitIndex;
                if(fieldIndex < lhsDataNum){
                    if(nullIndicatorLeft[(leftBitIndex+1)/CHAR_BIT] & (unsigned char) 1 << (unsigned) (7 - leftBitIndex%CHAR_BIT)){
                        nullIndicator[byteIndex] |= ((unsigned char) 1 << (unsigned) (7 - fieldIndex%CHAR_BIT));
                    }
                    leftBitIndex++;
                } else if(fieldIndex < lhsDataNum + rhsDataNum){
                    if(nullIndicatorRight[(rightBitIndex+1)/CHAR_BIT] & (unsigned char) 1 << (unsigned) (7 - rightBitIndex%CHAR_BIT)){
                        nullIndicator[byteIndex] |= ((unsigned char) 1 << (unsigned) (7 - fieldIndex%CHAR_BIT));
                    }
                    rightBitIndex++;
                }
                else{
                    break;
                }
            }
        }

        memcpy(data, nullIndicator, nullIndicatorSize);
        memcpy((char *)data+nullIndicatorSize, (char *)lhsTupleData+nullIndicatorSizeLeft, lhsTupleLen-nullIndicatorSizeLeft);
        memcpy((char *)data+nullIndicatorSize+lhsTupleLen-nullIndicatorSizeLeft, (char *)rhsTupleData+nullIndicatorSizeRight, rhsTupleLen-nullIndicatorSizeRight);

        free(nullIndicator);
        free(nullIndicatorLeft);
        free(nullIndicatorRight);
        return 0;
    }

} // namespace PeterDB