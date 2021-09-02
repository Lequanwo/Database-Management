#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <string>
#include <limits>
#include <unordered_map>
#include <cstring>
#include <climits>
#include <map>


#include "src/include/rm.h"
#include "src/include/ix.h"

namespace PeterDB {

#define QE_EOF (-1)  // end of the index scan
    typedef enum AggregateOp {
        MIN = 0, MAX, COUNT, SUM, AVG
    } AggregateOp;

    // The following functions use the following
    // format for the passed data.
    //    For INT and REAL: use 4 bytes
    //    For VARCHAR: use 4 bytes for the length followed by the characters

    typedef struct Value {
        AttrType type;          // type of value
        void *data;             // value
    } Value;

    typedef struct Condition {
        std::string lhsAttr;        // left-hand side attribute
        CompOp op;                  // comparison operator
        bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
        std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
        Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
    } Condition;

    RC extractFromReturnedData(const std::vector<Attribute> &attrs, const std::vector<std::string> &selAttrNames, const void *data, void *selData);

    bool compLeftRightVal(AttrType attrType, Condition condition, const void *leftData, const void *rightData, int leftOffset, int rightOffset);

    int getDataLength(const std::vector<Attribute> &attrs, const void *data);

    RC concatenateData(std::vector<Attribute> allAttributes, std::vector<Attribute> lhsAttributes, std::vector<Attribute> rhsAttributes, void *lhsTupleData, void *rhsTupleData, void *data);


    class Iterator {
        // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;

        virtual RC getAttributes(std::vector<Attribute> &attrs) const = 0;

        virtual ~Iterator() = default;

        PeterDB::RelationManager &rm = PeterDB::RelationManager::instance();
    };

    class TableScan : public Iterator {
        // A wrapper inheriting Iterator over RM_ScanIterator
    private:
        RelationManager &rm;
        RM_ScanIterator iter;
        std::string tableName;
        std::vector<Attribute> attrs;
        std::vector<std::string> attrNames;
        RID rid;
    public:
        TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
            //Set members
            this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            for (const Attribute &attr : attrs) {
                // convert to char *
                attrNames.push_back(attr.name);
            }

            // Call RM scan to get an iterator
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator() {
            iter.close();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);
        };

        RC getNextTuple(void *data) override {
            return iter.getNextTuple(rid, data);
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
        };

        ~TableScan() override {
            iter.close();
        };
    };

    class IndexScan : public Iterator {
        // A wrapper inheriting Iterator over IX_IndexScan
    private:
        RelationManager &rm;
        RM_IndexScanIterator iter;
        std::string tableName;
        std::string attrName;
        std::vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;
    public:
        IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName,
                  const char *alias = NULL) : rm(rm) {
            // Set members
            this->tableName = tableName;
            this->attrName = attrName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
            iter.close();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, iter);
        };

        RC getNextTuple(void *data) override {
            RC rc = iter.getNextEntry(rid, key);
            if (rc == 0) {
                rc = rm.readTuple(tableName, rid, data);
            }
            return rc;
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;


            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
        };

        ~IndexScan() override {
            iter.close();
        };
    };

    class Filter : public Iterator {
        // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );

        ~Filter() override = default;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    private:
        Iterator *input;
        Condition condition;
        AttrType attrType;

        bool isSatisfied(void *data);
    };

    class Project : public Iterator {
        // Projection operator
    public:
        Project(Iterator *input,                                // Iterator of input R
                const std::vector<std::string> &attrNames);     // std::vector containing attribute names
        ~Project() override = default;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

    private:
        Iterator *input;
        std::vector<Attribute> allAttrs;
        std::vector<Attribute> projectAttrs;
        std::vector<std::string> attrNames;
    };

    class BNLJoin : public Iterator {
        // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
                TableScan *rightIn,           // TableScan Iterator of input S
                const Condition &condition,   // Join condition
                const unsigned numPages       // # of pages that can be loaded into memory,
                //   i.e., memory block size (decided by the optimizer)
        );

        ~BNLJoin() override = default;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    private:
        Iterator *leftIn;
        TableScan *rightIn;
        Condition condition;
        unsigned numPageinBlock;
        AttrType joinTargetType;
        bool leftTableisOver;
        bool isFirstTime;
        bool restart;
        int lhsOffset;
        int rhsOffset;
        void* lhsTupleData;
        void* rhsTupleData;
        std::vector<Attribute> lhsAttributes;
        std::vector<Attribute> rhsAttributes;

        // hashmap: <value, tupleData>
        std::map<int, void*> intBlockBuffer;
        std::map<float, void*> floatBlockBuffer;
        std::map<std::string, void*> varcharBlockBuffer;
        std::map<int, void*>::iterator intCurrentPos;
        std::map<float, void*>::iterator floatCurrentPos;
        std::map<std::string, void*>::iterator varcharCurrentPos;
        std::vector<void*> leftTable;
        std::vector<void*>::iterator it;

        RC loadBlockBuffer(bool isFirst);

        RC cleanBlockBuffer();

        bool isBNLJoinSatisfied();
    };

    class INLJoin : public Iterator {
        // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
                IndexScan *rightIn,          // IndexScan Iterator of input S
                const Condition &condition   // Join condition
        );

        ~INLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

    private:
        Iterator *leftIn;
        IndexScan *rightIn;
        Condition condition;

        bool isNewLeftNeeded;
        bool isNewRight;

        std::vector<Attribute> leftInAttrs;
        std::vector<Attribute> rightInAttrs;
        std::vector<Attribute> allAttrs;

        std::vector<std::string> leftAttrName;
        std::vector<std::string> rightAttrName;

        void *leftValue;
        void *rightValue;
        void *leftAttrValue;
        void *rightAttrValue;
        void *keyValue;


    };

    // 10 extra-credit points
    class GHJoin : public Iterator {
        // Grace hash join operator
    public:
        GHJoin(Iterator *leftIn,               // Iterator of input R
               Iterator *rightIn,               // Iterator of input S
               const Condition &condition,      // Join condition (CompOp is always EQ)
               const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        );

        ~GHJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class Aggregate : public Iterator {
        // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
                  const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );

        ~Aggregate() override = default;

        RC getNextTuple(void *data) override;

        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrName = "MAX(rel.attr)"
        RC getAttributes(std::vector<Attribute> &attrs) const override;

    private:
        Iterator *input;
        std::vector<Attribute> allAttrs;
        Attribute aggAttr;
        AggregateOp op;
        bool opDone;

        RC doIntOp(void *data, int nullIndicatorSize, int &aggInt);

        RC doFloatOp(void *data, int nullIndicatorSize, float &aggFloat);
    };
} // namespace PeterDB

#endif // _qe_h_
