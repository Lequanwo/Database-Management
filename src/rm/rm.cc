#include "src/include/rm.h"

namespace PeterDB {
    RelationManager *RelationManager::_relation_manager = nullptr;


    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }


    RelationManager::RelationManager(){
        _rbfm = &RecordBasedFileManager::instance();
        createTablesRecordDescriptor();
        createColumnsRecordDescriptor();
        createIndexesRecordDescriptor();
    };


    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager::~RelationManager()  { delete _relation_manager; }

    RelationManager &RelationManager::operator=(const RelationManager &) = default;


    ///////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////////////////

    RC RelationManager::createTablesRecordDescriptor(){
        PeterDB::Attribute attr;

        attr.name = "table-id";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        _TablesDescriptor.push_back(attr);

        attr.name = "table-name";
        attr.type = PeterDB::TypeVarChar;
        attr.length = (PeterDB::AttrLength) 50;
        _TablesDescriptor.push_back(attr);

        attr.name = "file-name";
        attr.type = PeterDB::TypeVarChar;
        attr.length = (PeterDB::AttrLength) 50;
        _TablesDescriptor.push_back(attr);

        return 0;
    }


    RC RelationManager::createColumnsRecordDescriptor(){
        PeterDB::Attribute attr;

        attr.name = "table-id";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        _ColumnsDescriptor.push_back(attr);

        attr.name = "column-name";
        attr.type = PeterDB::TypeVarChar;
        attr.length = (PeterDB::AttrLength) 50;
        _ColumnsDescriptor.push_back(attr);

        attr.name = "column-type";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        _ColumnsDescriptor.push_back(attr);

        attr.name = "column-length";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        _ColumnsDescriptor.push_back(attr);

        attr.name = "column-position";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        _ColumnsDescriptor.push_back(attr);

        return 0;
    }


    RC RelationManager::createIndexesRecordDescriptor(){
        PeterDB::Attribute attr;

        attr.name = "table-id";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        _IndexesDescriptor.push_back(attr);

        attr.name = "table-name";
        attr.type = PeterDB::TypeVarChar;
        attr.length = (PeterDB::AttrLength) 50;
        _IndexesDescriptor.push_back(attr);

        attr.name = "attribute-name";
        attr.type = PeterDB::TypeVarChar;
        attr.length = (PeterDB::AttrLength) 50;
        _IndexesDescriptor.push_back(attr);

        attr.name = "index-filename";
        attr.type = PeterDB::TypeVarChar;
        attr.length = (PeterDB::AttrLength) 50;
        _IndexesDescriptor.push_back(attr);

        return 0;
    }


    RC RelationManager::prepareVarchar(int &length, std::string &varChar, void *data, int &offset){
        memcpy((char *)data + offset, &length, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)data + offset, varChar.c_str(), length);
        offset += length;
        return 0;
    }


    RC RelationManager::prepareTablesRecord(std::vector<PeterDB::Attribute> &recordDescriptor, int table_id, std::string table_name, std::string file_name, void* record){

        int offset = 0, varchar_len;
        bool nullBit;
        int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size()/CHAR_BIT);
        auto *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
        memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

        memcpy((char *)record+offset, nullsIndicator, nullFieldsIndicatorActualSize);
        offset += nullFieldsIndicatorActualSize;

        // table id
        nullBit = nullsIndicator[0] & (unsigned) 1 << (unsigned) 7;
        if(!nullBit){
            memcpy((char*)record + offset, &table_id, sizeof(int));
            offset += sizeof(int);
        }
        // table name
        nullBit = nullsIndicator[0] & (unsigned) 1 << (unsigned) 6;
        if(!nullBit){
            varchar_len = strlen(table_name.c_str());
            prepareVarchar(varchar_len, table_name, record, offset);

        }
        // filename
        nullBit = nullsIndicator[0] & (unsigned) 1 << (unsigned) 5;
        if(!nullBit){
            varchar_len = strlen(file_name.c_str());
            prepareVarchar(varchar_len, file_name, record, offset);
        }

        free(nullsIndicator);

        return 0;

    }


    RC RelationManager::insertReocord2Tables(FileHandle &fileHandle, int table_id, std::string table_name, std::string file_name ){
        void* record = malloc(PAGE_SIZE);
        RID rid;

        prepareTablesRecord(_TablesDescriptor, table_id, table_name, file_name, record);

        RC rc = _rbfm->insertRecord(fileHandle, _TablesDescriptor, record, rid);
        if(rc == 0){
            free(record);
            return 0 ;
        }
        else{
            free(record);
            return -1;
        }
    }


    RC RelationManager::init_Tables_table(FileHandle &fileHandle){
        if(_rbfm->openFile(TABLES_TABLE, fileHandle) == 0){

            RC rc2 = insertReocord2Tables(fileHandle, 0, TABLES_TABLE, TABLES_TABLE);
            if(rc2 != 0)
                return -1;

            RC rc1 = insertReocord2Tables(fileHandle, 1, COLUMNS_TABLE, COLUMNS_TABLE);
            if(rc1 != 0)
                return -1;

            RC rc3 = insertReocord2Tables(fileHandle, 2, INDEXES_TABLE, INDEXES_TABLE);
            if(rc3 != 0)
                return -3;

            _rbfm->closeFile(fileHandle);

        }
        else{
            return -1;
        }
        return 0;
    }


    RC RelationManager::prepareRecord4Cloumns(Attribute attr, int table_id, std::vector<Attribute> &tmpDescriptor, void* tmp_record, int col_pos){
        int offset = 0, varchar_len;
        bool nullBit;
        int nullFieldsIndicatorActualSize = ceil((double)tmpDescriptor.size()/CHAR_BIT);
        auto *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
        memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

        memcpy((char *)tmp_record+offset, nullsIndicator, nullFieldsIndicatorActualSize);
        offset += nullFieldsIndicatorActualSize;

        // table id
        nullBit = nullsIndicator[0] & (unsigned) 1 << (unsigned) 7;
        if(!nullBit){
            memcpy((char*)tmp_record + offset, &table_id, sizeof(int));
            offset += sizeof(int);
        }
        // column name
        nullBit = nullsIndicator[0] & (unsigned) 1 << (unsigned) 6;
        if(!nullBit){
            varchar_len = strlen(attr.name.c_str());
            prepareVarchar(varchar_len, attr.name, tmp_record, offset);
        }
        // column type
        nullBit = nullsIndicator[0] & (unsigned) 1 << (unsigned) 5;
        if(!nullBit){

            memcpy((char*)tmp_record + offset, &(attr.type), sizeof(int));
            offset += sizeof(int);
        }

        // column length
        nullBit = nullsIndicator[0] & (unsigned) 1 << (unsigned) 4;
        if(!nullBit){
            int temp_cil_length = attr.length;
            memcpy((char*)tmp_record + offset, &temp_cil_length, sizeof(int));
            offset += sizeof(int);

        }

        // column position , suppose it starts with 0
        nullBit = nullsIndicator[0] & (unsigned) 1 << (unsigned) 3;
        if(!nullBit){
            memcpy((char*)tmp_record + offset, &col_pos, sizeof(int));
            offset += sizeof(int);
        }

        free(nullsIndicator);
        return 0;

    }


    RC RelationManager::insertReocord2Columns(FileHandle &fileHandle, int table_id, const std::vector<Attribute> &recordDescriptor){

        int numAttr = recordDescriptor.size();
        int col_position = 1;
        void* tmp_record = malloc(PAGE_SIZE);

        for(int idx_descriptor = 0; idx_descriptor < numAttr; idx_descriptor++){
            Attribute this_attr = recordDescriptor[idx_descriptor];
            RID rid;
            std::vector<Attribute> tmpDescriptor;


            prepareRecord4Cloumns(this_attr, table_id, _ColumnsDescriptor, tmp_record, col_position);
            col_position++;
            RC rc = _rbfm->insertRecord(fileHandle, _ColumnsDescriptor, tmp_record, rid);
            if(rc != 0) {
                std::cout << "[ERROR]insertReocord2Columns [RelationManager::insertReocord2Columns]" << std::endl;
                free(tmp_record);
                return -1;
            }

        }
        free(tmp_record);
        return 0;
    }


    RC RelationManager::init_Columns_table(FileHandle &fileHandle){
        if(_rbfm->openFile(COLUMNS_TABLE, fileHandle) == 0){


            RC rc2 = insertReocord2Columns(fileHandle, 0, _TablesDescriptor);
            if(rc2 != 0)
                return -1;

            RC rc1 = insertReocord2Columns(fileHandle, 1, _ColumnsDescriptor);
            if(rc1 != 0)
                return -1;

            RC rc3 = insertReocord2Columns(fileHandle, 2, _IndexesDescriptor);
            if(rc3 != 0)
                return -1;

            _rbfm->closeFile(fileHandle);
        }
        else{
            return -1;
        }

        return 0;
    }


    RC RelationManager::createCatalog() {

        FileHandle fileHandle;

        if(_rbfm->createFile(TABLES_TABLE) != 0 || _rbfm->createFile(COLUMNS_TABLE) != 0 || _rbfm->createFile(INDEXES_TABLE) != 0){
            std::cout << "can not create two main CATALOG tables" << std::endl;
            return -1;
        };


        RC rc1 = init_Tables_table(fileHandle);
        if(rc1 != 0){
            return -1;
        }

        RC rc2 = init_Columns_table(fileHandle);
        if(rc2 != 0){
            return -1;
        }

        return 0;
    }


    RC RelationManager::deleteCatalog() {
        FileHandle fileHandle;

        // delete all tables registered in TABLES_TABLE
        RM_ScanIterator rm_ScanIterator_Tables;
        RC rc1 = _rbfm->openFile(TABLES_TABLE, fileHandle);
        if(rc1 != 0)
            return -1;

        std::vector<std::string> table_name_list;
        std::vector<std::string> attrNames;
        attrNames.emplace_back("table-name");

        auto* data = (char *)malloc(PAGE_SIZE);
        RID rid;

        RC rc3;
        int table_id = 2;

        int varcharLen;

        if(scan(TABLES_TABLE, "table-id", GT_OP,  &table_id, attrNames, rm_ScanIterator_Tables) == 0){
            while(rm_ScanIterator_Tables.getNextTuple(rid, data) != RM_EOF){

                memcpy(&varcharLen, data + 1, sizeof(int));
                auto* tempstring = (char* )malloc(varcharLen + 1);
                memcpy(tempstring, data + 1 + sizeof(int), varcharLen);
                tempstring[varcharLen] = '\0';

                printf ("%s \n", tempstring);
                table_name_list.emplace_back(tempstring);

                free(tempstring); //update 12/7
            }
            for(auto & onename : table_name_list){
                rc3 = _rbfm->destroyFile(onename);
//                if(rc3 != 0)
//                    return -1; // todo: error happens here, sometimes the filename is registered in TABLES,
                                 // but the file actually does not exist
            }
        }
        _rbfm->closeFile(fileHandle);
        rm_ScanIterator_Tables.close();

        //destroy catalogs
        _rbfm->destroyFile(TABLES_TABLE);
        _rbfm->destroyFile(COLUMNS_TABLE);

        _rbfm->destroyFile(INDEXES_TABLE); // 12/01 necessary to destroy all the index files?

        free(data);

        return 0;
    }


    RC RelationManager::getAvaliableTableId(int &tableid){

        RM_ScanIterator rm_ScanIterator;

        std::vector<std::string> attrs;
        attrs.emplace_back("table-id");

        bool tableid_arr[PAGE_SIZE];
        std::fill_n(tableid_arr, PAGE_SIZE, 0);
        int strating_table_id = 0;
        void *val = &strating_table_id;
        if( scan(TABLES_TABLE,"table-id",GE_OP, val, attrs,rm_ScanIterator) == 0 ) {
            while(true){
                //std::cout << "[getAvaliableTableId]" << std::endl;
                RID rid;
                auto *data=(char *)malloc(PAGE_SIZE);
                int cur_id; // starts from 0
                if(rm_ScanIterator.getNextTuple(rid, data) != RM_EOF){
                    memcpy(&cur_id,(char *)data + 1,sizeof(int));
                    tableid_arr[cur_id] = true;

                    free(data);
                }
                else{
                    free(data);
                    break;
                }
            }
            // find frist available id
            for(int temp_idx = 0; temp_idx < PAGE_SIZE; temp_idx++){
                if(!tableid_arr[temp_idx]){
                    tableid = temp_idx;
                    break;
                }
            }

        }
        else{
            rm_ScanIterator.close();
            //free(data);
            return -1;
        }

        rm_ScanIterator.close();
        //free(data);
        return 0;

    }


    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        FileHandle fileHandle;
        int this_table_id = -1;

        if(tableName == TABLES_TABLE || tableName == COLUMNS_TABLE || tableName == INDEXES_TABLE){
            return -1;
        }

        if(_rbfm->openFile(TABLES_TABLE, fileHandle) != 0)
            return -1;

        RC rc2 = getAvaliableTableId(this_table_id);
        if(rc2 != 0){
            return -1;
        }

        // create new file for new table
        RC rc3 = _rbfm->createFile(tableName);
        if(rc3 != 0){
            std::cout << "[ERROR]_rbfm->createFile(tableName)[RelationManager::createTable]" << std::endl;
            return -1;
        }

        // insert to TABLES
        insertReocord2Tables(fileHandle, this_table_id, tableName, tableName);
        _rbfm->closeFile(fileHandle);

        // insert to COLUMNS
        _rbfm->openFile(COLUMNS_TABLE, fileHandle);
        insertReocord2Columns(fileHandle, this_table_id, attrs);
        _rbfm->closeFile(fileHandle);

        return 0;
    }


    RC RelationManager::getTableIdByTableName(const std::string &tableName, int &tableId, RID &rid){

        RBFM_ScanIterator rbfmScanIterator;
        FileHandle fileHandle;

        RC rc;
        void *data = malloc(PAGE_SIZE);

        RC rc1 = _rbfm->openFile(TABLES_TABLE, fileHandle);
        if(rc1 != 0)
            return -1;

        // 1.1 prepare the value for table-name
        int length = strlen(tableName.c_str());
        void *table_name_value = malloc(length+4);
        memcpy(table_name_value, &length, 4);
        memcpy((char *)table_name_value+4, tableName.c_str(), length);

        // Only need to retrieve the tableId attribute.
        std::vector<std::string> attrNames;
        attrNames.emplace_back("table-id");

        // 1.3 initialize rbfmScanIterator
        rc = _rbfm->scan(fileHandle, _TablesDescriptor, "table-name", EQ_OP, table_name_value, attrNames, rbfmScanIterator);
        if(rc != 0){
            // std::cout << "[Error] getTableIdForCustomTable -> initiateRBFMScanIterator for Tabels" << std::endl;
            _rbfm->closeFile(fileHandle);
            free(table_name_value);
            free(data);
            return -1;
        }

        //1.4 get the record whose table-name is tableName, since there is no duplicate table names, so that the first result of getNextRecord is the final result.
        rc = rbfmScanIterator.getNextRecord(rid, data);
        if(rc != 0){
            // std::cout << "[Error] getTableIdForCustomTable -> getNextRecord for Tabels" << std::endl;
            _rbfm->closeFile(fileHandle);
            free(data);
            free(table_name_value);
            return -1;
        }
        rbfmScanIterator.close();

        // 1.5 retrieved data is in the original format, the first part is nullIndicator, then use memcpy to get tableId.
        int nullIndicatorSize = ceil(double(attrNames.size())/CHAR_BIT);
        memcpy(&tableId, (char *)data+nullIndicatorSize, 4);       // +1 -> 1 byte nullindicator

        _rbfm->closeFile(fileHandle); // update 12/7

        free(data);
        free(table_name_value);
        return 0;
    }


    RC RelationManager::deleteTable(const std::string &tableName) {

        if(tableName == TABLES_TABLE || tableName == COLUMNS_TABLE)
            return -1;

        FileHandle fileHandle;
        std::vector<Attribute> table_attrs;
        auto* data = (char *)malloc(PAGE_SIZE);
        std::vector<RID> rids;
        std::vector<std::string> attrNames;
        attrNames.emplace_back("table-id");


        // get table id by table name
        int table_id = -1;
        RID rid;
        RC rc2 = getTableIdByTableName(tableName, table_id, rid);
        if(rc2 != 0)
            return -1;


        RC rc1 = getAttributes(tableName, table_attrs);
        if(rc1 != 0)
            return -1;

        // delete records inside INDEXES_TABLE
        RC rc5 = deleteRecordsWithinIndexesCatalog(tableName, table_id);
        if(rc5 != 0){
            return -5;
        }


        //  delete record from TABLES_TABLE
        RM_ScanIterator rm_ScanIterator_Tables;
        _rbfm->openFile(TABLES_TABLE, fileHandle);
        RC rc3;
        if(scan(TABLES_TABLE, "table-id", EQ_OP,  &table_id, attrNames, rm_ScanIterator_Tables) == 0){
            while(rm_ScanIterator_Tables.getNextTuple(rid, data) != RM_EOF){
                // consider nultiple tabels with same ?????
                rids.push_back(rid);
            }
            for(auto & onerid : rids){
                rc3 = _rbfm->deleteRecord(fileHandle, _TablesDescriptor, onerid);
                if(rc3 != 0)
                    return -1;
            }
        }
        _rbfm->closeFile(fileHandle);
        rm_ScanIterator_Tables.close();

        //  delete record from COLUMNS_TABLE
        RM_ScanIterator rm_ScanIterator_Columns;
        _rbfm->openFile(COLUMNS_TABLE, fileHandle);
        RC rc4;
        if(scan(COLUMNS_TABLE, "table-id", EQ_OP,  &table_id, attrNames, rm_ScanIterator_Columns) == 0){
            while(rm_ScanIterator_Columns.getNextTuple(rid, data) != RM_EOF){
                rc2 = _rbfm->deleteRecord(fileHandle, _ColumnsDescriptor, rid);
                if(rc2 != 0) {
                    std::cout << "[ERROR]_rbfm->deleteRecord [RelationManager::deleteTable]" << std::endl;
                    return -1;
                }
            }
        }
        _rbfm->closeFile(fileHandle);
        _rbfm->destroyFile(tableName);
        rm_ScanIterator_Columns.close();

        free(data);
        return 0;
    }


    RC RelationManager::deleteRecordsWithinIndexesCatalog(const std::string &tableName, int &table_id){

        FileHandle fileHandle;
        _rbfm->openFile(INDEXES_TABLE, fileHandle);
        int numPages = fileHandle.getNumberOfPages();
        if(numPages == 0){
            return 0;
        }

        /*
         // prepare var char for table name
        void *varchar_tablename = malloc(PAGE_SIZE);
        int len_tablename = strlen(tableName.c_str());
        memcpy(varchar_tablename, &len_tablename, sizeof(int));
        memcpy((char* )varchar_tablename + sizeof(int), tableName.c_str(), len_tablename);
         */

        // prepare value for scan
        void *tableid = malloc(sizeof(int));
        memcpy((char* )tableid, &table_id, sizeof(int));

        RBFM_ScanIterator rbfmScanIterator4IndexesScan;
        RID tmp_rid;
        void *tmp_data = malloc(PAGE_SIZE);
        std::vector<std::string> wantedAttrs;
        wantedAttrs.emplace_back("index-filename");
        int nullIndicatorSize = ceil((double)wantedAttrs.size() / CHAR_BIT );
        int len_indexFilename;

        if(_rbfm->scan(fileHandle, _IndexesDescriptor, "table-id", EQ_OP, tableid, wantedAttrs, rbfmScanIterator4IndexesScan) == 0){

            while(rbfmScanIterator4IndexesScan.getNextRecord(tmp_rid, tmp_data) != RM_EOF){

                if(_rbfm->deleteRecord(fileHandle, _IndexesDescriptor, tmp_rid) != 0)
                    return -1;

                memcpy(&len_indexFilename, (char* )tmp_data + nullIndicatorSize, sizeof(int));
                auto* varchar_indexFilename = (char* )malloc(len_indexFilename + 1);
                memcpy(varchar_indexFilename, (char* )tmp_data + nullIndicatorSize + sizeof(int), len_indexFilename);
                varchar_indexFilename[len_indexFilename] = '\0';

                std::string tmpIndexFilename = std::string(varchar_indexFilename);

                if(_rbfm->destroyFile(tmpIndexFilename)) {
                    free(varchar_indexFilename);
                    return -2;
                }
                free(varchar_indexFilename);
            }

        }else{
            free(tmp_data);
            return -1;
        }

        rbfmScanIterator4IndexesScan.close();
        _rbfm->closeFile(fileHandle);
        free(tableid); // update 12/7
        free(tmp_data);
        return 0;
    }


    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        int tableId = -1;
        RID rid;

        RC rc1 = getTableIdFromTableTable(tableName, tableId, rid);
        if(rc1 != 0)
            return -1;

        RC rc2 = getAttributesGivenTableId(tableId, attrs);
        if(rc2 != 0)
            return -1;

        return 0;
    }


    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        if(tableName == TABLES_TABLE || tableName == COLUMNS_TABLE || tableName == INDEXES_TABLE){
            return -1; // update 11/30
        }

        FileHandle fileHandle;
        std::vector<Attribute> table_attrs;

        getAttributes(tableName, table_attrs);
        if(_rbfm->openFile(tableName, fileHandle)==0){
            if(_rbfm->insertRecord(fileHandle, table_attrs, data,rid) == 0){
                //std::cout <<"[SUCCESS] insert tuple [RelationManager::insertTuple]" << std::endl;
                _rbfm->closeFile(fileHandle);

                //todo: if you insert a tuple into a table using RelationManager::insertTuple(),
                // the tuple should be inserted into thetable (via the RBFM layer) and
                // each corresponding entry should be inserted into each associated index of the table (via the IX layer).

                RC rc = insertEntriesToExistingIndexesFiles(tableName, table_attrs, rid);
                if(rc == -1){
                    return -1;
                }


                return 0;
            }
            else{
                _rbfm->closeFile(fileHandle);
                std::cout <<"[ERROR] insert tuple [RelationManager::insertTuple]" << std::endl;
                return -1;
            }
        }

        return -1;
    }


    RC RelationManager::insertEntriesToExistingIndexesFiles(const std::string &tableName, std::vector<Attribute> &table_attrs, const RID &rid){
        // std::cout << "insertEntriesToExistingIndexesFiles"<< std::endl;
        std::map<std::pair<std::string, std::string>, RID> attr2ridMap;
        RC rc1 = extractIndexFileInfoFromIndexesCatalog(tableName, attr2ridMap);
        if(rc1 == -1){
            return -1;
        }
        if(rc1 == 1)
            return 1;

        FileHandle fileHandle;
        IXFileHandle ixFileHandle;


        void *returnedKey = malloc(PAGE_SIZE);
        RC rc2 = _rbfm->openFile(tableName, fileHandle);
        if(rc2 != 0){
            return -2;
        }
        RC rc3;
        for(auto it = attr2ridMap.begin(); it != attr2ridMap.end(); it++){
            if( _rbfm->readAttribute(fileHandle, table_attrs, rid, it->first.first, returnedKey) !=0 )
                return -1;

            char nullIndicator;
            memcpy(&nullIndicator, returnedKey, sizeof(char));
            int nullIndicatorSize = ceil((double(table_attrs.size())/CHAR_BIT));
            memmove(returnedKey, (char *)returnedKey+nullIndicatorSize, PAGE_SIZE-nullIndicatorSize);

            if(_indexManager->openFile(it->first.second, ixFileHandle) != 0)
                return -1;

            for(auto &attribute : table_attrs){
                if(attribute.name == it->first.first){
                    rc3 = _indexManager->insertEntry(ixFileHandle, attribute, returnedKey, rid);
                    if(rc3 != 0){
                        return -3;
                    }
                }
            }
            _indexManager->closeFile(ixFileHandle);
        }


        _rbfm->closeFile(fileHandle);
        free(returnedKey);
        return 0;
    }


    RC RelationManager::extractIndexFileInfoFromIndexesCatalog(const std::string &tableName, std::map<std::pair<std::string, std::string>, RID> &attr2ridMap){

        FileHandle fileHandle4IndexCatalog;
        RC rc1 = _rbfm->openFile(INDEXES_TABLE, fileHandle4IndexCatalog);
        int numPagesOfIndexesCatalog = fileHandle4IndexCatalog.getNumberOfPages();


        if(numPagesOfIndexesCatalog == 0){
            _rbfm->closeFile(fileHandle4IndexCatalog);
            return 1;
        }
        if(rc1 != 0){
            _rbfm->closeFile(fileHandle4IndexCatalog);
            return -1;
        }

        std::vector<std::string> indexAttrs;
        indexAttrs.emplace_back("attribute-name");
        indexAttrs.emplace_back("index-filename");
        RBFM_ScanIterator rbfmScanIterator;

        int tableId;
        RID rid;
        RC rc = getTableIdByTableName(tableName, tableId, rid);
        if(rc != 0){
            return -1;
        }

        RID indexRid;
        void *indexData = malloc(PAGE_SIZE);
        int nullIndicatorSize = ceil((double)indexAttrs.size()/CHAR_BIT);
        int tempOffset;

        if(_rbfm->scan(fileHandle4IndexCatalog, _IndexesDescriptor, "table-id", EQ_OP, &tableId, indexAttrs, rbfmScanIterator) == 0){
            while (rbfmScanIterator.getNextRecord(indexRid, indexData) != RM_EOF){
                tempOffset = nullIndicatorSize;
                int varcharLen_attributeName;
                memcpy(&varcharLen_attributeName, (char *) indexData + tempOffset, sizeof(int));

                tempOffset += sizeof(int);
                auto *varchar_attributeName = (char *) malloc(varcharLen_attributeName + 1);
                memcpy(varchar_attributeName, (char *) indexData + tempOffset, varcharLen_attributeName);
                tempOffset += varcharLen_attributeName;
                varchar_attributeName[varcharLen_attributeName] = '\0';

                int varcharLen_indexFilename;
                memcpy(&varcharLen_indexFilename, (char *) indexData + tempOffset, sizeof(int)); tempOffset += sizeof(int);
                auto *varchar_indexFilename = (char *) malloc(varcharLen_indexFilename + 1);
                memcpy(varchar_indexFilename, (char *) indexData + tempOffset, varcharLen_indexFilename);
                varchar_indexFilename[varcharLen_indexFilename] = '\0';

                attr2ridMap.insert(std::pair<std::pair<std::string, std::string>, RID>(std::pair<std::string, std::string>(varchar_attributeName, varchar_indexFilename), indexRid));
                free(varchar_attributeName);
                free(varchar_indexFilename);
            }
        }

        rbfmScanIterator.close();
        _rbfm->closeFile(fileHandle4IndexCatalog);

        return 0; // update 12/8

    }


    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        FileHandle fileHandle;
        std::vector<Attribute> table_attrs;

        getAttributes(tableName, table_attrs);
        if(_rbfm->openFile(tableName, fileHandle)==0){
            RC rc = _rbfm->deleteRecord(fileHandle, table_attrs,rid);
            if(rc == 0){
                _rbfm->closeFile(fileHandle);

                RC rc1 = deleteEntriesFromExistingIndexesFiles(tableName, table_attrs, rid);
                if(rc1 == -1)
                    return -1;

                // std::cout <<"[SUCCESS] delete tuple [RelationManager::deleteTuple]" << std::endl;
                return 0;
            }
            else{
                _rbfm->closeFile(fileHandle);
                return rc;
            }


        }

        return -1;
    }


    RC RelationManager::deleteEntriesFromExistingIndexesFiles(const std::string &tableName, std::vector<Attribute> &table_attrs, const RID &rid){
        std::map<std::pair<std::string, std::string>, RID> attr2ridMap;
        RC rc1 = extractIndexFileInfoFromIndexesCatalog(tableName, attr2ridMap);
        if(rc1 == -1){
            return -1;
        }
        if(rc1 == 1)
            return 1;

        FileHandle fileHandle;
        IXFileHandle ixFileHandle;


        void *returnedKey = malloc(PAGE_SIZE);
        RC rc2 = _rbfm->openFile(tableName, fileHandle);
        if(rc2 != 0){
            return -2;
        }
        RC rc3;
        for(auto it = attr2ridMap.begin(); it != attr2ridMap.end(); it++){
            if( _rbfm->readAttribute(fileHandle, table_attrs, rid, it->first.first, returnedKey) !=0 )
                return -1;

            char nullIndicator;
            memcpy(&nullIndicator, returnedKey, sizeof(char));
            int nullIndicatorSize = ceil((double(table_attrs.size())/CHAR_BIT));
            memmove(returnedKey, (char *)returnedKey+nullIndicatorSize, PAGE_SIZE-nullIndicatorSize);

            if(_indexManager->openFile(it->first.second, ixFileHandle) != 0)
                return -1;

            for(auto &attribute : table_attrs){
                if(attribute.name == it->first.first){
                    rc3 = _indexManager->deleteEntry(ixFileHandle, attribute, returnedKey, rid);
                    if(rc3 != 0){
                        return -3;
                    }
                }
            }
            _indexManager->closeFile(ixFileHandle);
        }


        _rbfm->closeFile(fileHandle);
        free(returnedKey);
        return 0;
    }



    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        FileHandle fileHandle;
        std::vector<Attribute> table_attrs;

        getAttributes(tableName, table_attrs);

        //delete entries from index files
        RC rc1 = deleteEntriesFromExistingIndexesFiles(tableName, table_attrs, rid);
        if(rc1 == -1){  //todo
            return -1;
        }


        if(_rbfm->openFile(tableName, fileHandle) == 0){

            if(_rbfm->updateRecord(fileHandle, table_attrs, data, rid) == 0){

                _rbfm->closeFile(fileHandle);
                return 0;
            }
            _rbfm->closeFile(fileHandle);


            RC rc2 = insertEntriesToExistingIndexesFiles(tableName, table_attrs, rid);

            if(rc2 == -1){
                return -1;
            }

        }
        return -1;
    }


    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        FileHandle fileHandle;
        std::vector<Attribute> table_attrs;

        getAttributes(tableName, table_attrs);

        if(_rbfm->openFile(tableName, fileHandle) == 0){

            if(_rbfm->readRecord(fileHandle, table_attrs, rid, data) == 0){

                _rbfm->closeFile(fileHandle);
                return 0;
            }
            _rbfm->closeFile(fileHandle);
        }
        // std::cout << "can not open file [RelationManager::readTuple]" << std::endl;
        return -1;
    }


    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return _rbfm->printRecord(attrs, data, out);
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        FileHandle fileHandle;
        std::vector<Attribute> table_attrs;

        if(getAttributes(tableName, table_attrs)  == 0){

            if(_rbfm->openFile(tableName, fileHandle) == 0){

                if(_rbfm->readAttribute(fileHandle, table_attrs, rid, attributeName, data) == 0){

                    // std::cout <<"[SUCCESS] read attribute [RelationManager::readAttribute]\n" << std::endl;

                    _rbfm->closeFile(fileHandle);
                    return 0;

                }
            }
        }
        return -1;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        std::vector<Attribute> attrs_table;
        FileHandle fileHandle;
        RC rc1 = getAttributes(tableName, attrs_table);
        if(rc1 != 0)
            return -1;
        else{
            _rbfm->openFile(tableName, fileHandle);

            RC rc2 = _rbfm->scan(fileHandle, attrs_table, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.getRBFMScanIterator());
            if(rc2 != 0)
                return -1;
            else
                return 0;
        }
        return 0;
    }


    RC RelationManager::getTableIdFromTableTable(const std::string &tableName, int &tableId, RID &rid) {
        RBFM_ScanIterator rbfmScanIterator;
        FileHandle fileHandle;

        RC rc = _rbfm->openFile(TABLES_TABLE, fileHandle);
        if (rc != 0) {
            return -1;
        }

        // write table name
        int length  = strlen(tableName.c_str());
        void *tableNameVale = malloc(length+sizeof(int));
        memcpy(tableNameVale, &length, sizeof(int));
        memcpy((char *)tableNameVale+sizeof(int), tableName.c_str(), length);

        // Only need to retrieve the tableId attribute.
        std::vector<std::string> attrNames;
        attrNames.emplace_back("table-id");

        // init scan iterator
        rc = _rbfm->scan(fileHandle, _TablesDescriptor, "table-name", EQ_OP, tableNameVale, attrNames, rbfmScanIterator);
        if (rc != 0) {
            _rbfm->closeFile(fileHandle);
            free(tableNameVale);
            return -2;
        }

        // get the next, and at the same time, the first and only record whose table name is tableName
        void* data = malloc(PAGE_SIZE);
        rc = rbfmScanIterator.getNextRecord(rid, data);
        if (rc != 0) {
            _rbfm->closeFile(fileHandle);
            free(tableNameVale);
            free(data);
            return -3;
        }

        rbfmScanIterator.close();
        _rbfm->closeFile(fileHandle); // update 12/7

        // get the table id
        int nullIndicatorSize = ceil((double(attrNames.size())/CHAR_BIT));
        memcpy(&tableId, (char *)data+nullIndicatorSize, 4);
        free(data);
        free(tableNameVale);
        return 0;
    }


    RC RelationManager::getAttributesGivenTableId(int tableId, std::vector<Attribute> &attrs) {
        RBFM_ScanIterator rbfmScanIterator;
        FileHandle fileHandle;

        RC rc = _rbfm->openFile(COLUMNS_TABLE, fileHandle);
        if (rc != 0) {
            return -1;
        }

        // prepare attribute vector
        std::vector<std::string> attrNames;
        attrNames.push_back("column-name");
        attrNames.push_back("column-type");
        attrNames.push_back("column-length");
        // attrNames.emplace_back("column_position");

        // clear descriptor
        //attrs.clear();
        //std::vector<Attribute>().swap(attrs);

        // init scan iterator
        rc = _rbfm->scan(fileHandle, _ColumnsDescriptor, "table-id", EQ_OP, &tableId, attrNames, rbfmScanIterator);
        if (rc != 0) {
            _rbfm->closeFile(fileHandle);
            return -2;
        }

        RID rid;
        void* data = malloc(PAGE_SIZE);

        while(true) {
            rc = rbfmScanIterator.getNextRecord(rid, data);
            // table_id == tableId
            if (rc == 0) {
//                PeterDB::Attribute tmp_attr;
//
//                tmp_attr.name = "xxx";
//                tmp_attr.type = PeterDB::TypeInt;
//                tmp_attr.length = (PeterDB::AttrLength) 4;
//
//                RC rcc = extractAttribute(attrNames, data, tmp_attr);
                attrs.push_back(extractAttribute(attrNames, data));
            } else {
                break;
            }
        }

        if (rbfmScanIterator.close() != 0) {
            free(data);
            return -3;
        }
        _rbfm->closeFile(fileHandle); // update 12/7
        free(data);
        return 0;
    }

    /*
     * Use attrNames and data to generate an Attribute.
     * Attribute struct has three variables: name, type and length.
     * data also contains three value from RBFM_ScanIterator: column-name, column-type and column-length.
     */
    Attribute RelationManager::extractAttribute(const std::vector<std::string> attrNames, void *data) {
        Attribute attr;
        int offsetData = 0;

        int attrLen = attrNames.size();
        int nullIndicatorSize = ceil((double)attrLen/CHAR_BIT);
        auto *nullsIndicator = (unsigned char *)malloc(nullIndicatorSize);
        memcpy((char *)nullsIndicator, data, nullIndicatorSize);

        offsetData += nullIndicatorSize;

        int columnType;
        int columnLen;
        int varCharLen;
        void *varChar;

        for (int ind = 0; ind < attrLen; ind++) {
            if (nullsIndicator[ind / CHAR_BIT] & (1 << (CHAR_BIT - 1 - (ind % CHAR_BIT)))) {
                continue;
            }

            if (attrNames[ind] == "column-name") {
                memcpy(&varCharLen, (char*)data+offsetData, sizeof(varCharLen));
                offsetData += sizeof(varCharLen);

                varChar = (char*)malloc(varCharLen+1);
                memcpy((char*)varChar, (char*)data+offsetData, varCharLen);
                ((char*)varChar)[varCharLen] = '\0';  //end indicator
                offsetData += varCharLen;
                attr.name = (char *)varChar;

                free(varChar);
            } else if (attrNames[ind] == "column-type") {
                memcpy(&columnType, (char *)data+offsetData, sizeof(int));
                attr.type = (AttrType)columnType;
                offsetData += sizeof(int);
            } else if (attrNames[ind] == "column-length") {
                memcpy(&columnLen, (char*)data+offsetData, sizeof(int));
                attr.length = columnLen;
                offsetData += sizeof(int);
            }
        }

        free(nullsIndicator);
        return attr;
    }


//    RM_ScanIterator::RM_ScanIterator() = default;
//
//    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        return _rbfmScanItearator.getNextRecord(rid, data);
    }

    RC RM_ScanIterator::close(){
        //_rbfmScanItearator.close();
        return 0;
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    RC RelationManager::prepareRecord4IndexesCatalog(FileHandle &fileHandle, const int &tableid, std::string &tableName, std::string &attributeName,
                                                     std::string &indexFilename, void * record){
        bool nullbit;
        int nullIndicatorActualSize = ceil((double )_IndexesDescriptor.size() / CHAR_BIT);
        auto *nullIndicator = (char* )malloc(nullIndicatorActualSize);
        memset(nullIndicator, 0, nullIndicatorActualSize);
        int offset = 0;
        memcpy((char* )record + offset, nullIndicator, nullIndicatorActualSize);
        offset += nullIndicatorActualSize;

        int varcharLen;

        // table-id
        nullbit = nullIndicator[0] & (unsigned)1 << (unsigned)7;
        if(!nullbit){
            memcpy((char*)record + offset, &tableid, sizeof(int));
            offset += sizeof(int);
        }

        // table-name
        nullbit = nullIndicator[0] & (unsigned)1 << (unsigned)6;
        if(!nullbit){
            varcharLen = strlen(tableName.c_str());
            prepareVarchar(varcharLen, tableName, record, offset);
        }

        //attribute-name
        nullbit = nullIndicator[0] & (unsigned)1 << (unsigned)5;
        if(!nullbit){
            varcharLen = strlen(attributeName.c_str());
            prepareVarchar(varcharLen, attributeName, record, offset);
        }

        // index-filename
        nullbit = nullIndicator[0] & (unsigned)1 << (unsigned)4;
        if(!nullbit){
            varcharLen = strlen(indexFilename.c_str());
            prepareVarchar(varcharLen, indexFilename, record, offset);
        }

        free(nullIndicator);
        return 0;


    }


    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {
        FileHandle fileHandle;
        IXFileHandle ixFileHandle;

        // check if table exists
        if(_rbfm->openFile(tableName, fileHandle) != 0)
            return -1;
        _rbfm->closeFile(fileHandle);

        // check if index file exists
        std::string indexFilename = tableName + "_" + attributeName + ".idx";
        std::string atableName = tableName;
        std::string aattributeName = attributeName;

        if(_indexManager->openFile(indexFilename, ixFileHandle) == 0)
            return -1;

        if(_indexManager->createFile(indexFilename) != 0)
            return -1;

        // insert one record to INDEXES_TABLE
        void* record = malloc(PAGE_SIZE);
        int tableId;
        RID rid;
        RC rc1 = getTableIdFromTableTable(tableName, tableId, rid);
        if(rc1 != 0)
            return -1;
        _rbfm->openFile(INDEXES_TABLE, fileHandle);
        prepareRecord4IndexesCatalog(fileHandle, tableId, atableName, aattributeName, indexFilename, record);
        RC rc = _rbfm->insertRecord(fileHandle, _IndexesDescriptor, record, rid);
        if(rc != 0)
            return -1;
        free(record);
        _rbfm->closeFile(fileHandle);


        // check if this attributeName exist in attributes of tableName
        Attribute  thisattr;
        bool isexist;
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        for(Attribute &attr: attrs){
            if(attr.name == attributeName){
                thisattr = attr;
                isexist = true;
                break;
            }
        }
        if(isexist == false){
            return -1;
        }
        else{
            // if this attributeName exist in attributes of tableName,
            // insert existing entries to index file
            void *oneRecord = malloc(PAGE_SIZE);
            std::vector<std::string> wantedAttrs;
            wantedAttrs.push_back(attributeName);
            RBFM_ScanIterator rbfmScanIterator;

            _rbfm->openFile(tableName, fileHandle);
            _indexManager->openFile(indexFilename, ixFileHandle);
            if(_rbfm->scan(fileHandle, attrs, "", NO_OP, NULL, wantedAttrs, rbfmScanIterator) == 0){
                int nullIndicatorSize = ceil((double)wantedAttrs.size()/CHAR_BIT);

                while(rbfmScanIterator.getNextRecord(rid, oneRecord) != RM_EOF){
                    auto* nullIndicator = (char* )malloc(nullIndicatorSize);
                    memcpy(nullIndicator, oneRecord, nullIndicatorSize);

                    if(nullIndicator[0] & (unsigned) 1 << (unsigned)7){
                        free(nullIndicator);
                        free(oneRecord);
                        return -1; //todo ??
                    }
                    free(nullIndicator);
                    memmove((char* )oneRecord, (char* )oneRecord + nullIndicatorSize, PAGE_SIZE - nullIndicatorSize);
                    RC rc2 = _indexManager->insertEntry(ixFileHandle, thisattr, oneRecord, rid);
                    if(rc2 != 0) {
                        free(oneRecord);
                        return -1;
                    }

                }


            }
            else{
                free(oneRecord);
                return -1;
            }

            rbfmScanIterator.close();
            free(oneRecord);
            if(_indexManager->closeFile(ixFileHandle) != 0)
                return -1;
            return 0;
        }






        return 0;
    }


    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {

        // varchar for index filename
        std::string indexFilename = tableName + "_" + attributeName + ".idx";
        void* indexFilenameVarchar = malloc(PAGE_SIZE);
        int lenIndexFilename = strlen(indexFilename.c_str());
        memcpy(indexFilenameVarchar, &lenIndexFilename, sizeof(int));
        memcpy((char *)indexFilenameVarchar + sizeof(int), indexFilename.c_str(), lenIndexFilename);

        FileHandle fileHandle;
        RBFM_ScanIterator rbfmScanIterator;
        std::vector<std::string> attrs;attrs.emplace_back("index-name");
        _rbfm->openFile(INDEXES_TABLE, fileHandle);
        if(_rbfm->scan(fileHandle, _IndexesDescriptor, "index-filename", EQ_OP, indexFilenameVarchar, attrs, rbfmScanIterator) == 0){
            RID rid;
            void *tmpdata = malloc(PAGE_SIZE);
            while(rbfmScanIterator.getNextRecord(rid, tmpdata) != RM_EOF){
                deleteTuple(INDEXES_TABLE, rid);
            }
            free(tmpdata);
        }
        else{
            free(indexFilenameVarchar);
            return -1;
        }

        free(indexFilenameVarchar);

        rbfmScanIterator.close();
        return 0;
    }


    RC RelationManager::indexScan(const std::string &tableName, const std::string &attributeName, const void *lowKey,
                                  const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
                                  RM_IndexScanIterator &rm_IndexScanIterator) {

        //get attributes by tablename
        Attribute thisAttribute;
        std::vector<Attribute> attrs;
        RC rc = getAttributes(tableName, attrs);
        if(rc != 0){
            return -1;
        }
        for(Attribute &attr:attrs){
            if(attr.name == attributeName){
                thisAttribute = attr;
            }
        }
        if(thisAttribute.name != attributeName){
            return -1;
        }

        std::string indexFilename = tableName + "_" + attributeName + ".idx";
        RC rc2 = _indexManager->openFile(indexFilename, rm_IndexScanIterator.getIXFileHandle());
        if(rc2 != 0)
            return -1;
        RC rc3 = _indexManager->scan(rm_IndexScanIterator.getIXFileHandle(), thisAttribute,
                                     lowKey, highKey, lowKeyInclusive, highKeyInclusive,
                                     rm_IndexScanIterator.getIX_ScanIterator());
        if(rc3 != 0)
            return -1;

        // _indexManager->closeFile(rm_IndexScanIterator.getIXFileHandle());



        return 0;
    }

    RM_IndexScanIterator::RM_IndexScanIterator() {

    }

    RM_IndexScanIterator::~RM_IndexScanIterator() {

    }

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
        return _ix_ScanItearator.getNextEntry(rid, key);
    }

    RC RM_IndexScanIterator::close() {
        _ix_ScanItearator.close();
        return 0;
    }

} // namespace PeterDB