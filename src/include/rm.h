#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <cmath>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "src/include/rbfm.h"
#include "src/include/ix.h"

#define TABLES_TABLE "Tables"
#define COLUMNS_TABLE "Columns"
#define INDEXES_TABLE "Indexes"

namespace PeterDB {
#define RM_EOF (-1)  // end of a scan operator

    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
    public:
        RM_ScanIterator() {};

        ~RM_ScanIterator() {};

        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        RBFM_ScanIterator &getRBFMScanIterator(){
            return _rbfmScanItearator;
        };

        RC close();
    private:
        RBFM_ScanIterator _rbfmScanItearator;
    };

    // RM_IndexScanIterator is an iterator to go through index entries
    class RM_IndexScanIterator {
    public:
        RM_IndexScanIterator();    // Constructor
        ~RM_IndexScanIterator();    // Destructor

        // "key" follows the same format as in IndexManager::insertEntry()
        RC getNextEntry(RID &rid, void *key);    // Get next matching entry
        RC close();                              // Terminate index scan

        IXFileHandle &getIXFileHandle(){
            return _ixFileHandle;
        }

        IX_ScanIterator &getIX_ScanIterator(){
            return _ix_ScanItearator;
        };

    private:
        IXFileHandle _ixFileHandle;
        IX_ScanIterator _ix_ScanItearator;
    };

    // Relation Manager
    class RelationManager {
    public:
        static RelationManager &instance();

        RC createCatalog();

        RC deleteCatalog();

        RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        RC deleteTable(const std::string &tableName);

        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

        RC insertTuple(const std::string &tableName, const void *data, RID &rid);

        RC deleteTuple(const std::string &tableName, const RID &rid);

        RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

        RC readTuple(const std::string &tableName, const RID &rid, void *data);

        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().

        RC printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out);

        RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const std::string &tableName,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        // Extra credit work (10 points)
        RC addAttribute(const std::string &tableName, const Attribute &attr);

        RC dropAttribute(const std::string &tableName, const std::string &attributeName);

        RC getTableIdFromTableTable(const std::string &tableName, int &tableId, RID &rid);

        // QE IX related
        RC createIndex(const std::string &tableName, const std::string &attributeName);

        RC destroyIndex(const std::string &tableName, const std::string &attributeName);

        // indexScan returns an iterator to allow the caller to go through qualified entries in index
        RC indexScan(const std::string &tableName,
                     const std::string &attributeName,
                     const void *lowKey,
                     const void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive,
                     RM_IndexScanIterator &rm_IndexScanIterator);


    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment


        static RC prepareVarchar(int &length, std::string &varChar, void *data, int &offset);


        RC prepareTablesRecord(std::vector<PeterDB::Attribute> &recordDescriptor, int table_id, std::string table_name,
                               std::string file_name, void *record);


        RC insertReocord2Tables(FileHandle &fileHandle, int table_id, std::string table_name, std::string file_name);

        RC init_Tables_table(FileHandle &fileHandle);

        RC init_Columns_table(FileHandle &fileHandle);

        RC prepareRecord4Cloumns(Attribute attr, int table_id, std::vector<Attribute> &tmpDescriptor, void *tmp_record,
                                 int col_pos);

        RC insertReocord2Columns(FileHandle &fileHandle, int table_id, const std::vector<Attribute> &recordDescriptor);

        RC createColumnsRecordDescriptor();

        RC createTablesRecordDescriptor();

        RC getAvaliableTableId(int &tableid);

        Attribute extractAttribute(const std::vector<std::string> attrNames, void *data);

        RC getTableIdByTableName(const std::string &tableName, int &tableId, RID &rid);

        RC getAttributesGivenTableId(int tableId, std::vector<Attribute> &attrs);

        RC createIndexesRecordDescriptor();

        RC deleteRecordsWithinIndexesCatalog(const std::string &tableName, int &table_id);

        RC prepareRecord4IndexesCatalog(FileHandle &fileHandle, const int &tableid, std::string &tableName,
                                        std::string &attributeName, std::string &indexFilename, void *record);

        RC deleteEntriesFromExistingIndexesFiles(const std::string &tableName, std::vector<Attribute> &table_attrs,
                                                 const RID &rid);

        RC extractIndexFileInfoFromIndexesCatalog(const std::string &tableName,
                                                  std::map<std::pair<std::string, std::string>, RID> &attr2ridMap);

        RC insertEntriesToExistingIndexesFiles(const std::string &tableName, std::vector<Attribute> &table_attrs, const RID &rid);


    private:
        IndexManager *_indexManager;

        std::vector<Attribute> _TablesDescriptor;
        std::vector<Attribute> _ColumnsDescriptor;
        std::vector<Attribute> _IndexesDescriptor;

        static RelationManager *_relation_manager;
        RecordBasedFileManager *_rbfm;



    };

} // namespace PeterDB

#endif // _rm_h_