#ifndef _rbfm_h_
#define _rbfm_h_

#include <vector>
#include <map>
#include <string>
#include <climits>

#include "src/include/pfm.h"

#define CHAR_BIT 8


# define FIELD_SIZE_LEN 2
# define FLAG_LEN 1

# define SOLID_RECORD_FLAG 1
# define FAKE_RECORD_FLAG 2
# define TS_LEN 9
# define INT_FIELD_LEN 4
# define REAL_FIELD_LEN 4
# define VARCHAR_FIELD_LENGTH_LEN 2
# define VARCHAR_FIELD_OFFSET_LEN 2

# define NO_WAY_AVAILABLE_PAGE -1
# define HAS_AVAILABLE_PAGE 0

#define NON_UPDATED 1
#define WAS_UPDATED 2

#define MIN_TS_LEN 9

namespace PeterDB {
    // Record ID
    typedef struct {
        unsigned pageNum;           // page number
        unsigned slotNum;     // slot number in the page
    } RID;

    // Attribute
    typedef enum {
        TypeInt = 0, TypeReal, TypeVarChar
    } AttrType;

    typedef unsigned AttrLength;



    typedef struct Attribute {
        std::string name;  // attribute name
        AttrType type;     // attribute type
        AttrLength length; // attribute length

    } Attribute;

    typedef struct DirSlot {
        char16_t ds_offset;   // 2 bytes
        char16_t ds_length;   // corresponding length of record, 2 bytes
    }SlotDir;

    typedef struct PageDir {
        char16_t numOfSlots;
        char16_t freeSpace;
    }PageDir;

    // Comparison Operator (NOT needed for part 1 of the project)
    typedef enum {
        EQ_OP = 0, // no condition// =
        LT_OP,      // <
        LE_OP,      // <=
        GT_OP,      // >
        GE_OP,      // >=
        NE_OP,      // !=
        NO_OP       // no condition
    } CompOp;


    /********************************************************************
    * The scan iterator is NOT required to be implemented for Project 1 *
    ********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

    //  RBFM_ScanIterator is an iterator to go through records
    //  The way to use it is like the following:
    //  RBFM_ScanIterator rbfmScanIterator;
    //  rbfm.open(..., rbfmScanIterator);
    //  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
    //    process the data;
    //  }
    //  rbfmScanIterator.close();

    class RBFM_ScanIterator {
    public:
        RBFM_ScanIterator();

        ~RBFM_ScanIterator() = default;

        RC initScanIterator(FileHandle &fileHandle,
                            const std::vector<Attribute> &recordDescriptor,
                            const std::string &conditionAttribute,
                            const CompOp compOp,                  // comparision type such as "<" and "="
                            const void *value,                    // used in the comparison
                            const std::vector<std::string> &attributeNames); // a list of projected attributes

        // Never keep the results in the memory. When getNextRecord() is called,
        // a satisfying record needs to be fetched from the file.
        // "data" follows the same format as RecordBasedFileManager::insertRecord().
        RC getNextRecord(RID &rid, void *data) ;

        RC close() ;

    private:
        // RecordBasedFileManager rbfm = RecordBasedFileManager::instance();

        FileHandle fileHandle;
        std::vector<Attribute> recordDescriptor;
        std::string conditionAttribute;
        CompOp compOp;
        void *value;
        std::vector<std::string> attributeNames;

        AttrType conditionAttributeType;
        std::vector<Attribute> attributesVector;

        RID cur_rid;
        PageDir cur_page_dir;
        unsigned num_of_pages;
        char16_t cur_num_slots_of_curPage;
        unsigned int maxRecordLen, maxAttrLen;

        // RC findNextRID(RID &rid);

        RC updateCurRid();


        RC helperCompOp(void *attribute_with_null);
    };

    class RecordBasedFileManager {
    public:
        static RecordBasedFileManager &instance();                          // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new record-based file

        RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

        RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

        //  Format of the data passed into the function is the following:
        //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
        //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
        //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
        //     Each bit represents whether each field value is null or not.
        //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
        //     If k-th bit from the left is set to 0, k-th field contains non-null values.
        //     If there are more than 8 fields, then you need to find the corresponding byte first,
        //     then find a corresponding bit inside that byte.
        //  2) Actual data is a concatenation of values of the attributes.
        //  3) For Int and Real: use 4 bytes to store the value;
        //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
        //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
        // For example, refer to the Q8 of Project 1 wiki page.

        // Insert a record into a file
        RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        RID &rid);


        // Read a record identified by the given rid.
        RC readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);

        // Print the record that is passed to this utility method.
        // This method will be mainly used for debugging/testing.
        // The format is as follows:
        // field1-name: field1-value  field2-name: field2-value ... \n
        // (e.g., age: 24  height: 6.1  salary: 9000
        //        age: NULL  height: 7.5  salary: 7500)
        //RC readRecordFromDisk(const std::vector<Attribute> &recordDescriptor, void *data, const void *reformattedRec);

        RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data, std::ostream &out);

        /*****************************************************************************************************
        * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
        * are NOT required to be implemented for Project 1                                                   *
        *****************************************************************************************************/
        // Delete a record identified by the given rid.
        RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

        // Assume the RID does not change after an update
        RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        const RID &rid);


        RC getFormattedRecord(FileHandle &fileHandle, const RID &rid, void* formattedRecord);

        RC getAttributefromOrgFormat(const std::vector<Attribute> &recordDescriptor, const std::string & attributeName, void * deformattedRecord, void *data);

        // Read an attribute given its name and the rid.
        RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                         const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        RC scan(FileHandle &fileHandle,
                const std::vector<Attribute> &recordDescriptor,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RBFM_ScanIterator &rbfm_ScanIterator);


        RC readAttributesGivenByRidAndAttributeNames(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::vector<std::string> &attributeNames, void *data);
        RC getAttributeSfromOrgFormat(const std::vector<Attribute> &recordDescriptor,
                                      const std::vector<std::string> &attributeNames,
                                      void *deformattedRecord, void *data);

        RC checkRecordFlag(FileHandle &fileHandle, RID &thisrid);

    protected:
        RecordBasedFileManager();                                                   // Prevent construction
        ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
        RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
        RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment

        RC getLengthBeforeVarChar(const std::vector<Attribute> &recordDescriptor,
                                  const void *data, void *info,
                                  unsigned char *nullFieldsIndicator);              // record length before VarChar

        RC formatRecord(const std::vector<Attribute> &recordDescriptor,
                        const void *data, void *info, void *record,
                        unsigned char *nullFieldsIndicator);                        // raw data to inline format

        RC varCharFormat(int &offsetRecord, int &offsetData,
                         char16_t &varCharLen_16, int &varCharLen,
                         char16_t &offsetVariableData, void *record,
                         const void *data);                                         // VarChar data to inline format

        RC deFormatRecord(const std::vector<Attribute> &recordDescriptor,
                          void *data, void *record);                                // VarChar in inline format to data

        RC varCharDeformat(int &offsetRecord, int &offsetData,
                           char16_t &varCharLen_16, int &varCharLen,
                           void *record, void *data);                               // VarChar from inline format to data

        RC isAvailablePage(FileHandle &fileHandle,
                           char16_t recordLength,
                           RID &rid, void* page);                                  // retrieve the record by the rif, check available flag

        RC fillGap(char16_t gapLength, const RID &rid, void *page );             // fill the gap caused by removing the old record

    private:
        static RecordBasedFileManager* _rbf_manager;



    };

} // namespace PeterDB

#endif // _rbfm_h_