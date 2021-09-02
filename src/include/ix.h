#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <assert.h>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

# define ROOT_FLAG 2
# define NONLEAF_FLAG 3
# define LEAF_FLAG 4

//#define DIR_SIZE 12 // refer to LeafDir: sizeof( char16_t + char16_t + int + int ) = 12

namespace PeterDB {
    class IX_ScanIterator;

    class IXFileHandle;

    typedef char16_t PAGE_FLAG;
    typedef int FREE_SPACE;
    typedef int RECORD_NUM;
    typedef int OFFSET;
    typedef unsigned NEXT_LEAF_NODE;
    typedef unsigned PAGE_ID;
    typedef int RECORD_ID;

    typedef struct LeafDir {
        PAGE_FLAG flag;
        FREE_SPACE freeSpace;
        RECORD_NUM recordNum;
        NEXT_LEAF_NODE nextLeafNode;
    }LeafDir;

    typedef struct NodePageDir {
        PAGE_FLAG flag;
        FREE_SPACE freeSpace;
        RECORD_NUM recordNum;
    }NodePageDir;

    class IndexManager {

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

        // New page for overflow leaf
        RC appendLeafPage(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

        RC searchStartingLeafPage(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *lowKey,
                               bool lowKeyInclusive,
                               PAGE_ID &curLeafPage, int &curRecordId, int &curOffset, void* ptr_curLeafPage);

        RC
        printCore(IXFileHandle &ixFileHandle, int curNode, const Attribute &attribute, int indentNum, bool isContinue,
                  std::ostream &out) const;

        RC
        printNode(IXFileHandle &ixFileHandle, PAGE_FLAG pageFlag, void *page, RECORD_NUM recordNum,
                  const Attribute &attribute,
                  std::ostream &out) const;

        OFFSET getKeyOccupiedSpace(const Attribute &attribute, const void *key, PAGE_FLAG pageFlag);

        RC insertEntry2NodeNoSplit(IXFileHandle &ixFileHandle, PAGE_ID pageID, PAGE_FLAG pageFlag, void *page,
                                   const Attribute &attribute, const void *key, const void *data);

        RC insertEntry2NodeCore(void *page, PAGE_FLAG pageFlag, const Attribute &attribute, const void *key,
                                const void *data);

        RC splitRootLeafPage(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        RC getSplitKey(PAGE_FLAG pageFlag, const Attribute &attribute, void *page, OFFSET &splitOffset, RECORD_NUM &leftRecordNum,
                       void *splitKey);

        RC redistributePage(PAGE_FLAG pageFlag, const Attribute &attribute, RECORD_NUM leftRecordNum, OFFSET splitOffset, void *page,
                         void *newPage);

        RC twoWayInsertEntry2Node(PAGE_FLAG pageFlag, const Attribute &attribute, const void *key, const void *data,
                              void *splitKey,
                              void *page, void *newPage);

        RC insertEntry2NodeNSplit(IXFileHandle &ixFileHandle, PAGE_FLAG pageFlag, const Attribute &attribute,
                                  PAGE_ID pageID,
                                  const void *key, const void *data, void *splitKey, void *page, void *newPage,
                                  PAGE_ID &newPageID);

        RC pushUpRootNode(PAGE_ID pageID, PAGE_ID newPageID, const Attribute &attribute, const void *splitKey,
                          void *rootPage);

        RC insertion(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid,
                     PAGE_ID curPageID,
                     void *splitKey, void *splitData, bool &hasChildEntry);

        RC splitRootNode(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const void *data,
                         PAGE_ID rootPageID);

        RC chooseSubtree(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, PAGE_ID curNodePageID,
                         PAGE_ID &nextNodePageID);

        RC
        findNextTempPageId(const Attribute &attribute, const void *thisPage, const int tmpPageId,
                           const RECORD_NUM numRecords,
                           const void *key, int &nextPageId);

        RC searchKeyInLeafPage(FileHandle &fileHandle, const Attribute &attribute, const void *key,
                               const bool lowKeyInclusive,
                               int &leafPageId, int &leafRecordId, int &curOffset, void *ptr_curLeafPage);
    };

    class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        RC checkEntryCursor(FileHandle &fileHandle);

        // Terminate index scan
        RC close();

        // initialize the ix ScanIterator
        RC init_IXScanIterator(IXFileHandle &ixFileHandle, const Attribute &attribute,
                               const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
                               PAGE_ID &curLeafPage, int &curRecordId, int &curOffset, void* ptr_curLeafPage);

    private:
        IXFileHandle *_ixFileHandle;
        Attribute _attribute;
        const void *_lowKey;
        const void *_highKey;
        bool _lowKeyInclusive;
        bool _highKeyInclusive;

        LeafDir _curLeafDir;
        PAGE_ID _curLeafPageId;
        RECORD_ID _curRecordId;
        OFFSET _curOffset;
        char* _curLeafPageBuffer;
    };

    class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

        FileHandle & getFileHandle();
    private:

        FileHandle fileHandle;
    };
}// namespace PeterDB
#endif // _ix_h_
