#include "src/include/ix.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        return PagedFileManager::instance().createFile(fileName);
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        return PagedFileManager::instance().destroyFile(fileName);
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        return PagedFileManager::instance().openFile(fileName, ixFileHandle.getFileHandle());
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        return PagedFileManager::instance().closeFile(ixFileHandle.getFileHandle());
    }

    RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        unsigned pageNum = ixFileHandle.getFileHandle().getNumberOfPages();
        //std::cout << "line 27 pageNum is: " << pageNum << std::endl;
        RC rc;

        if (pageNum == 0) {
            // append dummy head page
            auto *dmPage = (char *) malloc(PAGE_SIZE);
            PAGE_ID rootPageNum = 1;
            memcpy(dmPage, &rootPageNum, sizeof(PAGE_ID));
            rc = ixFileHandle.getFileHandle().appendPage(dmPage);
            if (rc != 0) {
                return -1;
            }
            free(dmPage);

            // append root-leaf page
            rc = appendLeafPage(ixFileHandle, attribute, key, rid);
            if (rc != 0) {
                return rc;
            }
        } else if (pageNum == 2) {
            OFFSET occupiedSpace = getKeyOccupiedSpace(attribute, key, LEAF_FLAG);
            if (occupiedSpace == -1) {
                return -2;
            }

            void *page = malloc(PAGE_SIZE);
            if(ixFileHandle.getFileHandle().readPage(1, page) != 0) {
                return -3;
            }

            LeafDir leafDir;
            memcpy(&leafDir, page, sizeof(LeafDir));

            if (occupiedSpace <= leafDir.freeSpace) {
                rc = insertEntry2NodeNoSplit(ixFileHandle, 1, LEAF_FLAG, page, attribute, key, &rid);
                if (rc != 0) {
                    return -4;
                }
            } else {
                rc = splitRootLeafPage(ixFileHandle, attribute, key, rid);
                if (rc != 0) {
                    return -5;
                }
            }
            free(page);

        } else {
            // std::cout << "page num in index file is big" << std::endl;
            PAGE_ID rootPageID;
            void *dmPage = malloc(PAGE_SIZE);
            if (ixFileHandle.getFileHandle().readPage(0, dmPage) != 0) {
                return -6;
            }
            memcpy(&rootPageID, (char *)dmPage, sizeof(PAGE_ID));

            void *splitKey = malloc(PAGE_SIZE);
            void *splitData = malloc(PAGE_SIZE);
            void *_key = malloc(PAGE_SIZE);

            if (attribute.type == TypeVarChar) {
                int keyLength;
                memcpy(&keyLength, key, 4);
                memcpy(_key, key, keyLength+4);
            } else if (attribute.type == TypeInt || attribute.type == TypeReal) {
                memcpy(_key, key, 4);
            } else {
                return -7; // should not be here
            }

            bool hasChildEntry = true;
            // std::cout << "go into insertion line95" << std::endl;
            if (insertion(ixFileHandle, attribute, _key, rid, rootPageID, splitKey, splitData, hasChildEntry) != 0) {
                return -8;
            }

            free(dmPage);
            free(splitData);
            free(splitKey);
            free(_key);
        }
        return 0;
    }


    RC IndexManager::insertion(IXFileHandle &ixFileHandle, const Attribute &attribute, const void * key, const RID &rid,
                               PAGE_ID curPageID, void *splitKey, void *splitData, bool &hasChildEntry) {
        void *page = malloc(PAGE_SIZE);
        if (ixFileHandle.getFileHandle().readPage(curPageID, page) != 0) {
            free(page);
            return -1; // no page availabel for current page id
        } else {
            // check page flag
            PAGE_FLAG pageFlag;
            memcpy(&pageFlag, (char *)page, sizeof(PAGE_FLAG));

            if (pageFlag == LEAF_FLAG) {
                LeafDir leafDir;
                memcpy(&leafDir, (char *)page, sizeof(leafDir));

                OFFSET occupiedSpace = getKeyOccupiedSpace(attribute, key, LEAF_FLAG);

                if (leafDir.freeSpace >= occupiedSpace) {
                    hasChildEntry = false;

                    if (insertEntry2NodeNoSplit(ixFileHandle, curPageID, LEAF_FLAG, page, attribute, key, &rid) != 0) {
                        return -2; // simplest insertion fail
                    }

                    free(page);

                    return 0;
                } else {
                    // for debug: set breakpoint here
                    hasChildEntry = true;

                    void *newLeafPage = malloc(PAGE_SIZE);
                    PAGE_ID newLeafPageID;

                    if (insertEntry2NodeNSplit(ixFileHandle, pageFlag, attribute, curPageID, key, &rid, splitKey,
                                               page, newLeafPage, newLeafPageID) != 0) {
                        return -3; // split and insertion fail
                    }

                    memcpy(splitData, &newLeafPageID, sizeof(PAGE_ID));
                    free(page);
                    free(newLeafPage);

                    return 0;
                }
            }
            else if (pageFlag == NONLEAF_FLAG || pageFlag == ROOT_FLAG) {
                PAGE_ID nextPageID;

                if (chooseSubtree(ixFileHandle, attribute, key, curPageID, nextPageID) != 0) {
                    return -4; // cannot locate the next page id
                }

                // std::cout << "nextPageID: " << nextPageID << std::endl;

                insertion(ixFileHandle, attribute, key, rid, nextPageID, splitKey, splitData, hasChildEntry);

                if (!hasChildEntry) {
                    free(page);
                    return 0; // where ends recursion
                } else {
                    NodePageDir nodePageDir;
                    memcpy(&nodePageDir, page, sizeof(NodePageDir));

                    OFFSET occupiedSpace = getKeyOccupiedSpace(attribute, key, LEAF_FLAG);

                    if (nodePageDir.freeSpace >= occupiedSpace) {
                        hasChildEntry = false;
                        if (insertEntry2NodeNoSplit(ixFileHandle, curPageID, NONLEAF_FLAG, page, attribute, splitKey, splitData) != 0) {
                            return -5; // insert and split fail
                        }
                        free(page);
                        return 0;
                    } else {
                        hasChildEntry = true;
                        if (pageFlag == NONLEAF_FLAG) {
                            PAGE_ID  newNodePageID;
                            void *splitNodeKey = malloc(PAGE_SIZE);
                            void *newNodePage = malloc(PAGE_SIZE);
                            //TODO NEVER GET HERE ?????
                            if (insertEntry2NodeNSplit(ixFileHandle, NONLEAF_FLAG, attribute, curPageID, splitKey, splitData, splitNodeKey,
                                                       page, newNodePage, newNodePageID) != 0) {
                                return -5; // insert and split fail
                            }

                            memcpy(splitKey, splitNodeKey, PAGE_SIZE);
                            memcpy(splitData, &newNodePageID, sizeof(PAGE_ID));

                            free(page);
                            free(newNodePage);
                            free(splitNodeKey);
                            return 0;
                        } else {
                            // split root node
                            if (splitRootNode(ixFileHandle, attribute, splitKey, splitData, curPageID) != 0) {
                                return -6; // append new root page fail
                            }

                            free(page);
                            return 0;
                        }
                    }
                }
            }
            else {
                free(page);
                return -7;
            }
        }
    }

    RC IndexManager::insertEntry2NodeCore(void *page, PAGE_FLAG pageFlag, const Attribute &attribute, const void *key,
                                          const void *data) {
        // the data is either pageID or RID
        OFFSET offset;
        FREE_SPACE freeSpace;
        RECORD_NUM recordNum;

        LeafDir leafDir;
        NodePageDir nodePageDir;

        int dataSize;

        if (pageFlag == LEAF_FLAG) {
            memcpy(&leafDir, page, sizeof(LeafDir));

            freeSpace = leafDir.freeSpace;
            recordNum = leafDir.recordNum;

            offset = sizeof(LeafDir);

            dataSize = sizeof(RID);
        } else if (pageFlag == NONLEAF_FLAG || pageFlag == ROOT_FLAG) {
            memcpy(&nodePageDir, page, sizeof(NodePageDir));

            freeSpace = nodePageDir.freeSpace;
            recordNum = nodePageDir.recordNum;

            offset = sizeof(NodePageDir) + sizeof(PAGE_ID); // the first page num is not inserted within this function

            dataSize = sizeof(PAGE_ID);
        } else {
            return -1; // undefined flag value
        }

        switch (attribute.type) {
            case TypeInt: {
                if (freeSpace < (4 + dataSize)) {
                    return -2; // no enough space
                }

                int keyValue, tempData;
                memcpy(&keyValue, key, 4);
                for (int i = 0; i < recordNum; i++) {
                    memcpy(&tempData, (char *) page + offset, 4);
                    if (keyValue <= tempData) {
                        break;
                    }
                    offset += (4 + dataSize);
                }
                // shift the data
                memmove((char *) page + offset + 4 + dataSize, (char *) page + offset, PAGE_SIZE - freeSpace - offset);
                // copy the inserted key and rid pair
                memcpy((char *) page + offset, key, 4);
                memcpy((char *) page + offset + 4, (char *) data, dataSize);

                // freespace reduction and num of record increment
                freeSpace -= (4 + dataSize);
                recordNum += 1;

                break;
            }
            case TypeReal: {
                if (freeSpace < (4 + dataSize)) {
                    return -2; // no enough space
                }

                float keyValue, tempData;
                memcpy(&keyValue, key, 4);

                for (int i = 0; i < recordNum; i++) {
                    memcpy(&tempData, (char *) page + offset, 4);
                    if (keyValue <= tempData) {
                        break;
                    }
                    offset += (4 + dataSize);
                }

                if (int(keyValue) == 578) {
                    RID rid;
                    memcpy(&rid, (char *)data, dataSize);
                    std::cout << rid.pageNum << ". " << rid.slotNum << std::endl;
                }

                // shift the data
                memmove((char *) page + offset + 4 + dataSize, (char *) page + offset, PAGE_SIZE - freeSpace - offset);
                // copy the inserted key and rid pair
                memcpy((char *) page + offset, key, 4);
                memcpy((char *) page + offset + 4, (char *) data, dataSize);

                // freespace reduction and num of record increment
                freeSpace -= (4 + dataSize);
                recordNum += 1;

                break;

            }
            case TypeVarChar: {
                int varCharLength;
                memcpy(&varCharLength, (char*)key, 4);
                if (freeSpace < (varCharLength + sizeof(varCharLength) + dataSize)) {
                    return -2; // no enough space
                }

                auto *keyValue = (char *) malloc(varCharLength + 1);
                memcpy(keyValue, (char *) key + 4, varCharLength);
                keyValue[varCharLength] = '\0';

                int tempCharLength;
                for (int i = 0; i < recordNum; i++) {
                    memcpy(&tempCharLength, (char *) page + offset, sizeof(int));
                    auto *tempData = (char *) malloc(tempCharLength + 1);
                    memcpy(tempData, (char *)page + offset + 4, tempCharLength);
                    tempData[tempCharLength] = '\0';

                    if (strcmp(keyValue, tempData) <= 0) {
                        free(tempData);
                        break;
                    }
                    offset += (4 + tempCharLength + dataSize); // debug=11/24
                    free(tempData);
                }
                // shift the data
                memmove((char *) page + offset + 4 + varCharLength + dataSize, (char *) page + offset, PAGE_SIZE - freeSpace - offset);
                // copy the inserted key and rid pair
                memcpy((char *) page + offset, (char*)key, 4 + varCharLength);
                memcpy((char *) page + offset + 4 + varCharLength, (char *) data, dataSize);

                // freespace reduction and num of record increment
                freeSpace -= (4 + varCharLength + dataSize);
                recordNum += 1;

                free(keyValue);

                break;

            }
            default:
                break;
        }

        if (pageFlag == LEAF_FLAG) {
            leafDir.recordNum = recordNum;
            leafDir.freeSpace = freeSpace;
            memcpy(page, &leafDir, sizeof(LeafDir));
        } else if (pageFlag == NONLEAF_FLAG || pageFlag == ROOT_FLAG) {
            nodePageDir.recordNum = recordNum;
            nodePageDir.freeSpace = freeSpace;
            memcpy(page, &nodePageDir, sizeof(NodePageDir));
        } else {
            return -3; // flag value has been wrongly changed within this function
        }

        return 0;
    }

    OFFSET IndexManager::getKeyOccupiedSpace(const Attribute &attribute, const void *key, PAGE_FLAG pageFlag) {
        int dataSize;
        if (pageFlag == LEAF_FLAG) {
            dataSize = sizeof(RID);
        } else if (pageFlag == NONLEAF_FLAG) {
            dataSize = sizeof(PAGE_ID);
        } else {
            return -1;
        }

        OFFSET occupiedSpace;
        switch (attribute.type) {
            case TypeInt: {
                occupiedSpace = sizeof(int) + dataSize;
                break;
            }
            case TypeReal: {
                occupiedSpace = sizeof(float) + dataSize;
                break;
            }
            case TypeVarChar: {
                OFFSET varCharLen;
                memcpy(&varCharLen, key, 4);

                occupiedSpace = 4 + varCharLen + dataSize;
                break;
            }
            default: {
                return -1;
            }
        }

        return occupiedSpace;
    }


    RC IndexManager::insertEntry2NodeNoSplit(IXFileHandle &ixFileHandle, PAGE_ID pageID, PAGE_FLAG pageFlag,
                                             void *page, const Attribute &attribute, const void* key, const void *data) {
        RC rc;

        rc = insertEntry2NodeCore(page, pageFlag, attribute, key, data);
        if (rc != 0) {
            return -1;
        }
        rc = ixFileHandle.getFileHandle().writePage(pageID, page);
        if (rc != 0) {
            return -2;
        }
        return 0;
    }

    RC IndexManager::splitRootLeafPage(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        RC rc;
        void *page = malloc(PAGE_SIZE);
        void *newPage = malloc(PAGE_SIZE);
        void *splitKey = malloc(PAGE_SIZE);
        void *rootPage = malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        memset(newPage, 0, PAGE_SIZE);
        memset(splitKey, 0, PAGE_SIZE);
        memset(rootPage, 0, PAGE_SIZE);

        memset(newPage, 0, PAGE_SIZE);

        // read the current page
        if (ixFileHandle.getFileHandle().readPage(1, page) != 0) {
            return -1; //read fail
        }

        // std::cout << "428 ixFileHandle.getFileHandle().getNumberOfPages(): " << ixFileHandle.getFileHandle().getNumberOfPages() << std::endl;

        PAGE_ID newPageID;

        // split the page (find the middle key value)
        if (insertEntry2NodeNSplit(ixFileHandle, LEAF_FLAG, attribute, 1, key, &rid, splitKey,
                                   page, newPage, newPageID) != 0) {
            return -2; //split fail
        }

// lequan 11/24 comment out
//        if(ixFileHandle.getFileHandle().appendPage(page) != 0) {
//            return -3; // append fail
//        }

        // std::cout << "441 ixFileHandle.getFileHandle().getNumberOfPages(): " << ixFileHandle.getFileHandle().getNumberOfPages() << std::endl;

        // std::cout << "push up a root line-443" << std::endl;
        if (pushUpRootNode(1, newPageID, attribute, splitKey, rootPage) != 0) {
            return -4; // new root page generation fail
        }

        if (ixFileHandle.getFileHandle().appendPage(rootPage) != 0) {
            return -5; // append root page fail
        }

        // std::cout << "452 ixFileHandle.getFileHandle().getNumberOfPages(): " << ixFileHandle.getFileHandle().getNumberOfPages() << std::endl;

        PAGE_ID rootPageID = ixFileHandle.getFileHandle().getNumberOfPages() - 1;
        void *dmPage = malloc(PAGE_SIZE);
        if (ixFileHandle.getFileHandle().readPage(0, dmPage) != 0) {
            return -6; // read dm page fail
        }
        memcpy((char *)dmPage, &rootPageID, sizeof(PAGE_ID));

        if (ixFileHandle.getFileHandle().writePage(0, dmPage) != 0) {
            return -7; // write dm page back fail
        }

        // std::cout << "465 ixFileHandle.getFileHandle().getNumberOfPages(): " << ixFileHandle.getFileHandle().getNumberOfPages() << std::endl;


        free(page);
        free(newPage);
        free(splitKey);
        free(rootPage);
        free(dmPage);

        return 0;
    }

    RC IndexManager::insertEntry2NodeNSplit(IXFileHandle &ixFileHandle, PAGE_FLAG pageFlag, const Attribute &attribute, PAGE_ID pageID,
                                            const void* key, const void *data, void *splitKey,
                                            void *page, void *newPage, PAGE_ID &newPageID) {
        int splitOffset, leftRecordNum;

        if (getSplitKey(pageFlag, attribute, page, splitOffset, leftRecordNum, splitKey) != 0) {
            return -1; // split key fail
        }

        if (redistributePage(pageFlag, attribute, leftRecordNum, splitOffset, page, newPage) != 0) {
            return -2; // redistribute fail
        }

        if (twoWayInsertEntry2Node(pageFlag, attribute, key, data, splitKey, page, newPage) != 0) {
            return -3; // two way insert entry fail
        }

        if (ixFileHandle.getFileHandle().appendPage(newPage) == 0) {
            newPageID = ixFileHandle.getFileHandle().getNumberOfPages() - 1;
        } else {
            return -4; // append fail
        }

        // if leaf page, link the new page to the previous page
        if (pageFlag == LEAF_FLAG) {
            LeafDir leafDir;
            memcpy(&leafDir, page, sizeof(LeafDir));
            leafDir.nextLeafNode = newPageID;
            memcpy(page, &leafDir, sizeof(LeafDir));
        }

        if (ixFileHandle.getFileHandle().writePage(pageID, page) != 0) {
            return -5; // write page back fail
        }

        return 0;
    }

    RC IndexManager::getSplitKey(PAGE_FLAG pageFlag, const Attribute &attribute, void *page, OFFSET &splitOffset, RECORD_NUM &leftRecordNum, void* splitKey) {
        OFFSET dataOffset;
        RECORD_NUM recordNum;
        OFFSET dataSize;
        FREE_SPACE freeSpace;

        if (pageFlag == LEAF_FLAG) {
            dataSize = sizeof(RID);
            dataOffset = sizeof(LeafDir);

            LeafDir leafDir;
            memcpy(&leafDir, page, sizeof(LeafDir));
            recordNum = leafDir.recordNum;
            freeSpace = leafDir.freeSpace;
        } else if (pageFlag == NONLEAF_FLAG || pageFlag == ROOT_FLAG) {
            dataSize = sizeof(PAGE_ID);
            dataOffset = sizeof(NodePageDir) + sizeof(PAGE_ID);

            NodePageDir nodePageDir;
            memcpy(&nodePageDir, page, sizeof(NodePageDir));
            recordNum = nodePageDir.recordNum;
            freeSpace = nodePageDir.freeSpace;
        } else {
            return -1; //undefined page flag
        }

        switch (attribute.type) {
            case TypeInt: {
                splitOffset = dataOffset + (4 + dataSize) * (recordNum / 2);
                leftRecordNum = recordNum / 2;
                memcpy(splitKey, (char *)page + splitOffset, 4);
                break;
            }
            case TypeReal: {
                splitOffset = dataOffset + (4 + dataSize) * (recordNum / 2 - 1);
                leftRecordNum = recordNum / 2 - 1;

                float tempReal, nextReal;
                while(true){
                    memcpy(&tempReal, (char *)page + splitOffset, sizeof(float));
                    memcpy(&nextReal, (char *)page + splitOffset + sizeof(float) + dataSize, sizeof(float));
                    if(tempReal != nextReal){
                        splitOffset += (sizeof(float) + dataSize);
                        leftRecordNum++;
                        break;
                    }
                    splitOffset += (sizeof(float) + dataSize);
                    leftRecordNum++;
                }

                memcpy(splitKey, (char *)page + splitOffset, sizeof(float));
                break;
            }
            case TypeVarChar: {
                splitOffset = dataOffset;
                OFFSET leftSpace;
                OFFSET spaceThreshold = (PAGE_SIZE - freeSpace - dataOffset) / 2 + dataOffset;

                int tempLength;

                for (leftRecordNum = 0; leftRecordNum < recordNum; leftRecordNum++) {
                    if (splitOffset >= spaceThreshold) {
                        break;
                    }

                    memcpy(&tempLength, (char *)page + splitOffset, 4);
                    splitOffset += (tempLength + 4 + dataSize);
                }

                int varCharLen;
                memcpy(&varCharLen, (char *)page + splitOffset, 4);
                memcpy(splitKey, (char *)page + splitOffset, 4 + varCharLen);

                break;
            }
            default:
                break;
        }
        return 0;
    }


    RC IndexManager::redistributePage(PAGE_FLAG pageFlag, const Attribute &attribute, int leftRecordNum, OFFSET splitOffset, void *page, void *newPage) {
        if (pageFlag == LEAF_FLAG) {
            LeafDir leafDir;
            memcpy(&leafDir, (char *) page, sizeof(LeafDir));

            memcpy((char *)newPage + sizeof(LeafDir), (char *)page + splitOffset, PAGE_SIZE - leafDir.freeSpace - splitOffset);
            memset((char *)page + splitOffset, 0, PAGE_SIZE - splitOffset);

            LeafDir newLeafDir = {LEAF_FLAG,
                                  static_cast<FREE_SPACE>(leafDir.freeSpace + splitOffset - sizeof(LeafDir)),
                                  leafDir.recordNum - leftRecordNum,
                                  leafDir.nextLeafNode};
            leafDir.freeSpace = PAGE_SIZE - splitOffset;
            leafDir.recordNum = leftRecordNum;

            memcpy(page, &leafDir, sizeof(LeafDir));
            memcpy(newPage, &newLeafDir, sizeof(LeafDir));
        } else if (pageFlag == NONLEAF_FLAG || pageFlag == ROOT_FLAG) {
            NodePageDir nodePageDir;
            memcpy(&nodePageDir, (char *)page, sizeof(nodePageDir));

            OFFSET splitKeyLen;
            if (attribute.type == TypeVarChar) {
                memcpy(&splitKeyLen, (char *)page + splitOffset, 4);
                splitKeyLen += 4;
            } else {
                splitKeyLen = 4;
            }

            memcpy((char *)newPage + sizeof(NodePageDir), (char *)page + splitOffset + splitKeyLen, PAGE_SIZE - nodePageDir.freeSpace - splitKeyLen - splitOffset);
            memset((char *)page + splitOffset, 0, PAGE_SIZE - splitOffset);

            NodePageDir newNodePageDir = {nodePageDir.flag,
                                          static_cast<FREE_SPACE>(nodePageDir.freeSpace + splitOffset + splitKeyLen - sizeof(NodePageDir)),
                                          nodePageDir.recordNum - leftRecordNum - 1};
            nodePageDir.freeSpace = PAGE_SIZE - splitOffset;
            nodePageDir.recordNum = leftRecordNum;

            memcpy((char *)newPage, &newNodePageDir, sizeof(nodePageDir));
            memcpy((char *)page, &nodePageDir, sizeof(nodePageDir));
        }

        return 0;
    }


    RC IndexManager::twoWayInsertEntry2Node(PAGE_FLAG pageFlag, const Attribute &attribute, const void* key, const void* data, void* splitKey, void* page, void* newPage) {
        switch (attribute.type) {
            case TypeInt:{
                int keyValue, splitValue;
                memcpy(&keyValue, key, 4);
                memcpy(&splitValue, (char *)splitKey, 4);

                if (splitValue <= keyValue) {
                    insertEntry2NodeCore(newPage, pageFlag, attribute, key, data);
                } else {
                    insertEntry2NodeCore(page, pageFlag, attribute, key, data);
                }
                break;
            }
            case TypeReal:{
                float keyValue, splitValue;
                memcpy(&keyValue, key, 4);
                memcpy(&splitValue, (char *)splitKey, 4);

                if (splitValue <= keyValue) {
                    insertEntry2NodeCore(newPage, pageFlag, attribute, key, data);
                } else {
                    insertEntry2NodeCore(page, pageFlag, attribute, key, data);
                }
                break;
            }
            case TypeVarChar:{
                int keyLength, splitLenght;
                memcpy(&keyLength, key, 4);
                memcpy(&splitLenght, (char *)splitKey, 4);

                auto *keyValue = (char *)malloc(keyLength + 1);
                auto *splitValue = (char *)malloc(splitLenght + 1);
                memcpy(keyValue, (char *)key + 4, keyLength);
                memcpy(splitValue, (char *)splitKey + 4, splitLenght);
                keyValue[keyLength] = '\0';
                splitValue[splitLenght] = '\0';

                if(std::strcmp(splitValue, keyValue) <= 0) {
                    insertEntry2NodeCore(newPage, pageFlag, attribute, key, data);
                } else {
                    insertEntry2NodeCore(page, pageFlag, attribute, key, data);
                }

                free(keyValue);
                free(splitValue);
                break;
            }
            default: {
                return -1; // unknown attribute type
            }
        }
        return 0;
    }


    RC IndexManager::pushUpRootNode(PAGE_ID pageID, PAGE_ID newPageID, const Attribute &attribute, const void *splitKey, void *rootPage) {
        // NodePageDir rootPageDir = {ROOT_FLAG, PAGE_SIZE - sizeof(NodePageDir), 0};
        NodePageDir rootPageDir = {ROOT_FLAG, PAGE_SIZE - sizeof(NodePageDir), 0};

        memcpy((char *)rootPage + sizeof(NodePageDir), &pageID, sizeof(PAGE_ID));
        rootPageDir.freeSpace -= sizeof(PAGE_ID);
        memcpy((char *)rootPage, &rootPageDir, sizeof(NodePageDir));


        if (insertEntry2NodeCore(rootPage, ROOT_FLAG, attribute, splitKey, &newPageID) != 0) {
            return -1; // insert entry fail
        }

        return 0;
    }


    RC IndexManager::splitRootNode(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const void* data, PAGE_ID oldRootPageID) {
        void *oldRootPage = malloc(PAGE_SIZE);
        void *newRootPage = malloc(PAGE_SIZE);
        void *newNodePage = malloc(PAGE_SIZE);
        void *splitRootKey = malloc(PAGE_SIZE);
        memset(oldRootPage, 0, PAGE_SIZE);
        memset(newRootPage, 0, PAGE_SIZE);
        memset(newNodePage, 0, PAGE_SIZE);
        memset(splitRootKey, 0, PAGE_SIZE);

        if (ixFileHandle.getFileHandle().readPage(oldRootPageID, oldRootPage) != 0) {
            return -1; // read old root page fail
        }

        // change the old root page to node page
        NodePageDir nodePageDir;
        memcpy(&nodePageDir, (char *)oldRootPage, sizeof(nodePageDir));
        nodePageDir.flag = NONLEAF_FLAG;
        memcpy((char *)oldRootPage, &nodePageDir, sizeof(nodePageDir));
        ixFileHandle.getFileHandle().writePage(oldRootPageID, oldRootPage);

        PAGE_ID newNodePageID;
        if (insertEntry2NodeNSplit(ixFileHandle, NONLEAF_FLAG, attribute, oldRootPageID, key, data, splitRootKey, oldRootPage, newNodePage, newNodePageID) != 0){
            // append page called
            return -2; // split and insertion fail
        }

        // std::cout << "PAGE_ID newNodePageID" << newNodePageID << std::endl;

        if (pushUpRootNode(oldRootPageID, newNodePageID, attribute, splitRootKey, newRootPage) != 0) {
            return -3; // generate new root page fail
        }

        if (ixFileHandle.getFileHandle().appendPage(newRootPage) != 0) {
            return -4; // append root page fail
        }

        PAGE_ID newRootPageID = ixFileHandle.getFileHandle().getNumberOfPages() - 1;

        void *dmPage = malloc(PAGE_SIZE);
        memset(dmPage, 0, PAGE_SIZE);
        if (ixFileHandle.getFileHandle().readPage(0, dmPage) != 0) {
            return -5; // read dummy page fail
        }
        memcpy((char *)dmPage, &newRootPageID, sizeof(PAGE_ID));

        if (ixFileHandle.getFileHandle().writePage(0, dmPage) != 0) {
            return -7; // write dm page back fail
        }

        free(oldRootPage);
        free(newRootPage);
        free(newNodePage);
        free(splitRootKey);
        free(dmPage);

        return 0;
    }


    RC IndexManager::appendLeafPage(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key,
                                    const RID &rid) {
        void *page = malloc(PAGE_SIZE);

        LeafDir leafDir = {LEAF_FLAG, PAGE_SIZE - sizeof(LeafDir), 0, 0};
        memset(page, 0, PAGE_SIZE);
        memcpy(page, &leafDir, sizeof(LeafDir));

        RC rc = insertEntry2NodeCore(page, LEAF_FLAG, attribute, key, &rid);
        if (rc != 0) {
            return rc;
        }


        RC rc1 = ixFileHandle.getFileHandle().appendPage(page);
        free(page);

        if (rc1 != 0) {
            return rc1;
        }
        return 0;
    }

    RC IndexManager::chooseSubtree(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const PAGE_ID curNodePageID,
                                   PAGE_ID &nextNodePageID){ //debug=11/24

        auto * page = (char* )malloc(PAGE_SIZE);
        ixFileHandle.getFileHandle().readPage(curNodePageID, page);

        PAGE_FLAG thisflag;
        memcpy(&thisflag, page, sizeof(char16_t));

        if(thisflag == LEAF_FLAG){
            free(page);
            return 1;
        }

        NodePageDir thisDir;
        memcpy(&thisDir, page, sizeof(NodePageDir));
        RECORD_NUM numKeys = thisDir.recordNum;
        OFFSET offset = sizeof(NodePageDir) + sizeof(PAGE_ID);  //  debug=11/24

        if(key == NULL){
            // return the first pointer
            memcpy(&nextNodePageID, (char *)page + sizeof(NodePageDir), sizeof(PAGE_ID));
            free(page);
            return 0;
        }

        switch (attribute.type) {
            case TypeInt:{
                int keyInt, tempInt;

                memcpy(&keyInt, key, sizeof(int));

                for(int idx = 0; idx < numKeys; idx++){
                    memcpy(&tempInt, (char *)page + offset, sizeof(int));
                    if(keyInt < tempInt){
                        // get the left pointer if data if less than the tempData
                        memcpy(&nextNodePageID, (char *)page + offset - sizeof(PAGE_ID), sizeof(PAGE_ID));
                        free(page);
                        return 0;
                    }
                    // key: 4B, ptr: 4B
                    offset += (sizeof(int) + sizeof(PAGE_ID));
                }
                memcpy(&nextNodePageID, (char *)page+offset-sizeof(PAGE_ID), sizeof(PAGE_ID));
                free(page);
                return 0;
            }
            case TypeReal:{
                float keyvalue;
                memcpy(&keyvalue, key, sizeof(float));

                float tempInteger;

                for (int idx = 0; idx < numKeys; idx++) {
                    memcpy(&tempInteger, (char *) page + offset, sizeof(float));
                    if (keyvalue < tempInteger) { //
                        memcpy(&nextNodePageID, (char *) page + offset - sizeof(PAGE_ID), sizeof(PAGE_ID));
                        return 0;
                    }
                    offset += sizeof(float) + sizeof(PAGE_ID);  // key + rid pair
                }
                memcpy(&nextNodePageID, (char *) page + offset - sizeof(PAGE_ID), sizeof(PAGE_ID));
                return 0;
            }
            case TypeVarChar:{
                int keyVarcharLen;
                memcpy(&keyVarcharLen, key, sizeof(int));
                auto *keyVarchar = (char *) malloc(keyVarcharLen);
                memcpy((char *) keyVarchar, (char *) key + sizeof(int), keyVarcharLen);

                int tempVarcharLen;

                for (int idx = 0; idx < numKeys; idx++) {
                    memcpy(&tempVarcharLen, (char *) page + offset, sizeof(int));
                    auto *tempVarchar = (char *) malloc(tempVarcharLen + 1);
                    memcpy(tempVarchar, (char *) page + offset + sizeof(int), tempVarcharLen);
                    tempVarchar[tempVarcharLen] = '\0';

                    if (strcmp(tempVarchar, keyVarchar) > 0) {
                        memcpy(&nextNodePageID, (char *) page + offset - sizeof(PAGE_ID), sizeof(PAGE_ID));
                        return 0;
                    }
                    offset += sizeof(int) + tempVarcharLen + sizeof(PAGE_ID);
                    free(tempVarchar);
                }

                free(keyVarchar);
                memcpy(&nextNodePageID, (char *) page + offset - sizeof(PAGE_ID), sizeof(PAGE_ID));
                return 0;
            }
        }
        free(page);
        return 0;
    }

    RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        int recordId, offset;
        PAGE_ID leafPageNum;
        RID thisRid;
        void* ptrLeafPage = malloc(PAGE_SIZE);

        RC rc = searchStartingLeafPage(ixFileHandle, attribute, key, true,
                                       leafPageNum, recordId, offset, ptrLeafPage);
        if(rc != 0){
            //todo error
            return -1;
        }

        LeafDir thisLeafDir;
        memcpy(&thisLeafDir, ptrLeafPage, sizeof(LeafDir));
        int numRecds = thisLeafDir.recordNum;

        switch (attribute.type) {

            case TypeInt:{
                int intKey, tempInt;
                memcpy(&tempInt, (char*)ptrLeafPage + offset, sizeof(int));
                memcpy(&intKey, (char*)key, sizeof(int));
                memcpy(&thisRid, (char*)ptrLeafPage + offset + sizeof(int), sizeof(RID));

                if(intKey == tempInt && rid.pageNum == thisRid.pageNum && rid.slotNum == thisRid.slotNum){
                    memmove((char*)ptrLeafPage+offset, (char*)ptrLeafPage + offset + sizeof(int)+ sizeof(RID), PAGE_SIZE - thisLeafDir.freeSpace - offset - (sizeof(int)+
                                                                                                                                                             sizeof(RID)));
                    thisLeafDir.freeSpace += (sizeof(int)+ sizeof(RID));
                    thisLeafDir.recordNum -= 1;
                    memcpy((char*)ptrLeafPage, &thisLeafDir, sizeof(LeafDir));
                    ixFileHandle.getFileHandle().writePage(leafPageNum, ptrLeafPage);

//                    std::cout << "deleteEntry -> Delete " << recordId << "'th entry inside " << pageNum << " RID is : " << rid.pageNum << "; " << rid.slotNum <<  std::endl;
                    free(ptrLeafPage);
                    return 0;
                }
                free(ptrLeafPage);
                return -1;
            }
            case TypeReal:{

                float realKey, tempReal;
                memcpy(&tempReal, (char*)ptrLeafPage + offset, sizeof(int));
                memcpy(&realKey, (char*)key, sizeof(int));
                memcpy(&thisRid, (char*)ptrLeafPage + offset + sizeof(int), sizeof(RID));

                if(realKey == tempReal && rid.pageNum == thisRid.pageNum && rid.slotNum == thisRid.slotNum){
                    memmove((char*)ptrLeafPage + offset, (char*)ptrLeafPage + offset + sizeof(float)+ sizeof(RID), PAGE_SIZE - thisLeafDir.freeSpace - offset - (sizeof(float)+
                                                                                                                                                                 sizeof(RID)));
                    thisLeafDir.freeSpace += (sizeof(float)+ sizeof(RID));
                    thisLeafDir.recordNum -= 1;
                    memcpy((char*)ptrLeafPage, &thisLeafDir, sizeof(LeafDir));
                    ixFileHandle.getFileHandle().writePage(leafPageNum, ptrLeafPage);

//                    std::cout << "deleteEntry -> Delete " << recordId << "'th entry inside " << pageNum << " RID is : " << rid.pageNum << "; " << rid.slotNum <<  std::endl;
                    free(ptrLeafPage);
                    return 0;
                }
                free(ptrLeafPage);
                return -1;
            }
            case TypeVarChar:{
                int keyVachaLen;
                memcpy(&keyVachaLen, (char*)key, sizeof(int));
                auto* keyVacha = (char*)malloc(keyVachaLen);
                memcpy(keyVacha, (char*)key + sizeof(int), keyVachaLen+1);
                keyVacha[keyVachaLen] = '\0';

                int thisVachaLen;
                memcpy(&thisVachaLen, (char*)ptrLeafPage + offset, sizeof(int));
                auto* thisVacha = (char*)malloc(thisVachaLen);
                memcpy(thisVacha, (char*)ptrLeafPage + sizeof(int), thisVachaLen);
                thisVacha[thisVachaLen] = '\0';

                memcpy(&thisRid, (char*)ptrLeafPage + offset + sizeof(int) + thisVachaLen, sizeof(RID));

                if(strcmp(keyVacha, thisVacha) == 0 && rid.pageNum == thisRid.pageNum && rid.slotNum == thisRid.slotNum) {
                    memmove((char*)ptrLeafPage + offset, (char*)ptrLeafPage + offset + sizeof(int) + thisVachaLen + sizeof(RID), PAGE_SIZE - thisLeafDir.freeSpace - offset - (sizeof(int) + thisVachaLen +
                                                                                                                                                                               sizeof(RID)));
                    thisLeafDir.freeSpace += (sizeof(int) + thisVachaLen + sizeof(RID));
                    thisLeafDir.recordNum -= 1;
                    memcpy((char*)ptrLeafPage, &thisLeafDir, sizeof(LeafDir));
                    ixFileHandle.getFileHandle().writePage(leafPageNum, ptrLeafPage);

                    // std::cout << "deleteEntry -> Delete " << recordId << "'th entry inside " << pageNum << std::endl;
                    free(keyVacha);
                    free(thisVacha);
                    free(ptrLeafPage);
                    return 0;
                }
                free(keyVacha);
                free(thisVacha);
                free(ptrLeafPage);
                return -1;
            }

        }


    }


    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {

        int numPages = ixFileHandle.getFileHandle().getNumberOfPages();
        if(numPages == 0){
            // this ixFileHandle didi not open any file
            std::cout << "this ixFileHandle did not open any file [IndexManager::scan]" << std::endl;
            return -1;
        }

        // search for the starting entry

        PAGE_ID curLeafPage = 0;
        int curRecordId = -1;
        int curOffset = -1;

        void* ptr_curLeafPage = malloc(PAGE_SIZE);

        RC rc = searchStartingLeafPage(ixFileHandle, attribute, lowKey, lowKeyInclusive,
                                       curLeafPage, curRecordId, curOffset, ptr_curLeafPage);
//        if(rc != 0)
//            return -1;  // update 12/8


        ix_ScanIterator.init_IXScanIterator(ixFileHandle, attribute,
                                            lowKey, highKey,  lowKeyInclusive, highKeyInclusive,
                                            curLeafPage, curRecordId, curOffset, ptr_curLeafPage);

        free(ptr_curLeafPage);

        return 0;
    }


    RC IndexManager::searchStartingLeafPage(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *lowKey,
                                            bool lowKeyInclusive,
                                            PAGE_ID &curLeafPage, int &curRecordId, int &curOffset, void* ptr_curLeafPage) {
        // before getNextEntry, find the first entry satisfying the condition

        FileHandle &fileHandle = ixFileHandle.getFileHandle();
        unsigned numPages = fileHandle.getNumberOfPages();

        if (numPages == 0) {
            std::cout << "no page to read [IndexManager::searchStartingLeafPage]" << std::endl;
            return -1;
        }
        // if numPages is not 0, so there should be at least 2 pages (dummy head page and root-leaf page)

        //find the page id of root page, page id of root should be stored in the page whose id is 0
        void *page = malloc(PAGE_SIZE);
        int rootPageId = -1;
        fileHandle.readPage(0, page);
        memcpy(&rootPageId, page, sizeof(int));
        free(page);

        if (numPages == 2) {
            // leaf page and root page is same page
            RC rc = searchKeyInLeafPage(fileHandle, attribute, lowKey, lowKeyInclusive,
                                        rootPageId, curRecordId, curOffset, ptr_curLeafPage);
            curLeafPage = rootPageId;
            if (rc != 0) {
                return -1;
            }
            return 0;
        }

        // start from root, search and find the leaf node
        int tmpPageId = rootPageId;
        auto *tmpPage = (char *) malloc(PAGE_SIZE);
        int nextPageId;

        while (true) {
            RC rc = fileHandle.readPage(tmpPageId, tmpPage);
            if(rc != 0)
                return -1;
            PAGE_FLAG tmpFlag;
            RECORD_NUM numRecords;
            // here we need to keep same format of dir for three tyoes of nodes
            NodePageDir tmpDir; int tempsiz = sizeof(NodePageDir);

            memcpy(&tmpDir, tmpPage, sizeof(NodePageDir));
            tmpFlag = tmpDir.flag;
            numRecords = tmpDir.recordNum;
            //memcpy(&tmpFlag, tmpPage, sizeof(PAGE_FLAG));

            // std::cout << "tmpFlag: " << tmpFlag << std::endl;


//            if(tmpFlag != LEAF_FLAG){
//                NodePageDir tmpDir;
//                memcpy(&tmpDir, tmpPage, sizeof(NodePageDir));
//                numRecords = tmpDir.recordNum;
//            }


            if (tmpFlag == LEAF_FLAG) {
                // we find the leaf node, break
                break;
            }

            findNextTempPageId(attribute, tmpPage, tmpPageId, numRecords,
                               lowKey,
                               nextPageId);

            tmpPageId = nextPageId;

        }

        RC rc = searchKeyInLeafPage(fileHandle, attribute, lowKey, lowKeyInclusive,
                                    tmpPageId, curRecordId, curOffset, ptr_curLeafPage);
        curLeafPage = tmpPageId;
        if (rc != 0) {
            return -1;
        }
        return 0;

    }

    RC IndexManager::searchKeyInLeafPage(FileHandle &fileHandle, const Attribute &attribute, const void *key, const bool lowKeyInclusive,
                                         int &leafPageId, int &leafRecordId, int &curOffset, void* ptr_curLeafPage) {

        auto *page = (char*)malloc(PAGE_SIZE);
        fileHandle.readPage(leafPageId, page);

        memcpy((char*)ptr_curLeafPage, page, PAGE_SIZE);

        LeafDir thisLeafDir;
        memcpy(&thisLeafDir, page, sizeof(LeafDir));
        if (thisLeafDir.flag != LEAF_FLAG) {
            return -1;
        }

        int numRecords = thisLeafDir.recordNum;
        int offset = sizeof(LeafDir);

        if(key == NULL){
            leafRecordId = 0;
            curOffset = offset;
            free(page);
            return 0;
        }

        switch (attribute.type) {
            case TypeInt: {

                int keyvalue = 0;
                int tempInteger;

                memcpy(&keyvalue, key, sizeof(int));

                for (int idx = 0; idx < numRecords; idx++) {
                    memcpy(&tempInteger, (char *) page + offset, sizeof(int));
                    if ((lowKeyInclusive == false && keyvalue < tempInteger) || (lowKeyInclusive == true && keyvalue <= tempInteger)) {
                        leafRecordId = idx;
                        curOffset = offset;
                        free(page);
                        return 0;
                    }
                    offset += sizeof(int) + sizeof(RID);  // key + rid pair
                }

                break;
            }
            case TypeReal: {

                float keyvalue = 0.0;
                memcpy(&keyvalue, key, sizeof(float));

                float tempInteger;

                for (int idx = 0; idx < numRecords; idx++) {
                    memcpy(&tempInteger, (char *) page + offset, sizeof(float));
                    if ((lowKeyInclusive == false && keyvalue < tempInteger) ||
                        (lowKeyInclusive == true && keyvalue <= tempInteger)) {
                        curOffset = offset;

                        leafRecordId = idx;
                        free(page);
                        return 0;
                    }
                    offset += sizeof(float) + sizeof(RID);  // key + rid pair
                }

                break;
            }
            case TypeVarChar: {

                int keyVarcharLen = 0;
                memcpy(&keyVarcharLen, key, sizeof(int));
                auto *keyVarchar = (char *) malloc(keyVarcharLen);
                memcpy((char *) keyVarchar, (char *) key + sizeof(int), keyVarcharLen);

                int tempVarcharLen;

                for (int idx = 0; idx < numRecords; idx++) {
                    memcpy(&tempVarcharLen, (char *) page + offset, sizeof(int));
                    auto *tempVarchar = (char *) malloc(tempVarcharLen + 1);
                    memcpy(tempVarchar, (char *) page + offset + sizeof(int), tempVarcharLen);
                    tempVarchar[tempVarcharLen] = '\0';

                    if ((lowKeyInclusive == false && strcmp(tempVarchar, keyVarchar) > 0) ||
                        (lowKeyInclusive == true && strcmp(tempVarchar, keyVarchar) >= 0)) {
                        curOffset = offset;
                        leafRecordId = idx;
                        free(page);
                        free(keyVarchar);
                        free(tempVarchar);
                        return 0;
                    }

                    offset += sizeof(int) + tempVarcharLen + sizeof(RID);
                    free(tempVarchar);
                }

                free(keyVarchar);
                break;
            }
            default:
                break;

        }

        free(page);
        return -1;

    }


    RC IndexManager::findNextTempPageId(const Attribute &attribute, const void *thisPage, const int tmpPageId,
                                        const RECORD_NUM numRecords,
                                        const void *key,
                                        int &nextPageId) {

        int offset =
                sizeof(NodePageDir) + sizeof(PAGE_ID);  // dir(12 Byte) | pointer1(4 Byte) | key1(4 Byte) | pointer2(4 Byte) | .......

        if (key == NULL) {
            // no key
            memcpy(&nextPageId, (char *) thisPage + sizeof(NodePageDir), sizeof(PAGE_ID));
            return 0;
        }

        switch (attribute.type) {
            case TypeInt: {
                int keyvalue;
                memcpy(&keyvalue, key, sizeof(int));

                int tempInteger;

                for (int idx = 0; idx < numRecords; idx++) {
                    memcpy(&tempInteger, (char *) thisPage + offset, sizeof(int));
                    if (keyvalue < tempInteger) {
                        // bigger than or equal to
                        memcpy(&nextPageId, (char *) thisPage + offset - sizeof(unsigned), sizeof(unsigned)); // take left pointer
                        return 0;
                    }
                    offset += sizeof(int) + sizeof(unsigned);  // key + rid pair
                }
                memcpy(&nextPageId, (char *) thisPage + offset - sizeof(unsigned), sizeof(unsigned));
                return 0;
            }
            case TypeReal: {

                float keyvalue;
                memcpy(&keyvalue, key, sizeof(float));

                float tempInteger;

                for (int idx = 0; idx < numRecords; idx++) {
                    memcpy(&tempInteger, (char *) thisPage + offset, sizeof(float));
                    if (keyvalue < tempInteger) {
                        memcpy(&nextPageId, (char *) thisPage + offset - sizeof(unsigned), sizeof(unsigned));
                        return 0;
                    }
                    offset += sizeof(float) + sizeof(unsigned);  // key + rid pair
                }
                memcpy(&nextPageId, (char *) thisPage + offset - sizeof(unsigned), sizeof(unsigned));
                return 0;
            }
            case TypeVarChar: {

                int keyVarcharLen;
                memcpy(&keyVarcharLen, key, sizeof(int));
                auto *keyVarchar = (char *) malloc(keyVarcharLen);
                memcpy((char *) keyVarchar, (char *) key + sizeof(int), keyVarcharLen);

                int tempVarcharLen;

                for (int idx = 0; idx < numRecords; idx++) {
                    memcpy(&tempVarcharLen, (char *) thisPage + offset, sizeof(int));
                    auto *tempVarchar = (char *) malloc(tempVarcharLen + 1);
                    memcpy(tempVarchar, (char *) thisPage + offset + sizeof(int), tempVarcharLen);
                    tempVarchar[tempVarcharLen] = '\0';

                    if (strcmp(tempVarchar, keyVarchar) > 0) {
                        memcpy(&nextPageId, (char *) thisPage + offset - sizeof(unsigned), sizeof(unsigned));
                        return 0;
                    }
                    offset += sizeof(int) + tempVarcharLen + sizeof(unsigned);
                    free(tempVarchar);
                }

                free(keyVarchar);
                memcpy(&nextPageId, (char *) thisPage + offset - sizeof(unsigned), sizeof(unsigned));
                return 0;

            }
            default:
                break;

        }
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        int pageNum = ixFileHandle.getFileHandle().getNumberOfPages();

        if (pageNum <= 1) {
            std::cout << "Empty B+ tree" << std::endl;
//            out << "Empty B+ tree" << std::endl;
            return 0;
        } else if (pageNum == 2) {
            printCore(ixFileHandle, 1, attribute, 0, false, out);
        } else {
            void *dmpage = malloc(PAGE_SIZE);
            ixFileHandle.getFileHandle().readPage(0, dmpage);

            PAGE_ID rootPageID;
            memcpy(&rootPageID, (char *)dmpage, sizeof(PAGE_ID));

            printCore(ixFileHandle, rootPageID, attribute, 0, false, out);
        }
        return 0;
    }

    RC IndexManager::printCore(IXFileHandle &ixFileHandle, int curNode, const Attribute &attribute, int indentNum, bool isContinue, std::ostream &out) const {
        void *page = malloc(PAGE_SIZE);
        if (ixFileHandle.getFileHandle().readPage(curNode, page) != 0) {
            return -1; // get node page fail
        }

        PAGE_FLAG pageFlag;
        memcpy(&pageFlag, page, sizeof(PAGE_FLAG));

        RECORD_NUM recordNum;
        if (pageFlag == LEAF_FLAG) {
            LeafDir leafDir;
            memcpy(&leafDir, page, sizeof(LeafDir));
            recordNum = leafDir.recordNum;
        } else if (pageFlag == NONLEAF_FLAG || pageFlag == ROOT_FLAG) {
            NodePageDir nodePageDir;
            memcpy(&nodePageDir, page, sizeof(nodePageDir));
            recordNum = nodePageDir.recordNum;
        } else {
            return -2; // undefined flag
        }

        std::string indent;
        if (indentNum == 0) {
            out << "{" << std::endl;
        } else {
            for (int i = 0; i < indentNum; i++) {
                indent += "    ";
            }
            out << indent << "{";
        }

        printNode(ixFileHandle, pageFlag, page, recordNum, attribute, out);

        if (pageFlag == NONLEAF_FLAG || pageFlag == ROOT_FLAG) {
            OFFSET offset = sizeof(NodePageDir);

            out << "," << std::endl;

            out << indent << "\"children\":[" << std::endl;

            PAGE_ID nextPageID;

            for (int i = 0; i <= recordNum; i++) {
                if (attribute.type == TypeInt || attribute.type == TypeReal) {
                    memcpy(&nextPageID, (char *)page + offset, sizeof(PAGE_ID));
                    offset += (sizeof(PAGE_ID) + 4);
                } else if (attribute.type == TypeVarChar) {
                    memcpy(&nextPageID, (char *)page + offset, sizeof(PAGE_ID));

                    int varCharLen;
                    memcpy(&varCharLen, (char *)page + offset + sizeof(PAGE_ID), 4);
                    offset += (4 + sizeof(PAGE_ID) + varCharLen);
                } else {
                    return -3;
                }

                printCore(ixFileHandle, nextPageID, attribute, indentNum + 1, i != recordNum, out);
            }

            out << indent << "]";
        }

//        if (indentNum == 0) {
//            out << std::endl;
//        }

        if (isContinue) {
            out << "}," << std::endl;
        } else {
            out << "}" << std::endl;
        }

        free(page);
        return 0;
    }


    RC IndexManager::printNode(IXFileHandle &ixFileHandle, PAGE_FLAG pageFlag, void *page,
                               RECORD_NUM recordNum, const Attribute &attribute, std::ostream &out) const{
        OFFSET nodeOffset;
        int dataSize;
        if (pageFlag == LEAF_FLAG) {
            nodeOffset = sizeof(LeafDir);
            dataSize = sizeof(RID);
        } else {
            nodeOffset = sizeof(NodePageDir) + sizeof(PAGE_ID);
            dataSize = sizeof(PAGE_ID);
        }

        OFFSET prevNodeOffset = nodeOffset;

        out << "\"keys\": [";
        for (int i = 0; i < recordNum; i++) {
            switch (attribute.type) {
                case TypeInt: {
                    int data;
                    memcpy(&data, (char *)page + nodeOffset, 4);
                    nodeOffset += (4 + dataSize);

                    if (i != 0 && pageFlag == LEAF_FLAG) {
                        int prevData;
                        memcpy(&prevData, (char *)page + prevNodeOffset, 4);

                        if (prevData == data) {
                            out << ",";
                        } else {
                            out << "]\",\"" << data << ":[";
                        }
                        prevNodeOffset += (4 + dataSize);
                    } else if (i == 0 ) {
                        out << "\"";
                        out << data;
                        if (pageFlag == LEAF_FLAG) {
                            out << ":[";
                        }
                    } else if (pageFlag == LEAF_FLAG){
                        out << "]\",\"";
                        out << data;
                        out << ":[";
                        prevNodeOffset += (4 + dataSize);
                    } else {
                        out << "\",\"" << data;
                        prevNodeOffset += (4 + dataSize);
                    }

                    break;
                }
                case TypeReal: {
                    float data;
                    memcpy(&data, (char *)page + nodeOffset, 4);
                    nodeOffset += (4 + dataSize);

                    if (i != 0 && pageFlag == LEAF_FLAG) {
                        float prevData;
                        memcpy(&prevData, (char *)page + prevNodeOffset, 4);

                        if (prevData == data) {
                            out << ",";
                        } else {
                            out << "]\",\"";
                            out << data;
                            out << ":[";
                        }
                        prevNodeOffset += (4 + dataSize);
                    } else if (i == 0 ) {
                        out << "\"";
                        out << data;
                        if (pageFlag == LEAF_FLAG) {
                            out << ":[";
                        }
                    } else if (pageFlag == LEAF_FLAG){
                        out << "]\",\"";
                        out << data;
                        out << ":[";
                        prevNodeOffset += (4 + dataSize);
                    } else {
                        out << "\",\"" << data;
                        prevNodeOffset += (4 + dataSize);
                    }
                    break;
                }
                case TypeVarChar: {
                    int varCharLength;

                    memcpy(&varCharLength, (char *)page+nodeOffset, 4);

                    auto *data = (char *)malloc(varCharLength + 1);
                    memcpy(data, (char *)page + nodeOffset + 4, varCharLength);
                    data[varCharLength] = '\0';
                    nodeOffset += (4 + varCharLength + dataSize);

                    if (i != 0 && pageFlag == LEAF_FLAG) {
                        int prevVarCharLength;
                        memcpy(&prevVarCharLength, (char *)page+prevNodeOffset, 4);

                        auto *prevData = (char *)malloc(prevVarCharLength + 1);
                        memcpy(prevData, (char *)page + prevNodeOffset + 4, prevVarCharLength);
                        prevData[varCharLength] = '\0';

                        if (std::strcmp(prevData, data) == 0) {
                            out << ",";
                        } else {
                            out << "]\",\"";
                            out << data;
                            out << ":[";
                        }
                        prevNodeOffset += (4 + prevVarCharLength + dataSize);
                    } else if (i == 0 ) {
                        out << "\"";
                        out << data;
                        if (pageFlag == LEAF_FLAG) {
                            out << ":[";
                        }
                    } else if (pageFlag == LEAF_FLAG){
                        out << "]\",\"";
                        out << data;
                        out << ":[";
                        prevNodeOffset += (4 + dataSize);
                    } else {
                        out << "\",\"" << data;
                        prevNodeOffset += (4 + dataSize);
                    }
                    break;
                }
                default: {
                    break;
                }
            }

            if (pageFlag == LEAF_FLAG) {
                RID rid;
                memcpy(&rid, (char *) page + nodeOffset - dataSize, sizeof(RID));
                out << "(" << rid.pageNum << ", " << rid.slotNum << ")";
            }

//            nodeOffset += dataSize;

            if (i == recordNum - 1) {
                if (pageFlag == LEAF_FLAG) {
                    out << "]\"";
                } else {
                    out << "\"";
                }
            }
        }
        out << "]";
        return 0;
    }


    /*
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    */
    IX_ScanIterator::IX_ScanIterator() {}

    IX_ScanIterator::~IX_ScanIterator() {}


    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        if(_curLeafPageId == -1){
            return IX_EOF;
        }
        FileHandle &fileHandle = _ixFileHandle->getFileHandle();

        // move entry cursor to next, this method return either 0 or IX_EOF
        RC rc = checkEntryCursor(fileHandle);

        if(rc == IX_EOF){
            return IX_EOF;
        }

        // memcpy(&leafDir, page, DIR_SIZE);
        int numRecds = _curLeafDir.recordNum;

        switch (_attribute.type) {
            case TypeInt:{
                int thisInt;
                memcpy(&thisInt, _curLeafPageBuffer + _curOffset, sizeof(int));

                int intHighKey;
                if(_highKey != NULL) {
                    memcpy(&intHighKey, (char*)_highKey, sizeof(int));
                }

                if((_highKey == NULL) || (thisInt <= intHighKey && _highKeyInclusive) || (thisInt < intHighKey)){
                    memcpy(key, _curLeafPageBuffer + _curOffset, sizeof(int));
                    memcpy(&rid, _curLeafPageBuffer + _curOffset + sizeof(int), sizeof(RID));

                    _curRecordId ++;
                    if(_curRecordId == _curLeafDir.recordNum ){
                        _curLeafPageId = _curLeafDir.nextLeafNode;

                        _ixFileHandle->getFileHandle().readPage(_curLeafPageId, _curLeafPageBuffer);
                        memcpy(&_curLeafDir, _curLeafPageBuffer, sizeof(LeafDir));
                        _curOffset = sizeof(LeafDir);
                        _curRecordId = 0;
                    }
                    else{
                        _curOffset += sizeof(int) + sizeof(RID);
                    }
                    return 0;

                }
                else{
                    return -1;
                }

            }
            case TypeReal:{
                float thisReal;
                memcpy(&thisReal, _curLeafPageBuffer + _curOffset, sizeof(float));

                float realHighKey;
                if(_highKey != NULL) {
                    memcpy(&realHighKey, (char*)_highKey, sizeof(float));
                }

                if((_highKey == NULL) || (thisReal <= realHighKey && _highKeyInclusive) || (thisReal < realHighKey)){
                    memcpy(key, _curLeafPageBuffer + _curOffset, sizeof(float));
                    memcpy(&rid, _curLeafPageBuffer + _curOffset + sizeof(float), sizeof(RID));

                    _curRecordId ++;
                    if(_curRecordId == _curLeafDir.recordNum ){
                        _curLeafPageId = _curLeafDir.nextLeafNode;
                        std::cout << "#################get to next page in getnextentry" << std::endl;

                        _ixFileHandle->getFileHandle().readPage(_curLeafPageId, _curLeafPageBuffer);
                        memcpy(&_curLeafDir, _curLeafPageBuffer, sizeof(LeafDir));
                        _curOffset = sizeof(LeafDir);
                        _curRecordId = 0;
                    }
                    else{
                        _curOffset += sizeof(float) + sizeof(RID);
                    }
                    return 0;

                }
                else{
                    return -1;
                }

            }
            case TypeVarChar:{

                int thisVarcharLen;
                memcpy(&thisVarcharLen, _curLeafPageBuffer + _curOffset, sizeof(int));
                auto * thisVarchar = (char* )malloc(thisVarcharLen + 1);
                memcpy(thisVarchar, _curLeafPageBuffer + _curOffset + sizeof(int), thisVarcharLen);
                thisVarchar[thisVarcharLen] = '\0';


                char *varcharHikey;
                if(_highKey != NULL) {
                    int highkeyLen;
                    memcpy(&highkeyLen, (char*)_highKey, sizeof(int));
                    varcharHikey = (char*)malloc(highkeyLen);
                    memcpy(varcharHikey, (char*)_highKey + sizeof(int), highkeyLen);
                }

                if( (_highKey == NULL) || (strcmp(thisVarchar, varcharHikey) < 0) || (_highKeyInclusive && strcmp(thisVarchar, varcharHikey) <= 0) ){
                    memcpy(key, _curLeafPageBuffer + _curOffset, sizeof(int) + thisVarcharLen);
                    memcpy(&rid, _curLeafPageBuffer + _curOffset + sizeof(int) + thisVarcharLen, sizeof(RID));

                    _curRecordId ++;
                    if(_curRecordId == _curLeafDir.recordNum ){
                        _curLeafPageId = _curLeafDir.nextLeafNode;

                        _ixFileHandle->getFileHandle().readPage(_curLeafPageId, _curLeafPageBuffer);
                        memcpy(&_curLeafDir, _curLeafPageBuffer, sizeof(LeafDir));
                        _curOffset = sizeof(LeafDir);
                        _curRecordId = 0;
                    }
                    else{
                        _curOffset += sizeof(float) + thisVarcharLen + sizeof(RID);
                    }


                    free(thisVarchar);
                    if(_highKey != NULL) {
                        free(varcharHikey);
                    }
                    return 0;

                }
                else{
                    return -1;
                }
            }

            default:
                std::cout << "type not known " << std::endl;
                return -1;

        }

        return -1;


    }


    RC IX_ScanIterator::checkEntryCursor(FileHandle &fileHandle) {

        if(_curRecordId >= _curLeafDir.recordNum && _curLeafDir.recordNum != 0 && _curLeafDir.nextLeafNode == 0){
            return  IX_EOF;
        }

        if(_curLeafPageId == 0){
            return IX_EOF;
        }

        while(_curLeafDir.recordNum == 0){
            // std::cout << "while loop in checkEntryCursor" << std::endl;
            if(_curLeafDir.nextLeafNode != 0){
                _curLeafPageId = _curLeafDir.nextLeafNode;
                fileHandle.readPage(_curLeafPageId, _curLeafPageBuffer);
                memcpy(&_curLeafDir, _curLeafPageBuffer, sizeof(LeafDir));
                _curOffset = sizeof(LeafDir);
                _curRecordId = 0;
            }
            else{
                return IX_EOF;
            }

        }

        return 0;
    }


    RC IX_ScanIterator::close() {
        IndexManager::instance().closeFile(*_ixFileHandle); // todo
        this->_ixFileHandle = nullptr;
        this->_lowKey = nullptr;
        this->_highKey = nullptr;
        free(this->_curLeafPageBuffer);
        this->_curLeafPageBuffer = nullptr;
        return 0;
    }



    RC IX_ScanIterator::init_IXScanIterator(IXFileHandle &ixFileHandle, const Attribute &attribute,
                                            const void *lowKey, const void *highKey,
                                            bool lowKeyInclusive, bool highKeyInclusive,
                                            PAGE_ID &curLeafPage, int &curRecordId, int &curOffset, void* ptr_curLeafPage) {
        // initilaize
        this->_ixFileHandle = &ixFileHandle;
        this->_attribute = attribute;
        this->_lowKey = lowKey;
        this->_highKey = highKey;
        this->_lowKeyInclusive = lowKeyInclusive;
        this->_highKeyInclusive = highKeyInclusive;

        this->_curLeafPageId = curLeafPage;
        this->_curRecordId = curRecordId;
        this->_curOffset = curOffset;
        this->_curLeafPageBuffer = (char*)malloc(PAGE_SIZE);

        //ixFileHandle.getFileHandle().readPage(curLeafPage, _curPage);
        memcpy(_curLeafPageBuffer, (char*)ptr_curLeafPage, PAGE_SIZE);
        memcpy(&_curLeafDir, _curLeafPageBuffer, sizeof(LeafDir));

        return 0;
    }
    /*
    //////////////////////////////////////////////////////
*/
    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        RC rc = fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
        if(rc == 0){
            return 0;
        }
        else{
            return rc;
        }
    }

    FileHandle& IXFileHandle::getFileHandle() {
        return fileHandle;
    }

} // namespace PeterDB