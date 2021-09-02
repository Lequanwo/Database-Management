#include "src/include/rbfm.h"
#include "src/include/pfm.h"
#include <cmath>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <math.h>

using namespace std;

namespace PeterDB {

    RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = nullptr;

    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;
    //modified here DELETE
    RecordBasedFileManager::~RecordBasedFileManager() { delete _rbf_manager; }

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    //modified here
    RC RecordBasedFileManager::createFile(const std::string &fileName) {

        PagedFileManager& pfm_ins = PagedFileManager::instance();

        return pfm_ins.createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        PagedFileManager& pfm_ins = PagedFileManager::instance();

        return pfm_ins.destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        PagedFileManager& pfm_ins = PagedFileManager::instance();

        return pfm_ins.openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        PagedFileManager& pfm_ins = PagedFileManager::instance();

        return pfm_ins.closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        // get nullsindicator size
        int nullFieldsIndicatorSize = ceil((double(recordDescriptor.size())/CHAR_BIT));
        // get nullsindicator
        auto *nullFieldsIndicator = (unsigned char*) malloc(nullFieldsIndicatorSize);
        memcpy((char *) nullFieldsIndicator, data, nullFieldsIndicatorSize);

        // length before VarChar data in format record
        void *offsetAndVarLen = (char *)malloc(8);
        getLengthBeforeVarChar(recordDescriptor, data, offsetAndVarLen, nullFieldsIndicator);

        char16_t recordLength = ((int *)offsetAndVarLen)[0];
        void *record;
        // consider tombstone ???
        if(recordLength > MIN_TS_LEN){
            record = malloc(recordLength);        // allocate a space for record with recordLength
        }
        else{
            // std::cout << "recordLength < MIN_TS_LEN [insertRecord]" << std::endl;
            record = malloc(MIN_TS_LEN);
            recordLength = MIN_TS_LEN ;
        }
        // record = malloc(recordLength); // not consider tombstone

        // format data
        formatRecord(recordDescriptor, data, offsetAndVarLen, record, nullFieldsIndicator);

        // look for page with enough free space
        void *page = malloc(PAGE_SIZE);

        RC rc = isAvailablePage(fileHandle, recordLength, rid, page);

        if (rc == HAS_AVAILABLE_PAGE) {
            char* PD_ptr = (char*) page + PAGE_SIZE - sizeof(PageDir);
            auto* thisPage = (PageDir*) PD_ptr;

            // new slot
            SlotDir newSlot;
            newSlot.ds_length = recordLength;
            char16_t record_offset = PAGE_SIZE - sizeof(PageDir) - (thisPage->numOfSlots) * sizeof(SlotDir) - thisPage->freeSpace;
            newSlot.ds_offset = record_offset;

            // append new slot or replace the deleted slot
            char16_t slot_offset = PAGE_SIZE - sizeof(PageDir) - (rid.slotNum + 1) * sizeof(SlotDir);
            memcpy((char*) page + slot_offset, &newSlot, sizeof(SlotDir));

            // append record
            memcpy((char*) page + record_offset, record, recordLength);

            thisPage->numOfSlots++;
            thisPage->freeSpace -= (recordLength + sizeof(SlotDir));

            if (fileHandle.writePage(rid.pageNum, page) == 0) {
                free(record);
                free(page);
                free(offsetAndVarLen);
                free(nullFieldsIndicator);
                return 0;
            } else {
                free(record);
                free(page);
                free(offsetAndVarLen);
                free(nullFieldsIndicator);
                return -1;
            }
        }
        else if (rc == NO_WAY_AVAILABLE_PAGE) {
//            std::cout << " $$$$$$$$$$$$ append a new page $$$$$$$$$$$$" << std::endl;
            char* PD_ptr = (char*) page + PAGE_SIZE - sizeof(PageDir);
            auto* thisPage = (PageDir*) PD_ptr;
            thisPage->numOfSlots = 1;
            thisPage->freeSpace = PAGE_SIZE - sizeof(PageDir) - sizeof(SlotDir) - recordLength;

            // new slot
            SlotDir newSlot;
            newSlot.ds_length = recordLength;
            newSlot.ds_offset = 0;

            // append new slot
            char16_t slot_offset = PAGE_SIZE - sizeof(PageDir) - sizeof(SlotDir);
            memcpy((char*) page + slot_offset, &newSlot, sizeof(SlotDir));

            // append record
            memcpy((char*) page, record, recordLength);

            // append page
            RC m = fileHandle.appendPage(page);
            if (m == 0) {
                free(page);
                free(record);
                free(offsetAndVarLen);
                free(nullFieldsIndicator);
                return 0;
            } else {
                free(page);
                free(record);
                free(offsetAndVarLen);
                free(nullFieldsIndicator);
                return m;
            }
        }
        else {
            free(record);
            free(page);
            free(offsetAndVarLen);
            free(nullFieldsIndicator);
            return -1;
        }
    }
/*

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        void *record = malloc(PAGE_SIZE);

        void *page = (char *)malloc(PAGE_SIZE);
        char* slot;
        SlotDir* thisSlot;
        RID solidRid = rid;

        char flag = FAKE_RECORD_FLAG;

        while (flag == FAKE_RECORD_FLAG) {
            if (fileHandle.readPage(solidRid.pageNum, page) == 0) {
                // almost done
                slot = (char *)page + PAGE_SIZE - sizeof(PageDir) - (solidRid.slotNum + 1) * sizeof(SlotDir);
                thisSlot = (SlotDir*)slot;

                if (thisSlot->ds_length == 0) {
                    free(record);
                    free(page);

                    return -2; // record has been deleted
                }

                memcpy(&flag, (char*)page + thisSlot->ds_offset, 1);

                if (flag == SOLID_RECORD_FLAG) {
                    memcpy((char*)record, (char*)page + thisSlot->ds_offset, thisSlot->ds_length);
                    free(page);
                    RC rc = deFormatRecord(recordDescriptor, data, record);
                    free(record);
                    return rc;
                } else if (flag == FAKE_RECORD_FLAG) {
                    char* tsRecord = (char*)page + thisSlot->ds_offset + 1;
                    auto* tsRid = (RID *)tsRecord;

                    memcpy(&solidRid, tsRid, sizeof(RID));

                    memset(page, 0, PAGE_SIZE);
                } else {
                    free(page);
                    free(record);
                    return -3; // strange
                }
            } else {
                free(page);
                free(record);
                return -4; // read fail
            }
        }

        free(record);
        free(page);

        return 0;
    }
*/


    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        void *formattedRecord = malloc(PAGE_SIZE);
        RC rc = getFormattedRecord(fileHandle, rid, formattedRecord);
        if (rc != 0) {
            free(formattedRecord);
            return rc;
        } else {
            rc = deFormatRecord(recordDescriptor, data, formattedRecord);
            free(formattedRecord);
            return rc;
        }
    }


    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        // get nullsindicator size
        int nullFieldsIndicatorSize = ceil((double(recordDescriptor.size())/CHAR_BIT));
        // get nullsindicator
        auto *nullFieldsIndicator = (unsigned char*) malloc(nullFieldsIndicatorSize);
        memcpy((char *) nullFieldsIndicator, data, nullFieldsIndicatorSize);

        int offsetData = nullFieldsIndicatorSize;

        // placeholder
        int intData;
        float floatData;
        int varCharLen;

        for (int ind = 0; ind < recordDescriptor.size(); ind++) {
            Attribute info = recordDescriptor[ind];

            if (info.length != 0) {
                // not delete
                if (nullFieldsIndicator[ind/8] & (1 << (7-(ind%8)))) {
//                    std::cout << info.name << ": " << "NULL"<< ", ";
                    out << info.name << ": " << "NULL" << ", ";
                    continue;
                }
                switch (info.type) {
                    case TypeInt: {
                        memcpy(&intData, (char *)data+offsetData, sizeof(intData));
                        offsetData += sizeof(intData);
//                        std::cout << info.name << ": " << intData << ", ";
                        out << info.name << ": " << intData << ", ";
                        break;
                    }
                    case TypeReal: {
                        memcpy(&floatData, (char *) data + offsetData, sizeof(floatData));
                        offsetData += sizeof(floatData);
//                        std::cout << info.name << ": " << floatData << ", ";
                        out << info.name << ": "<< floatData << ", ";
                        break;
                    }
                    case TypeVarChar: {
                        memcpy(&varCharLen, (char *)data+offsetData, sizeof(varCharLen));
                        offsetData += sizeof(varCharLen);

                        // add \0 to the last
                        auto* varChar = (char *)malloc(varCharLen+1);
                        memcpy((char *)varChar, (char *)data+offsetData, varCharLen);
                        ((char *)varChar)[varCharLen] = '\0';
                        offsetData += varCharLen;

                        if(ind == recordDescriptor.size() - 1){
//                            std::cout << info.name << ": " << varChar;
                            out << info.name << ": " << varChar;
                        }
                        else{
//                            std::cout << info.name << ": " << varChar << ", ";
                            out << info.name << ": " << varChar << ", ";
                        }

                        free(varChar);
                        break;
                    }
                    default: {
                        free(nullFieldsIndicator);
                        return -1;
                    }
                }
            }
        }
        std::cout << std::endl;

        free(nullFieldsIndicator);

        return 0;
    }


   /******************************************************************************************************************************************************
    * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for Project 1 *
    ******************************************************************************************************************************************************/

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        void *page = malloc(PAGE_SIZE);
        char flag = FAKE_RECORD_FLAG;
        RID solidRid = rid;

        RID old_rid;

        while (flag == FAKE_RECORD_FLAG) {
            if (fileHandle.readPage(solidRid.pageNum, page) == -1) {
                return -1; //read page fail
            }

            // read page dir and slot dir
            char* page_dir_pointer = (char*) page + PAGE_SIZE - sizeof(PageDir);
            auto* page_dir = (PageDir*) page_dir_pointer;

            if (solidRid.slotNum + 1 > page_dir->numOfSlots) {
                free(page);

                return -2; // request slot number is too big
            }

            char* slot_dir_pointer = (char*) page + PAGE_SIZE - sizeof(PageDir) - (solidRid.slotNum + 1) * sizeof(SlotDir);
            auto* slot_dir = (SlotDir*) slot_dir_pointer;

            // check whether this record has been deleted
            if (slot_dir->ds_length == 0) {
                free(page);
                std::cout << " slot_dir->ds_length == 0, return -1" << std::endl;
                return -3; // repeated delete
            }

            // get the flag
            memcpy(&flag, (char *)page + slot_dir->ds_offset, 1);

            if (flag == SOLID_RECORD_FLAG) {


                // update slot and page info
                char16_t recordLength = slot_dir->ds_length;
                slot_dir->ds_length = 0;
                page_dir->freeSpace += recordLength;

                // remove this record
                memset((char *)page + slot_dir->ds_offset, 0, slot_dir->ds_length);

                // move the left record forward
                fillGap(recordLength, solidRid, page);

                if (fileHandle.writePage(solidRid.pageNum, page) == -1) {
                    free(page);
                    return -4; // write fails
                } else {
//                    char* pageDirPtr = (char *)page + PAGE_SIZE - sizeof(PageDir);
//                    auto* pageDir = (PageDir *)pageDirPtr;
                    free(page);
                    return 0;
                }
            }
            else if(flag == FAKE_RECORD_FLAG){
                // it is a tombstone record

                // remove this record
                // memset((char *)page + slot_dir->ds_offset, 0, slot_dir->ds_length);

                // update slot and page info
                char16_t recordLength = slot_dir->ds_length;
                slot_dir->ds_length = 0;
                page_dir->freeSpace += recordLength;

                // we have to go deep based on the info written in tombstone
                char *tsRecord = (char *)page + slot_dir->ds_offset + 1;
                auto *tsRid = (RID *)tsRecord;

                old_rid = solidRid;

                memcpy(&solidRid, tsRid, sizeof(RID));

                memset((char *)page + slot_dir->ds_offset, 0, slot_dir->ds_length);

                // move the left record forward
                fillGap(recordLength, old_rid, page);

                if (fileHandle.writePage(old_rid.pageNum, page) == -1) {
                    free(page);
                    return -4; // write fails
                }
            }
            else{
                free(page);
                std::cout << "the flag is unknown" << std::endl;
                return -10;
            }
        }
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        // prepare the data, format the data, the same as the insert record
        // bacause the rid is const, we should assign a new RID for modify
        RID solidRid = rid;
//        std::cout << "==============================START updateRecord()====================================" << std::endl;
//        std::cout << "fileHandle.getNumberOfPages(): " << std::to_string(fileHandle.getNumberOfPages() ) << std::endl;
//        std::cout << "fileHandle.getNumberOfPages(): " << std::to_string(fileHandle.getNumberOfPages() ) << std::endl;

        // get nullsindicator size
        int nullFieldsIndicatorSize = ceil((double(recordDescriptor.size())/CHAR_BIT));
        // get nullsindicator
        auto *nullFieldsIndicator = (unsigned char*) malloc(nullFieldsIndicatorSize);
        memcpy((char *) nullFieldsIndicator, data, nullFieldsIndicatorSize);

        // length before VarChar data in format record
        void *offsetAndVarLen = (char *)malloc(8);
        getLengthBeforeVarChar(recordDescriptor, data, offsetAndVarLen, nullFieldsIndicator);

        // char16_t recordLength = ((int *)offsetAndVarLen)[0];
        char16_t recordLength = ((int *)offsetAndVarLen)[0];
        void *record;

        if(recordLength > MIN_TS_LEN){
            record = malloc(recordLength);        // allocate a space for record with recordLength
        }
        else{
            std::cout << "recordLength < MIN_TS_LEN [updateRecord]" << std::endl;
            record = malloc(MIN_TS_LEN);
            recordLength = MIN_TS_LEN ;
        }

        // format data
        formatRecord(recordDescriptor, data, offsetAndVarLen, record, nullFieldsIndicator);

        void *page = malloc(PAGE_SIZE);
        char flag = FAKE_RECORD_FLAG;

        while (flag == FAKE_RECORD_FLAG) {

            if (fileHandle.readPage(solidRid.pageNum, page) == 0) {
                // read page dir and slot dir
                char* page_dir_pointer = (char*) page + PAGE_SIZE - sizeof(PageDir);
                auto* page_dir = (PageDir*) page_dir_pointer;

                if (solidRid.slotNum + 1 > page_dir->numOfSlots) {
                    free(page);
                    free(nullFieldsIndicator);
                    free(record);
                    free(offsetAndVarLen);
                    std::cout << "return -2" << std::endl;
                    return -2; // request slot number is too big
                }
                else {
                    char* slot_dir_pointer = (char*) page + PAGE_SIZE - sizeof(PageDir) - (solidRid.slotNum + 1) * sizeof(SlotDir);
                    auto* slot_dir = (SlotDir*) slot_dir_pointer;

                    // check whether this record has been deleted
                    if (slot_dir->ds_length == 0) {
                        free(page);
                        free(nullFieldsIndicator);
                        free(record);
                        free(offsetAndVarLen);
                        std::cout << "return -3, slot_dir->ds_length == 0 " << std::endl;
                        std::cout << "Page Info: " << std::endl;
                        char16_t numslots = page_dir->numOfSlots;

                        std::cout << std::to_string(numslots) << std::endl;
                        return -3; // repeated delete
                    }

                    // get the flag to check whether it is a tombstone or not
                    memcpy(&flag, (char *)page + slot_dir->ds_offset, 1);

                    if (flag == SOLID_RECORD_FLAG) {
                        // we finally find where the record is
                        break;

                    }
                    else if(flag ==  FAKE_RECORD_FLAG){
                        // std::cout << "FAKE_RECORD_FLAG, solidRid should be updated" << std::endl;
                        // we have to go deep based on the info written in tombstone
                        char *tsRecord = (char *)page + slot_dir->ds_offset + 1;
                        auto *tsRid = (RID *)tsRecord;
                        memcpy(&solidRid, tsRid, sizeof(RID));
                        //free(tsRecord);
                        memset(page, 0, PAGE_SIZE);
                    }
                    else{
                        std::cout << "flag is not identified here" << std::endl;
                        std::cout << "the flag is: " << std::to_string(flag) << std::endl;
                        free(page);
                        free(nullFieldsIndicator);
                        free(record);
                        free(offsetAndVarLen);
                        return -1;
                    }
                }
            }

            else {
                free(page);
                free(nullFieldsIndicator);
                free(record);
                free(offsetAndVarLen);
                std::cout << "read page fail" << std::endl;
                return -1; //read page fail
            }

        }

        char* page_dir_pointer = (char*) page + PAGE_SIZE - sizeof(PageDir);
        auto* page_dir = (PageDir*) page_dir_pointer;
        char* slot_dir_pointer = (char*) page + PAGE_SIZE - sizeof(PageDir) - (solidRid.slotNum + 1) * sizeof(SlotDir);
        auto* slot_dir = (SlotDir*) slot_dir_pointer;

        //char16_t  updatedRecordLength = slot_dir->ds_length;
        int updatedRecordLength = slot_dir->ds_length;

        if (recordLength == updatedRecordLength) {
            // easiest, just replace

            // free the old space
            memset((char*)page+slot_dir->ds_offset, 0, updatedRecordLength);
            // fill the updating record
            memcpy((char*)page+slot_dir->ds_offset, record, recordLength);

        }
        else if (recordLength < updatedRecordLength) {
            // old space can fit the update record

            // free the old space
            memset((char*)page+slot_dir->ds_offset, 0, updatedRecordLength);
            // fill the updating record
            memcpy((char*)page+slot_dir->ds_offset, record, recordLength);

            // fill the gap
            char16_t gapLength = slot_dir->ds_length - recordLength;

            // update the page and slot info
            page_dir->freeSpace += (updatedRecordLength - recordLength);
            slot_dir->ds_length = recordLength;

            fillGap(gapLength, solidRid, page);

        }
        else {
//            std::cout << "##recordLength is bigger than updatedRecordLength" << std::endl;
            // old space cannot fit the update record
            // 1. remove the updated record,
            // 2. zero the length info in slot dir, add the removed space to free space of the page
            // 3. move the left record to fill the gap

            // 1
            memset((char*)page + slot_dir->ds_offset, 0, updatedRecordLength);

            // 2
            page_dir->freeSpace += slot_dir->ds_length;
            char16_t gapLength = slot_dir->ds_length;
            slot_dir->ds_length = 0;

            // 3
            fillGap(gapLength, solidRid, page);

            if (page_dir->freeSpace >= recordLength) {
                // std::cout << "#### freeSpace is enough " << std::endl;
                // 4. write the new slot info
                slot_dir->ds_length = recordLength;
                slot_dir->ds_offset = PAGE_SIZE - sizeof(PageDir) - page_dir->numOfSlots * sizeof(SlotDir) -
                        page_dir->freeSpace;

                // 5. append new record
                memcpy((char *)page + slot_dir->ds_offset, record, slot_dir->ds_length);

                // 6. update page freespace
                page_dir->freeSpace -= slot_dir->ds_length;
            }
            else {
//                std::cout << "#### find some available page to update" << std::endl;
                // no enough freespace
                slot_dir->ds_offset = PAGE_SIZE - sizeof(PageDir) - page_dir->numOfSlots * sizeof(SlotDir) - page_dir->freeSpace;
                // 4. insert the updating record to one page
                RID  newRid;
                // call insertRecord()
                if (insertRecord(fileHandle, recordDescriptor, data, newRid) == -1) {
                    free(page);
                    free(nullFieldsIndicator);
                    free(record);
                    free(offsetAndVarLen);
                    std::cout << "-4" << std::endl;
                    std::cout << "cannot insert in updateRecord() " << std::endl;
                    return -4; // insert fail
                } else {
                    // 5. update the page and slot info
                    slot_dir->ds_length = TS_LEN;
                    page_dir->freeSpace -= TS_LEN;

                    // 6. fill the fake record, tombstone
                    memset((char *)page + slot_dir->ds_offset, FAKE_RECORD_FLAG, 1);
                    memcpy((char *)page + slot_dir->ds_offset + 1, &newRid, sizeof(newRid));

//                    std::cout << " ###### newRid: " << std::endl;
//                    std::cout << " ###### "  << std::to_string(newRid.pageNum)  << std::endl;
//                    std::cout << " ###### " << std::to_string(newRid.slotNum) << std::endl;

                }

            }
        }
//        std::cout <<  "==================================END updateRecord()===============================" << std::endl;

        RC rc = fileHandle.writePage(solidRid.pageNum, page); // solidRid or rid ????

        if (rc != 0) {
            free(page);
            free(nullFieldsIndicator);
            free(record);
            free(offsetAndVarLen);
            std::cout << "-5" << std::endl;
            return -5;
        }
        free(page);
        free(nullFieldsIndicator);
        free(record);
        free(offsetAndVarLen);

        return 0;
    }


    RC RecordBasedFileManager::fillGap(char16_t gapLength, const RID &rid, void *page) {

        char* ptr_PageDir = (char*) page + PAGE_SIZE - sizeof(PageDir);
        char* ptr_SlotDir = (char*) page + PAGE_SIZE - sizeof(PageDir) - (rid.slotNum + 1) * sizeof(SlotDir);
        auto* thisPage = (PageDir*)ptr_PageDir;
        auto* thisSlot = (SlotDir*)ptr_SlotDir;

        char16_t locToShift = thisSlot->ds_offset + thisSlot->ds_length;

        // a sorted hashmap to store each record's <offset, #slot>.
        std::map<char16_t, unsigned> recordsMap;

        // iterate through all slots and compare it with this records.offset
        for(int i = 0; i < thisPage->numOfSlots; i++){
            char* _slot = (char*) page + PAGE_SIZE - sizeof(PageDir) - (i + 1) * sizeof(SlotDir);
            auto* this_slot = (SlotDir*)_slot;

            // There are two requirements which we need to satisfy:
            // 1. its offset is behind
            // 2. it is not been deleted.
            if((this_slot->ds_offset > locToShift) && (this_slot->ds_length > 0)){
                // this record needs to be inserted in map and shift afterward.
                // And they will be stored in ascending order (default).
                recordsMap.insert(std::pair<char16_t, unsigned>(this_slot->ds_offset, i));
            }
        }

        // shift all the records
        std::map<char16_t, unsigned>::iterator it;
        for(it = recordsMap.begin(); it != recordsMap.end(); it++) {
            char* ptr_slot = (char*) page + PAGE_SIZE - sizeof(PageDir) - (it->second + 1) * sizeof(SlotDir);
            auto* this_slot = (SlotDir*)ptr_slot;

            void *CopyRecord = malloc(this_slot->ds_length);
            memcpy(CopyRecord, (char *)page + this_slot->ds_offset, this_slot->ds_length);
            memcpy((char *)page + this_slot->ds_offset - gapLength, CopyRecord, this_slot->ds_length); //shift
            free(CopyRecord);

            this_slot->ds_offset -= gapLength;
            // reset or free this space, beginning from the end of current record.
            memset((char*)page + this_slot->ds_offset + this_slot->ds_length, 0, gapLength);
        }

        return 0;

    }


    RC RecordBasedFileManager::getFormattedRecord(FileHandle &fileHandle, const RID &rid, void* formattedRecord) {
        void *page = (char *)malloc(PAGE_SIZE);
        char* slot;
        SlotDir* thisSlot;
        RID solidRid = rid;

        char flag = FAKE_RECORD_FLAG;

        while (flag == FAKE_RECORD_FLAG) {
            if (fileHandle.readPage(solidRid.pageNum, page) == 0) {
                // almost done
                slot = (char *)page + PAGE_SIZE - sizeof(PageDir) - (solidRid.slotNum + 1) * sizeof(SlotDir);
                thisSlot = (SlotDir*)slot;

                if (thisSlot->ds_length == 0) {
                    free(page);
                    return -2; // record has been deleted
                }

                memcpy(&flag, (char*)page + thisSlot->ds_offset, 1);

                if (flag == SOLID_RECORD_FLAG) {
                    memcpy((char*)formattedRecord, (char*)page + thisSlot->ds_offset, thisSlot->ds_length);

                    free(page);

                    return 0;
                } else if (flag == FAKE_RECORD_FLAG) {
                    char* tsRecord = (char*)page + thisSlot->ds_offset + 1;
                    auto* tsRid = (RID *)tsRecord;

                    memcpy(&solidRid, tsRid, sizeof(RID));

                    memset(page, 0, PAGE_SIZE);
                } else {
                    free(page);
                    return -3; // strange
                }
            } else {
                free(page);
                return -4; // read fail
            }
        }
    }



    RC RecordBasedFileManager::getAttributefromOrgFormat(const std::vector<Attribute> &recordDescriptor, const std::string & attributeName, void * deformattedRecord, void *data) {
        // only one attribute here

        int varCharLen;
        int fieldLength = recordDescriptor.size();
        int offsetData = 0;


        int nullFieldsIndicatorActualSize = ceil((double(fieldLength)/CHAR_BIT));
        auto *nullFieldsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
        memcpy((char *) nullFieldsIndicator, deformattedRecord, nullFieldsIndicatorActualSize);

        int oneByteReserved4nullIndicator = 1;
        auto *nullsIndicatorForAttr = (unsigned char *) malloc(oneByteReserved4nullIndicator);
        memset(nullsIndicatorForAttr, 0, oneByteReserved4nullIndicator); // all bits set to 0

        offsetData += nullFieldsIndicatorActualSize;


        // int lengthBeforeVarChar = ceil((double(recordDescriptor.size())/CHAR_BIT));


        for (int ind = 0; ind < recordDescriptor.size(); ind++) {
            Attribute info = recordDescriptor[ind];

            if (info.length != 0) {
                // not delete
                if (nullFieldsIndicator[ind / 8] & ((unsigned)1 << (unsigned) (7 - (ind % 8))) && info.name == attributeName) {
                    // desired attribute is null
                    //nullsIndicatorForAttr[0] |= ((unsigned char)1); // 0000 0001 if null

                    *((unsigned char *) nullsIndicatorForAttr) = 128u; // update 12/2

                    memcpy(data, nullsIndicatorForAttr, oneByteReserved4nullIndicator);
                    return 0;
                }

                // update 12/2
                if (nullFieldsIndicator[ind / 8] & ((unsigned)1 << (unsigned) (7 - (ind % 8)))){
                    continue;
                }
                // update 12/2

                switch (info.type) {
                    case TypeInt: {

                        if(info.name == attributeName){
                            memcpy((char* )data + oneByteReserved4nullIndicator, (char *) deformattedRecord + offsetData, INT_FIELD_LEN);
                            memcpy(data, nullsIndicatorForAttr, oneByteReserved4nullIndicator);
                            free(nullsIndicatorForAttr);
                            free(nullFieldsIndicator);
                            return 0 ;
                        }
                        offsetData += 4;
                        break;

                    }
                    case TypeReal: {
                        if(info.name == attributeName){
                            memcpy((char* )data + oneByteReserved4nullIndicator, (char *) deformattedRecord + offsetData, REAL_FIELD_LEN);
                            memcpy(data, nullsIndicatorForAttr, oneByteReserved4nullIndicator);
                            free(nullsIndicatorForAttr);
                            free(nullFieldsIndicator);
                            return 0 ;
                        }
                        offsetData += 4;
                        break;
                    }
                    case TypeVarChar: {
                        memcpy(&varCharLen, (char *) deformattedRecord + offsetData, sizeof(int));
                        offsetData += 4;
                        offsetData += varCharLen;
                        if(info.name == attributeName){

                            memcpy((char* )data + oneByteReserved4nullIndicator, &varCharLen, sizeof(int));
                            memcpy((char* )data + oneByteReserved4nullIndicator + sizeof(int), (char* )deformattedRecord + offsetData - varCharLen, varCharLen);

                            // nullsIndicatorForAttr[ind/8] &= (~((unsigned) 1 << (unsigned) ( 7- (ind % 8) )));
                            memcpy((char* )data, nullsIndicatorForAttr, oneByteReserved4nullIndicator);
                            free(nullsIndicatorForAttr);
                            free(nullFieldsIndicator);
                            return 0 ;

                        }

                        break;
                    }
                    default: {
                        break;
                    }
                }
            }
        }
    }


    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {

        void* wantedRecord = malloc(PAGE_SIZE);
        // RC rc1 = getFormattedRecord(fileHandle, rid, formattedRecord);
        RC rc1 = readRecord(fileHandle, recordDescriptor, rid, wantedRecord);

        if(rc1 == 0){

            RC rc3 = getAttributefromOrgFormat(recordDescriptor, attributeName, wantedRecord, data);

            if(rc3 != 0){
                //std::cout << "[ERROE]: getAttribute() fail" << std::endl;
                return -1;
            }
            free(wantedRecord);
            // free(deformattedRecord);
            //std::cout << "[SUCCESS]: readAttribute() succeed ! " << std::endl;
            return 0 ;

        }else{
            free(wantedRecord);
            return -1;
        }
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {

        return rbfm_ScanIterator.initScanIterator(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);

    }

    RC RecordBasedFileManager::getLengthBeforeVarChar(const std::vector<Attribute> &recordDescriptor,
                                                      const void *data, void *info,
                                                      unsigned char *nullFieldsIndicator) {
        // initialize
        int offsetData = ceil((double(recordDescriptor.size())/CHAR_BIT));;
        int lengthBeforeVarChar = ceil((double(recordDescriptor.size())/CHAR_BIT));
        int varCharLen;

        for (int ind = 0; ind < recordDescriptor.size(); ind++) {
            Attribute info = recordDescriptor[ind];

            if (info.length != 0) {
                // not delete
                if (nullFieldsIndicator[ind / 8] & (1 << (7 - (ind % 8)))) {
                    continue;
                }
                switch (info.type) {
                    case TypeInt: {
                        offsetData += 4;
                        lengthBeforeVarChar += 4;
                        break;
                    }
                    case TypeReal: {
                        offsetData += 4;
                        lengthBeforeVarChar += 4;
                        break;
                    }
                    case TypeVarChar: {
                        memcpy(&varCharLen, (char *) data + offsetData, sizeof(varCharLen));
                        offsetData += varCharLen;
                        offsetData += 4;
                        lengthBeforeVarChar += 4;
                        break;
                    }
                    default: {
                        break;
                    }
                }
            }
        }

        offsetData += FIELD_SIZE_LEN;
        offsetData += FLAG_LEN;
        lengthBeforeVarChar += FIELD_SIZE_LEN;
        lengthBeforeVarChar += FLAG_LEN;

        memcpy((char *)info, &offsetData, 4);
        memcpy((char *)info+4, &lengthBeforeVarChar, 4);

        return 0;
    }

    RC RecordBasedFileManager::formatRecord(const std::vector<Attribute> &recordDescriptor,
                                            const void *data, void *info, void *record,
                                            unsigned char *nullFieldsIndicator) {
        int offsetRecord = 0;
        int offsetData = 0;
        char16_t fieldLength = recordDescriptor.size();
        char16_t offsetVarChar = ((int *)info)[1];

        memset((char *)record, SOLID_RECORD_FLAG, 1);
        offsetRecord += FLAG_LEN;

        memcpy((char *)record + offsetRecord, &fieldLength, FIELD_SIZE_LEN);
        offsetRecord += FIELD_SIZE_LEN;

        // get nullsindicator size
        int nullFieldsIndicatorSize = ceil((double(recordDescriptor.size())/CHAR_BIT));
        memcpy((char *) record+offsetRecord, (char *)data+offsetData, nullFieldsIndicatorSize);
        offsetData += nullFieldsIndicatorSize;
        offsetRecord += nullFieldsIndicatorSize;

        int varCharLen;
        char16_t varCharLen_16;

        for (int ind = 0; ind < recordDescriptor.size(); ind++) {
            Attribute info = recordDescriptor[ind];

            if (info.length != 0) {
                // not delete
                if (nullFieldsIndicator[ind / CHAR_BIT] & (1 << (CHAR_BIT - 1 - (ind % CHAR_BIT)))) {
                    continue;
                }
                switch (info.type) {
                    case TypeInt: {
                        memcpy((char *)record+offsetRecord, (char *)data+offsetData, 4);
                        offsetData += 4;
                        offsetRecord += INT_FIELD_LEN;
                        break;
                    }
                    case TypeReal: {
                        memcpy((char *)record+offsetRecord, (char *)data+offsetData, 4);
                        offsetData += 4;
                        offsetRecord += REAL_FIELD_LEN;
                        break;
                    }
                    case TypeVarChar: {
                        varCharFormat(offsetRecord, offsetData, varCharLen_16, varCharLen, offsetVarChar, record, data);
                        break;
                    }
                    default: {
                        break;
                    }
                }
            }
        }

        return 0;
    }

    RC RecordBasedFileManager::varCharFormat(int &offsetRecord, int &offsetData, char16_t &varCharLen_16,
                                             int &varCharLen, char16_t &offsetVarChar, void *record,
                                             const void *data) {
        memcpy(&varCharLen, (char *)data+offsetData, 4);
        varCharLen_16 = varCharLen;
        memcpy((char *)record+offsetRecord, &varCharLen_16, VARCHAR_FIELD_LENGTH_LEN);
        offsetData += 4;
        offsetRecord += VARCHAR_FIELD_LENGTH_LEN;  // two bytes to represent length of variable field.

        memcpy((char *)record+offsetRecord, &offsetVarChar, VARCHAR_FIELD_OFFSET_LEN);
        memcpy((char *)record+offsetVarChar, (char *)data+offsetData, varCharLen);

        offsetData += varCharLen;
        offsetRecord += VARCHAR_FIELD_OFFSET_LEN;  // two bytes to represent offset of variable data.

        offsetVarChar += varCharLen;
        return 0;
    }

   /*
      RC RecordBasedFileManager::deFormatRecord(const std::vector<Attribute> &recordDescriptor,
                                              void *data, void *record) {// data is returned , record is formatted
        int offsetRecord = 0;
        int offsetData = 0;

        char flag;
        memcpy(&flag, record, 1);
        offsetRecord  += FLAG_LEN;

        char16_t fieldLength;
        memcpy(&fieldLength, (char *)record+offsetRecord, FIELD_SIZE_LEN);
        offsetRecord += FIELD_SIZE_LEN;

        // get nullsindicator size
        int nullFieldsIndicatorSize = ceil((double(fieldLength)/CHAR_BIT));
        auto *tempNullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorSize);
        memcpy((char *) tempNullFieldsIndicator, (char *)record+offsetRecord, nullFieldsIndicatorSize);
        memcpy((char *) data+offsetData, (char *)record+offsetRecord, nullFieldsIndicatorSize);
        offsetData += nullFieldsIndicatorSize;
        offsetRecord += nullFieldsIndicatorSize;

        int varCharLen;
        char16_t varCharLen_16;

        for (int ind = 0; ind < fieldLength; ind++) {
            Attribute info = recordDescriptor[ind];

            if (tempNullFieldsIndicator[ind / CHAR_BIT] & (1 << (CHAR_BIT - 1 - (ind % CHAR_BIT)))) {
                continue;
            }
            switch (info.type) {
                case TypeInt: {
                    memcpy((char *)data+offsetData, (char *)record+offsetRecord, 4);
                    offsetData +=4 ;
                    offsetRecord += INT_FIELD_LEN;
                    break;
                }
                case TypeReal: {
                    memcpy((char *)data+offsetData, (char *)record+offsetRecord, 4);
                    offsetData +=4 ;
                    offsetRecord += REAL_FIELD_LEN;
                    break;
                }
                case TypeVarChar: {
                    varCharDeformat(offsetRecord, offsetData, varCharLen_16,
                                    varCharLen, record, data);
                    break;
                }
                default: {
                    break;
                }
            }
        }

        free(tempNullFieldsIndicator);
        return 0;
    }
*/
    RC RecordBasedFileManager::deFormatRecord(const std::vector<Attribute> &recordDescriptor,
                                              void *data, void *record) {
        int offsetRecord = 0;
        int offsetData = 0;

        char flag;
        memcpy(&flag, (char*)record, 1);
        offsetRecord  += FLAG_LEN;

        char16_t fieldLength;
        memcpy(&fieldLength, (char *)record+offsetRecord, FIELD_SIZE_LEN);
        offsetRecord += FIELD_SIZE_LEN;

        // get nullsindicator size
        int nullFieldsIndicatorSize = ceil((double(fieldLength)/CHAR_BIT));
        auto *tempNullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorSize);
        memcpy((char *) tempNullFieldsIndicator, (char *)record+offsetRecord, nullFieldsIndicatorSize);
        memcpy((char *) data+offsetData, (char *)record+offsetRecord, nullFieldsIndicatorSize);
        offsetData += nullFieldsIndicatorSize;
        offsetRecord += nullFieldsIndicatorSize;

        int varCharLen;
        char16_t varCharLen_16;

        for (int ind = 0; ind < fieldLength; ind++) {
            Attribute info = recordDescriptor[ind];

            if (tempNullFieldsIndicator[ind / CHAR_BIT] & (1 << (CHAR_BIT - 1 - (ind % CHAR_BIT)))) {
                continue;
            }
            switch (info.type) {
                case TypeInt: {
                    memcpy((char *)data+offsetData, (char *)record+offsetRecord, 4);
                    offsetData +=4 ;
                    offsetRecord += INT_FIELD_LEN;
                    break;
                }
                case TypeReal: {
                    memcpy((char *)data+offsetData, (char *)record+offsetRecord, 4);
                    offsetData +=4 ;
                    offsetRecord += REAL_FIELD_LEN;
                    break;
                }
                case TypeVarChar: {
                    varCharDeformat(offsetRecord, offsetData, varCharLen_16,
                                    varCharLen, record, data);
                    break;
                }
                default: {
                    break;
                }
            }
        }

        free(tempNullFieldsIndicator);
        return 0;
    }


    RC RecordBasedFileManager::varCharDeformat(int &offsetRecord, int &offsetData, char16_t &varCharLen_16,
                                               int &varCharLen, void *record, void *data) {

        memcpy(&varCharLen_16, (char *)record+offsetRecord, VARCHAR_FIELD_LENGTH_LEN);
        varCharLen = varCharLen_16;
        memcpy((char *)data+offsetData, &varCharLen, 4);
        offsetRecord += VARCHAR_FIELD_LENGTH_LEN;
        offsetData += 4;

        char16_t offsetVarChar;
        memcpy(&offsetVarChar, (char *)record+offsetRecord, VARCHAR_FIELD_OFFSET_LEN);
        memcpy((char *)data+offsetData, (char *)record+offsetVarChar, varCharLen);

        offsetRecord += VARCHAR_FIELD_OFFSET_LEN;
        offsetData += varCharLen;
        return 0;
    }

    RC RecordBasedFileManager::isAvailablePage(FileHandle &fileHandle, char16_t recordLength, RID &rid, void* page) {
        if (recordLength > (PAGE_SIZE - sizeof(PageDir) - sizeof(SlotDir))) {
            // no page can contain such record data
            rid.slotNum = 0;
            rid.pageNum = fileHandle.getNumberOfPages();
            return -2;
        }

        if (fileHandle.getNumberOfPages() > 0) {
            for (int page_ind = (int)(fileHandle.getNumberOfPages()); page_ind > 0; page_ind--) {
                if (fileHandle.readPage(page_ind-1, page) == 0) {
                    char* PD_ptr = (char*) page + PAGE_SIZE - sizeof(PageDir);
                    auto* pageDir = (PageDir*) PD_ptr;

                    if (pageDir->freeSpace >= (recordLength + sizeof(SlotDir))) {
                        // page freespace is enough

                        for (short int slot_ind = 0; slot_ind < pageDir->numOfSlots; slot_ind++) {
                            char *SL_ptr = PD_ptr - (slot_ind + 1) * sizeof(SlotDir);
                            auto *thisSlot = (SlotDir *) SL_ptr;

                            // find the deleted record, reuse it
                            if (thisSlot->ds_length == 0) {
                                rid.slotNum = slot_ind;
                                rid.pageNum = page_ind;

                                return HAS_AVAILABLE_PAGE;
                            }
                        }

                        // create a new slot
                        rid.slotNum = pageDir->numOfSlots;
                        rid.pageNum = page_ind-1;

                        return HAS_AVAILABLE_PAGE;
                    }
                }
            }
        }
        rid.slotNum = 0;
        rid.pageNum = fileHandle.getNumberOfPages();
        return NO_WAY_AVAILABLE_PAGE;
    }



    RC RecordBasedFileManager::checkRecordFlag(FileHandle &fileHandle, RID &thisrid){

        auto* page = (char* )malloc(PAGE_SIZE);
        RC rc1 = fileHandle.readPage(thisrid.pageNum, page);
        if(rc1 != 0){
            std::cout << "[Warning] fileHandle.readPage() fail! [RecordBasedFileManager::checkFirstByte()]" << std::endl;
            free(page);
            return -1;
        }
        else{
            SlotDir thisSlot;
            memcpy(&thisSlot, (char*)page + PAGE_SIZE - sizeof(PageDir) - (thisrid.slotNum + 1)*sizeof(SlotDir), sizeof(SlotDir));
            if(thisSlot.ds_length == 0) {
                // was deleted
                free(page);
                return -2;
            }
            else{
                char flag;
                memcpy(&flag, (char*)page + thisSlot.ds_offset, 1);
                if(flag == 1) {
                    free(page);
                    return 0;
                }
                else{
                    // was updated then moved to other page
                    free(page);
                    return -3;
                }
            }
        }
    }


    RC RecordBasedFileManager::getAttributeSfromOrgFormat(const std::vector<Attribute> &recordDescriptor, const vector<string> &attributeNames,
                                                          void* deformattedRecord, void* data){
        //void* data;  // substitute of 'data'
        // the attributes in attributeNames do not have to be in original order in recordDescriptor
        // so need three for loops

        int recordDescriptor_size = recordDescriptor.size();
        int attributeNames_size = attributeNames.size();
        int offset_record = 0;
        int offset_returned_data = 0;
        int idx_attributeNames ;

        // nulls indicator of returned data
        int nullIndicator_attributes_siz = ceil((double(attributeNames_size)/CHAR_BIT));
        auto* nullIndicator_attributes = (char* )malloc(nullIndicator_attributes_siz);
        memset(nullIndicator_attributes, 0, nullIndicator_attributes_siz); // all set to 0
        offset_returned_data += nullIndicator_attributes_siz; // save space for storing nulls indicator later

        // deformattedRecord
        int nullFieldsIndicatorActualSize = ceil((double(recordDescriptor_size)/CHAR_BIT));
        auto *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
        memcpy(nullsIndicator, (char* )deformattedRecord + offset_record, nullFieldsIndicatorActualSize);
        offset_record += nullFieldsIndicatorActualSize;

        int idx_field, temp_varvhar_len;
        for(idx_attributeNames = 0; idx_attributeNames < attributeNames_size; idx_attributeNames++) {

            std::string wantedAttrName = attributeNames[idx_attributeNames];
            offset_record = nullFieldsIndicatorActualSize;

            // for each wanted attribute name, loop over recordDescriptor
            for (int idx_byte = 0; idx_byte < nullFieldsIndicatorActualSize; idx_byte++) {
                for (int idx_bit = 0; idx_bit < CHAR_BIT; idx_bit++) {
                    idx_field = idx_byte * CHAR_BIT + idx_bit;
                    if (idx_field == recordDescriptor_size)
                        // end search
                        break;

                    Attribute thisattribute = recordDescriptor[idx_field];


                    if (nullsIndicator[idx_byte] & (unsigned) 1 << (unsigned) (7 - idx_bit)) {
                        //if null
                        if (thisattribute.name == wantedAttrName) {
                            // if null, set corresponding bit to 1 !
                            nullIndicator_attributes[idx_attributeNames / CHAR_BIT] |= ((unsigned char) 1
                                    << (unsigned) (7 - idx_attributeNames % CHAR_BIT));
                            //idx_attributeNames++;
                            continue;
                        } else {
                            continue;
                        }
                    }

                    switch (thisattribute.type) {
                        case TypeInt: {
                            if (thisattribute.name == wantedAttrName) {
                                memcpy((char *) data + offset_returned_data, (char *) deformattedRecord + offset_record,
                                       INT_FIELD_LEN);


                                offset_returned_data += 4;
                                //idx_attributeNames++;
                            }
                            offset_record += INT_FIELD_LEN;
                            break;
                        }
                        case TypeReal: {
                            if (thisattribute.name == wantedAttrName) {
                                memcpy((char *) data + offset_returned_data, (char *) deformattedRecord + offset_record,
                                       REAL_FIELD_LEN);
                                offset_returned_data += 4;
                                //idx_attributeNames++;
                            }
                            offset_record += REAL_FIELD_LEN;
                            break;
                        }
                        case TypeVarChar: {
                            memcpy(&temp_varvhar_len, (char *) deformattedRecord + offset_record, 4);
                            offset_record += 4;
                            if (thisattribute.name == wantedAttrName) {
                                memcpy((char *) data + offset_returned_data, &temp_varvhar_len, sizeof(int));
                                offset_returned_data += 4;
                                memcpy((char *) data + offset_returned_data, (char *) deformattedRecord + offset_record,
                                       temp_varvhar_len);
                                offset_returned_data += temp_varvhar_len;
                                //idx_attributeNames++;
                            }
                            offset_record += temp_varvhar_len;
                            break;

                        }
                    }
                }
            }

        }


        memcpy(data, nullIndicator_attributes, nullIndicator_attributes_siz);
        free(nullsIndicator);
        free(nullIndicator_attributes);
        return 0;


    }

    RC RecordBasedFileManager::readAttributesGivenByRidAndAttributeNames(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                                                 const RID &rid, const vector<string> &attributeNames,
                                                                 void* data) {
        void *wantedRecord = malloc(PAGE_SIZE);
        // read formatted record by rid
        RC rc1 = readRecord(fileHandle, recordDescriptor, rid, wantedRecord);

        if(rc1 == 0){

//            void* deformattedRecord = nullptr;
//            // recover to original data
//            RC rc2 = deFormatRecord(recordDescriptor, deformattedRecord, formattedRecord);
//
//            if(rc2 != 0){
//                std::cout << "[ERROE]: deFormatRecord() fail [readAttributesGivenAttributeNames]" << std::endl;
//                return -1;
//            }
            // read contents given attributeNames in the record, returned data should have null-indicators

            RC rc3 = getAttributeSfromOrgFormat(recordDescriptor, attributeNames, wantedRecord, data);

            if(rc3 != 0){
                std::cout << "[ERROE]: getAttributeSfromOrgFormat() fail [readAttributesGivenAttributeNames]" << std::endl;
                return -1;
            }
            free(wantedRecord);
            //free(deformattedRecord);
            // std::cout << "[SUCCESS]: readAttribute() succeed ! [readAttributesGivenAttributeNames]" << std::endl;
            return 0 ;

        }else{
            // std::cout << "[ERROE]: getFormattedRecord() fail [readAttributesGivenAttributeNames]" << std::endl;
            free(wantedRecord);
            return -1;
        }

    }


    /*RBFM_ScanIterator*/

    RBFM_ScanIterator::RBFM_ScanIterator(){
        maxAttrLen = -1;
        maxRecordLen = -1;
        cur_num_slots_of_curPage = -1;
    }


    RC RBFM_ScanIterator::initScanIterator(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute,
                                           const CompOp compOp, const void *value, const vector<string> &attributeNames){

        this->fileHandle = fileHandle;
        this->recordDescriptor = recordDescriptor;
        this->conditionAttribute = conditionAttribute;
        this->compOp = compOp;
        this->value = (char*)value;
        this->attributeNames = attributeNames;


        this->cur_rid.pageNum = 0;
        this->cur_rid.slotNum = -1;


        void *page = malloc(PAGE_SIZE);
        fileHandle.readPage(cur_rid.pageNum, page);
        auto *pageDir_ptr = (PageDir *)((char *)page + PAGE_SIZE - sizeof(PageDir));
        cur_num_slots_of_curPage = pageDir_ptr->numOfSlots;
        free(page);

        num_of_pages = fileHandle.getNumberOfPages();

        for(auto & it : recordDescriptor) {
            if(it.name == conditionAttribute){
                conditionAttributeType = it.type;
            }
        }

        int idx = 0;  // index for looping through attributeNames
        while(idx < attributeNames.size()){
            for(auto & attr_i : recordDescriptor){
                if(attr_i.name == attributeNames[idx]){
                    attributesVector.push_back(attr_i);
                    idx++;
                    break;
                }
            }
        }

        for(auto & it : this->recordDescriptor){
            maxRecordLen += it.length;
            if(it.length > maxAttrLen)
                maxAttrLen = it.length;
        }



        return 0;

    }


    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){

        bool is_found = false;
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        auto* attribute_with_nullIndicator = (char* )malloc(maxAttrLen); // or PAGE_SIZE ?? this caused the failure of conditional scan test case

        //find rid
        while(!is_found){
            // std::cout << "[Warning] " << std::endl;
            RC rc1 = updateCurRid();

            if(rc1 != 0){
                // std::cout << "[Warning] updateCurRid() [RBFM_ScanIterator::getNextRecord()]" << std::endl;
                free(attribute_with_nullIndicator);
                return rc1;
            }
            else{
                RC rc_check = rbfm.checkRecordFlag(fileHandle, cur_rid);
                if(rc_check != 0){
                    // this record was deleted or updated(moved to other page)
                    // continue to check next rid
                    continue;
                }
                else{
                    if(conditionAttribute.empty()){
                        rid.pageNum = cur_rid.pageNum;
                        rid.slotNum = cur_rid.slotNum;
                        free(attribute_with_nullIndicator); // update 12/7
                        // if condition attribute is "", then there is no need to do Comporation.
                        break;
                    }


                    RC rc2 = rbfm.readAttribute(fileHandle, recordDescriptor, cur_rid, conditionAttribute, attribute_with_nullIndicator);
                    // returned is attribute_with_nullIndicator: 1 Byte + data

                    if(rc2 == -1){
                        std::cout << "[ERROR] rbfm.readAttribute() [RBFM_ScanIterator::getNextRecord()]" << std::endl;
                        // free(attribute_with_nullIndicator);
                        continue;
                    }
                    else {

                        RC rc3 = helperCompOp(attribute_with_nullIndicator);
                        if(rc3 > 0){
                            // condition satisfied
                            rid.slotNum = cur_rid.slotNum;
                            rid.pageNum = cur_rid.pageNum;
                            free(attribute_with_nullIndicator);
                            is_found = true;
                            //break;
                        }
                        else{
                            // condition not satisfied
                            // continue to next rid
                            continue;
                        }

                    }

                }
            }
        }

        // here we find next rid satisfying the condition successfully

        RC rc4 = rbfm.readAttributesGivenByRidAndAttributeNames(fileHandle, recordDescriptor, rid, attributeNames, data);
        // free(attribute_with_nullIndicator);
        return 0;

    }


    RC RBFM_ScanIterator::updateCurRid() {
        cur_rid.slotNum++;
        if(num_of_pages == 0){
            return RBFM_EOF;
        }

        if (cur_rid.slotNum >= cur_num_slots_of_curPage) {
            cur_rid.slotNum = 0;
            cur_rid.pageNum++;

            if (cur_rid.pageNum >= num_of_pages) {
                cur_num_slots_of_curPage = 0;
                // std::cout << "[Warning] get the last page of file [RBFM_ScanIterator::findNext_cur_rid()]" << std::endl;
                return RBFM_EOF;
            } else {
                void *page = malloc(PAGE_SIZE);
                if (fileHandle.readPage(cur_rid.pageNum, page) != 0) {
                    free(page);
                    //std::cout << "[Warning] can not read the next page [RBFM_ScanIterator::findNext_cur_rid()]" << std::endl;
                    return -2;
                } else {
                    auto *_page_dir = ((char *) page + PAGE_SIZE - sizeof(PageDir));
                    cur_num_slots_of_curPage = ((PageDir *) _page_dir)->numOfSlots;
                    free(page);
                    return 0;
                }

            }
        }

        return 0;
    }


    RC RBFM_ScanIterator::helperCompOp(void * attributeWithFlag){
        // update 12/2
        if( *((unsigned char *) attributeWithFlag) == 128u){
            //null here, continue to check next rid
            return -1;
        }
        // update 12/2

        int nullIndicatorSize = 1;
        // private: conditionAttributeType
        switch(conditionAttributeType){
            case TypeInt:{
                int attr_value, condition_value;
                memcpy(&attr_value, (char*)attributeWithFlag + nullIndicatorSize, sizeof(int) );
                memcpy(&condition_value, (char*)value, sizeof(int));
                switch (compOp){
                    case EQ_OP:{
                        return attr_value == condition_value;
                    }
                    case LT_OP:{
                        return attr_value < condition_value;
                    }
                    case LE_OP:{
                        return attr_value <= condition_value;
                    }
                    case GT_OP:{
                        return attr_value > condition_value;
                    }
                    case GE_OP:{
                        return attr_value >= condition_value;
                    };
                    case NE_OP:{
                        return attr_value != condition_value;
                    }
                    case NO_OP:{
                        return 1;
                    }
                }

            }
            case TypeReal:{
                float attr_value, condition_value;
                memcpy(&attr_value, (char*)attributeWithFlag + nullIndicatorSize, sizeof(float) );
                memcpy(&condition_value, (char*)value, sizeof(float));
                switch (compOp){
                    case EQ_OP:{
                        return attr_value == condition_value;
                    }
                    case LT_OP:{
                        return attr_value < condition_value;
                    }
                    case LE_OP:{
                        return attr_value <= condition_value;
                    }
                    case GT_OP:{
                        return attr_value > condition_value;
                    }
                    case GE_OP:{
                        return attr_value >= condition_value;
                    };
                    case NE_OP:{
                        return attr_value != condition_value;
                    }
                    case NO_OP:{
                        return 1;
                    }
                }

            }
            case TypeVarChar:{
                // update 11/30
                int varchar_attr_length;
                memcpy(&varchar_attr_length, (char*)attributeWithFlag + nullIndicatorSize, sizeof(int));
                auto* varcharAttr = (char* )malloc(varchar_attr_length + 1);
                memcpy(varcharAttr, (char* )attributeWithFlag + nullIndicatorSize + sizeof(int), varchar_attr_length);
                varcharAttr[varchar_attr_length] = '\0';


                int varchar_condition_length;
                memcpy(&varchar_condition_length, (char*)value, sizeof(int));
                auto* varcharCond = (char* )malloc(varchar_condition_length + 1);
                memcpy(varcharCond, (char*)value + sizeof(int), varchar_condition_length);
                varcharCond[varchar_condition_length] = '\0';

                int CompOpVarchar;
                switch (compOp){
                    case EQ_OP:{
                        CompOpVarchar = strcmp(varcharAttr, varcharCond) == 0;
                        free(varcharAttr);
                        free(varcharCond);
                        return CompOpVarchar;
                    }
                    case LT_OP:{
                        CompOpVarchar = strcmp(varcharAttr, varcharCond) < 0;
                        free(varcharAttr);
                        free(varcharCond);
                        return CompOpVarchar;
                    }
                    case LE_OP:{
                        CompOpVarchar = strcmp(varcharAttr, varcharCond) <= 0;
                        free(varcharAttr);
                        free(varcharCond);
                        return CompOpVarchar;
                    }
                    case GT_OP:{
                        CompOpVarchar = strcmp(varcharAttr, varcharCond) > 0;
                        free(varcharAttr);
                        free(varcharCond);
                        return CompOpVarchar;
                    }
                    case GE_OP:{
                        CompOpVarchar = strcmp(varcharAttr, varcharCond) >= 0;
                        free(varcharAttr);
                        free(varcharCond);
                        return CompOpVarchar;
                    };
                    case NE_OP:{
                        CompOpVarchar = strcmp(varcharAttr, varcharCond) != 0;
                        free(varcharAttr);
                        free(varcharCond);
                        return CompOpVarchar;
                    }
                    case NO_OP:{
                        free(varcharAttr);
                        free(varcharCond);
                        return 1;
                    }
                }
            }

        }

    }


    RC RBFM_ScanIterator::close() {
        RecordBasedFileManager::instance().closeFile(fileHandle);
        return 0;
    };


} // namespace PeterDB

