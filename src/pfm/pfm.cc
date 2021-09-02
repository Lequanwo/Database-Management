#include "src/include/pfm.h"

using namespace std;

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        // file handler
        fstream fs;
        fs.open(fileName, ios::in | ios::binary);

        if (fs){
            // file already exists
            if (fs.is_open()) {
                fs.close();
            }
            return -1;
        }
        else {
            // create empty-page file, out mode
            fs.open(fileName, ios::out | ios::binary);
            if (fs.is_open()){
                // creating file successes
                fs.close();
                return 0;
            }else{
                // creating file fails
                return -1;
            }
        }
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        // file handler
        fstream fs;
        fs.open(fileName, ios::in | ios::binary);

        if (fs) {
            // file exists, delete
            if (fs.is_open()) {
                fs.close();
            }
            // filename converts to a pointer
            if (remove(fileName.c_str()) != 0) {
                return -1;
            } else {
                return 0;
            }
        } else {
            if (fs.is_open()) {
                fs.close();
            }
            return -1;
        }
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {

        auto *fs = new fstream();
        fs->open(fileName.c_str(),fstream::in | fstream::out | fstream::binary);
        if(!(*fs)){
            //file not exist
            return -1;
        }
        return fileHandle.openFile(fs);
    }

/*    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {

        fstream *fs = new fstream();
        fs->open(fileName.c_str(),fstream::in | fstream::out | fstream::binary);
        if(!(*fs)){
            //file not exist
            delete fs;
            return -1;
        }
        RC rc = fileHandle.openFile(fs);
        delete fs;
        return rc;
    }*/

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        return fileHandle.closeFile();
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        isOpen = false;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (_file->is_open()) {
            unsigned pages_total_num = getNumberOfPages();
            if (pageNum > pages_total_num) {
                // overflow
                return -1;
            } else {
                //locate the to-read page head
                _file->seekg((pageNum+1) * PAGE_SIZE, ios::beg);

                if (!_file->read(static_cast<char*>(data), PAGE_SIZE)) {
                    // read fail
                    return -1;
                } else {
                    readPageCounter++;
                    writeCounterValues();
                    return 0;
                }
            }
        } else {
            // file not open, read fail
            return -1;
        }
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if (_file->is_open()) {
            unsigned pages_total_num = getNumberOfPages();
            if (pageNum>pages_total_num) {
                return -1;
            } else {
                //locate the to-write page head
                _file->seekg((pageNum+1) * PAGE_SIZE, ios::beg);
                if (!_file->write((char*)(data), PAGE_SIZE)) {
                    // write fail
                    return -1;
                } else {
                    _file->flush();
                    writePageCounter++;
                    writeCounterValues();
                    return 0;
                }
            }
        } else {
            // file not open, write fail
            return -1;
        }
    }

    RC FileHandle::appendPage(const void *data) {
        if (!_file->good()) {
            // write new page fail
            return -5;
        }
        if (_file->is_open()) {
            unsigned pageNum = getNumberOfPages() + 1;

            //locate the to-write page head
            _file->seekg(pageNum*PAGE_SIZE, ios::beg);
            _file->write(static_cast<const char *>(data), PAGE_SIZE);
            _file->seekg(0, ios::beg);
            _file->flush();
            appendPageCounter++;
            npages++;
            writeCounterValues();
            return 0;
//            if (!_file->good()){
//                // write new page fail
//                return -3;
//            } else {
//                _file->seekg(0, ios::beg);
//                if (!_file->flush()) {
//                    return -4;
//                } else {
//                    appendPageCounter++;
//                    npages++;
//                    writeCounterValues();
//                    return 0;
//                }
//            }
        } else {
            // file not open, write fail
            return -1;
        }
    }

    unsigned FileHandle::getNumberOfPages() {
        // This method returns the total number of pages currently in the file.

        return npages;

    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readCounterValues();
        readPageCount = this->readPageCounter;
        writePageCount = this->writePageCounter;
        appendPageCount = this->appendPageCounter;
        return 0;
    }

    RC FileHandle::readCounterValues() {
        // read file first 4Byte data, which is the counter data, to the FileHandle instance
        _file->seekg(PAGE_SIZE - 3 * sizeof(unsigned), ios::beg);
        _file->read((char*)&readPageCounter, sizeof(unsigned));
        _file->read((char*)&writePageCounter, sizeof(unsigned));
        _file->read((char*)&appendPageCounter, sizeof(unsigned));
        if (_file->good()) {
            return 0;
        }
        else {
            // file to read read/write/append counter
            return -1;
        }
    }

    RC FileHandle::writeCounterValues() {
        // read file first 4Byte data, which is the counter data, to the FileHandle instance
        _file->seekg(PAGE_SIZE - 3 * sizeof(unsigned), ios::beg);
        _file->write((char*)&readPageCounter, sizeof(unsigned));
        _file->write((char*)&writePageCounter, sizeof(unsigned));
        _file->write((char*)&appendPageCounter, sizeof(unsigned));
        if (_file->good()) {
            return 0;
        }
        else {
            // fail to write read/write/append counter
            return -1;
        }
    }

    RC FileHandle::initCounterValues() {
        writeCounterValues();
        if (_file->good()) {
            return 0;
        } else {
            return -1;
        }
    }

    RC FileHandle::openFile(fstream* file) {
        if (!file) {
            return -1;
        }

        if (isOpen) {
            return -2; // duplicate open
        }

        _file = file;

        //_file->copyfmt(*file); // not sure

        _file->seekg(0, ios::end);

        if (_file->tellg() == 0) {
            initCounterValues();
        }
        npages = _file->tellg() / PAGE_SIZE - 1;
//        _file->seekg(PAGE_SIZE, ios::beg);
        readCounterValues();
        isOpen = true;
        return 0;
    }

    RC FileHandle::closeFile() {
        if (!(_file->is_open())) {
            // file not open
            return -1;
        }
        if(!isOpen)
            return -1;

        _file->flush();
        _file->close();
        delete(_file);
        isOpen = false;
        return 0;
    }

} // namespace PeterDB