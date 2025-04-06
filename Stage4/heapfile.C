#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File *file;
    Status status;
    FileHdrPage *hdrPage;
    int hdrPageNo;
    int newPageNo;
    Page *newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {

        // File doesn't exist so we create it
        status = db.createFile(fileName);
        if (status != OK)
            return status;

        // Open file so we can allocate pages to it
        status = db.openFile(fileName, file);
        if (status != OK)
            return status;

        // Allocate the file header page
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status != OK)
            return status;

        // Cast Page* pointer to FileHdrPage*
        hdrPage = (FileHdrPage *)newPage;

        // Initialize header page values
        strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE);
        hdrPage->pageCnt = 1;
        hdrPage->recCnt = 0;

        // Allocate the first data page
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK)
            return status;

        // Initialize the values in the header page
        newPage->init(newPageNo);

        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt++;

        // Unpin both pages and mark them as dirty
        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if (status != OK)
            return status;

        status = bufMgr->unPinPage(file, newPageNo, true);
        if (status != OK)
            return status;

        db.closeFile(file); 
        if(status != OK)
            return status;
        return OK;
    }
    db.closeFile(file); 
    return (FILEEXISTS);
}

const Status destroyHeapFile(const string fileName)
{
    Status status;
    File *file;
    bool fileWasOpen = true;
    
    // Try to open the file to get a handle
    status = db.openFile(fileName, file);
    if (status != OK) {
        // If we can't open it, it might be because it doesn't exist
        // In that case, just return the error
        if (status != FILENOTOPEN) 
            return status;
        fileWasOpen = false;
    }
    
    if (fileWasOpen) {
        // Flush all pages for this file from the buffer
        status = bufMgr->flushFile(file);
        if (status != OK) {
            db.closeFile(file);
            return status;
        }
        
        // Close the file
        status = db.closeFile(file);
        if (status != OK)
            return status;
    }
    
    // Now destroy the file
    return db.destroyFile(fileName);
}

// constructor opens the underlying file
HeapFile::HeapFile(const string &fileName, Status &returnStatus)
{

    Status status;
    Page *pagePtr;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {

        // Ask the file for the first page (header page)
        status = filePtr->getFirstPage(headerPageNo);
        if (status != OK)
        {
            cerr << "getFirstPage failed\n";
            returnStatus = status;
            return;
        }

        // Read and pin the header page
        cout << "Reading header page..." << endl;
        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
        if (status != OK)
        {
            cerr << "failed to read header page\n";
            bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
            db.closeFile(filePtr);
            returnStatus = status;
            return;
        }

        headerPage = (FileHdrPage *)pagePtr;

        hdrDirtyFlag = false;

        // Pin the first data page
        curPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);

        if (status != OK)
        {
            cerr << "failed to read first data page\n";
            bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
            db.closeFile(filePtr);
            returnStatus = status;
            return;
        }
        curDirtyFlag = false;
        curRec = NULLRID;

        returnStatus = OK;
    }
    else
    {
        cerr << "open of heap file failed\n";
        returnStatus = status;
        return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        if (status != OK)
            cerr << "error in unpin of date page\n";
    }

    // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK)
        cerr << "error in unpin of header page\n";

    // status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
    // if (status != OK) cerr << "error in flushFile call\n";
    // before close the file
    status = db.closeFile(filePtr);
    if (status != OK)
    {
        cerr << "error in closefile call\n";
        Error e;
        e.print(status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
    return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID &rid, Record &rec)
{
    // If the current page is already pinned, check for the record.
    Status status;
    if (curPage == NULL || curPageNo != rid.pageNo)
    {
        if (curPage != NULL)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
                return status;
        }

        // Read the page containing the record
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK)
            return status;

        curPageNo = rid.pageNo;
        curDirtyFlag = false; // Page default is clean right after being read in.
    }

    status = curPage->getRecord(rid, rec);
    if (status != OK)
        return status;

    curRec = rid;

    return OK;
}

HeapFileScan::HeapFileScan(const string &name,
                           Status &status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
                                     const int length_,
                                     const Datatype type_,
                                     const char *filter_,
                                     const Operator op_)
{
    if (!filter_)
    { // no filtering requested
        filter = NULL;
        return OK;
    }

    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int) || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}

const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo)
    {
        if (curPage != NULL)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
                return status;
        }
        // restore curPageNo and curRec values
        curPageNo = markedPageNo;
        curRec = markedRec;
        // then read the page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;
        curDirtyFlag = false; // it will be clean
    }
    else
        curRec = markedRec;
    return OK;
}

const Status HeapFileScan::scanNext(RID &outRid)
{
    Status status;
    Record rec;
    
    while (true) {
        if (curRec.pageNo == -1) {
            status = curPage->firstRecord(curRec);
            if (status != OK) {
                if (status != NORECORDS) return status;
                
                int nextPageNo;
                status = curPage->getNextPage(nextPageNo);
                if (status != OK) return status;
                
                // End of file
                if (nextPageNo == -1) return FILEEOF;
                
                // Unpin current page
                status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
                if (status != OK) return status;
                
                // Read next page
                status = bufMgr->readPage(filePtr, nextPageNo, curPage);
                if (status != OK) return status;
                
                curPageNo = nextPageNo;
                curDirtyFlag = false;
                
                // Stay at the beginning of the new page
                continue;
            }
        } else {
            // Try to get the next record on the current page
            RID nextRid;
            status = curPage->nextRecord(curRec, nextRid);
            if (status != OK) {
                // End of page, try the next page
                if (status != ENDOFPAGE) return status;
                
                int nextPageNo;
                status = curPage->getNextPage(nextPageNo);
                if (status != OK) return status;
                
                // End of file
                if (nextPageNo == -1) return FILEEOF;
                
                // Unpin current page
                status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
                if (status != OK) return status;
                
                // Read next page
                status = bufMgr->readPage(filePtr, nextPageNo, curPage);
                if (status != OK) return status;
                
                curPageNo = nextPageNo;
                curDirtyFlag = false;
                
                // Mark that we're at the beginning of a new page
                curRec.pageNo = -1;
                continue;
            }
            
            // Update current record to the next one
            curRec = nextRid;
        }
        
        // Check for match
        if (filter) {
            status = curPage->getRecord(curRec, rec);
            if (status != OK) return status;
            
            if (!matchRec(rec)) {
                continue;
            }
        }
        
        // Found a matching record
        outRid = curRec;
        return OK;
    }
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page

const Status HeapFileScan::getRecord(Record &rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file.
const Status HeapFileScan::deleteRecord()
{
    Status status = OK;
    
    // Make sure we have a valid record
    if (curRec.pageNo == -1 || curRec.slotNo == -1)
        return NORECORDS;
        
    // Try to delete the record from the current page
    status = curPage->deleteRecord(curRec);
    
    // If error is INVALIDSLOTNO, the record might have been deleted already
    // We should treat this as success rather than propagating the error
    if (status != OK && status != INVALIDSLOTNO)
        return status;
        
    // Only decrement record count and mark dirty if we actually deleted something
    if (status == OK) {
        // Update header page and mark it dirty
        headerPage->recCnt--;
        hdrDirtyFlag = true;
        curDirtyFlag = true;
    }
    
    return OK;
}
// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record &rec) const
{
    // no filtering requested
    if (!filter)
        return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length - 1) >= rec.length)
        return false;

    float diff = 0; // < 0 if attr < fltr
    switch (type)
    {

    case INTEGER:
        int iattr, ifltr; // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr; // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch (op)
    {
    case LT:
        if (diff < 0.0)
            return true;
        break;
    case LTE:
        if (diff <= 0.0)
            return true;
        break;
    case EQ:
        if (diff == 0.0)
            return true;
        break;
    case GTE:
        if (diff >= 0.0)
            return true;
        break;
    case GT:
        if (diff > 0.0)
            return true;
        break;
    case NE:
        if (diff != 0.0)
            return true;
        break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string &name,
                               Status &status) : HeapFile(name, status)
{
    // Do nothing. Heapfile constructor will bread the header page and the first
    //  data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK)
            cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record &rec, RID &outRid)
{
    Page *newPage;
    int newPageNo;
    Status status, unpinstatus;
    RID rid;

    // check for very large records
    if ((unsigned int)rec.length > PAGESIZE - DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    // If current page is NULL, load last page
    if (curPage == NULL)
    {
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;
        curDirtyFlag = false;
    }

    // Try to insert record into current page
    status = curPage->insertRecord(rec, outRid);

    // If there is no space on current page
    if (status == NOSPACE)
    {

        // Allocate and initialize new page
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);

        if (status != OK)
            return status;
        newPage->init(newPageNo);
        newPage->setNextPage(-1);

        // Link the old page to the new one
        status = curPage->setNextPage(newPageNo);
        if (status != OK)
            return status;

        // Unpin old page
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        if (status != OK)
            return status;

        // Update header
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;

        curPage = newPage;
        curPageNo = newPageNo;

        curDirtyFlag = true;

        // Try inserting again into new page
        status = curPage->insertRecord(rec, outRid);
    }
    if (status != OK)
        return status;

    headerPage->recCnt++;
    hdrDirtyFlag = true;
    curDirtyFlag = true;
    return OK;
}
