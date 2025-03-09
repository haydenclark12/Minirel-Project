// Description: Buffer Manager Implementation, responsible for managing the buffer pool and handling page reads and writes
// Authors: Theo Leao Larrieux (leolarrieux), Hayden Clark (TODO ID), and Luke Janssen (TODO ID)

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)                                              \
    {                                                          \
        if (!(c))                                              \
        {                                                      \
            cerr << "At line " << __LINE__ << ":" << endl      \
                 << "  ";                                      \
            cerr << "This condition should hold: " #c << endl; \
            exit(1);                                           \
        }                                                      \
    }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++)
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true)
        {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}

const Status BufMgr::allocBuf(int &frame)
{
    int scanned = 0;

    while (scanned < numBufs)
    {
        BufDesc *buf = &bufTable[clockHand];

        if (!buf->valid)
        { // Check if valid set
            frame = clockHand;
            buf->valid = false;
            advanceClock();
            return OK;
        }

        if (buf->refbit)
        { // If page hasn't been recently used
            buf->refbit = false;
            advanceClock();
            scanned++;
            continue;
        }

        if (buf->pinCnt > 0)
        { // If page isn't pinned
            advanceClock();
            scanned++;
            continue;
        }
        if (buf->dirty == true)
        { // If page needs to be flushed to disk
            Status status = buf->file->writePage(buf->pageNo, &(bufPool[clockHand]));
            if (status != OK)
            {
                return status;
            }
            buf->dirty = false;
        }
        hashTable->remove(buf->file, buf->pageNo);

        frame = clockHand;
        buf->valid = true;
        buf->file = NULL; // Clear old file reference
        buf->pageNo = -1; // Reset page number
        advanceClock();
        return OK;
    }
    return BUFFEREXCEEDED;
}

// Given a file and page number, reads the page into the buffer pool
// If the page is already in the buffer pool, increments the pin count and updates the refbit
const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{
    // Check if page is already in buffer pool, if so increment pin count, update refbit, and return page pointer
    int frameNo = 0;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    if (status == OK)
    {
        bufTable[frameNo].pinCnt++;
        bufTable[frameNo].refbit = true;
        page = &bufPool[frameNo];
        return OK;
    }

    // Otherwise, find a free frame in the buffer pool and read the page into it
    else if (status == HASHNOTFOUND)
    {
        status = allocBuf(frameNo);
        if (status != OK)
        {
            return status;
        }
        status = file->readPage(PageNo, &bufPool[frameNo]);
        if (status != OK)
        {
            return status;
        }
        bufTable[frameNo].Set(file, PageNo);
        page = &bufPool[frameNo];
        return OK;
    }
    return status;
}

// Given a file and its page number, decrements the pin count of the page in the buffer pool
// If the dirty flag is set, updates the dirty bit of the page in the buffer pool
const Status BufMgr::unPinPage(File *file, const int PageNo,
                               const bool dirty)
{
    // Check if page is in buffer pool
    int frameNo = 0;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    if (status != OK)
    {
        return status;
    }
    // Pin count is 0, so page is not pinned
    if (bufTable[frameNo].pinCnt == 0)
    {
        return PAGENOTPINNED;
    }
    // Decrement pin count, updating dirty bit if necessary
    bufTable[frameNo].pinCnt--;
    if (dirty)
    {
        bufTable[frameNo].dirty = true;
    }
    return OK;
}

// Given a file, we allocate a new page for it and add it to the buffer pool
const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page)
{
    Status status = file->allocatePage(pageNo);
    if (status != OK)
    {
        return status;
    }
    int frameNo = 0;
    status = allocBuf(frameNo);
    if (status != OK)
    {
        return status;
    }
    
    status = hashTable->insert(file, pageNo, frameNo);
    if (status != OK)
    {
        return status;
    }
    bufTable[frameNo].Set(file, pageNo);
    return OK;
}

const Status BufMgr::disposePage(File *file, const int pageNo)
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File *file)
{
    Status status;

    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file)
        {

            if (tmpbuf->pinCnt > 0)
                return PAGEPINNED;

            if (tmpbuf->dirty == true)
            {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo
                     << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                                      &(bufPool[i]))) != OK)
                    return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file, tmpbuf->pageNo);

            tmpbuf->file = NULL;
            tmpbuf->pageNo = -1;
            tmpbuf->valid = false;
        }

        else if (tmpbuf->valid == false && tmpbuf->file == file)
            return BADBUFFER;
    }

    return OK;
}

void BufMgr::printSelf(void)
{
    BufDesc *tmpbuf;

    cout << endl
         << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char *)(&bufPool[i])
             << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
