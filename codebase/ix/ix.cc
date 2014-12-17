
#include "ix.h"
#include <iostream>
#include "math.h"
#include "string.h"
#include "malloc.h"
#include <functional>

using namespace std;



unsigned getEntryLen(const Attribute &attr, const void *key)
{
    int entryLen = 0;
    entryLen += sizeof(int);  //offset len
    entryLen += sizeof(int);  //attribute
    entryLen += sizeof(int);  //keyLen
    int keyLen = 4;
    if(attr.type == TypeVarChar)
    {
        keyLen = *(int *)key;
        key = (char *)key + keyLen;
    }
    entryLen += keyLen;
    entryLen += sizeof(RID);
    return entryLen;
}

void prepareEntry(const Attribute &attr, const void *key, const RID &rid, char *output)
{
    //offset
    int offset = -1;
    memcpy(output, &offset, sizeof(int));
    output += sizeof(int);

    //attribute
    memcpy(output, &attr.type, sizeof(int));
    output += sizeof(int);

    //keyLen
    int keyLen = 4;
    if(attr.type == TypeVarChar)
    {
        keyLen = *(int *)key;
        key = (char *)key + sizeof(int);
    }
    memcpy(output, &keyLen, sizeof(int));
    output += sizeof(int);

    //key
    memcpy(output, key, keyLen);
    output += keyLen;

    //RID
    memcpy(output, &rid, sizeof(RID));
}

void recoverEntry(void *buffer, EntryMeta *entryMeta)
{
 
    char *pBuffer = (char *)buffer;

    entryMeta->offset = *(int *)pBuffer;
    pBuffer += sizeof(int);
    
    entryMeta->attrType = *(int *)pBuffer;
    pBuffer += sizeof(int);

    entryMeta->keyLen = *(int *)pBuffer;
    pBuffer += sizeof(int);

    memset(entryMeta->key, 0, PAGE_SIZE);
    if(entryMeta->attrType == TypeVarChar)
    {
        memcpy(entryMeta->key, &(entryMeta->keyLen), sizeof(int));
        memcpy((entryMeta->key) + sizeof(int), pBuffer, entryMeta->keyLen);
    }
    else
    {
        memcpy((entryMeta->key), pBuffer, entryMeta->keyLen);
    }
    
    pBuffer += entryMeta->keyLen;
 
    memcpy(&entryMeta->rid, pBuffer, sizeof(RID));
}


MetaPAGE::MetaPAGE()
{
    memset(buffer, 0, sizeof(PAGE_SIZE));
    pIxMeta = (IxMeta *)(buffer + PAGE_SIZE - sizeof(IxMeta));
}

MetaPAGE::~MetaPAGE()
{

}

void MetaPAGE::init(int N)
{
    memset(buffer, 0, sizeof(PAGE_SIZE));
    pIxMeta = (IxMeta *)(buffer + PAGE_SIZE - sizeof(IxMeta));
    pIxMeta->N = N;
    pIxMeta->next = 0;
    pIxMeta->level = 0;
}

void MetaPAGE::reset()
{
    init(pIxMeta->N);
}

void MetaPAGE::inc_next()
{
    pIxMeta->next += 1;
    if(pIxMeta->next == pIxMeta->N * pow(2, pIxMeta->level))
    {
        pIxMeta->next = 0;
        pIxMeta->level += 1;
    }
}

void MetaPAGE::des_next()
{
    pIxMeta->next -= 1;
    if(pIxMeta->next == -1)
    {
        pIxMeta->level -= 1;
        pIxMeta->next = pIxMeta->N * int(pow(2, pIxMeta->level)) - 1; 
    }
}

HashPAGE::HashPAGE()
{
    memset(buffer, 0, sizeof(PAGE_SIZE));
    pHashMeta = (HashMeta *)(buffer + PAGE_SIZE - sizeof(HashMeta));
    pHashSlotList = (HashSlot *)(char *)pHashMeta;
}

HashPAGE::~HashPAGE()
{

}

void HashPAGE::init(int isOverFlow, int selfHash)
{
    pHashMeta = (HashMeta *)(buffer + PAGE_SIZE - sizeof(HashMeta));
    pHashMeta->isOverFlow  = isOverFlow;
    pHashMeta->selfHash    = selfHash;
    pHashMeta->spaceOffset = 0;
    pHashMeta->next        = -1;
    
    pHashSlotList = (HashSlot *)(char *)pHashMeta;
    for(int i = 0; i < pHashMeta->selfHash; ++i)
    {
        HashSlot *slot = (HashSlot *)((char *)pHashSlotList - (i + 1) * sizeof(HashSlot));
        slot->offset = -1;
    }
}

void HashPAGE::reset()
{
    init(pHashMeta->isOverFlow, pHashMeta->selfHash);
}

unsigned HashPAGE::getFreeSpace()
{
    unsigned freeSpace = PAGE_SIZE;
    freeSpace -= sizeof(HashMeta);
    freeSpace -= pHashMeta->selfHash * sizeof(HashSlot);
    freeSpace -= pHashMeta->spaceOffset;
    return freeSpace;
}

bool HashPAGE::isEmpty()
{
    for(int i = 0; i < pHashMeta->selfHash; ++i)
    {
        HashSlot *slot = (HashSlot *)((char *)pHashSlotList - (i + 1) * sizeof(HashSlot));
        if(-1 != slot->offset) return false;
    }
    return true;
}

void HashPAGE::print(int &numOfEntries)
{
    int nextOffset = 0;
    numOfEntries = 0;
    EntryMeta entryMeta;
    for(int i = 0; i < pHashMeta->selfHash; ++i)
    {
        HashSlot *slot = (HashSlot *)(pHashSlotList - (i + 1));
        nextOffset = slot->offset;
        while(-1 != nextOffset)
        {
            numOfEntries ++;
            void *entry = buffer + nextOffset;
            recoverEntry(entry, &entryMeta);

            cout << "[";
            if(entryMeta.attrType == TypeInt)
                cout << *(int *)entryMeta.key << "/";
            if(entryMeta.attrType == TypeReal)
                cout << *(float *)entryMeta.key << "/";
            if(entryMeta.attrType == TypeVarChar)
                cout << entryMeta.key << "/";
            cout << entryMeta.rid.pageNum << ", " << entryMeta.rid.slotNum;
            cout << "]" << " ";

            nextOffset = entryMeta.offset;
        }
    }
}


IndexManager* IndexManager::_index_manager = 0;
MetaPAGE IndexManager::metaPage;
HashPAGE IndexManager::hashPage;


IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
    isLock = false;
    isDuringSplit = false;
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName, const unsigned &numberOfPages)
{
    string metaFileName = fileName + "_meta";
    string mainFileName = fileName + "_main";
    RC rc = createMetaFile(metaFileName, numberOfPages);
    if(0 != rc) return -1;
    rc = createMainFile(mainFileName, numberOfPages);
    if(0 != rc) return -1;

    return 0;
}


RC IndexManager::destroyFile(const string &fileName)
{
    string metaFileName = fileName + "_meta";
    string mainFileName = fileName + "_main";
    PagedFileManager *pfm = PagedFileManager::instance();
    RC rc = pfm->destroyFile(metaFileName.c_str());
    if(0 != rc) return -1;
    rc = pfm->destroyFile(mainFileName.c_str());
    if(0 != rc) return -1;
    return 0;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixFileHandle)
{
    string metaFileName = fileName + "_meta";
    string mainFileName = fileName + "_main";
    PagedFileManager *pfm = PagedFileManager::instance();
    RC rc = pfm->openFile(metaFileName.c_str(), ixFileHandle.metaFileHandle);
    if(0 != rc) return -1;
    rc = pfm->openFile(mainFileName.c_str(), ixFileHandle.mainFileHandle);
    if(0 != rc) return -1;

    rc = ixFileHandle.metaFileHandle.readPage(0, metaPage.getBuffer());
    if(0 != rc) return -1;
    //rc = ixFileHandle.mainFileHandle.readPage(0, hashPage.getBuffer());
    //if(0 != rc) return -1;


    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    RC rc = pfm->closeFile(ixfileHandle.metaFileHandle);
    if(0 != rc) return -1;
    rc = pfm->closeFile(ixfileHandle.mainFileHandle);
    if(0 != rc) return -1;
    return 0; 
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    int next = metaPage.getNext();
    unsigned hashVal = hash(attribute, key);
    unsigned bucketId = hashVal2BucketId(hashVal);

    int entryLen = getEntryLen(attribute, key);
    char entry[entryLen];
    prepareEntry(attribute, key, rid, entry);


    int pageNumber;
    findPageForInsert(ixfileHandle, bucketId, entryLen, pageNumber);
    //when finding a proper page, split may be triggered
    //and the bucketid may changed
    if(next != metaPage.getNext())
    {
        bucketId = hashVal2BucketId(hashVal);
        findPageForInsert(ixfileHandle, bucketId, entryLen, pageNumber);
    }

    RC rc = insertEntry(ixfileHandle, hashVal, entry, entryLen);
    if(0 != rc) return -1;

    writePage(ixfileHandle, pageNumber);
    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    unsigned hashVal = hash(attribute, key);
    unsigned bucketId = hashVal2BucketId(hashVal);
    ixfileHandle.mainFileHandle.readPage(bucketId, hashPage.getBuffer());

    unsigned prePageNum;
    getNumberOfAllPages(ixfileHandle, prePageNum);

    RC rc = -1;
    do
    {
        rc = deleteEntry(ixfileHandle, attribute, key, rid, hashVal);
        if(0 == rc)
        {
            ixfileHandle.mainFileHandle.writePage(bucketId, hashPage.getBuffer());
            break;
        }
        
        while(-1 != hashPage.pHashMeta->next)
        {
            int pageNumber = hashPage.pHashMeta->next;
            ixfileHandle.metaFileHandle.readPage(pageNumber, hashPage.getBuffer());
            rc = deleteEntry(ixfileHandle, attribute, key, rid, hashVal);
            if(0 == rc)
            {
                ixfileHandle.metaFileHandle.writePage(pageNumber, hashPage.getBuffer());
                break;
            }
        }
    }while(0);

    if(0 == rc)
    {
        steal(ixfileHandle, bucketId, prePageNum);
    }

    return rc;
}

RC IndexManager::reorganize(IXFileHandle &ixfileHandle)
{
    unsigned numOfPrimaryPages;
    getNumberOfPrimaryPages(ixfileHandle, numOfPrimaryPages);
    for(unsigned i = 0; i < numOfPrimaryPages; ++i)
    {
        reorganizeHashPages(ixfileHandle, i);
    }
    return 0;
}

RC IndexManager::steal(IXFileHandle &ixfileHandle, int primaryPageNumber, int prePageNum)
{
    if(!isLock)
    {
        isLock = true;
        unsigned aftPageNum;
        reorganizeHashPages(ixfileHandle, primaryPageNumber);
        getNumberOfAllPages(ixfileHandle, aftPageNum);
        for(unsigned i = 0; i < prePageNum - aftPageNum; ++i)
        {
            merge(ixfileHandle);
        } 
        isLock = false;
    }
    return 0;

}

RC IndexManager::merge(IXFileHandle &ixfileHandle)
{
    metaPage.des_next();
    ixfileHandle.metaFileHandle.writePage(0, metaPage.getBuffer());
    reorganizeHashPages(ixfileHandle, metaPage.getNext());
    reorganizeHashPages(ixfileHandle, metaPage.getNext() + metaPage.getN() * int(pow(2, metaPage.getLevel())));
    return 0;
}

unsigned IndexManager::hash(const Attribute &attribute, const void *key)
{
    int hashVal = 0;
    switch(attribute.type)
    {
    case TypeInt:
        hashVal = *(int *)(key); 
        return hashVal > 0 ? hashVal : -hashVal;
    case TypeReal:
        memcpy(&hashVal, key, sizeof(float)); 
        return hashVal > 0 ? hashVal : -hashVal;
    case TypeVarChar:
        int x = 0;
        int len = *(int *)key; 
        key = (char *)key + sizeof(int);
        for(int i = 0; i < len; ++i)
        {
            hashVal = (hashVal << 4) + (*(char *)key);
            if((x = hashVal & 0xF0000000L) != 0)
            {
                hashVal ^= (x >> 24);
            }
            hashVal &= ~x;
            key = (char *)key + sizeof(char);
        }
        return hashVal;
    }
    return 0;
}

RC IndexManager::printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber) 
{
    int numberOfEntires = 0;
    int totalNumberOFEntries = 0;
    HashPAGE hashPage;
    ixfileHandle.mainFileHandle.readPage(primaryPageNumber, hashPage.getBuffer());
    cout << "primary page No." << primaryPageNumber << endl;
    hashPage.print(numberOfEntires);
    if(0 != numberOfEntires) cout << endl;
    cout << "number of entries:" << numberOfEntires << endl;
    totalNumberOFEntries = numberOfEntires;
   
    int nextOverFlowPage = hashPage.pHashMeta->next;
    cout << "next over flow page:" << nextOverFlowPage << endl;
    while(-1 != nextOverFlowPage)
    {
        ixfileHandle.metaFileHandle.readPage(nextOverFlowPage, hashPage.getBuffer());
        cout << "overflow page No." << nextOverFlowPage << endl;
        hashPage.print(numberOfEntires);
        if(0 != numberOfEntires) cout << endl;
        cout << "number of entries:" << numberOfEntires << endl;
        totalNumberOFEntries += numberOfEntires; 
        nextOverFlowPage = hashPage.pHashMeta->next;
    }
    cout << "Number of total entries in the pages:" << totalNumberOFEntries << endl << endl;
    
    return 0;
}

RC IndexManager::getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages) 
{
    numberOfPrimaryPages = metaPage.getNext() + metaPage.getN() * int(pow(2, metaPage.getLevel()));
    return 0;
}

RC IndexManager::getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages) 
{
    numberOfAllPages = 0;
    unsigned numberOfPrimaryPages = 0;
    HashPAGE hp;
    for(unsigned i = 1; i < ixfileHandle.metaFileHandle.getNumberOfPages(); ++i)
    {
        ixfileHandle.metaFileHandle.readPage(i, hp.getBuffer());
        if(!hp.isEmpty()) numberOfAllPages++;
    }
    getNumberOfPrimaryPages(ixfileHandle, numberOfPrimaryPages); 

    numberOfAllPages += numberOfPrimaryPages;
    numberOfAllPages += 1;
    return 0;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool	    lowKeyInclusive,
    bool            highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
    return ix_ScanIterator.init(ixfileHandle, attribute
        , lowKey, highKey, lowKeyInclusive, highKeyInclusive);
}

RC IndexManager::createMetaFile(const string  &fileName, const unsigned &numberOfPages)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    RC rc = pfm->createFile(fileName.c_str());
    if(0 != rc) return -1;

    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    if(0 != rc) return -1;

    metaPage.init(numberOfPages);
    rc = fileHandle.appendPage(metaPage.getBuffer());
    if(0 != rc) return -1;

    rc = pfm->closeFile(fileHandle);
    if(0 != rc) return -1;

    return 0;
}


RC IndexManager::createMainFile(const string &fileName, const unsigned &numberOfPages)
{
    PagedFileManager *pfm = PagedFileManager::instance();

    RC rc = pfm->createFile(fileName.c_str());
    if(0 != rc) return -1;

    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    if(0 != rc) return -1;

    hashPage.init();
    for(unsigned i = 0; i < numberOfPages; ++i)
    {
        rc = fileHandle.appendPage(hashPage.getBuffer());
        if(0 != rc) break;
    }
    if(0 != rc) return -1;

    rc = pfm->closeFile(fileHandle);
    if(0 != rc) return -1;

    return 0;
}

unsigned IndexManager::hashVal2BucketId(const unsigned hashVal)
{
    int bucketId = hashVal % unsigned(pow(2, metaPage.getLevel()) * metaPage.getN());
    if (bucketId < metaPage.getNext())
    {
        bucketId = hashVal % unsigned(pow(2, metaPage.getLevel() + 1) * metaPage.getN());
    }
    return bucketId;
}

RC IndexManager::findPageForInsert(IXFileHandle &ixfileHandle, int bucketId, int entryLen, int &pageNumber)
{
    pageNumber = bucketId;
    ixfileHandle.mainFileHandle.readPage(bucketId, hashPage.getBuffer());
    if(hashPage.getFreeSpace() > (unsigned)entryLen) return 0;

    while(-1 != hashPage.pHashMeta->next)
    {
        pageNumber = hashPage.pHashMeta->next;
        ixfileHandle.metaFileHandle.readPage(pageNumber, hashPage.getBuffer());
        if(hashPage.getFreeSpace() > (unsigned)entryLen) return 0;
    }

    int emptyPageNumber = findEmptyPage(ixfileHandle);
    if(0 < emptyPageNumber)
    {
        hashPage.pHashMeta->next = emptyPageNumber;
        writePage(ixfileHandle, pageNumber);
        ixfileHandle.metaFileHandle.readPage(emptyPageNumber, hashPage.getBuffer());
        pageNumber = emptyPageNumber;
    }
    else if(-1 == emptyPageNumber)
    {
        emptyPageNumber = ixfileHandle.metaFileHandle.getNumberOfPages(); 
        hashPage.pHashMeta->next = emptyPageNumber;
        writePage(ixfileHandle, pageNumber);
        hashPage.init(true);
        ixfileHandle.metaFileHandle.appendPage(hashPage.getBuffer());
        ixfileHandle.metaFileHandle.readPage(emptyPageNumber, hashPage.getBuffer());
        pageNumber = emptyPageNumber;
        /* --------------------------------------------------------*/
    }
    split(ixfileHandle);
    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attr, const void *key, const RID &rid, int hashVal)
{
    int entryLen = getEntryLen(attr, key);
    int slotNumber = hashVal % hashPage.pHashMeta->selfHash;
    HashSlot *slot = (HashSlot *)(hashPage.pHashSlotList - (slotNumber + 1));
    if(-1 == slot->offset) return -1;

    EntryMeta entryMeta;
    void *entry1 = hashPage.getBuffer() + slot->offset;
    recoverEntry(entry1, &entryMeta);
    if(memcmp(key, entryMeta.key, entryMeta.keyLen) == 0 && memcmp(&rid, &entryMeta.rid, sizeof(RID)) == 0)
    {
        slot->offset = entryMeta.offset;
        memset(entry1, 0, entryLen);
        return 0;
    }

    while(-1 != entryMeta.offset)
    {
        void *entry2 = hashPage.getBuffer() + entryMeta.offset;
        recoverEntry(entry2, &entryMeta);
        if(memcmp(key, entryMeta.key, entryMeta.keyLen) == 0 && memcmp(&rid, &entryMeta.rid, sizeof(RID)) == 0)
        {
            memcpy(entry1, &entryMeta.offset, sizeof(int));
            memset(entry2, 0, entryLen);
            return 0;
        }
        entry1 = entry2; 
    }
    return -1;
}

RC IndexManager::writePage(IXFileHandle &ixfileHandle, int pageNumber)
{
    if(hashPage.pHashMeta->isOverFlow)
    {
        ixfileHandle.metaFileHandle.writePage(pageNumber, hashPage.getBuffer());
    }
    else
    {
        ixfileHandle.mainFileHandle.writePage(pageNumber, hashPage.getBuffer());
    }
    return 0;
}

RC IndexManager::split(IXFileHandle &ixfileHandle)
{
    if(isLock) return 0;
    isLock = true;

    HashPAGE hp;
    hp.init(false);
    
    unsigned numOfPrimaryPages; 
    getNumberOfPrimaryPages(ixfileHandle, numOfPrimaryPages); 
    if(ixfileHandle.mainFileHandle.getNumberOfPages() <= numOfPrimaryPages)
    {
        ixfileHandle.mainFileHandle.appendPage(hp.getBuffer());
    }
    
    int primaryPageNumber = metaPage.getNext();

    metaPage.inc_next();
    ixfileHandle.metaFileHandle.writePage(0, metaPage.getBuffer());
    
    RC rc = reorganizeHashPages(ixfileHandle, primaryPageNumber);
    if(0 != rc) return -1;
    
    isLock = false; 
    return 0;
}

RC IndexManager::reorganizeHashPages(IXFileHandle &ixfileHandle, int primaryPageNumber)
{

    vector<HashPAGE> hashPageArray;
    HashPAGE hp1, hp2;


    ixfileHandle.mainFileHandle.readPage(primaryPageNumber, hp1.getBuffer()); 
    int nextOverFlowPage = hp1.pHashMeta->next;
    hashPageArray.push_back(hp1);

    hp2.init(false);
    ixfileHandle.mainFileHandle.writePage(primaryPageNumber, hp2.getBuffer());

    while(-1 != nextOverFlowPage)
    {
        ixfileHandle.metaFileHandle.readPage(nextOverFlowPage, hp1.getBuffer());
        hashPageArray.push_back(hp1);
        hp2.init(true);
        ixfileHandle.metaFileHandle.writePage(nextOverFlowPage, hp2.getBuffer());
        nextOverFlowPage = hp1.pHashMeta->next;
    }

    HashPAGE hp;
    EntryMeta entryMeta;
    int nextOffset;
    Attribute attr;
    for(unsigned i = 0; i < hashPageArray.size(); ++i)
    {
        memcpy(hp.getBuffer(), hashPageArray[i].getBuffer(), PAGE_SIZE); 
        
        for(int j = 0; j < hp.pHashMeta->selfHash; ++j)
        {
            HashSlot *slot = (HashSlot *)(hp.pHashSlotList - (j + 1));
            if(-1 == slot->offset) continue;
            nextOffset = slot->offset;
            while(-1 != nextOffset)
            {
                void *buffer = hp.getBuffer() + nextOffset;
                recoverEntry(buffer, &entryMeta);
                attr.type = (AttrType)entryMeta.attrType;
                insertEntry(ixfileHandle, attr, entryMeta.key, entryMeta.rid);
                nextOffset = entryMeta.offset;
            }
        }
    }
    return 0;
}

RC IndexManager::findEmptyPage(IXFileHandle &ixfileHandle)
{
    HashPAGE hp;
    for(unsigned i = 1; i < ixfileHandle.metaFileHandle.getNumberOfPages(); ++i)
    {
        ixfileHandle.metaFileHandle.readPage(i, hp.getBuffer());
        if(hp.isEmpty()) return i; 
    }
    return -1;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, int hashVal, const void *entry, int entryLen)
{
    unsigned slotId = hashVal % hashPage.pHashMeta->selfHash;
    HashSlot *slot = (HashSlot *)(hashPage.pHashSlotList - (slotId + 1));
    
    if(-1 == slot->offset)
    {
        slot->offset = hashPage.pHashMeta->spaceOffset;
        memcpy(hashPage.getBuffer() + hashPage.pHashMeta->spaceOffset, entry, entryLen);
        hashPage.pHashMeta->spaceOffset += entryLen;
        return 0;
    }

    int nextOffset = slot->offset;
    EntryMeta entryMeta;
    void *tmp = NULL;
    while(-1 != nextOffset)
    {
        tmp = hashPage.getBuffer() + nextOffset;
        if(memcmp(tmp, entry, entryLen) == 0) return -1;
        recoverEntry(tmp, &entryMeta);
        nextOffset = entryMeta.offset; 
    }
    memcpy(tmp, &hashPage.pHashMeta->spaceOffset, sizeof(int));
    memcpy(hashPage.getBuffer() + hashPage.pHashMeta->spaceOffset, entry, entryLen);
    hashPage.pHashMeta->spaceOffset += entryLen;

    return 0;
}

RC IndexManager::getLock()
{
    if(!isLock)
    {
        isLock = true;
        return 0;
    }
    return -1; 
}

RC IndexManager::releaseLock()
{
    if(isLock)
    {
        isLock = false;
        return 0;
    }
    return -1;
}


IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::init(IXFileHandle &ixfileHandle, Attribute attribute
        , const void *lowKey, const void *highKey
        , bool lowKeyInclusive, bool highKeyInclusive)
{
    cfg.ixfileHandle = &ixfileHandle;
    cfg.attribute = attribute;
    cfg.lowKey = lowKey;
    cfg.highKey = highKey;
    cfg.lowKeyInclusive = lowKeyInclusive;
    cfg.highKeyInclusive = highKeyInclusive;
    ctx.isOverFlow = false;
    ctx.pageNumber = 0;
    ctx.primaryPageNumber = 0;
    ctx.slotNumber = 0;
    ctx.offset = 0;
    
    IndexManager *indexManager = IndexManager::instance();
    indexManager->getLock();

    if(isExtractMatch())
    {
        unsigned hashVal = indexManager->hash(attribute, lowKey);
        unsigned bucketId = indexManager->hashVal2BucketId(hashVal);
        ixfileHandle.mainFileHandle.readPage(bucketId, scanHashPage.getBuffer());
        ctx.slotNumber = hashVal % scanHashPage.pHashMeta->selfHash;
    }
    else
    {
        ixfileHandle.mainFileHandle.readPage(0, scanHashPage.getBuffer());
    }
    return 0;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    if(isExtractMatch())
    {
        return IX_ScanExactMatch(rid, key);
    }
    return IX_ScanRangeSearch(rid, key);
}

RC IX_ScanIterator::IX_ScanExactMatch(RID &rid, void *key)
{
    while(true)
    {
        HashSlot *slot = (HashSlot *)(scanHashPage.pHashSlotList - (ctx.slotNumber + 1));
        if(-1 == slot->offset || -1 == ctx.offset)
        {
            int nextPage = scanHashPage.pHashMeta->next; 
            if(-1 == scanHashPage.pHashMeta->next) return -1;
            cfg.ixfileHandle->metaFileHandle.readPage(nextPage, scanHashPage.getBuffer());
            ctx.offset = 0;
            continue;
        }
        
        EntryMeta entryMeta;
        ctx.offset = (ctx.offset == 0) ? slot->offset : ctx.offset;
        void *entry = scanHashPage.getBuffer() + ctx.offset;
        recoverEntry(entry, &entryMeta);
        ctx.offset = entryMeta.offset;

        if(entryMeta.attrType == TypeVarChar)
        {
            if(*(int *)cfg.lowKey != entryMeta.keyLen || *(int *)entryMeta.key != entryMeta.keyLen) continue;
            if(strncmp((char *)cfg.lowKey + 4, (char *)entryMeta.key + 4, entryMeta.keyLen) == 0)
            {
                memcpy(key, entryMeta.key, entryMeta.keyLen);
                memcpy(&rid, &entryMeta.rid, sizeof(RID));
                return 0;
            }
        }
        else if(entryMeta.attrType == TypeReal)
        {
            if(*(float *)cfg.lowKey == *(float *)entryMeta.key) 
            {
                memcpy(key, entryMeta.key, entryMeta.keyLen);
                memcpy(&rid, &entryMeta.rid, sizeof(RID));
                return 0;
            }
        }
        else
        {
            if((*(int *)cfg.lowKey) == (*(int *)entryMeta.key)) 
            {
                memcpy(key, entryMeta.key, entryMeta.keyLen);
                memcpy(&rid, &entryMeta.rid, sizeof(RID));
                return 0;
            }
        }
    }
    return 0;
}



RC IX_ScanIterator::IX_ScanRangeSearch(RID &rid, void *key)
{
    EntryMeta entryMeta;   
    while(true)
    {
        RC rc = readNextPage();
        if(0 != rc) return -1;

        HashSlot *slot = (HashSlot *)(scanHashPage.pHashSlotList - (ctx.slotNumber + 1));
        if(-1 == slot->offset || -1 == ctx.offset) 
        {
            ctx.slotNumber += 1;
            ctx.offset = 0;
            continue;
        }

        ctx.offset = (ctx.offset == 0) ? slot->offset : ctx.offset;
        void *entry = scanHashPage.getBuffer() + ctx.offset;
        recoverEntry(entry, &entryMeta);
        ctx.offset = entryMeta.offset;
        if(meetCondition(entryMeta.key, entryMeta.keyLen))
        {
            memcpy(&rid, &entryMeta.rid, sizeof(RID));
            memcpy(key, entryMeta.key, entryMeta.keyLen);
            return 0;
        }
    }
    return -1;
}

RC IX_ScanIterator::readNextPage()
{
    if(ctx.slotNumber == scanHashPage.pHashMeta->selfHash)
    {
        if(-1 != scanHashPage.pHashMeta->next)
        {
            int pageNumber = scanHashPage.pHashMeta->next;
            cfg.ixfileHandle->metaFileHandle.readPage(pageNumber, scanHashPage.getBuffer());
            ctx.isOverFlow = true;
            ctx.pageNumber = pageNumber;
            ctx.slotNumber = 0;
            ctx.offset = 0;
        }
        else if(unsigned(ctx.primaryPageNumber) == cfg.ixfileHandle->mainFileHandle.getNumberOfPages() - 1)
        {
            return -1;
        }
        else
        {
            ctx.primaryPageNumber += 1;
            cfg.ixfileHandle->mainFileHandle.readPage(ctx.primaryPageNumber, scanHashPage.getBuffer());
            ctx.isOverFlow = false;
            ctx.slotNumber = 0;
            ctx.offset = 0;
        }
    }        
    return 0;
}

RC IX_ScanIterator::close()
{
    IndexManager *indexManager = IndexManager::instance();
    indexManager->reorganize(*cfg.ixfileHandle);
    indexManager->releaseLock();
    return 0;
}


IXFileHandle::IXFileHandle()
{
}

IXFileHandle::~IXFileHandle()
{
}

bool IX_ScanIterator::isExtractMatch()
{
    if(cfg.lowKey == NULL || cfg.highKey == NULL) return false;
    if(!cfg.lowKeyInclusive || !cfg.highKeyInclusive) return false;
    if(cfg.attribute.type == TypeVarChar && strcmp((char*)cfg.lowKey, (char*)cfg.highKey) == 0) return true;
    if(cfg.attribute.type == TypeInt && *(int *)cfg.lowKey == *(int *)cfg.highKey) return true;
    if(cfg.attribute.type == TypeReal && *(float *)cfg.lowKey == *(float *)cfg.highKey) return true;
    return false;
}

bool IX_ScanIterator::meetCondition(void *key, int len)
{
    if(meetLowKeyCondition(key, len) && meetHighKeyCondition(key, len))
        return true;
    return false;
}

bool IX_ScanIterator::meetLowKeyCondition(void *key, int len)
{
    if(cfg.lowKey == NULL) return true;
    if(cfg.attribute.type == TypeVarChar && cfg.lowKeyInclusive)
    {
        if(*(int *)key == *(int *)cfg.lowKey)
        {
            return strncmp((char *)key + 4, (char *)cfg.lowKey + 4, *(int *)key) >= 0;
        }
        return *(int *)key > *(int *)cfg.lowKey;
    }
    if(cfg.attribute.type == TypeVarChar && !cfg.lowKeyInclusive)
    {
        if(*(int *)key == *(int *)cfg.lowKey)
        {
            return strncmp((char *)key + 4, (char *)cfg.lowKey + 4, *(int *)key) > 0;
        }
        return *(int *)key > *(int *)cfg.lowKey;
    }
    if(cfg.attribute.type == TypeInt && cfg.lowKeyInclusive)
    {
        return *(int *)key >= *(int *)cfg.lowKey;
    }
    if(cfg.attribute.type == TypeInt && !cfg.lowKeyInclusive)
    {
        return *(int *)key > *(int *)cfg.lowKey;
    }
    if(cfg.attribute.type == TypeReal && cfg.lowKeyInclusive)
    {
        return *(float *)key >= *(float *)cfg.lowKey;
    }
    if(cfg.attribute.type == TypeReal && !cfg.lowKeyInclusive)
    {
        return *(float *)key > *(float *)cfg.lowKey;
    }
    return false;
}

bool IX_ScanIterator::meetHighKeyCondition(void *key, int len)
{
    if(cfg.highKey == NULL) return true;
    if(cfg.attribute.type == TypeVarChar && cfg.highKeyInclusive)
    {
        if(*(int *)key == *(int *)cfg.highKey)
        {
            return strncmp((char *)key + 4, (char *)cfg.highKey + 4, *(int *)key) <= 0;
        }
        return *(int *)key < *(int *)cfg.highKey;
    }
    if(cfg.attribute.type == TypeVarChar && !cfg.highKeyInclusive)
    {
        if(*(int *)key == *(int *)cfg.highKey)
        {
            return strncmp((char *)key + 4, (char *)cfg.highKey + 4, *(int *)key) < 0;
        }
        return *(int *)key < *(int *)cfg.highKey;
    }
    if(cfg.attribute.type == TypeInt && cfg.highKeyInclusive)
    {
        return *(int *)key <= *(int *)cfg.highKey;
    }
    if(cfg.attribute.type == TypeInt && !cfg.highKeyInclusive)
    {
        return *(int *)key < *(int *)cfg.highKey;
    }
    if(cfg.attribute.type == TypeReal && cfg.highKeyInclusive)
    {
        return *(float *)key <= *(float *)cfg.highKey;
    }
    if(cfg.attribute.type == TypeReal && !cfg.highKeyInclusive)
    {
        return *(float *)key < *(float *)cfg.highKey;
    }
    return false;
}



RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    unsigned metaFileReadPageCount, metaFileWritePageCount, metaFileAppendPageCount;
    unsigned mainFileReadPageCount, mainFileWritePageCount, mainFileAppendPageCount;
    metaFileHandle.collectCounterValues(metaFileReadPageCount, metaFileWritePageCount, metaFileAppendPageCount);
    mainFileHandle.collectCounterValues(mainFileReadPageCount, mainFileWritePageCount, mainFileAppendPageCount);
    readPageCount   = metaFileReadPageCount + mainFileReadPageCount;
    writePageCount  = metaFileWritePageCount + mainFileWritePageCount;
    appendPageCount = metaFileAppendPageCount + mainFileAppendPageCount;
    readPageCounter = readPageCount;
    writePageCounter = writePageCount;
    appendPageCounter = appendPageCount;
    return 0;
}

void IX_PrintError (RC rc)
{
}
