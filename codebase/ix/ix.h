#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"
#include "../rbf/pfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

struct EntryMeta
{
    int offset;     
    int attrType;
    int keyLen;
    char  key[PAGE_SIZE];
    RID   rid;
};


unsigned getEntryLen(const Attribute &attr, const void *key);
void prepareEntry(const Attribute &attr, const void *key, const RID &rid, void *output);
void recoverEntry(void *buffer, EntryMeta *entryMeta); 


class MetaPAGE
{
public:
    struct IxMeta
    {
        int N;
        int next;
        int level;
    };
public:
    MetaPAGE();
    ~MetaPAGE();
public:
    void init(int N);
    void reset();
    void inc_next();
    void des_next();
    inline char *getBuffer() {return buffer;}
    inline int  getN()       {return pIxMeta->N;}
    inline int  getNext()    {return pIxMeta->next;}
    inline int  getLevel()   {return pIxMeta->level;}
private:
    char buffer[PAGE_SIZE];
    IxMeta *pIxMeta;
};

struct HashSlot
{
    int offset;
};

class HashPAGE
{
public:
    struct HashMeta
    {
        bool isOverFlow; 
        int  selfHash;
        int  spaceOffset;
        int  next;  //next overflow page id
    };
    
    public:
    HashPAGE();
    ~HashPAGE();
public:
    void init(int isOverFlow = false, int selfHash = 64);
    void reset();
    bool isEmpty();
    unsigned getFreeSpace();
    void print(int &numOfEntries);
    inline char *getBuffer(){return buffer;}
    inline bool getIsOverFlow(){return pHashMeta->isOverFlow;}
    inline int  getNext(){return pHashMeta->next;}
public:
    HashMeta *pHashMeta;
    HashSlot *pHashSlotList;
private:
    char buffer[PAGE_SIZE];
};


class IndexManager {
 public:
  static IndexManager* instance();

  // Create index file(s) to manage an index
  RC createFile(const string &fileName, const unsigned &numberOfPages);

  // Delete index file(s)
  RC destroyFile(const string &fileName);

  // Open an index and returns an IXFileHandle
  RC openFile(const string &fileName, IXFileHandle &ixFileHandle);

  // Close an IXFileHandle. 
  RC closeFile(IXFileHandle &ixfileHandle);


  // The following functions  are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For INT and REAL: use 4 bytes to store the value;
  //     For VarChar: use 4 bytes to store the length of characters, then store the actual characters.

  // Insert an entry to the given index that is indicated by the given IXFileHandle
  RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // Delete an entry from the given index that is indicated by the given IXFileHandle
  RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  RC reorganize(IXFileHandle &ixfileHandle);

  // scan() returns an iterator to allow the caller to go through the results
  // one by one in the range(lowKey, highKey).
  // For the format of "lowKey" and "highKey", please see insertEntry()
  // If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
  // should be included in the scan
  // If lowKey is null, then the range is -infinity to highKey
  // If highKey is null, then the range is lowKey to +infinity
  
  // Initialize and IX_ScanIterator to supports a range search
  RC scan(IXFileHandle &ixfileHandle,
      const Attribute &attribute,
	  const void        *lowKey,
      const void        *highKey,
      bool        lowKeyInclusive,
      bool        highKeyInclusive,
      IX_ScanIterator &ix_ScanIterator);

  // Generate and return the hash value (unsigned) for the given key
  unsigned hash(const Attribute &attribute, const void *key);
  unsigned hashVal2BucketId(const unsigned hashVal);
  
  
  // Print all index entries in a primary page including associated overflow pages
  // Format should be:
  // Number of total entries in the page (+ overflow pages) : ?? 
  // primary Page No.??
  // # of entries : ??
  // entries: [xx] [xx] [xx] [xx] [xx] [xx]
  // overflow Page No.?? liked to [primary | overflow] page No.??
  // # of entries : ??
  // entries: [xx] [xx] [xx] [xx] [xx]
  // where [xx] shows each entry.
  RC printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber);
  
  // Get the number of primary pages
  RC getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages);

  // Get the number of all pages (primary + overflow)
  RC getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages);

  RC getLock();
  RC releaseLock();

  
 protected:
  IndexManager   ();                            // Constructor
  ~IndexManager  ();                            // Destructor

 private:
  RC createMetaFile(const string &fileName, const unsigned &numberOfPages);
  RC createMainFile(const string &fileName, const unsigned &numberOfPages);

  RC writePage(IXFileHandle &ixfileHandle, int pageNumber);
  RC findEmptyPage(IXFileHandle &ixfileHandle);
  RC findPageForInsert(IXFileHandle &ixfileHandle, int bucketId, int entryLen, int &pageNumber);
  RC split(IXFileHandle &ixfileHandle);
  RC steal(IXFileHandle &ixfileHandle, int primaryPageNumber, int prePageNum);
  RC merge(IXFileHandle &ixfileHandle);
  RC reorganizeHashPages(IXFileHandle &ixfileHandle, int primaryPageNumber);
  RC insertEntry(IXFileHandle &ixfileHandle, int hashVal, const void *entry, int entryLen);
  RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attr, const void *key, const RID &rid, int hashVal);
    


 private:
  static IndexManager *_index_manager;
  static MetaPAGE metaPage;
  static HashPAGE hashPage;
  bool isLock;
  bool isDuringSplit;
};


class IX_ScanIterator {
public:
  struct IX_SCAN_CONFIG
  {
      IXFileHandle *ixfileHandle;
      Attribute    attribute;
      const void   *lowKey;
      const void   *highKey;
      bool lowKeyInclusive;
      bool highKeyInclusive;
  };
  struct IX_SCAN_CONTEXT
  {
      bool isOverFlow;
      int primaryPageNumber;
      int pageNumber;
      int slotNumber;
      int offset;
  };

  IX_ScanIterator();  							// Constructor
  ~IX_ScanIterator(); 							// Destructor
  RC init(IXFileHandle &ixfileHandle, Attribute attribute
          , const void *lowKey, const void *highKey
          , bool lowKeyInclusive, bool highKeyInclusive);

  RC getNextEntry(RID &rid, void *key);  		                // Get next matching entry
  RC close();             						// Terminate index scan
private:
  RC readNextPage();
  bool meetCondition(void *key, int len);
  bool meetLowKeyCondition(void *key, int len);
  bool meetHighKeyCondition(void *key, int len);
  bool isExtractMatch();
  RC IX_ScanRangeSearch(RID &rid, void *key);
  RC IX_ScanExactMatch(RID &rid, void *key);
private:
  IX_SCAN_CONFIG cfg;
  IX_SCAN_CONTEXT ctx;
  HashPAGE scanHashPage;
};


class IXFileHandle {
public:
	// Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
 
    FileHandle metaFileHandle;
    FileHandle mainFileHandle;

    IXFileHandle();  							// Constructor
    ~IXFileHandle(); 							// Destructor
    
private:
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
