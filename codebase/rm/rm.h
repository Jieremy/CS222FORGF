#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <map>
#include <sys/stat.h>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator
// RM_ScanIterator is an iteratr to go through tuples
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

class RM_ScanIterator {
public:
	RM_ScanIterator();
	~RM_ScanIterator();

	// "data" follows the same format as RelationManager::insertTuple()
	RC initialize(const vector<Attribute> &recordDescriptor,
			const CompOp compOp, const void *value,
			const vector<string> &attributeNames,
			const string &conditionAttribute);
	RC getNextTuple(RID &rid, void *data);
	RC close();

public:
	FileHandle fileHandle;
private:
	RBFM_ScanIterator _rbfm_scanner;
	RecordBasedFileManager *_rbfm;
};

enum TABLE_TYPE {
	TABLE_TYPE_SYSTEM = 1, TABLE_TYPE_USER = 2,
};

class RM_IndexScanIterator {
public:
  RM_IndexScanIterator();  	// Constructor
  ~RM_IndexScanIterator(){}; 	// Destructor

  // "key" follows the same format as in IndexManager::insertEntry()
  RC initialize(IXFileHandle &ixfileHandle,
		    const Attribute &attribute,
		    const void      *lowKey,
		    const void      *highKey,
		    bool	    lowKeyInclusive,
		    bool            highKeyInclusive);
  RC getNextEntry(RID &rid, void *key);  	// Get next matching entry
  RC close();             			// Terminate index scan
public:
  IXFileHandle ixfileHandle;
  IX_ScanIterator _ix_scanner;
private:
  IndexManager *_im;
};

struct TABLE_META_DATA {
	int Id;
	string tableName;
	string fileName;
	TABLE_TYPE tableType;
};

struct COLUMN_META_DATA {
	int tableId;
	string columnName;
	int columnType;
	int columnLength;
	int columnPosition;
};

// Relation Manager
class RelationManager {
public:
	static RelationManager* instance();

	RC createTable(const string &tableName, const vector<Attribute> &attrs);

	RC deleteTable(const string &tableName);

	RC getAttributes(const string &tableName, vector<Attribute> &attrs);

	RC insertTuple(const string &tableName, const void *data, RID &rid);

	RC deleteTuples(const string &tableName);

	RC deleteTuple(const string &tableName, const RID &rid);

	// Assume the rid does not change after update
	RC updateTuple(const string &tableName, const void *data, const RID &rid);

	RC readTuple(const string &tableName, const RID &rid, void *data);

	RC readAttribute(const string &tableName, const RID &rid,
			const string &attributeName, void *data);

	RC reorganizePage(const string &tableName, const unsigned pageNumber);

	// scan returns an iterator to allow the caller to go through the results one by one.
	RC scan(const string &tableName, const string &conditionAttribute,
			const CompOp compOp, // comparision type such as "<" and "="
			const void *value, // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RM_ScanIterator &rm_ScanIterator);
	RC createIndex(const string &tableName, const string &attributeName);

	RC destroyIndex(const string &tableName, const string &attributeName);

	// indexScan returns an iterator to allow the caller to go through qualified entries in index
	RC indexScan(const string &tableName, const string &attributeName,
			const void *lowKey, const void *highKey, bool lowKeyInclusive,
			bool highKeyInclusive, RM_IndexScanIterator &rm_IndexScanIterator);

// Extra credit
public:
	RC dropAttribute(const string &tableName, const string &attributeName);

	RC addAttribute(const string &tableName, const Attribute &attr);

	RC reorganizeTable(const string &tableName);

protected:
	RelationManager();
	~RelationManager();

private:

	RecordBasedFileManager *_rbfm;
	IndexManager *_im;
	vector<Attribute> tableAttr;
	vector<Attribute> columnAttr;
	map<string, map<int, RID> *> tableCache;
	map<int, vector<RID> *> columnCache;
	static int tableId;
private:
	static RelationManager *_rm;

	/*----------------------------------------------*/
private:
	void init();
	void initCatalog();
	void loadCatalog();
	void loadTablesCatalog();
	void loadColumnCatalog();

	bool fileExist(const string &fileName);
	bool tableExist(const string &tableName);
	bool isSystemTable(const string &tableName);

	void setTableAttr();
	void setColumnAttr();

	void insertTableCache(const string &tableName, int tableId, const RID &rid);
	void insertColumnCache(int tableId, const RID &rid);

	RC insertTableCatalog(TABLE_META_DATA data);
	RC insertColumnCatalog(const vector<Attribute> &attrs);

	RC createTable(const string &tableName, vector<Attribute> attrs,
			TABLE_TYPE tableType);
	void appendData(char *recordBuffer, const void *field,
			const int fieldLength, AttrType attrType, int &offset);

	int computeRecordMaxMemory(const vector<Attribute> &attrs);

};

#endif
