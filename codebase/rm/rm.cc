
#include "rm.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <vector>


RelationManager* RelationManager::_rm = 0;
int RelationManager::tableId = 1;


RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
    _rbfm = RecordBasedFileManager::instance();
    _im = IndexManager::instance();
    init();

}

void RelationManager::init()
{
    setTableAttr();
    setColumnAttr();
    return fileExist("Tables")? loadCatalog() : initCatalog();
}

void RelationManager::initCatalog()
{
    _rbfm->createFile("Tables");
    _rbfm->createFile("Columns");
    createTable("Tables", tableAttr, TABLE_TYPE_SYSTEM);
    createTable("Columns", columnAttr, TABLE_TYPE_SYSTEM);
}

void RelationManager::loadTablesCatalog()
{
    vector<string> attributeNames;
    attributeNames.push_back(tableAttr[0].name); //tableId
    attributeNames.push_back(tableAttr[1].name); //tableName

    RM_ScanIterator rmsi;
    scan("Tables", tableAttr[0].name, NO_OP, NULL, attributeNames, rmsi);

    static char *buffer = (char *)malloc(PAGE_SIZE);
    char *headOfBuffer = buffer;
    RID rid;

    while(rmsi.getNextTuple(rid, buffer) != RM_EOF)
    {
        int tableId = *(int *)buffer;
        buffer += sizeof(int);

        int tableNameLength = *(int *)buffer;
        buffer += sizeof(int);

        string tableName = string(buffer, tableNameLength);
        buffer += tableNameLength; 

        insertTableCache(tableName, tableId, rid);
        buffer = headOfBuffer;
        RelationManager::tableId = tableId + 1;
    }
    rmsi.close();
}

void RelationManager::insertTableCache(const string &tableName, int tableId, const RID &rid)
{
    map<int, RID> *tableId2RID = new map<int, RID>;
    (*tableId2RID)[tableId] = rid;
    tableCache[tableName] = tableId2RID;
}

void RelationManager::loadColumnCatalog()
{
    vector<string> attributeNames;
    attributeNames.push_back(columnAttr[0].name); //tableId
    attributeNames.push_back(columnAttr[3].name); //columnPosition
    
    RM_ScanIterator rmsi;
    scan("Columns", columnAttr[0].name, NO_OP, NULL, attributeNames, rmsi);

    static char *buffer = (char *)malloc(PAGE_SIZE);
    char *headOfBuffer = buffer;
    RID rid;

    while(rmsi.getNextTuple(rid, buffer) != RM_EOF)
    {
        int tableId = *(int *)buffer;
        buffer += sizeof(int);

        int columnPosition = *(int *)buffer;
        insertColumnCache(tableId, rid);
        buffer = headOfBuffer;
    }
    rmsi.close();
}

void RelationManager::insertColumnCache(int tableId, const RID &rid)
{
    if(columnCache.find(tableId) != columnCache.end())
    {
        (*columnCache[tableId]).push_back(rid);
    }
    else
    {
        vector<RID> *ridVec =  new vector<RID>;
        (*ridVec).push_back(rid);
        columnCache[tableId] = ridVec;
    }
}

void RelationManager::loadCatalog()
{
    loadTablesCatalog();
    loadColumnCatalog();
}


RelationManager::~RelationManager()
{
    for(map<string, map<int, RID> *>::iterator it = tableCache.begin(); it != tableCache.end(); ++it) {
    	delete it->second;
    }

    for(map<int, vector<RID> *>::iterator it = columnCache.begin(); it != columnCache.end(); ++it) {
    	delete it->second;
    }
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    string fileName = tableName;
    if(fileExist(fileName)) return -1;
    if(isSystemTable(tableName)) return -1;
    return createTable(tableName, attrs, TABLE_TYPE_USER);
}

RC RelationManager::createTable(const string &tableName, vector<Attribute> attrs, TABLE_TYPE tableType)
{
    string fileName = tableName;
    int ret = 0;

    if(tableType == TABLE_TYPE_USER)
    {
        ret = _rbfm->createFile(fileName.c_str());
        if (0 != ret) return -1; 
    }
    
    TABLE_META_DATA table_meta;
    table_meta.tableName = tableName;
    table_meta.fileName = fileName;
    table_meta.tableType = tableType;

    ret = insertTableCatalog(table_meta);
    if (0 != ret) return -1;

    ret = insertColumnCatalog(attrs); 
    if (0 != ret) return -1;
    
    RelationManager::tableId += 1;
    
    return 0;
}

RC RelationManager::insertTableCatalog(TABLE_META_DATA data)
{
    FileHandle fileHandle;
    int ret = _rbfm->openFile("Tables", fileHandle);
    if (0 != ret) return -1;

    int bufferLength = computeRecordMaxMemory(tableAttr);
    static char *buffer = (char *)malloc(bufferLength);
    int offset = 0;
    int tableId = RelationManager::tableId;
    string tableName = data.tableName;
    string fileName = data.fileName;
    int tableType = data.tableType;

    appendData(buffer, &tableId, sizeof(int), tableAttr[0].type, offset);
    appendData(buffer, tableName.c_str(), (int)tableName.size(), tableAttr[1].type, offset);
    appendData(buffer, fileName.c_str(), (int)fileName.size(), tableAttr[2].type, offset);
    appendData(buffer, &tableType, sizeof(int), tableAttr[3].type, offset);
    
    RID rid;
    ret = _rbfm->insertRecord(fileHandle, tableAttr, buffer, rid);
    if (0 != ret) 
    {
        _rbfm->closeFile(fileHandle);
        return -1;
    }
    insertTableCache(data.tableName, tableId, rid);
    return _rbfm->closeFile(fileHandle);
}

RC RelationManager::insertColumnCatalog(const vector<Attribute> &attrs) 
{
    FileHandle fileHandle;
    int ret = _rbfm->openFile("Columns", fileHandle);
    if (0 != ret) return -1;

    int tableId = RelationManager::tableId;
    int bufferLength = computeRecordMaxMemory(columnAttr);
    static char *buffer = (char *)malloc(bufferLength);

    for(int colPos = 0; colPos < int(attrs.size()); ++colPos)
    {
        int offset = 0;
        string columnName = attrs[colPos].name;
        int columnType = attrs[colPos].type;
        int columnLength = attrs[colPos].length;
        int columnPosition = colPos + 1;

        appendData(buffer, &tableId,          sizeof(int), columnAttr[0].type, offset);
        appendData(buffer, columnName.c_str(),(int)columnName.size(), columnAttr[1].type, offset);
        appendData(buffer, &columnType,       sizeof(int), columnAttr[2].type, offset);
        appendData(buffer, &columnPosition,   sizeof(int), columnAttr[3].type, offset);
        appendData(buffer, &columnLength,     sizeof(int), columnAttr[4].type, offset);
    
        RID rid;
        ret = _rbfm->insertRecord(fileHandle, columnAttr, buffer, rid);
        insertColumnCache(tableId, rid);
        if(0 != ret) break;
    }

    if (0 != ret) 
    {
        _rbfm->closeFile(fileHandle);
        return -1;
    }
    return _rbfm->closeFile(fileHandle);
}


RC RelationManager::deleteTable(const string &tableName)
{
    if(!tableExist(tableName)) return -1;

    map<int, RID> *tableId2RID = tableCache[tableName];
    int tableId = (*tableId2RID).begin()->first;

    vector<RID> *ridVec = columnCache[tableId];

    int ret = 0;
    RID rid;
    for(int i = 0; i < (*ridVec).size(); ++i)
    {
       rid = (*ridVec)[i];
       ret = deleteTuple("Columns", rid);
       if(0 != ret) break;
    }
    if(0 != ret) return -2;

    delete(ridVec);
    columnCache.erase(tableId);

    rid = (*tableId2RID).begin()->second;

    ret = deleteTuple("Tables", rid);
    if(0 != ret) return -3;
    
    delete(tableId2RID);
    tableCache.erase(tableName);

    ret = _rbfm->destroyFile(tableName);
    if (0 != ret) return -4;

    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    if(!tableExist(tableName)) return -1;   
 
    map<int, RID> *tableId2RID = tableCache[tableName];
    int tableId = (*tableId2RID).begin()->first;
    vector<RID> *ridVec = columnCache[tableId];
    
    FileHandle fileHandle;
    int ret = _rbfm->openFile("Columns", fileHandle);
    if (0 != ret) return -1;

    static char *buffer = (char *)malloc(PAGE_SIZE);
    char *headOfBuffer = buffer;

    Attribute attr;
    for(int i = 0; i < (*ridVec).size(); ++i)
    {
        RID rid = (*ridVec)[i];
        int ret = _rbfm->readRecord(fileHandle, columnAttr, rid, buffer);
        if (0 != ret) return -1;
        
        buffer += sizeof(int); //skip tableId
        
        int nameLength = *(int *)buffer;
        buffer += sizeof(int); 
        attr.name = string(buffer, nameLength);  //columnName
        buffer = buffer + nameLength;
        
        memcpy(&attr.type, buffer, sizeof(int));
        buffer += sizeof(int); //column type

        buffer += sizeof(int); //skip column position
        
        unsigned len;
        memcpy(&len, buffer, sizeof(AttrLength));
        attr.length = len;
        
        attrs.push_back(attr);
        buffer = headOfBuffer;
    }

    return _rbfm->closeFile(fileHandle); 
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{

    if(!tableExist(tableName)) return -1;

    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    
    FileHandle fileHandle;
    int ret = _rbfm->openFile(tableName, fileHandle);
    if(0 != ret) return -1;
    
    ret = _rbfm->insertRecord(fileHandle, attrs, data, rid);
    if(0 != ret)
    {
        _rbfm->closeFile(fileHandle);
        return -1;
    }

    void *key=(void*)malloc(PAGE_SIZE);
    IXFileHandle ixfileHandle;
    for(int i = 0; i < attrs.size(); i++)
    {
    	_rbfm->readAttribute(fileHandle,attrs,rid,attrs.at(i).name,key);
    	string indexFileName = tableName + "." + attrs.at(i).name + "_index";
    	ret = _im->openFile(indexFileName,ixfileHandle);
    	if(ret == 0)
    	{
    		_im->insertEntry(ixfileHandle,attrs.at(i),key,rid);
    		_im->closeFile(ixfileHandle);
    	}
    }
    free(key);
    return _rbfm->closeFile(fileHandle);
}

RC RelationManager::deleteTuples(const string &tableName)
{
    if(!tableExist(tableName)) return -1;
    FileHandle fileHandle;
    int ret = _rbfm->openFile(tableName, fileHandle);
    if(0 != ret) return -1;
   
    ret = _rbfm->deleteRecords(fileHandle);
    if(0 != ret)
    {
        _rbfm->closeFile(fileHandle);
        return -1;
    }
    vector<Attribute> attrs;
    getAttributes(tableName,attrs);
    for(int i = 0; i < attrs.size(); i++)
    {
    	ret = _im->destroyFile(tableName + "_" + attrs.at(i).name + "_index");
    	if(ret == 0)
    		_im->createFile(tableName + "_" + attrs.at(i).name + "_index",1);
    }

    return _rbfm->closeFile(fileHandle);
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    if(!tableExist(tableName)) return -1;

    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    
    FileHandle fileHandle;
    int ret = _rbfm->openFile(tableName, fileHandle);
    if(0 != ret) return -1;
    
    ret = _rbfm->deleteRecord(fileHandle, attrs, rid);
	if (0 != ret) {
		_rbfm->closeFile(fileHandle);
		return -1;
	}

	void *key = (void*) malloc(PAGE_SIZE);
	IXFileHandle ixfileHandle;
	for (int i = 0; i < attrs.size(); i++) {
		_rbfm->readAttribute(fileHandle, attrs, rid, attrs.at(i).name, key);
		string indexFileName = tableName + "_" + attrs.at(i).name + "_index";
		if (_im->openFile(indexFileName, ixfileHandle) == 0) {
			_im->deleteEntry(ixfileHandle, attrs.at(i), key, rid);
			_im->closeFile(ixfileHandle);
		}
	}
	free(key);

    return _rbfm->closeFile(fileHandle);
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    if(!tableExist(tableName)) return -1;

    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    
    FileHandle fileHandle;
    int ret = _rbfm->openFile(tableName, fileHandle);
    if(0 != ret) return -1;
    
    ret = _rbfm->updateRecord(fileHandle, attrs, data, rid);
    if(0 != ret)
    {
        _rbfm->closeFile(fileHandle);
        return -1;
    }

    return _rbfm->closeFile(fileHandle);
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    if(!tableExist(tableName)) return -1;

    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    
    FileHandle fileHandle;
    int ret = _rbfm->openFile(tableName, fileHandle);
    if(0 != ret) return -1;
    
    ret = _rbfm->readRecord(fileHandle, attrs, rid, data);
    if(0 != ret)
    {
        _rbfm->closeFile(fileHandle);
        return -1;
    }

    return _rbfm->closeFile(fileHandle);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    if(!tableExist(tableName)) return -1;   

    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    
    FileHandle fileHandle;
    int ret = _rbfm->openFile(tableName, fileHandle);
    if(0 != ret) return -1;
    
    ret = _rbfm->readAttribute(fileHandle, attrs, rid, attributeName, data);
    if(0 != ret)
    {
        _rbfm->closeFile(fileHandle);
        return -1;
    }

    return _rbfm->closeFile(fileHandle);

}

void RelationManager::setTableAttr()
{
    Attribute attr;

    attr.name = "TableID";
    attr.type = TypeInt;
    attr.length = 4;
    tableAttr.push_back(attr);

    attr.name = "TableName";
    attr.type = TypeVarChar;
    attr.length = 128; 
    tableAttr.push_back(attr);

    attr.name = "FileName"; 
    attr.type = TypeVarChar;
    attr.length = 128; 
    tableAttr.push_back(attr);

    attr.name = "TableType";
    attr.type = TypeInt;
    attr.length = 4;
    tableAttr.push_back(attr);
}

void RelationManager::setColumnAttr()
{
    Attribute attr;

    attr.name = "TableID";
    attr.type = TypeInt;
    attr.length = 4;
    columnAttr.push_back(attr);
        
    attr.name = "ColumnName";
    attr.type = TypeVarChar;
    attr.length = 128;
    columnAttr.push_back(attr);

    attr.name = "ColumnType";
    attr.type = TypeInt;
    attr.length = 4;
    columnAttr.push_back(attr);

    attr.name = "ColumnPosition";
    attr.type = TypeInt;
    attr.length = 4;
    columnAttr.push_back(attr);

    attr.name = "MaxLength";
    attr.type = TypeInt;
    attr.length = 4;
    columnAttr.push_back(attr);
}


bool RelationManager::fileExist(const string &filename)
{
    struct stat buffer;
    if (stat(filename.c_str(), &buffer))
    {
        return false;
    }
    return true;
}

bool RelationManager::tableExist(const string &tableName)
{
    if(tableCache.find(tableName) == tableCache.end())
    {
        return false;
    }
    return true;
}

bool RelationManager::isSystemTable(const string &tableName)
{
    return (tableName == "Tables" || tableName == "Columns");
}

int RelationManager::computeRecordMaxMemory(const vector<Attribute> &attribute)
{
    int ret = 0;
    for(unsigned int i = 0; i < attribute.size(); ++i)
    {
        if (attribute[i].type == TypeVarChar)
        {
            ret += sizeof(int);
        }
        ret +=  attribute[i].length;
    }
    return ret;
}


void RelationManager::appendData(char *recordBuffer, const void *field, const int fieldLength, AttrType attrType, int &offset)
{
    if (attrType == TypeVarChar)
    {
        memcpy(recordBuffer + offset, &fieldLength, sizeof(int));
        offset += sizeof(int);
    }
    memcpy(recordBuffer + offset, field, fieldLength);
    offset += fieldLength;
}


RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
    if(!tableExist(tableName)) return -1;

    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    
    FileHandle fileHandle;
    int ret = _rbfm->openFile(tableName, fileHandle);
    if(0 != ret) return -1;
    
    ret = _rbfm->reorganizePage(fileHandle, attrs, pageNumber);
    if(0 != ret)
    {
        _rbfm->closeFile(fileHandle);
        return -1;
    }

    return _rbfm->closeFile(fileHandle);
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    string fileName = tableName;
    int ret = _rbfm->openFile(fileName, rm_ScanIterator.fileHandle);
    if(0 != ret) return ret;
    
    if(tableName.compare("Tables") == 0)
    {
        return rm_ScanIterator.initialize(tableAttr, compOp, value, attributeNames, conditionAttribute);
    }
    if(tableName.compare("Columns") == 0)
    {
        return rm_ScanIterator.initialize(columnAttr, compOp, value, attributeNames, conditionAttribute);
    }
    vector<Attribute> recordDescriptor;
    ret = getAttributes(tableName, recordDescriptor);
    if (0 != ret) return ret;
    return rm_ScanIterator.initialize(recordDescriptor, compOp, value, attributeNames, conditionAttribute);
}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    map<int, RID> *tableId2RID = tableCache[tableName];
    int tableId = (*tableId2RID).begin()->first;

    vector<Attribute> attrs;
    getAttributes(tableName, attrs);

    int i = 0;
    for(; i < attrs.size(); ++i)
    {
        if(attrs[i].name == attributeName)
        {
            break;
        }
    }
    
    FileHandle fileHandle;
    int ret = _rbfm->openFile("Columns", fileHandle);
    if(0 != ret) return ret;

    vector<RID> *ridVec = columnCache[tableId];
    RID rid = (*ridVec)[i];
    ret = _rbfm->deleteRecord(fileHandle, columnAttr, rid); 
    if(0 != ret) 
    {
        _rbfm->closeFile(fileHandle);
        return ret;
    }
    
    std::vector<RID>::iterator it = (*ridVec).begin() + i;
    (*ridVec).erase(it);
    
    return _rbfm->closeFile(fileHandle);
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    map<int, RID> *tableId2RID = tableCache[tableName];
    int tableId = (*tableId2RID).begin()->first;
    
    vector<RID> *ridVec = columnCache[tableId];

    FileHandle fileHandle;
    int ret = _rbfm->openFile("Columns", fileHandle);
    if (0 != ret) return -1;

    int bufferLength = computeRecordMaxMemory(columnAttr);
    static char *buffer = (char *)malloc(bufferLength);

    int offset = 0;
    string columnName = attr.name;
    int columnType = attr.type;
    int columnLength = attr.length;
    int columnPosition = (*ridVec).size() + 1;

    appendData(buffer, &tableId,          sizeof(int), columnAttr[0].type, offset);
    appendData(buffer, columnName.c_str(),(int)columnName.size(), columnAttr[1].type, offset);
    appendData(buffer, &columnType,       sizeof(int), columnAttr[2].type, offset);
    appendData(buffer, &columnPosition,   sizeof(int), columnAttr[3].type, offset);
    appendData(buffer, &columnLength,     sizeof(int), columnAttr[4].type, offset);
    
    RID rid;
    ret = _rbfm->insertRecord(fileHandle, columnAttr, buffer, rid);
    if (0 != ret)
    {
        _rbfm->closeFile(fileHandle);
        return ret;
    }
    insertColumnCache(tableId, rid);

    return _rbfm->closeFile(fileHandle);
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
    string fileName = tableName;
    FileHandle fileHandle;
    int ret = _rbfm->openFile(fileName, fileHandle);
    if (0 != ret) return -1;

    vector<Attribute> attrs;
    getAttributes(tableName, attrs);

    ret = _rbfm->reorganizeFile(fileHandle, attrs); 
    if(0 != ret)
    {
        _rbfm->closeFile(fileHandle);
        return ret;
    }
   
    return _rbfm->closeFile(fileHandle);
}


/************************************************/
RM_ScanIterator::RM_ScanIterator()
{
    _rbfm = RecordBasedFileManager::instance();
}

RM_ScanIterator::~RM_ScanIterator()
{

}

RC RM_ScanIterator::initialize(
      const vector<Attribute> &recordDescriptor,
      const CompOp compOp,
      const void *value,
      const vector<string> &attributeNames,
      const string &conditionAttribute)
{
    return _rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, _rbfm_scanner);
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    return _rbfm_scanner.getNextRecord(rid, data);
}

RC RM_ScanIterator::close()
{
    _rbfm_scanner.close();
    return _rbfm->closeFile(fileHandle);
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	string indexFileName = tableName + "." + attributeName + "_index";
	vector<Attribute> *attrs = new vector<Attribute>();
	vector<Attribute> *name = new vector<Attribute>();
	getAttributes(tableName, *attrs);
	int vectorSize = attrs->size();
	for(int i = 0; i < vectorSize;i++)
	{
		if(attrs->at(i).name == attributeName)
		{
			name->push_back(attrs->at(i));
			break;
		}
	}
	// create indexfile
	_im->createFile(indexFileName,1);
	// insert index
	vector<string> judgeAttrs;
	judgeAttrs.push_back(name->at(0).name);
	RBFM_ScanIterator rmsi;
	FileHandle fileHandle;
	_rbfm->openFile(tableName, fileHandle);
	if (_rbfm->scan(fileHandle, *attrs, "",  NO_OP, NULL, judgeAttrs, rmsi) == 0) {
		RID rid;
		void *returnedData = (void*) malloc(PAGE_SIZE);
		IXFileHandle ixfileHandle;
		_im->openFile(indexFileName, ixfileHandle);
		while (rmsi.getNextRecord(rid, returnedData) != RM_EOF) {
			_im->insertEntry(ixfileHandle, name->at(0), returnedData,
					rid);
		}
		_im->closeFile(ixfileHandle);
		_rbfm->closeFile(fileHandle);
		free(returnedData);
	}
	attrs->clear();
	name->clear();
	judgeAttrs.clear();
	return 0;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	string indexFileName = tableName + "_" + attributeName + "_index";
	if(_im->destroyFile(indexFileName) != 0)
	{
		return -1;
	}
	return 0;
}

RC RelationManager::indexScan(const string &tableName, const string &attributeName,
			const void *lowKey, const void *highKey, bool lowKeyInclusive,
			bool highKeyInclusive, RM_IndexScanIterator &rm_IndexScanIterator)
{
	// get the attributeName
	string usedAttributeName;
	usedAttributeName = attributeName.substr(attributeName.find('.') + 1,attributeName.length());
	string indexFileName =  attributeName + "_index";
	IXFileHandle ixfileHandle;


	vector<Attribute> attrs;
	getAttributes(tableName, attrs);
	int rc = _im->openFile(indexFileName,rm_IndexScanIterator.ixfileHandle);
	if(rc != 0) return -1;
	for (int i = 0; i < attrs.size(); i++) {
		if (attrs[i].name == usedAttributeName) {
			return _im->scan(rm_IndexScanIterator.ixfileHandle,attrs[i],lowKey,highKey,lowKeyInclusive,highKeyInclusive,rm_IndexScanIterator._ix_scanner);
		}
	}
	return -1;
}

RM_IndexScanIterator::RM_IndexScanIterator()
{
	_im = IndexManager::instance();
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key)
{
	return _ix_scanner.getNextEntry(rid,key);

}

RC RM_IndexScanIterator::close()
{
//	return _ix_scanner.close();
	return 0;
}
