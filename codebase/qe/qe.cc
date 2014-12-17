#include "qe.h"

#include <string.h>
#include <sstream>
#include <iostream>
#include <math.h>

using namespace std;

Filter::Filter(Iterator* input, const Condition &condition) {
	this->iter=input;
	this->con=condition;
}
RC Filter::getNextTuple(void *data)
{
	RC rc= iter->getNextTuple(data);
	if(rc!=0)
		return rc;
	while(rc==0)
	{
		vector<Attribute> *tempAttr=new vector<Attribute>();
		iter->getAttributes(*tempAttr);
		int offset=0;
		int result=0;
		for(int i=0;i<(int)tempAttr->size();i++)
		{
			if(tempAttr->at(i).name==con.lhsAttr)
			{
				int len=0;
				if(tempAttr->at(i).type==TypeInt)
				{
					len=sizeof(int);
					int left,right;
					memcpy(&left,(char*)data+offset,len);
					memcpy(&right,(char*)con.rhsValue.data,len);
					if(con.op==NO_OP)
					{
						result=1;
						break;
					}
					else if(con.op==EQ_OP)
					{
						if(left==right)
							result=1;
						break;
					}
					else if(con.op==LE_OP)
					{
						if(left<=right)
							result=1;
						break;
					}
					else if(con.op==GE_OP)
					{
						if(left>=right)
							result=1;
						break;
					}
					else if(con.op==LT_OP)
					{
						if(left<right)
							result=1;
						break;
					}
					else if(con.op==GT_OP)
					{
						if(left>right)
							result=1;
						break;
					}
					else
					{
						if(left!=right)
							result=1;
						break;
					}
				}
				else if(tempAttr->at(i).type==TypeReal)
				{
					len=sizeof(float);
					float left,right;
					memcpy(&left,(char*)data+offset,len);
					memcpy(&right,(char*)con.rhsValue.data,len);
					if(con.op==NO_OP)
					{
						result=1;
						break;
					}
					else if(con.op==EQ_OP)
					{
						if(left==right)
							result=1;
						break;
					}
					else if(con.op==LE_OP)
					{
						if(left<=right)
							result=1;
						break;
					}
					else if(con.op==GE_OP)
					{
						if(left>=right)
							result=1;
						break;
					}
					else if(con.op==LT_OP)
					{
						if(left<right)
							result=1;
						break;
					}
					else if(con.op==GT_OP)
					{
						if(left>right)
							result=1;
						break;
					}
					else
					{
						if(left!=right)
							result=1;
						break;
					}
				}
				else if(tempAttr->at(i).type==TypeVarChar)
				{
					int len_1,len_2;
					void* result_s=(void*)malloc(PAGE_SIZE);
					void* comp_s=(void*)malloc(PAGE_SIZE);
					memcpy(&len_1,(char*)data+offset,sizeof(int));
					memcpy(&len_2,(char*)con.rhsValue.data,sizeof(int));
					result_s=(void*)malloc(len_1+1);
					comp_s=(void*)malloc(len_2+1);
					memset(result_s,'\0',len_1+1);
					memset(comp_s,'\0',len_2+1);
					memcpy((char*)result_s,(((char*)(data))+offset+sizeof(int)),len_1);
					memcpy((char*)comp_s,(((char*)(con.rhsValue.data))+sizeof(int)),len_2);
					int r=strcmp((char*)result_s,(char*)comp_s);
					free(result_s);
					free(comp_s);
					if(con.op==NO_OP)
					{
						result=1;
						break;
					}
					else if(con.op==EQ_OP)
					{
						if(r==0)
							result=1;
						break;
					}
					else if(con.op==LE_OP)
					{
						if(r<=0)
							result=1;
						break;
					}
					else if(con.op==GE_OP)
					{
						if(r>=0)
							result=1;
						break;
					}
					else if(con.op==LT_OP)
					{
						if(r<0)
							result=1;
						break;
					}
					else if(con.op==GT_OP)
					{
						if(r>0)
							result=1;
						break;
					}
					else
					{
						if(r!=0)
							result=1;
						break;
					}
				}
			}
			else
			{
				int len=0;
				switch(tempAttr->at(i).type)
				{
				case TypeInt:
					len = sizeof(int);
					break;
				case TypeReal:
					len = sizeof(float);
					break;
				case TypeVarChar:
					memcpy(&len, (char*) data + offset, sizeof(int));
					len += sizeof(int);
					break;
				}
				offset+=len;
			}
		}
		tempAttr->clear();
		if(result==1)
		{
			return 0;
		}
		rc= iter->getNextTuple(data);
	}
	return rc;
}
void Filter::getAttributes(vector<Attribute> &attrs) const
{
	iter->getAttributes(attrs);
}
Project::Project(Iterator *input,                            // Iterator of input R
               const vector<string> &attrNames)
{
	this->iter=input;
	this->attrNames=new vector<Attribute>();
	vector<Attribute> *tmp=new vector<Attribute>();
	input->getAttributes(*tmp);
	int j=0;
	for(int i=0;i<(int)tmp->size();i++)
	{
		for(int j = 0; j < attrNames.size(); j++)
		{
			if(tmp->at(i).name==attrNames.at(j))
				this->attrNames->push_back(tmp->at(i));
		}
	}
	tmp->clear();
}
RC Project::getNextTuple(void *data)
{
	void *tmp = (void *) malloc(PAGE_SIZE);
	RC rc = iter->getNextTuple(tmp);
	if (rc != 0) {
		free(tmp);
		return rc;
	} else {
		int offset = 0;
		int off = 0;
		int j = 0;
		vector<Attribute> *tempAttr = new vector<Attribute>();
		iter->getAttributes(*tempAttr);
		for (int i = 0; i < (int) tempAttr->size(); i++) {
			if (tempAttr->at(i).name == attrNames->at(j).name) {
				int len = 0;
				switch(tempAttr->at(i).type)
				{
				case TypeInt:
					len = sizeof(int);
					break;
				case TypeReal:
					len = sizeof(float);
					break;
				case TypeVarChar:
					memcpy(&len, (char*) tmp + offset, sizeof(int));
					len += sizeof(int);
					break;
				}
				memcpy((char*) data + off, (char*) tmp + offset, len);
				off += len;
				offset += len;
				j++;
				if(j == attrNames->size())
					break;
			} else {
				int len = 0;
				switch(tempAttr->at(i).type)
				{
				case TypeInt:
					len = sizeof(int);
					break;
				case TypeReal:
					len = sizeof(float);
					break;
				case TypeVarChar:
					memcpy(&len, (char*) tmp + offset, sizeof(int));
					len += sizeof(int);
					break;
				}
				offset += len;
			}
		}
		tempAttr->clear();
		free(tmp);
		return rc;
	}
}
void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs=*(this->attrNames);
}

RC GHJoin::SetPartitionFiles()
{
    RC ret = 0;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    void * data = malloc(PAGE_SIZE);

    vector<Attribute> attrs;
    lIter->getAttributes(attrs);


    stringstream strStream;
    for(int i =0;i<numPartitions;i++)
    {
    	strStream << i;
    	string stringi;
    	strStream >> stringi;
    	strStream.clear();
    	strStream << joinnumber;
    	string stringjoinNumber;
    	strStream >> stringjoinNumber;
        string lFileName = "left_join" + stringjoinNumber + "_" + stringi;
        ret = rbfm->createFile(lFileName.c_str());
        if(ret != 0) return -1;
        string rFileName = "right_join" + stringjoinNumber + "_" + stringi;
        ret = rbfm->createFile(rFileName.c_str());
        if(ret != 0) return -1;
        strStream.clear();
    }

    int key = 0;
    // create the left partitions
	while (this->lIter->getNextTuple(data) != -1) {
		void * attrvalue = malloc(PAGE_SIZE);
		int offset = 0;
		int size = 0;
		for (int i = 0; i < attrs.size(); i++) {
			if (attrs[i].name == condition.lhsAttr) {
				switch (attrs[i].type) {
				case TypeInt:
					size = sizeof(int);
					break;
				case TypeReal:
					size = sizeof(float);
					break;
				case TypeVarChar:
					memcpy(&size, (char *) data + offset, sizeof(int));
					size += sizeof(int);
					break;
				default:
					break;
				}
				memcpy(attrvalue, (char *) data + offset, size);
				break;
			} else {
				switch (attrs[i].type) {
				case TypeInt: {
					offset += sizeof(int);
					break;
				}
				case TypeReal: {
					offset += sizeof(float);
					break;
				}
				case TypeVarChar: {
					int clen = 0;
					memcpy(&clen, (char *) data + offset, sizeof(int));
					offset += clen + sizeof(int);
					break;
				}
				default:
					break;
				}

			}
		}

		switch (conditionAttr.type) {
		case TypeInt: {
			int temp = 0;
			memcpy(&temp, (char *) attrvalue, sizeof(int));
			key = (int) fmod(temp, (double) numPartitions);
			break;
		}
		case TypeReal: {
			float temp2 = 0;
			memcpy(&temp2, (char *) attrvalue, sizeof(float));
			key = (int) fmod(temp2, (double) numPartitions);
			break;
		}
		case TypeVarChar: {
			int len = 0;
			memcpy(&len, (char *) attrvalue, sizeof(int));
			char* key_char = new char[len + 1];
			key_char[len] = '\0';
			memcpy(key_char, ((char *) attrvalue + sizeof(int)), len);
			key = (int) fmod((double) (hashString(key_char)),
					(double) numPartitions);
			free(key_char);
			break;
		}
		}

		strStream << key;
		string stringkey;
		strStream >> stringkey;
		strStream.clear();
		strStream << joinnumber;
		string stringjoinNumber;
		strStream >> stringjoinNumber;
		string fileName = "left_join" + stringjoinNumber + "_" + stringkey;
		FileHandle fileHandle;
		ret = rbfm->openFile(fileName, fileHandle);
		if (ret != 0)
			return -1;
		RID rid;
		ret = rbfm->insertRecord(fileHandle, attrs, data, rid);
		if (ret != 0)
			return -1;
		rbfm->closeFile(fileHandle);
		free(attrvalue);
		strStream.clear();

	}

    // create right partitions
	rIter->getAttributes(attrs);
    while (rIter->getNextTuple(data) != -1) {
        void * attrvalue = malloc(PAGE_SIZE);
		int offset = 0;
		int size = 0;
		for (int i = 0; i < attrs.size(); i++) {
			if (attrs[i].name == condition.rhsAttr) {
				switch (attrs[i].type) {
				case TypeInt:
					size = sizeof(int);
					break;
				case TypeReal:
					size = sizeof(float);
					break;
				case TypeVarChar:
					memcpy(&size, (char *) data + offset, sizeof(int));
					size += sizeof(int);
					break;
				default:
					break;
				}
				memcpy(attrvalue, (char *) data + offset, size);
				break;
			} else {
				switch (attrs[i].type) {
				case TypeInt: {
					offset += sizeof(int);
					break;
				}
				case TypeReal: {
					offset += sizeof(float);
					break;
				}
				case TypeVarChar: {
					int clen = 0;
					memcpy(&clen, (char *) data + offset, sizeof(int));
					offset += clen + sizeof(int);
					break;
				}
				default:
					break;
				}

			}
		}

		switch (conditionAttr.type) {
		case TypeInt: {
			int temp = 0;
			memcpy(&temp, (char *) attrvalue, sizeof(int));
			key = (int) fmod(temp, (double) numPartitions);
			break;
		}
		case TypeReal: {
			float temp2 = 0;
			memcpy(&temp2, (char *) attrvalue, sizeof(float));
			key = (int) fmod(temp2, (double) numPartitions);
			break;
		}
		case TypeVarChar: {
			int len = 0;
			memcpy(&len, (char *) attrvalue, sizeof(int));
			char* key_char = new char[len + 1];
			key_char[len] = '\0';
			memcpy(key_char, ((char *) attrvalue + sizeof(int)), len);
			key = (int) fmod((double) (hashString(key_char)),
					(double) numPartitions);
			free(key_char);
			break;
		}
		}
		strStream << key;
		string stringkey;
		strStream >> stringkey;
		strStream.clear();
		strStream << joinnumber;
		string stringjoinNumber;
		strStream >> stringjoinNumber;
		string fileName = "right_join" + stringjoinNumber + "_" + stringkey;
        FileHandle rFileHandle;
        ret = rbfm->openFile(fileName, rFileHandle);
        if (ret != 0) return -1;
        RID rRid;
        //rbfm->printRecord(attrs,data);
        ret = rbfm->insertRecord(rFileHandle, attrs, data, rRid);
        if (ret != 0) return -1;
        rbfm->closeFile(rFileHandle);
        free(attrvalue);
        strStream.clear();
    }
    free(rbfm);
    free(data);
    return 0;
}

// SDBMHash
unsigned int GHJoin::hashString(const char* str)
{
    unsigned int hash = 0;

        while (*str)
        {
            // equivalent to: hash = 65599*hash + (*str++);
            hash = (*str++) + (hash << 6) + (hash << 16) - hash;
        }

        return (hash & 0x7FFFFFFF);

    return (hash & 0x7FFFFFFF);
}

RC GHJoin::buildLeftHashTable(int partitionNum)
{
	RC ret = 0;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle filehandle;
	stringstream strStream;
	strStream << partitionNum;
	string stringParNum;
	strStream >> stringParNum;
	strStream.clear();
	strStream << joinnumber;
	string stringjoinNumber;
	strStream >> stringjoinNumber;
	string fileName = "left_join" + stringjoinNumber + "_" + stringParNum;
	ret = rbfm->openFile(fileName,filehandle);
	strStream.clear();
	vector<Attribute> attrs;
	vector<string> attrNames;
	lIter->getAttributes(attrs);
	for (int i = 0; i < attrs.size(); i++)
	{
		attrNames.push_back(attrs.at(i).name);
	}
	if (filehandle.getNumberOfPages() == 0)
		return -1;
	else
	{
		RBFM_ScanIterator * rbfm_iterator = new RBFM_ScanIterator();
		rbfm->scan(filehandle, attrs, "", NO_OP, NULL, attrNames,
				*rbfm_iterator);
		RID rid;
		void * data = malloc(PAGE_SIZE);
		while (rbfm_iterator->getNextRecord(rid, data) != -1) {
			switch (conditionAttr.type) {
			case (TypeInt): {
				void *intData = malloc(4);
				int offset = 0;
				int size = 0;
				for (int i = 0; i < attrs.size(); i++) {
					if (attrs[i].name == conditionAttr.name) {
						memcpy(intData, (char *) data + offset, sizeof(int));
						break;
					} else {
						switch (attrs[i].type) {
						case TypeInt: {
							offset += sizeof(int);
							break;
						}
						case TypeReal: {
							offset += sizeof(float);
							break;
						}
						case TypeVarChar: {
							int len = 0;
							memcpy(&len, (char *) data + offset, sizeof(int));
							offset += len + sizeof(int);
							break;
						}
						}
					}
				}
				int recordLength = rbfm->getRecordSize4Cli(attrs, data);
				if (_inthashTable.find(*(int*) intData)
						== _inthashTable.end()) {
					DataDir tempDataDir;
					tempDataDir.dataIndex.push_back(recordLength);
					tempDataDir.data = malloc(recordLength);
					memcpy(tempDataDir.data, data, recordLength);
					_inthashTable[*(int*) intData] = tempDataDir;
				} else {
					int dataoffset = 0;
					for(int i = 0; i < _inthashTable[*(int*) intData].dataIndex.size(); i++)
						dataoffset += _inthashTable[*(int*) intData].dataIndex.at(i);
					_inthashTable[*(int*) intData].data = realloc(
							_inthashTable[*(int*) intData].data,
							dataoffset + recordLength);
					memcpy((char *)(_inthashTable[*(int*) intData].data)
									+ dataoffset,
							data, recordLength);
					_inthashTable[*(int*) intData].dataIndex.push_back(recordLength);
				}
				free(intData);
				break;
			}
			case (TypeReal): {
				void *floatData = malloc(4);
				int offset = 0;
				int size = 0;
				for (int i = 0; i < attrs.size(); i++) {
					if (attrs[i].name == conditionAttr.name) {
						memcpy(floatData, (char *) data + offset, sizeof(float));
						break;
					} else {
						switch (attrs[i].type) {
						case TypeInt: {
							offset += sizeof(int);
							break;
						}
						case TypeReal: {
							offset += sizeof(float);
							break;
						}
						case TypeVarChar: {
							int len = 0;
							memcpy(&len, (char *) data + offset, sizeof(int));
							offset += len + sizeof(int);
							break;
						}
						}
					}
				}
				int recordLength = rbfm->getRecordSize4Cli(attrs, data);
				if (_floathashTable.find(*(float*) floatData)
						== _floathashTable.end()) {
					DataDir tempDataDir;
					tempDataDir.data = malloc(recordLength);
					tempDataDir.dataIndex.push_back(recordLength);
					memcpy(tempDataDir.data, data, recordLength);
					_floathashTable[*(float*) floatData] = tempDataDir;
				} else {
					int dataoffset = 0;
					for(int i = 0; i < _floathashTable[*(float*) floatData].dataIndex.size(); i++)
						dataoffset += _floathashTable[*(float*) floatData].dataIndex.at(i);
					_floathashTable[*(float*) floatData].data = realloc(
							_floathashTable[*(float*) floatData].data,
							dataoffset + recordLength);
					memcpy((char *) (_floathashTable[*(float*) floatData].data) + dataoffset,
							data, recordLength);
					_floathashTable[*(float*) floatData].dataIndex.push_back(recordLength);
				}
				free(floatData);
				break;
			}
			case (TypeVarChar): {
				void *stringData = malloc(PAGE_SIZE);
				int offset = 0;
				int size = 0;
				for (int i = 0; i < attrs.size(); i++) {
					if (attrs[i].name == conditionAttr.name) {
						int len = 0;
						memcpy(&len, (char *) data + offset, sizeof(int));
						memcpy(stringData, (char *) data + offset, len + sizeof(int));
						break;
					} else {
						switch (attrs[i].type) {
						case TypeInt: {
							offset += sizeof(int);
							break;
						}
						case TypeReal: {
							offset += sizeof(float);
							break;
						}
						case TypeVarChar: {
							int len = 0;
							memcpy(&len, (char *) data + offset, sizeof(int));
							offset += len + sizeof(int);
							break;
						}
						}
					}
				}
				int length = 0;
				memcpy(&length, stringData, sizeof(int));
				char * tempData = new char[length + 1];
				tempData[length] = '\0';
				memcpy(tempData, (char*) stringData + sizeof(int), length);
				//test whether we can find the same key
				//if not, add a new one
				int recordLength = rbfm->getRecordSize4Cli(attrs, data);
				if (_stringhashTable.find((string) tempData)
						== _stringhashTable.end()) {
					DataDir tampDataDir;
					tampDataDir.data = malloc(recordLength);
					tampDataDir.dataIndex.push_back(recordLength);
					memcpy(tampDataDir.data, data, recordLength);
					_stringhashTable[(string) tempData] = tampDataDir;
				} else {
					int dataOffset = 0;
					for(int i = 0; i < _stringhashTable[(string) tempData].dataIndex.size(); i++)
						dataOffset += _stringhashTable[(string) tempData].dataIndex.at(i);
					_stringhashTable[(string) tempData].data = realloc(
							_stringhashTable[(string) tempData].data,
							dataOffset + recordLength);
					memcpy((char *) (_stringhashTable[(string) tempData].data) + dataOffset,
							data, recordLength);
					_stringhashTable[(string) tempData].dataIndex.push_back(recordLength);
				}
				free(tempData);
				break;
			}
			default:
				break;
			}
		}
		free(data);
	}
	return 0;
}

GHJoin::GHJoin(Iterator *leftIn,               // Iterator of input R
               Iterator *rightIn,               // Iterator of input S
               const Condition &condition,      // Join condition (CompOp is always EQ)
               const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
)
{
    lIter = leftIn;
    rIter = rightIn;
    this->numPartitions = numPartitions;
    this->condition = condition;
    usingPartitionNum = 0;
    rightrbfm = RecordBasedFileManager::instance();
    rightscan = new RBFM_ScanIterator();
//    hashDataIndex = 0;
    vector<Attribute> attrs;
    lIter->getAttributes(attrs);
    shouldBuildHashtable = 0;
    joinnumber = 1;

    //get the condition attribute type
    for (unsigned i = 0; i < attrs.size(); i++) {
        if (attrs.at(i).name == condition.lhsAttr) {
            conditionAttr = attrs.at(i);
            break;
        }
    }


    RC ret = SetPartitionFiles();
    while(ret != 0)
    {
    	joinnumber++;
    	ret = SetPartitionFiles();
    }
}

RC GHJoin::probe(void * rightData,void * data)
{
	void *hashkey = malloc(PAGE_SIZE);
	vector<Attribute> attrs;
	vector<Attribute> rightattrs;
	rIter->getAttributes(rightattrs);
	lIter->getAttributes(attrs);
	int offset = 0;
	int size = 0;
	for (int i = 0; i < rightattrs.size(); i++) {
		if (rightattrs[i].name == condition.rhsAttr) {
			switch (rightattrs[i].type) {
			case TypeInt:
				size = sizeof(int);
				break;
			case TypeReal:
				size = sizeof(float);
				break;
			case TypeVarChar:
				memcpy(&size, (char *) rightData + offset, sizeof(int));
				size += sizeof(int);
				break;
			default:
				break;
			}
			memcpy(hashkey, (char *) rightData + offset, size);
			break;
		} else {
			switch (attrs[i].type) {
			case TypeInt: {
				offset += sizeof(int);
				break;
			}
			case TypeReal: {
				offset += sizeof(float);
				break;
			}
			case TypeVarChar: {
				int clen = 0;
				memcpy(&clen, (char *) rightData + offset, sizeof(int));
				offset += clen + sizeof(int);
				break;
			}
			default:
				break;
			}
		}
	}
	switch (conditionAttr.type) {
	case (TypeInt): {
		if (0 < _inthashTable[*(int*) hashkey].dataIndex.size()) {
			int dataOffset = 0;
			for (int i = 0; i < 0; i++)
				dataOffset += _inthashTable[*(int*) hashkey].dataIndex.at(i);
			int recordLength =
					_inthashTable[*(int*) hashkey].dataIndex.at(0);
			void * leftData = malloc(recordLength);
			memcpy(leftData,
					(char *) _inthashTable[*(int*) hashkey].data + dataOffset,
					recordLength);
			memcpy((char *) data, leftData, recordLength);
			int rightrecordLength = rightrbfm->getRecordSize4Cli(rightattrs,
					rightData);
			memcpy((char *) data + recordLength, rightData, rightrecordLength);
			free(leftData);
		} else {
			free(hashkey);
			return -1;
		}
		break;
	}
	case (TypeReal): {
		if (0
				< _floathashTable[*(float*) hashkey].dataIndex.size()) {
			int recordLength = _floathashTable[*(float*) hashkey].dataIndex.at(0);
			int dataOffset = 0;
			for (int i = 0; i < 0; i++)
				dataOffset += _floathashTable[*(float*) hashkey].dataIndex.at(i);
			void * leftData = malloc(recordLength);
			memcpy(leftData,
					(char *) _floathashTable[*(float*) hashkey].data
							+ dataOffset, recordLength);
			memcpy((char *) data, leftData, recordLength);
			int rightrecordLength = rightrbfm->getRecordSize4Cli(rightattrs,
					rightData);
			memcpy((char *) data + recordLength, rightData, rightrecordLength);
			free(leftData);
		} else {
			free(hashkey);
			return -1;
		}
		break;
	}
	case (TypeVarChar): {
		int length = 0;
		memcpy(&length, hashkey, sizeof(int));
		char * tempData = new char[length + 1];
		tempData[length] = '\0';
		memcpy(tempData, (char*) hashkey + sizeof(int), length);
		if (0
				< _stringhashTable[(string) (char *) tempData].dataIndex.size()) {
			int recordLength =
					_stringhashTable[(string) (char *) tempData].dataIndex.at(0);
			void * leftData = malloc(recordLength);
			int dataOffset = 0;
			for (int i = 0; i < 0; i++)
				dataOffset += _stringhashTable[(string)(char*) tempData].dataIndex.at(i);
			memcpy(leftData,
					(char *) _stringhashTable[(string) (char *) tempData].data
							+ dataOffset, recordLength);
			memcpy((char *) data, leftData, recordLength);
			int rightrecordLength = rightrbfm->getRecordSize4Cli(rightattrs,
					rightData);
			memcpy((char *) data + recordLength, rightData, rightrecordLength);
			free(leftData);
		} else {
			free(hashkey);
			return -1;
		}
		break;
	}
	}
	free(hashkey);
	return 0;
}

void GHJoin::clearFile()
{
	stringstream strStream;
	strStream << joinnumber;
	string stringjoinNumber;
	strStream >> stringjoinNumber;
	strStream.clear();
	for(int i = 0 ;i < numPartitions; i++)
	{
		string stringi;
		strStream << i;
		strStream >> stringi;
		strStream.clear();
		rightrbfm->destroyFile("left_join" + stringjoinNumber + "_" + stringi);
		rightrbfm->destroyFile("right_join" + stringjoinNumber + "_" + stringi);
	}
}

RC GHJoin::getNextTuple(void *data)
{
	int ret;
	vector<Attribute> attrs;
	rIter->getAttributes(attrs);
	void * rightData = malloc(PAGE_SIZE);
	vector<string> attrNames;
	for(int i = 0; i < attrs.size(); i++)
	{
		attrNames.push_back(attrs.at(i).name);
	}
	while(usingPartitionNum < numPartitions)
	{
		if(shouldBuildHashtable == 0)
		{
			ret = buildLeftHashTable(usingPartitionNum);
			// cannot create lefthashtable;
			if(ret != 0)
			{
				usingPartitionNum++;
				continue;
			}
			shouldBuildHashtable = -1;
			stringstream strStream;
			strStream << usingPartitionNum;
			string stringNum;
			strStream >> stringNum;
			strStream.clear();
			strStream << joinnumber;
			string stringjoinNumber;
			strStream >> stringjoinNumber;
			string fileName = "right_join" + stringjoinNumber + "_" + stringNum;
			ret = rightrbfm->openFile(fileName,rightfilehandle);
			rightrbfm->scan(rightfilehandle ,attrs, "", NO_OP, NULL, attrNames, *rightscan);
			strStream.clear();
		}

		RID rid;

		if(rightscan->getNextRecord(rid,rightData) == 0)
		{
			ret = probe(rightData, data);
			if(ret != 0)
			{
				continue;
			}
			else
			{
				free(rightData);
				return 0;
			}
		}
		else
		{
			usingPartitionNum++;
			shouldBuildHashtable = 0;
			rightrbfm->closeFile(rightfilehandle);
			_stringhashTable.clear();
			_inthashTable.clear();
			_floathashTable.clear();
		}
	}
	clearFile();
	free(rightData);
	return -1;
}

void GHJoin::getAttributes(vector<Attribute> &attrs) const
{
	vector<Attribute> lAttrs;
	vector<Attribute> rAttrs;
	lAttrs.clear();
	rAttrs.clear();
	this->lIter->getAttributes(lAttrs);
	this->rIter->getAttributes(rAttrs);
	for (int i = 0; i < lAttrs.size(); i++) {
	attrs.push_back(lAttrs[i]);
	}
	for (int j = 0; j < rAttrs.size(); j++) {
	attrs.push_back(rAttrs[j]);
	}
}

RC BNLJoin::buildLeftHashTable()
{
	RC ret = 0;
	vector<Attribute> attrs;
	lIter->getAttributes(attrs);
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	void * data = malloc(PAGE_SIZE);
	while (lIter->getNextTuple(data) != -1) {
		switch (conditionAttr.type) {
		case (TypeInt): {
			void *intData = malloc(4);
			int offset = 0;
			int size = 0;
			for (int i = 0; i < attrs.size(); i++) {
				if (attrs[i].name == conditionAttr.name) {
					memcpy(intData, (char *) data + offset, sizeof(int));
					break;
				} else {
					switch (attrs[i].type) {
					case TypeInt: {
						offset += sizeof(int);
						break;
					}
					case TypeReal: {
						offset += sizeof(float);
						break;
					}
					case TypeVarChar: {
						int len = 0;
						memcpy(&len, (char *) data + offset, sizeof(int));
						offset += len + sizeof(int);
						break;
					}
					}
				}
			}
			int recordLength = rbfm->getRecordSize4Cli(attrs, data);
			if (_inthashTable.find(*(int*) intData) == _inthashTable.end()) {
				DataDir tempDataDir;
				tempDataDir.dataIndex.push_back(recordLength);
				tempDataDir.data = malloc(recordLength);
				memcpy(tempDataDir.data, data, recordLength);
				_inthashTable[*(int*) intData] = tempDataDir;
			} else {
				int dataoffset = 0;
				for (int i = 0;
						i < _inthashTable[*(int*) intData].dataIndex.size();
						i++)
					dataoffset += _inthashTable[*(int*) intData].dataIndex.at(
							i);
				_inthashTable[*(int*) intData].data = realloc(
						_inthashTable[*(int*) intData].data,
						dataoffset + recordLength);
				memcpy(
						(char *) (_inthashTable[*(int*) intData].data)
								+ dataoffset, data, recordLength);
				_inthashTable[*(int*) intData].dataIndex.push_back(
						recordLength);
			}
			free(intData);
			break;
		}
		case (TypeReal): {
			void *floatData = malloc(4);
			int offset = 0;
			int size = 0;
			for (int i = 0; i < attrs.size(); i++) {
				if (attrs[i].name == conditionAttr.name) {
					memcpy(floatData, (char *) data + offset, sizeof(float));
					break;
				} else {
					switch (attrs[i].type) {
					case TypeInt: {
						offset += sizeof(int);
						break;
					}
					case TypeReal: {
						offset += sizeof(float);
						break;
					}
					case TypeVarChar: {
						int len = 0;
						memcpy(&len, (char *) data + offset, sizeof(int));
						offset += len + sizeof(int);
						break;
					}
					}
				}
			}
			int recordLength = rbfm->getRecordSize4Cli(attrs, data);
			if (_floathashTable.find(*(float*) floatData)
					== _floathashTable.end()) {
				DataDir tempDataDir;
				tempDataDir.data = malloc(recordLength);
				tempDataDir.dataIndex.push_back(recordLength);
				memcpy(tempDataDir.data, data, recordLength);
				_floathashTable[*(float*) floatData] = tempDataDir;
			} else {
				int dataoffset = 0;
				for (int i = 0;
						i
								< _floathashTable[*(float*) floatData].dataIndex.size();
						i++)
					dataoffset +=
							_floathashTable[*(float*) floatData].dataIndex.at(
									i);
				_floathashTable[*(float*) floatData].data = realloc(
						_floathashTable[*(float*) floatData].data,
						dataoffset + recordLength);
				memcpy(
						(char *) (_floathashTable[*(float*) floatData].data)
								+ dataoffset, data, recordLength);
				_floathashTable[*(float*) floatData].dataIndex.push_back(
						recordLength);
			}
			free(floatData);
			break;
		}
		case (TypeVarChar): {
			void *stringData = malloc(PAGE_SIZE);
			int offset = 0;
			int size = 0;
			for (int i = 0; i < attrs.size(); i++) {
				if (attrs[i].name == conditionAttr.name) {
					int len = 0;
					memcpy(&len, (char *) data + offset, sizeof(int));
					memcpy(stringData, (char *) data + offset,
							len + sizeof(int));
					break;
				} else {
					switch (attrs[i].type) {
					case TypeInt: {
						offset += sizeof(int);
						break;
					}
					case TypeReal: {
						offset += sizeof(float);
						break;
					}
					case TypeVarChar: {
						int len = 0;
						memcpy(&len, (char *) data + offset, sizeof(int));
						offset += len + sizeof(int);
						break;
					}
					}
				}
			}
			int length = 0;
			memcpy(&length, stringData, sizeof(int));
			char * tempData = new char[length + 1];
			tempData[length] = '\0';
			memcpy(tempData, (char*) stringData + sizeof(int), length);
			//test whether we can find the same key
			//if not, add a new one
			int recordLength = rbfm->getRecordSize4Cli(attrs, data);
			if (_stringhashTable.find((string) tempData)
					== _stringhashTable.end()) {
				DataDir tampDataDir;
				tampDataDir.data = malloc(recordLength);
				tampDataDir.dataIndex.push_back(recordLength);
				memcpy(tampDataDir.data, data, recordLength);
				_stringhashTable[(string) tempData] = tampDataDir;
			} else {
				int dataOffset = 0;
				for (int i = 0;
						i < _stringhashTable[(string) tempData].dataIndex.size();
						i++)
					dataOffset +=
							_stringhashTable[(string) tempData].dataIndex.at(i);
				_stringhashTable[(string) tempData].data = realloc(
						_stringhashTable[(string) tempData].data,
						dataOffset + recordLength);
				memcpy(
						(char *) (_stringhashTable[(string) tempData].data)
								+ dataOffset, data, recordLength);
				_stringhashTable[(string) tempData].dataIndex.push_back(
						recordLength);
			}
			free(tempData);
			break;
		}
		default:
			break;
		}
	}
	free(data);
	return 0;
}
RC BNLJoin::probe(void * rightData)
{
	int ret = -1;
	void *hashkey = malloc(PAGE_SIZE);
	void *data = malloc(PAGE_SIZE);
	RecordBasedFileManager *rightrbfm = RecordBasedFileManager::instance();
	vector<Attribute> attrs;
	vector<Attribute> rightattrs;
	rIter->getAttributes(rightattrs);
	lIter->getAttributes(attrs);
	int offset = 0;
	int size = 0;
	for (int i = 0; i < rightattrs.size(); i++) {
		if (rightattrs[i].name == condition.rhsAttr) {
			switch (rightattrs[i].type) {
			case TypeInt:
				size = sizeof(int);
				break;
			case TypeReal:
				size = sizeof(float);
				break;
			case TypeVarChar:
				memcpy(&size, (char *) rightData + offset, sizeof(int));
				size += sizeof(int);
				break;
			default:
				break;
			}
			memcpy(hashkey, (char *) rightData + offset, size);
			break;
		} else {
			switch (attrs[i].type) {
			case TypeInt: {
				offset += sizeof(int);
				break;
			}
			case TypeReal: {
				offset += sizeof(float);
				break;
			}
			case TypeVarChar: {
				int clen = 0;
				memcpy(&clen, (char *) rightData + offset, sizeof(int));
				offset += clen + sizeof(int);
				break;
			}
			default:
				break;
			}
		}
	}
	switch (conditionAttr.type) {
	case (TypeInt): {
		for (int j = 0; j < _inthashTable[*(int*) hashkey].dataIndex.size(); j++) {
			ret = 0;
			int dataOffset = 0;
			for (int i = 0; i < 0; i++)
				dataOffset += _inthashTable[*(int*) hashkey].dataIndex.at(i);
			int recordLength =
					_inthashTable[*(int*) hashkey].dataIndex.at(j);
			void * leftData = malloc(recordLength);
			memcpy(leftData,
					(char *) _inthashTable[*(int*) hashkey].data + dataOffset,
					recordLength);
			memcpy((char *) data, leftData, recordLength);
			int rightrecordLength = rightrbfm->getRecordSize4Cli(rightattrs,
					rightData);
			memcpy((char *) data + recordLength, rightData, rightrecordLength);
			_dataOutputBuffer.push(data);
			free(leftData);
		}
		break;
	}
	case (TypeReal): {
		for (int j = 0; j < _floathashTable[*(float*) hashkey].dataIndex.size(); j++) {
			ret = 0;
			int recordLength = _floathashTable[*(float*) hashkey].dataIndex.at(j);
			int dataOffset = 0;
			for (int i = 0; i < 0; i++)
				dataOffset += _floathashTable[*(float*) hashkey].dataIndex.at(i);
			void * leftData = malloc(recordLength);
			memcpy(leftData,
					(char *) _floathashTable[*(float*) hashkey].data
							+ dataOffset, recordLength);
			memcpy((char *) data, leftData, recordLength);
			int rightrecordLength = rightrbfm->getRecordSize4Cli(rightattrs,
					rightData);
			memcpy((char *) data + recordLength, rightData, rightrecordLength);
			_dataOutputBuffer.push(data);
			free(leftData);
		}
		break;
	}
	case (TypeVarChar): {
		int length = 0;
		memcpy(&length, hashkey, sizeof(int));
		char * tempData = new char[length + 1];
		tempData[length] = '\0';
		memcpy(tempData, (char*) hashkey + sizeof(int), length);
		for (int j = 0; j < _stringhashTable[(string) (char *) tempData].dataIndex.size(); j++) {
			ret = 0;
			int recordLength =
					_stringhashTable[(string) (char *) tempData].dataIndex.at(j);
			void * leftData = malloc(recordLength);
			int dataOffset = 0;
			for (int i = 0; i < 0; i++)
				dataOffset += _stringhashTable[(string)(char*) tempData].dataIndex.at(i);
			memcpy(leftData,
					(char *) _stringhashTable[(string) (char *) tempData].data
							+ dataOffset, recordLength);
			memcpy((char *) data, leftData, recordLength);
			int rightrecordLength = rightrbfm->getRecordSize4Cli(rightattrs,
					rightData);
			memcpy((char *) data + recordLength, rightData, rightrecordLength);
			_dataOutputBuffer.push(data);
			free(leftData);
		}
		break;
	}
	}
	free(hashkey);
	return ret;
}

BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numRecords     // # of records can be loaded into memory, i.e., memory block size (decided by the optimizer)
)
{
	lIter = leftIn;
	rIter = rightIn;
	this->condition = condition;
	this->numRecords = numRecords;
    //get the condition attribute type
    vector<Attribute> attrs;
    lIter->getAttributes(attrs);
    for (unsigned i = 0; i < attrs.size(); i++) {
        if (attrs.at(i).name == condition.lhsAttr) {
            conditionAttr = attrs.at(i);
            break;
        }
    }
    buildLeftHashTable();
}

RC BNLJoin::getNextTuple(void *data)
{
	int ret = 0;
	if(!_dataOutputBuffer.empty())
	{
		data = _dataOutputBuffer.front();
		_dataOutputBuffer.pop();
		return 0;
	}
	// recreate outputbuffer
	void * rightTuple = malloc(PAGE_SIZE);
	// if inputbuffer empty then create one
	if(_dataIntputBuffer.empty())
	{
		while (rIter->getNextTuple(rightTuple) != -1 && _dataIntputBuffer.size() < numRecords)
		{
			void * tempTuple = malloc(PAGE_SIZE);
			memcpy(tempTuple, rightTuple, PAGE_SIZE);
			_dataIntputBuffer.push(tempTuple);
		}
	}
	while (!_dataIntputBuffer.empty()) {
		rightTuple = _dataIntputBuffer.front();
		_dataIntputBuffer.pop();
		if (_dataIntputBuffer.empty()) {
			// inputbuffer empty recreate!
			while (rIter->getNextTuple(rightTuple) != -1
					&& _dataIntputBuffer.size() < numRecords) {
				void * tempTuple = malloc(PAGE_SIZE);
				memcpy(tempTuple, rightTuple, PAGE_SIZE);
				_dataIntputBuffer.push(tempTuple);
			}
		}
		ret = probe(rightTuple);
		// create outputbuffer success
		if (ret != -1) {
			RecordBasedFileManager *temprbfm = RecordBasedFileManager::instance();
			void * tempData = _dataOutputBuffer.front();
			memcpy(data,tempData,PAGE_SIZE);
			vector<Attribute> tempAttrs;
			getAttributes(tempAttrs);
			_dataOutputBuffer.pop();
			free(rightTuple);
			return 0;
		}
	}
	free(rightTuple);
	return -1;
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const
{
	vector<Attribute> lattrs;
	vector<Attribute> rattrs;
	lattrs.clear();
	rattrs.clear();
	lIter->getAttributes(lattrs);
	rIter->getAttributes(rattrs);
	for(int i = 0; i < lattrs.size(); i++)
	{
		attrs.push_back(lattrs.at(i));
	}
	for(int i = 0; i < rattrs.size(); i++)
	{
		attrs.push_back(rattrs.at(i));
	}
}

RC INLJoin::buildLeftHashTable()
{
	RC ret = 0;
	vector<Attribute> attrs;
	lIter->getAttributes(attrs);
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	void * data = malloc(PAGE_SIZE);
	while (lIter->getNextTuple(data) != -1) {
		switch (conditionAttr.type) {
		case (TypeInt): {
			void *intData = malloc(4);
			int offset = 0;
			int size = 0;
			for (int i = 0; i < attrs.size(); i++) {
				if (attrs[i].name == conditionAttr.name) {
					memcpy(intData, (char *) data + offset, sizeof(int));
					break;
				} else {
					switch (attrs[i].type) {
					case TypeInt: {
						offset += sizeof(int);
						break;
					}
					case TypeReal: {
						offset += sizeof(float);
						break;
					}
					case TypeVarChar: {
						int len = 0;
						memcpy(&len, (char *) data + offset, sizeof(int));
						offset += len + sizeof(int);
						break;
					}
					}
				}
			}
			int recordLength = rbfm->getRecordSize4Cli(attrs, data);
			if (_inthashTable.find(*(int*) intData) == _inthashTable.end()) {
				DataDir tempDataDir;
				tempDataDir.dataIndex.push_back(recordLength);
				tempDataDir.data = malloc(recordLength);
				memcpy(tempDataDir.data, data, recordLength);
				_inthashTable[*(int*) intData] = tempDataDir;
			} else {
				int dataoffset = 0;
				for (int i = 0;
						i < _inthashTable[*(int*) intData].dataIndex.size();
						i++)
					dataoffset += _inthashTable[*(int*) intData].dataIndex.at(
							i);
				_inthashTable[*(int*) intData].data = realloc(
						_inthashTable[*(int*) intData].data,
						dataoffset + recordLength);
				memcpy(
						(char *) (_inthashTable[*(int*) intData].data)
								+ dataoffset, data, recordLength);
				_inthashTable[*(int*) intData].dataIndex.push_back(
						recordLength);
			}
			free(intData);
			break;
		}
		case (TypeReal): {
			void *floatData = malloc(4);
			int offset = 0;
			int size = 0;
			for (int i = 0; i < attrs.size(); i++) {
				if (attrs[i].name == conditionAttr.name) {
					memcpy(floatData, (char *) data + offset, sizeof(float));
					break;
				} else {
					switch (attrs[i].type) {
					case TypeInt: {
						offset += sizeof(int);
						break;
					}
					case TypeReal: {
						offset += sizeof(float);
						break;
					}
					case TypeVarChar: {
						int len = 0;
						memcpy(&len, (char *) data + offset, sizeof(int));
						offset += len + sizeof(int);
						break;
					}
					}
				}
			}
			int recordLength = rbfm->getRecordSize4Cli(attrs, data);
			if (_floathashTable.find(*(float*) floatData)
					== _floathashTable.end()) {
				DataDir tempDataDir;
				tempDataDir.data = malloc(recordLength);
				tempDataDir.dataIndex.push_back(recordLength);
				memcpy(tempDataDir.data, data, recordLength);
				_floathashTable[*(float*) floatData] = tempDataDir;
			} else {
				int dataoffset = 0;
				for (int i = 0;
						i
								< _floathashTable[*(float*) floatData].dataIndex.size();
						i++)
					dataoffset +=
							_floathashTable[*(float*) floatData].dataIndex.at(
									i);
				_floathashTable[*(float*) floatData].data = realloc(
						_floathashTable[*(float*) floatData].data,
						dataoffset + recordLength);
				memcpy(
						(char *) (_floathashTable[*(float*) floatData].data)
								+ dataoffset, data, recordLength);
				_floathashTable[*(float*) floatData].dataIndex.push_back(
						recordLength);
			}
			free(floatData);
			break;
		}
		case (TypeVarChar): {
			void *stringData = malloc(PAGE_SIZE);
			int offset = 0;
			int size = 0;
			for (int i = 0; i < attrs.size(); i++) {
				if (attrs[i].name == conditionAttr.name) {
					int len = 0;
					memcpy(&len, (char *) data + offset, sizeof(int));
					memcpy(stringData, (char *) data + offset,
							len + sizeof(int));
					break;
				} else {
					switch (attrs[i].type) {
					case TypeInt: {
						offset += sizeof(int);
						break;
					}
					case TypeReal: {
						offset += sizeof(float);
						break;
					}
					case TypeVarChar: {
						int len = 0;
						memcpy(&len, (char *) data + offset, sizeof(int));
						offset += len + sizeof(int);
						break;
					}
					}
				}
			}
			int length = 0;
			memcpy(&length, stringData, sizeof(int));
			char * tempData = new char[length + 1];
			tempData[length] = '\0';
			memcpy(tempData, (char*) stringData + sizeof(int), length);
			//test whether we can find the same key
			//if not, add a new one
			int recordLength = rbfm->getRecordSize4Cli(attrs, data);
			if (_stringhashTable.find((string) tempData)
					== _stringhashTable.end()) {
				DataDir tampDataDir;
				tampDataDir.data = malloc(recordLength);
				tampDataDir.dataIndex.push_back(recordLength);
				memcpy(tampDataDir.data, data, recordLength);
				_stringhashTable[(string) tempData] = tampDataDir;
			} else {
				int dataOffset = 0;
				for (int i = 0;
						i < _stringhashTable[(string) tempData].dataIndex.size();
						i++)
					dataOffset +=
							_stringhashTable[(string) tempData].dataIndex.at(i);
				_stringhashTable[(string) tempData].data = realloc(
						_stringhashTable[(string) tempData].data,
						dataOffset + recordLength);
				memcpy(
						(char *) (_stringhashTable[(string) tempData].data)
								+ dataOffset, data, recordLength);
				_stringhashTable[(string) tempData].dataIndex.push_back(
						recordLength);
			}
			free(tempData);
			break;
		}
		default:
			break;
		}
	}
	free(data);
	return 0;
}

RC INLJoin::probe(void * rightData)
{
	int ret = -1;
	void *hashkey = malloc(PAGE_SIZE);
	void *data = malloc(PAGE_SIZE);
	RecordBasedFileManager *rightrbfm = RecordBasedFileManager::instance();
	vector<Attribute> attrs;
	vector<Attribute> rightattrs;
	rIter->getAttributes(rightattrs);
	lIter->getAttributes(attrs);
	int offset = 0;
	int size = 0;
	for (int i = 0; i < rightattrs.size(); i++) {
		if (rightattrs[i].name == condition.rhsAttr) {
			switch (rightattrs[i].type) {
			case TypeInt:
				size = sizeof(int);
				break;
			case TypeReal:
				size = sizeof(float);
				break;
			case TypeVarChar:
				memcpy(&size, (char *) rightData + offset, sizeof(int));
				size += sizeof(int);
				break;
			default:
				break;
			}
			memcpy(hashkey, (char *) rightData + offset, size);
			break;
		} else {
			switch (attrs[i].type) {
			case TypeInt: {
				offset += sizeof(int);
				break;
			}
			case TypeReal: {
				offset += sizeof(float);
				break;
			}
			case TypeVarChar: {
				int clen = 0;
				memcpy(&clen, (char *) rightData + offset, sizeof(int));
				offset += clen + sizeof(int);
				break;
			}
			default:
				break;
			}
		}
	}
	switch (conditionAttr.type) {
	case (TypeInt): {
		for (int j = 0; j < _inthashTable[*(int*) hashkey].dataIndex.size(); j++) {
			ret = 0;
			int dataOffset = 0;
			for (int i = 0; i < 0; i++)
				dataOffset += _inthashTable[*(int*) hashkey].dataIndex.at(i);
			int recordLength =
					_inthashTable[*(int*) hashkey].dataIndex.at(j);
			void * leftData = malloc(recordLength);
			memcpy(leftData,
					(char *) _inthashTable[*(int*) hashkey].data + dataOffset,
					recordLength);
			memcpy((char *) data, leftData, recordLength);
			int rightrecordLength = rightrbfm->getRecordSize4Cli(rightattrs,
					rightData);
			memcpy((char *) data + recordLength, rightData, rightrecordLength);
			_dataOutputBuffer.push(data);
			free(leftData);
		}
		break;
	}
	case (TypeReal): {
		for (int j = 0; j < _floathashTable[*(float*) hashkey].dataIndex.size(); j++) {
			ret = 0;
			int recordLength = _floathashTable[*(float*) hashkey].dataIndex.at(j);
			int dataOffset = 0;
			for (int i = 0; i < 0; i++)
				dataOffset += _floathashTable[*(float*) hashkey].dataIndex.at(i);
			void * leftData = malloc(recordLength);
			memcpy(leftData,
					(char *) _floathashTable[*(float*) hashkey].data
							+ dataOffset, recordLength);
			memcpy((char *) data, leftData, recordLength);
			int rightrecordLength = rightrbfm->getRecordSize4Cli(rightattrs,
					rightData);
			memcpy((char *) data + recordLength, rightData, rightrecordLength);
			_dataOutputBuffer.push(data);
			free(leftData);
		}
		break;
	}
	case (TypeVarChar): {
		int length = 0;
		memcpy(&length, hashkey, sizeof(int));
		char * tempData = new char[length + 1];
		tempData[length] = '\0';
		memcpy(tempData, (char*) hashkey + sizeof(int), length);
		for (int j = 0; j < _stringhashTable[(string) (char *) tempData].dataIndex.size(); j++) {
			ret = 0;
			int recordLength =
					_stringhashTable[(string) (char *) tempData].dataIndex.at(j);
			void * leftData = malloc(recordLength);
			int dataOffset = 0;
			for (int i = 0; i < 0; i++)
				dataOffset += _stringhashTable[(string)(char*) tempData].dataIndex.at(i);
			memcpy(leftData,
					(char *) _stringhashTable[(string) (char *) tempData].data
							+ dataOffset, recordLength);
			memcpy((char *) data, leftData, recordLength);
			int rightrecordLength = rightrbfm->getRecordSize4Cli(rightattrs,
					rightData);
			memcpy((char *) data + recordLength, rightData, rightrecordLength);
			_dataOutputBuffer.push(data);
			free(leftData);
		}
		break;
	}
	}
	free(hashkey);
	return ret;
}

INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
		IndexScan *rightIn,          // IndexScan Iterator of input S
		const Condition &condition   // Join condition
		)
{
	lIter = leftIn;
	rIter = rightIn;
	this->condition = condition;
    //get the condition attribute type
    vector<Attribute> attrs;
    lIter->getAttributes(attrs);
    for (unsigned i = 0; i < attrs.size(); i++) {
        if (attrs.at(i).name == condition.lhsAttr) {
            conditionAttr = attrs.at(i);
            break;
        }
    }
    buildLeftHashTable();
}

RC INLJoin::getNextTuple(void *data)
{
	int ret = 0;
	if(!_dataOutputBuffer.empty())
	{
		data = _dataOutputBuffer.front();
		_dataOutputBuffer.pop();
		return 0;
	}
	// recreate outputbuffer
	void * rightTuple = malloc(PAGE_SIZE);
	while(rIter->getNextTuple(rightTuple) != -1)
	{
		ret = probe(rightTuple);
		// create outputbuffer success
		if (ret != -1) {
			RecordBasedFileManager *temprbfm = RecordBasedFileManager::instance();
			void * tempData = _dataOutputBuffer.front();
			memcpy(data,tempData,PAGE_SIZE);
			vector<Attribute> tempAttrs;
			getAttributes(tempAttrs);
			_dataOutputBuffer.pop();
			free(rightTuple);
			return 0;
		}
	}
	free(rightTuple);
	return -1;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	vector<Attribute> lattrs;
	vector<Attribute> rattrs;
	lattrs.clear();
	rattrs.clear();
	lIter->getAttributes(lattrs);
	rIter->getAttributes(rattrs);
	for(int i = 0; i < lattrs.size(); i++)
	{
		attrs.push_back(lattrs.at(i));
	}
	for(int i = 0; i < rattrs.size(); i++)
	{
		attrs.push_back(rattrs.at(i));
	}
}

RC Aggregate::setDataOutputmap()
{
	RC ret = 0;
	vector<Attribute> attrs;
	iter->getAttributes(attrs);
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	void * data = malloc(PAGE_SIZE);
	while (iter->getNextTuple(data) != -1) {
		if (isgroup == 1) {
			switch (groupAttr.type) {
			case (TypeInt): {
				void *intData = malloc(4);
				AggreDir tempAggr;
				tempAggr.value = malloc(sizeof(int));
				int offset = 0;
				int size = 0;
				// get the group and attr value
				for (int i = 0; i < attrs.size(); i++) {
					if (attrs[i].name == groupAttr.name) {
						memcpy(intData, (char *) data + offset, sizeof(int));
					}
					if (attrs[i].name == aggAttr.name) {
						memcpy(tempAggr.value, (char *) data + offset, sizeof(int));
					}
					switch (attrs[i].type) {
					case TypeInt: {
						offset += sizeof(int);
						break;
					}
					case TypeReal: {
						offset += sizeof(float);
						break;
					}
					case TypeVarChar: {
						int len = 0;
						memcpy(&len, (char *) data + offset, sizeof(int));
						offset += len + sizeof(int);
						break;
					}
					}

				}
				if (_intOutMap.find(*(int*) intData)
						== _intOutMap.end()) {
					tempAggr.count = 1;
					_intOutMap[*(int*) intData] = tempAggr;
				} else {
					float origindata, lasteddata;
					memcpy(&origindata,_intOutMap[*(int*) intData].value,sizeof(float));
					memcpy(&lasteddata,tempAggr.value,sizeof(float));
					switch(op)
					{
					case MIN:
						if(lasteddata < origindata)
							_intOutMap[*(int*) intData] = tempAggr;
						break;
					case MAX:
						if(lasteddata > origindata)
							_intOutMap[*(int*) intData] = tempAggr;
						break;
					case SUM:
					case AVG:
					case COUNT:
						lasteddata += origindata;
						memcpy(tempAggr.value,&lasteddata,sizeof(int));
						tempAggr.count++;
						_intOutMap[*(int*) intData] = tempAggr;
						break;
					}
				}
				free(intData);
				break;
			}
			case (TypeReal): {
				void *floatData = malloc(4);
				AggreDir tempAggr;
				tempAggr.value = malloc(sizeof(float));
				int offset = 0;
				int size = 0;
				// get the group and attr value
				for (int i = 0; i < attrs.size(); i++) {
					if (attrs[i].name == groupAttr.name) {
						memcpy(floatData, (char *) data + offset, sizeof(float));
					}
					if (attrs[i].name == aggAttr.name) {
						memcpy(tempAggr.value, (char *) data + offset, sizeof(float));
					}
					switch (attrs[i].type) {
					case TypeInt: {
						offset += sizeof(int);
						break;
					}
					case TypeReal: {
						offset += sizeof(float);
						break;
					}
					case TypeVarChar: {
						int len = 0;
						memcpy(&len, (char *) data + offset, sizeof(int));
						offset += len + sizeof(int);
						break;
					}
					}

				}
				if (_floatOutMap.find(*(float*) floatData)
						== _floatOutMap.end()) {
					tempAggr.count = 1;
					_floatOutMap[*(float*) floatData] = tempAggr;
				} else {
					float origindata, lasteddata;
					memcpy(&origindata,_floatOutMap[*(float*) floatData].value,sizeof(float));
					memcpy(&lasteddata,tempAggr.value,sizeof(float));
					switch(op)
					{
					case MIN:
						if(lasteddata < origindata)
							_floatOutMap[*(float*) floatData] = tempAggr;
						break;
					case MAX:
						if(lasteddata > origindata)
							_floatOutMap[*(float*) floatData] = tempAggr;
						break;
					case SUM:
					case AVG:
					case COUNT:
						lasteddata += origindata;
						memcpy(tempAggr.value,&lasteddata,sizeof(int));
						tempAggr.count++;
						_floatOutMap[*(float*) floatData] = tempAggr;
						break;
					}
				}
				free(floatData);
				break;
			}
			case (TypeVarChar): {
				void *stringData = malloc(PAGE_SIZE);
				AggreDir tempAggr;
				tempAggr.value = malloc(sizeof(float));
				int offset = 0;
				int size = 0;
				for (int i = 0; i < attrs.size(); i++) {
					if (attrs[i].name == groupAttr.name) {
						int len = 0;
						memcpy(&len, (char *) data + offset, sizeof(int));
						memcpy(stringData, (char *) data + offset,
								len + sizeof(int));
						len = len + sizeof(int);
						tempAggr.groupvalue = malloc(len);
						memcpy(&tempAggr.grouplength,&len,sizeof(int));
						memcpy(tempAggr.groupvalue,(char *) data + offset,len);
					}
					if (attrs[i].name == aggAttr.name)
					{
						memcpy(tempAggr.value, (char *) data + offset, sizeof(float));
					}
					switch (attrs[i].type) {
					case TypeInt: {
						offset += sizeof(int);
						break;
					}
					case TypeReal: {
						offset += sizeof(float);
						break;
					}
					case TypeVarChar: {
						int len = 0;
						memcpy(&len, (char *) data + offset, sizeof(int));
						offset += len + sizeof(int);
						break;
					}
					}
				}
				int length = 0;
				memcpy(&length, stringData, sizeof(int));
				char * tempData = new char[length + 1];
				tempData[length] = '\0';
				memcpy(tempData, (char*) stringData  + sizeof(int), length);
				//test whether we can find the same key
				//if not, add a new one
				if (_stringOutMap.find((string) tempData)
						== _stringOutMap.end()) {
					tempAggr.count = 1;
					_stringOutMap[(string) tempData] = tempAggr;
				} else {
					float origindata, lasteddata;
					memcpy(&origindata,_stringOutMap[(string) tempData].value,sizeof(float));
					memcpy(&lasteddata,tempAggr.value,sizeof(float));
					switch(op)
					{
					case MIN:
						if(lasteddata < origindata)
							_stringOutMap[(string) tempData] = tempAggr;
						break;
					case MAX:
						if(lasteddata > origindata)
							_stringOutMap[(string) tempData] = tempAggr;
						break;
					case SUM:
					case AVG:
					case COUNT:
						lasteddata += origindata;
						memcpy(tempAggr.value,&lasteddata,sizeof(int));
						tempAggr.count++;
						_stringOutMap[(string) tempData] = tempAggr;
						break;
					}
				}
				free(tempData);
				break;
			}
			default:
				break;
			}
		}
		else
		{
			void *intData = malloc(4);
			int judge = 0;
			memcpy(intData,&judge,sizeof(int));
			AggreDir tempAggr;
			tempAggr.value = malloc(sizeof(int));
			int offset = 0;
			int size = 0;
			// get the group and attr value
			for (int i = 0; i < attrs.size(); i++) {
				if (attrs[i].name == aggAttr.name) {
					memcpy(tempAggr.value, (char *) data + offset, sizeof(int));
				}
				switch (attrs[i].type) {
				case TypeInt: {
					offset += sizeof(int);
					break;
				}
				case TypeReal: {
					offset += sizeof(float);
					break;
				}
				case TypeVarChar: {
					int len = 0;
					memcpy(&len, (char *) data + offset, sizeof(int));
					offset += len + sizeof(int);
					break;
				}
				}

			}
			map<int,AggreDir>::iterator mapiter = _intOutMap.find(*(int*) intData);
			if (mapiter == _intOutMap.end()) {
				tempAggr.count = 1;
				_intOutMap[*(int*) intData] = tempAggr;
			} else {
				float origindata, lasteddata;
				memcpy(&origindata, mapiter->second.value,
						sizeof(float));
				memcpy(&lasteddata, tempAggr.value, sizeof(float));
				switch (op) {
				case MIN:
					if (lasteddata < origindata)
						mapiter->second = tempAggr;
					break;
				case MAX:
					if (lasteddata > origindata)
						mapiter->second = tempAggr;
					break;
				case SUM:
				case AVG:
				case COUNT:
					lasteddata += origindata;
					memcpy(tempAggr.value, &lasteddata, sizeof(int));
					tempAggr.count++;
					mapiter->second = tempAggr;
					break;
				}
			}
			free(intData);
		}
	}
	free(data);
	return 0;
}

Aggregate::Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        )
{
//	cout << aggAttr.name << endl;
//	cout << op << endl;
//	vector<Attribute> attrs;
//	input->getAttributes(attrs);
//	void * data = malloc(PAGE_SIZE);
//	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
//	if(input->getNextTuple(data) != -1)
//		rbfm->printRecord(attrs,data);

	iter = input;
	this->aggAttr = aggAttr;
	this->op = op;
	isgroup = 0;
	setDataOutputmap();

}

Aggregate::Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op,              // Aggregate operation
                  const unsigned numPartitions // Number of partitions for input (decided by the optimizer)
        )
{
//	cout << aggAttr.name << endl;
//	cout << groupAttr.name << endl;
//	cout << op << endl;
//	vector<Attribute> attrs;
//	input->getAttributes(attrs);
//	void * data = malloc(PAGE_SIZE);
//	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
//	if(input->getNextTuple(data) != -1)
//		rbfm->printRecord(attrs,data);
	iter = input;
	this->aggAttr = aggAttr;
	this->groupAttr = groupAttr;
	this->op = op;
	this->numPartitions = numPartitions;
	isgroup = 1;
	setDataOutputmap();
}

RC Aggregate::getNextTuple(void *data)
{
	map<int,AggreDir>::iterator iter;
	map<float,AggreDir>::iterator floatiter;
	map<string,AggreDir>::iterator stringiter;;
	float sum;
	float avg;
	void * stringData;
	int offset = 0;
	int stringlength = 0;
	if(isgroup == 0)
	{
		iter = _intOutMap.find(0);
		if(iter != _intOutMap.end())
		{
			switch (op) {
			case MIN:
			case MAX:
			case SUM:
				memcpy(data, _intOutMap[0].value, sizeof(float));
				break;
			case AVG:
				memcpy(&sum, _intOutMap[0].value, sizeof(float));
				avg = sum / (float) _intOutMap[0].count;
				memcpy(data, &avg, sizeof(float));
				break;
			case COUNT:
				memcpy(data, &_intOutMap[0].count, sizeof(int));
				break;
			}
			_intOutMap.erase(0);
			return 0;
		}
	}
	else
	{
		switch(groupAttr.type)
		{
		case TypeInt:
			iter = _intOutMap.begin();
			if(iter != _intOutMap.end())
			{
				memcpy(data, &iter->first, sizeof(int));
				offset = sizeof(int);
				switch (op) {
				case MIN:
				case MAX:
				case SUM:
					memcpy(data + offset, iter->second.value, sizeof(float));
					break;
				case AVG:
					memcpy(&sum, iter->second.value, sizeof(float));
					avg = sum / (float) iter->second.count;
					memcpy(data + offset, &avg, sizeof(float));
					break;
				case COUNT:
					memcpy(data + offset, &iter->second.count, sizeof(int));
					break;
				}
				_intOutMap.erase(iter->first);
				return 0;
			}
			break;
		case TypeReal:
			floatiter = _floatOutMap.begin();
			if (floatiter != _floatOutMap.end()) {
				memcpy(data, &floatiter->first, sizeof(float));
				offset = sizeof(float);
				switch (op) {
				case MIN:
				case MAX:
				case SUM:
					memcpy(data + offset, floatiter->second.value, sizeof(float));
					break;
				case AVG:
					memcpy(&sum, floatiter->second.value, sizeof(float));
					avg = sum / (float) floatiter->second.count;
					memcpy(data + offset, &avg, sizeof(float));
					break;
				case COUNT:
					memcpy(data + offset, &floatiter->second.count, sizeof(int));
					break;
				}
				_floatOutMap.erase(floatiter->first);
				return 0;
			}
			break;
		case TypeVarChar:
			stringiter = _stringOutMap.begin();
			if (stringiter != _stringOutMap.end()) {
				memcpy(&stringlength,&stringiter->second.grouplength,sizeof(int));
				memcpy(data, stringiter->second.groupvalue, stringlength);
				offset = stringlength;
				switch (op) {
				case MIN:
				case MAX:
				case SUM:
					memcpy(data + offset, stringiter->second.value, sizeof(float));
					break;
				case AVG:
					memcpy(&sum, stringiter->second.value, sizeof(float));
					avg = sum / (float) stringiter->second.count;
					memcpy(data + offset, &avg, sizeof(float));
					break;
				case COUNT:
					memcpy(data + offset, &stringiter->second.count, sizeof(int));
					break;
				}
				_stringOutMap.erase(stringiter->first);
				return 0;
			}
			break;
		}
	}
	return -1;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const
{
	if(isgroup == 1)
		attrs.push_back(groupAttr);
	if(op == COUNT)
	{
		Attribute tempAttr;
		tempAttr.name = aggAttr.name;
		tempAttr.length = aggAttr.length;
		tempAttr.type = TypeInt;
		attrs.push_back(tempAttr);
	}
	else
	{
		attrs.push_back(aggAttr);
	}

	const char * aggOpName[] = {"MIN", "MAX", "SUM", "AVG", "COUNT"};
	attrs[attrs.size() -1].name = (string)aggOpName[op] + "(" + attrs[attrs.size() -1].name + ")";
}


