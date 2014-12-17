#include "rbfm.h"

#include <iostream>
#include <string.h>
#include <stdlib.h>

using namespace std;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	PagedFileManager * pfm = PagedFileManager::instance();
	return pfm->createFile(fileName.c_str());
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	PagedFileManager * pfm = PagedFileManager::instance();
	return pfm->destroyFile(fileName.c_str());
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	PagedFileManager * pfm = PagedFileManager::instance();
	return pfm->openFile(fileName.c_str(),fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	PagedFileManager * pfm = PagedFileManager::instance();
	return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	int recordSize = 0, recordOffset = 0, fieldOffset = 0, offset = 0;
	recordSize = getRecordSize(recordDescriptor,data);
	int pageNumber = getFreePage4Select(fileHandle,recordSize,data);
	int recordsNumber = recordDescriptor.size();
	void * buffer = malloc(PAGE_SIZE);
	struct SlotDir * slotDir = (struct SlotDir *)malloc(sizeof(struct SlotDir));
	struct RecordDir * recordDir = (struct RecordDir *)malloc(sizeof(struct RecordDir));
	void * recordBuffer = malloc(recordSize);
	memcpy(recordBuffer,&recordsNumber,sizeof(int));
	recordOffset = sizeof(int) + recordsNumber * sizeof(struct RecordDir);
	for(int i = 0; i < recordsNumber; i++)
	{
		int attrNameLength = recordDescriptor[i].name.length();
		int fieldLength = 0;

		switch(recordDescriptor[i].type)
		{
		case 0:
			fieldLength = sizeof(int);
			offset += fieldLength;
			break;
		case 1:
			fieldLength = sizeof(float);
			offset += fieldLength;
			break;
		case 2:
			int nameLength;
			memcpy(&nameLength,(char *)data + offset,sizeof(int));
			fieldLength = nameLength + sizeof(int);
			offset += fieldLength;
			break;
		}
		recordDir->attrNameLength = attrNameLength;
		recordDir->fieldLength = fieldLength;
		recordDir->offset = recordOffset;
		memcpy(recordBuffer + sizeof(int) + i * sizeof(struct RecordDir),recordDir,sizeof(struct RecordDir));
		memcpy((char *)recordBuffer + recordOffset, recordDescriptor[i].name.c_str(),attrNameLength);
		memcpy(recordBuffer + recordOffset + attrNameLength, data + fieldOffset ,fieldLength);
		recordOffset = recordOffset + attrNameLength + fieldLength;
		fieldOffset += fieldLength;
	}
	if(pageNumber == -1)
	{
		slotDir->id = 1;
		slotDir->offset = 0;
		slotDir->length = recordSize;
		slotDir->iftombstone = 0;
		rid.slotNum = 1;
		rid.pageNum = fileHandle.getNumberOfPages();
		int freespace = recordSize, slotnumber = 1;
		memcpy(buffer,(char *)recordBuffer,recordSize);
		memcpy((char *)buffer + PAGE_SIZE - sizeof(int), &freespace, sizeof(int));
		memcpy((char *)buffer + PAGE_SIZE - 2 * sizeof(int), &slotnumber, sizeof(int));
		memcpy((char *)buffer + PAGE_SIZE - 2 * sizeof(int) - sizeof(struct SlotDir), slotDir, sizeof(struct SlotDir));
		fileHandle.appendPage(buffer);
	}
	else
	{
		fileHandle.readPage(pageNumber,buffer);
		int slotnumber, freespace, slotnewid;
		memcpy(&freespace,(char *)buffer + PAGE_SIZE - sizeof(int),sizeof(int));
		memcpy(&slotnumber,(char *)buffer + PAGE_SIZE - 2 * sizeof(int),sizeof(int));
		memcpy(&slotnewid,(char *)buffer + PAGE_SIZE - 2 * sizeof(int) - slotnumber * sizeof(struct SlotDir),sizeof(unsigned));
		slotnewid++;
		memcpy((char *)buffer + freespace,recordBuffer,recordSize);
		slotDir->id = slotnewid;
		slotDir->offset = freespace;
		slotDir->length = recordSize;
		slotDir->iftombstone = 0;
		rid.pageNum = pageNumber;
		rid.slotNum = slotnewid;
		memcpy((char *)buffer + PAGE_SIZE - 2 * sizeof(int) - (slotnumber + 1) * sizeof(struct SlotDir),slotDir,sizeof(struct SlotDir));
		freespace = freespace + recordSize;
		slotnumber++;
		memcpy((char *)buffer + PAGE_SIZE - sizeof(int),&freespace,sizeof(int));
		memcpy((char *)buffer + PAGE_SIZE - 2 * sizeof(int),&slotnumber,sizeof(int));
		fileHandle.writePage(pageNumber,buffer);
	}
	free(buffer);
	free(slotDir);
	free(recordBuffer);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	unsigned pagenum = rid.pageNum;
	unsigned slotnum = rid.slotNum;
	if(pagenum >= fileHandle.getNumberOfPages())
	{
		return -1;
	}
	void * buffer = malloc(PAGE_SIZE);
	int slotnumber;
	struct SlotDir * slotDir = (struct SlotDir *)malloc(sizeof(struct SlotDir));
	struct RecordDir * recordDir = (struct RecordDir *)malloc(sizeof(struct RecordDir));
	fileHandle.readPage(pagenum,buffer);
	memcpy(&slotnumber,(char *) buffer + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
	for(int i = 0; i < slotnumber; i++)
	{
		memcpy(slotDir,(char *) buffer + PAGE_SIZE - 2 * sizeof(int) - (i + 1) * sizeof(struct SlotDir),sizeof(struct SlotDir));
		if(slotDir->id == (int)slotnum)
		{
			if(slotDir->iftombstone == 0)
			{
				int fieldNumber = 0, offset = 0;;
				memcpy(&fieldNumber, buffer + slotDir->offset, sizeof(int));
				for(int j = 0; j < fieldNumber; j++)
				{
					memcpy(recordDir,buffer + slotDir->offset + sizeof(int) + j * sizeof(struct RecordDir), sizeof(struct RecordDir));
					void * attrName = malloc(recordDir->attrNameLength);

					memcpy(attrName,(char *)buffer + slotDir->offset + recordDir->offset,recordDir->attrNameLength);
					for(int z = 0; z < recordDescriptor.size(); z++)
					{
						if(recordDescriptor[z].name.length() == recordDir->attrNameLength && memcmp(attrName,recordDescriptor[z].name.c_str(),recordDir->attrNameLength) ==  0)
						{
							memcpy(data + offset,buffer + slotDir->offset + recordDir->offset + recordDir->attrNameLength,recordDir->fieldLength);
							offset += recordDir->fieldLength;
						}
					}
					free(attrName);
				}
			}
			else
			{
				void * prid = malloc(sizeof(RID));
				memcpy(prid, (char *)buffer + slotDir->offset, slotDir->length);
				RID rid2 = *(RID *)prid;
				readRecord(fileHandle,recordDescriptor,rid2,data);
			}
			free(slotDir);
			free(buffer);
			free(recordDir);
			return 0;
		}
	}
	return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	int attributeSize = recordDescriptor.size();
	int offset = 0, namelength = 0;
	int intval;
	float floatval;
	for(int i = 0; i < attributeSize; i++){
		switch(recordDescriptor[i].type)
		{
		case 0:
			memcpy(&intval,(char *)data + offset,sizeof(int));
			cout << recordDescriptor[i].name << ":" << intval << endl;
			offset += sizeof(int);
			break;
		case 1:
			memcpy(&floatval,(char *)data + offset,sizeof(float));
			cout << recordDescriptor[i].name << ":" << floatval << endl;
			offset += sizeof(float);
			break;
		case 2:
			memcpy(&namelength,(char *)data + offset,sizeof(int));
			char * name = (char *)malloc(namelength + 1);
			offset += sizeof(int);
			memset(name,'\0',namelength + 1);
			memcpy(name,(char *)data + offset,namelength);
			cout << recordDescriptor[i].name << ":" << name << endl;
			offset += namelength;
			free(name);
			break;
		}
	}
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
	int recordSize = getRecordSize(recordDescriptor,data);
	unsigned pagenum = rid.pageNum;
	unsigned slotnum = rid.slotNum;
	if(pagenum >= fileHandle.getNumberOfPages())
	{
			return -1;
	}
	void * buffer = malloc(PAGE_SIZE);
	int slotnumber;
	struct SlotDir * slotDir = (struct SlotDir *)malloc(sizeof(struct SlotDir));
	fileHandle.readPage(pagenum,buffer);
	int freespace;
	memcpy(&freespace,(char *)buffer + PAGE_SIZE - sizeof(int), sizeof(int));
	memcpy(&slotnumber,(char *) buffer + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
	for(int i = 0; i < slotnumber; i++)
	{
		memcpy(slotDir,(char *) buffer + PAGE_SIZE - 2 * sizeof(int) - (i + 1) * sizeof(struct SlotDir),sizeof(struct SlotDir));
		if(slotDir->id == (int)slotnum)
		{
			if(slotDir->iftombstone == 1)
			{
				void * prid = malloc(sizeof(RID));
				memcpy(prid,(char *)buffer + slotDir->offset, slotDir->length);
				RID rid2 = *(RID *)prid;
				deleteRecord(fileHandle, recordDescriptor, rid2);
				free(prid);
			}
			int addLength;
			int pageNumber = getFreePage4Update(fileHandle,recordSize,rid);
			int newRecordSize;
			void * tempData = malloc(freespace - slotDir->offset - slotDir->length);
			if(pageNumber == (int)pagenum)
			{
				newRecordSize = recordSize;
				//update the record
				memcpy(tempData,(char *)buffer + slotDir->offset + slotDir->length,freespace - slotDir->offset - slotDir->length);
				memcpy((char *)buffer + slotDir->offset + newRecordSize,tempData,freespace - slotDir->offset - slotDir->length);
//				memcpy((char *)buffer + slotDir->offset,data,newRecordSize);
				int recordsNumber = 0, offset = 0, recordOffset = 0, fieldOffset = 0;
				void * recordBuffer = malloc(recordSize);
				struct RecordDir * recordDir = (struct RecordDir *)malloc(sizeof(struct RecordDir));
				recordsNumber = recordDescriptor.size();
				memcpy(recordBuffer,&recordsNumber,sizeof(int));
				recordOffset = sizeof(int) + recordsNumber * sizeof(struct RecordDir);
				for(int j = 0; j < recordsNumber; j++)
				{
					int attrNameLength = recordDescriptor[j].name.length();
					int fieldLength = 0;
					switch(recordDescriptor[j].type)
					{
					case 0:
						fieldLength = sizeof(int);
						offset += fieldLength;
						break;
					case 1:
						fieldLength = sizeof(float);
						offset += fieldLength;
						break;
					case 2:
						int nameLength;
						memcpy(&nameLength,(char *)data + offset,sizeof(int));
						fieldLength = nameLength + sizeof(int);
						offset += fieldLength;
						break;
					}
					recordDir->attrNameLength = attrNameLength;
					recordDir->fieldLength = fieldLength;
					recordDir->offset = recordOffset;
					memcpy(recordBuffer + sizeof(int) + j * sizeof(struct RecordDir),recordDir,sizeof(struct RecordDir));
					memcpy((char *)recordBuffer + recordOffset, recordDescriptor[j].name.c_str(),attrNameLength);
					memcpy(recordBuffer + recordOffset + attrNameLength, data + fieldOffset ,fieldLength);
					recordOffset = recordOffset + attrNameLength + fieldLength;
					fieldOffset += fieldLength;
				}
				memcpy((char *)buffer + slotDir->offset,recordBuffer,newRecordSize);
				freespace = freespace - slotDir->length + newRecordSize;
				addLength = newRecordSize - slotDir->length;
				slotDir->length = newRecordSize;
				slotDir->iftombstone = 0;
				free(recordBuffer);
				free(recordDir);
			}
			else
			{
				RID newrid;
				insertRecord(fileHandle, recordDescriptor, data, newrid);
				RID * pnewrid = (RID *)malloc(sizeof(RID *));
				pnewrid = &newrid;
				newRecordSize = sizeof(RID);
				memcpy(tempData,(char *)buffer + slotDir->offset + slotDir->length,freespace - slotDir->offset - slotDir->length);
				memcpy((char *)buffer + slotDir->offset + newRecordSize,tempData,freespace - slotDir->offset - slotDir->length);
				memcpy((char *)buffer + slotDir->offset,pnewrid,newRecordSize);
				freespace = freespace - slotDir->length + newRecordSize;
				addLength = newRecordSize - slotDir->length;
				slotDir->length = newRecordSize;
				slotDir->iftombstone = 1;
			}
			memcpy((char *)buffer + PAGE_SIZE - 2 * sizeof(int) - (i + 1) * sizeof(struct SlotDir),slotDir,sizeof(struct SlotDir));
			//replace the new offset to every slotDir
			for(int j = i + 1; j < slotnumber; j++)
			{
				memcpy(slotDir,(char *)buffer + PAGE_SIZE - 2 * sizeof(int) - (j + 1) * sizeof(struct SlotDir),sizeof(struct SlotDir));
				slotDir->offset += addLength;
				memcpy((char *)buffer + PAGE_SIZE - 2 * sizeof(int) - (j + 1) * sizeof(struct SlotDir),slotDir,sizeof(struct SlotDir));
			}
			memcpy((char *)buffer + PAGE_SIZE - sizeof(int),&freespace,sizeof(int));
			fileHandle.writePage(pagenum,buffer);
			free(slotDir);
			free(buffer);
			free(tempData);
			return 0;
		}
	}
	return -1;
}

RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle)
{
	const char* fileName = fileHandle.getFileName();
	closeFile(fileHandle);
	destroyFile(fileName);
	createFile(fileName);
	openFile(fileName,fileHandle);
	return 0;

}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
	unsigned pagenum = rid.pageNum;
	unsigned slotnum = rid.slotNum;
	if(pagenum >= fileHandle.getNumberOfPages())
	{
		return -1;
	}
	void * buffer = malloc(PAGE_SIZE);
	int slotnumber;
	struct SlotDir * slotDir = (struct SlotDir *)malloc(sizeof(struct SlotDir));
	fileHandle.readPage(pagenum,buffer);
	memcpy(&slotnumber,(char *) buffer + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
	for(int i = 0; i < slotnumber; i++)
	{
		memcpy(slotDir,(char *) buffer + PAGE_SIZE - 2 * sizeof(int) - (i + 1) * sizeof(struct SlotDir),sizeof(struct SlotDir));
		if(slotDir->id == (int)slotnum)
		{
			if(slotDir->iftombstone == 1)
			{
				void * prid = malloc(sizeof(RID));
				memcpy(prid,(char *)buffer + slotDir->offset, slotDir->length);
				RID rid2 = *(RID *)prid;
				deleteRecord(fileHandle, recordDescriptor, rid2);
				free(prid);
			}
			slotDir->id = -1;
			memcpy((char *)buffer + PAGE_SIZE - 2 * sizeof(int) - (i + 1) * sizeof(struct SlotDir),slotDir,sizeof(struct SlotDir));
			fileHandle.writePage(pagenum,buffer);
			free(slotDir);
			free(buffer);
			reorganizePage(fileHandle,recordDescriptor,pagenum);
			return 0;
		}
	}
	free(slotDir);
	free(buffer);
	return -1;
}

RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber)
{
	PageNum pageNums = fileHandle.getNumberOfPages();
	if(pageNumber >= pageNums)
		return -1;
	void * buffer = malloc(PAGE_SIZE);
	int slotnumber;
	struct SlotDir * slotDir = (struct SlotDir *)malloc(sizeof(struct SlotDir));
    fileHandle.readPage(pageNumber, buffer);
	memcpy(&slotnumber,(char *)buffer + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
	for(int i = 0; i < slotnumber; i++)
	{
		memcpy(slotDir,(char *)buffer + PAGE_SIZE - 2 * sizeof(int) - (i + 1) * sizeof(struct SlotDir), sizeof(struct SlotDir));
		if(slotDir->id == -1)
		{
			int deleteOffset = slotDir->offset;
			int deleteLength = slotDir->length;
			int freespace;
			memcpy(&freespace,(char *)buffer + PAGE_SIZE - sizeof(int), sizeof(int));
			//delete the record
			if(freespace > 0)
			{
				void *tempData = malloc(freespace - deleteOffset - deleteLength);
				memcpy(tempData,(char *)buffer + deleteOffset + deleteLength,freespace - deleteOffset - deleteLength);
				memcpy((char *)buffer + deleteOffset, tempData, freespace - deleteOffset - deleteLength);
				//save the new freespace
				freespace = freespace - deleteLength;
				memcpy((char *)buffer + PAGE_SIZE - sizeof(int), &freespace, sizeof(int));
				free(tempData);
			}
			//delete the slotDir
			void *tempSlots = malloc((slotnumber - 1 - i) * sizeof(struct SlotDir));
			memcpy(tempSlots,(char *)buffer + PAGE_SIZE - 2 * sizeof(int) - slotnumber * sizeof(struct SlotDir),(slotnumber - 1 - i) * sizeof(struct SlotDir));
			memcpy((char *)buffer + PAGE_SIZE - 2 * sizeof(int) - (slotnumber - 1) * sizeof(struct SlotDir),tempSlots, (slotnumber - 1 - i) * sizeof(struct SlotDir));
			//save the new slotnumber
			slotnumber--;
			memcpy((char *)buffer + PAGE_SIZE - 2 * sizeof(int),&slotnumber,sizeof(int));
			//replace the new offset to every slotDir
			for(int j = i; j < slotnumber; j++)
			{
				memcpy(slotDir,(char *)buffer + PAGE_SIZE - 2 * sizeof(int) - (j + 1) * sizeof(struct SlotDir), sizeof(struct SlotDir));
				slotDir->offset -= deleteLength;
				memcpy((char *)buffer + PAGE_SIZE - 2 * sizeof(int) - (j + 1) * sizeof(struct SlotDir),slotDir,sizeof(struct SlotDir));
			}
			fileHandle.writePage(pageNumber,buffer);
			free(tempSlots);
			free(buffer);
			free(slotDir);
			return 0;
		}
	}
	free(buffer);
	free(slotDir);
	return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data)
{
	void *buffer = malloc(PAGE_SIZE);
	if(readRecord(fileHandle,recordDescriptor,rid,buffer) == -1)
	{
		free(buffer);
		return -1;
	}
	int attributeSize = recordDescriptor.size();
	int offset = 0, namelength = 0;
	for(int i = 0; i < attributeSize; i++){
		switch(recordDescriptor[i].type)
		{
		case 0:
			if(recordDescriptor[i].name == attributeName)
			{
				memcpy(data,(char *)buffer+offset,sizeof(int));
				return 0;
			}
			offset += sizeof(int);
			break;
		case 1:
			if(recordDescriptor[i].name == attributeName)
			{
				memcpy(data,(char *)buffer+offset,sizeof(float));
				return 0;
			}
			offset += sizeof(float);
			break;
		case 2:
			if(recordDescriptor[i].name == attributeName)
			{
				memcpy(&namelength,(char *)buffer + offset,sizeof(int));
				memcpy(data,(char *)buffer+offset,namelength+sizeof(int));
				return 0;
			}
			memcpy(&namelength,(char *)buffer + offset,sizeof(int));
			offset += sizeof(int);
			offset += namelength;
			break;
		}
	}
	free(buffer);
    return 0;
}

RC RecordBasedFileManager::getFreePage4Select(FileHandle &fileHandle,RC recordSize, const void *data)
{
	PageNum pageNums = fileHandle.getNumberOfPages();
	if(pageNums == 0)
		return -1;
	int slotDirnum;
	void *buffer = malloc(PAGE_SIZE);
	for(unsigned i = 0; i < pageNums; i++)
	{
		fileHandle.readPage(i, buffer);
		memcpy(&slotDirnum,(char *) buffer + PAGE_SIZE - 2 * sizeof(int),sizeof(int));
		int freeendsize = PAGE_SIZE - slotDirnum * sizeof(struct SlotDir) - 2 * sizeof(int);
		int freebeginsize;
		memcpy(&freebeginsize,(char *)buffer + PAGE_SIZE - sizeof(int),sizeof(int));
		if(freeendsize - freebeginsize  >= recordSize + (int)sizeof(struct SlotDir))
		{
			free(buffer);
			return i;
		}
	}
	free(buffer);
	return -1;
}

RC RecordBasedFileManager::getFreePage4Update(FileHandle &fileHandle,RC recordSize,const RID &rid)
{
	PageNum pageNums = fileHandle.getNumberOfPages();
	PageNum pageNum = rid.pageNum;
	unsigned slotNum = rid.slotNum;
	struct SlotDir * slotDir = (struct SlotDir *)malloc(sizeof(struct SlotDir));
	int slotDirnum;
	int oldRecordLength = 0;
	void *buffer = malloc(PAGE_SIZE);
	for(unsigned i = pageNum; i < pageNums; i++)
	{
		fileHandle.readPage(i, buffer);
		memcpy(&slotDirnum,(char *) buffer + PAGE_SIZE - 2 * sizeof(int),sizeof(int));
		if(i == pageNum)
		{
			for(int j = 0; j < slotDirnum; j++)
			{
				memcpy(slotDir,(char *) buffer + PAGE_SIZE - 2 * sizeof(int) - (j + 1) * sizeof(struct SlotDir),sizeof(struct SlotDir));
				if(slotDir->id == int(slotNum))
				{
					oldRecordLength = slotDir->length;
				}
			}
		}
		int freeendsize = PAGE_SIZE - slotDirnum * sizeof(struct SlotDir) - 2 * sizeof(int);
		int freebeginsize;
		memcpy(&freebeginsize,(char *)buffer + PAGE_SIZE - sizeof(int),sizeof(int));
		if(i == pageNum)
		{
			//int freespace = freeendsize - freebeginsize - recordSize + oldRecordLength;
			if(freeendsize - freebeginsize  >= recordSize - oldRecordLength)
			{
				free(buffer);
				free(slotDir);
				return i;
			}
		}
		else
		{
			if(freeendsize - freebeginsize  >= recordSize + (int)sizeof(struct SlotDir))
			{
				free(buffer);
				free(slotDir);
				return i;
			}
		}
	}
	free(buffer);
	free(slotDir);
	return -1;
}

RC RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data)
{
	int resultSize = 0;
	int attributeSize = recordDescriptor.size();
	int namelength;
	for(int i = 0; i < attributeSize; i++){
		switch(recordDescriptor[i].type)
		{
		case 0:
			resultSize += sizeof(int);
			break;
		case 1:
			resultSize += sizeof(float);
			break;
		case 2:
			memcpy(&namelength,(char *)data + resultSize,sizeof(int));
			resultSize = resultSize + namelength + sizeof(int);
			break;
		}
	}
	resultSize += attributeSize * sizeof(struct RecordDir);
	for(int i = 0; i < attributeSize; i++)
		resultSize += recordDescriptor[i].name.length();
	resultSize += sizeof(int);
    return resultSize;
}

RC RecordBasedFileManager::getRecordSize4Cli(const vector<Attribute> &recordDescriptor, const void *data)
{
	int resultSize = 0;
	int attributeSize = recordDescriptor.size();
	int namelength;
	for(int i = 0; i < attributeSize; i++){
		switch(recordDescriptor[i].type)
		{
		case 0:
			resultSize += sizeof(int);
			break;
		case 1:
			resultSize += sizeof(float);
			break;
		case 2:
			memcpy(&namelength,(char *)data + resultSize,sizeof(int));
			resultSize = resultSize + namelength + sizeof(int);
			break;
		}
	}
    return resultSize;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,
      const void *value,
      const vector<string> &attributeNames,
      RBFM_ScanIterator &rbfm_ScanIterator)
{
	rbfm_ScanIterator.filehandle = fileHandle;
	rbfm_ScanIterator.recordDescriptor = recordDescriptor;
	rbfm_ScanIterator.conditionAttribute = conditionAttribute;
	rbfm_ScanIterator.compOp = compOp;
	rbfm_ScanIterator.value = (void *)value;
	rbfm_ScanIterator.attributeNames = attributeNames;
	rbfm_ScanIterator.preRid.pageNum = 0;
	rbfm_ScanIterator.preRid.slotNum = 0;
	return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
	int flagfind = 0;
	void *buffer = malloc(PAGE_SIZE);
	struct SlotDir * slotDir = (struct SlotDir *)malloc(sizeof(struct SlotDir));
	struct RecordDir * recordDir = (struct RecordDir *)malloc(sizeof(struct RecordDir));
	unsigned pagenum = preRid.pageNum;
	unsigned slotnum = preRid.slotNum + 1;
	unsigned pageNumbers = filehandle.getNumberOfPages();
	//RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	for(unsigned i = pagenum; i < pageNumbers; i++)
	{
		filehandle.readPage(i,buffer);
		int slotnumber;
		memcpy(&slotnumber,(char *)buffer + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
		for(int j = 0; j < slotnumber; j++)
		{
			memcpy(slotDir,(char *) buffer + PAGE_SIZE - 2 * sizeof(int) - (j + 1) * sizeof(struct SlotDir),sizeof(struct SlotDir));
			if(i == pagenum && slotDir->id < (int)slotnum)
			{
				continue;
			}
			int offset = 0, fieldNumber = 0, fieldoffset = 0;
			int resultAttributeSize = attributeNames.size();
			int resultflag = 0;
			memcpy(&fieldNumber, buffer + slotDir->offset, sizeof(int));
			for(int x = 0; x < fieldNumber; x++)
			{
				memcpy(recordDir,buffer + slotDir->offset + sizeof(int) + x * sizeof(struct RecordDir), sizeof(struct RecordDir));
				void * attrName = malloc(recordDir->attrNameLength);
				memcpy(attrName,(char *)buffer + slotDir->offset + recordDir->offset,recordDir->attrNameLength);
				for(int y = 0; y < recordDescriptor.size(); y++)
				{
					for(int b = 0; b < resultAttributeSize; b++)
					{
						if(recordDescriptor[y].name.length() == recordDir->attrNameLength && memcmp(attrName,recordDescriptor[y].name.c_str(),recordDir->attrNameLength) ==  0
								&& recordDescriptor[y].name == attributeNames[b])
						{
							memcpy(data + fieldoffset,buffer + slotDir->offset + recordDir->offset + recordDir->attrNameLength,recordDir->fieldLength);
							fieldoffset += recordDir->fieldLength;
							if(conditionAttribute == "")
							{
								resultflag = 1;
								flagfind = 1;
								break;
							}
						}
						if(conditionAttribute == "")
							resultflag = 1;
						if(recordDescriptor[y].name.length() == recordDir->attrNameLength && memcmp(attrName,recordDescriptor[y].name.c_str(),recordDir->attrNameLength) ==  0
								&& recordDescriptor[y].name == conditionAttribute)
						{
							if(value == NULL)
							{
								resultflag = 1;
							}
							else
							{
								int result = memcmp(buffer + slotDir->offset + recordDir->offset + recordDir->attrNameLength,value,recordDir->fieldLength);
								switch(compOp)
								{
								case 0:
									if(result == 0)
									{
										resultflag = 1;
									}
									break;
								case 1:
									if(result < 0)
									{
										resultflag = 1;
									}
									break;
								case 2:
									if(result > 0)
									{
										resultflag = 1;
									}
									break;
								case 3:
									if(result <= 0)
									{
										resultflag = 1;
									}
									break;
								case 4:
									if(result >= 0)
									{
										resultflag = 1;
									}
									break;
								case 5:
									if(result != 0)
									{
										resultflag = 1;
									}
									break;
								case 6:
									resultflag = 1;
									break;
								}
							}
						}
					}
					if(flagfind == 1)
					{
						flagfind = 0;
						break;
					}
				}
				free(attrName);
			}
			if(resultflag == 1)
			{
				//data already save;
				preRid.pageNum = i;
				preRid.slotNum = slotDir->id;
				rid.pageNum = i;
				rid.slotNum = slotDir->id;
				free(buffer);
				free(slotDir);
				free(recordDir);
				return 0;
			}
		}
	}
	free(buffer);
	free(slotDir);
	free(recordDir);
	return RBFM_EOF;
}

RC RBFM_ScanIterator::close()
{
	return 0;
}

RC RecordBasedFileManager::reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor)
{
	void *buffer = malloc(PAGE_SIZE);
	void *buffer2 = malloc(PAGE_SIZE);
	struct SlotDir * slotDir = (struct SlotDir *)malloc(sizeof(struct SlotDir));
	struct SlotDir * slotDir2 = (struct SlotDir *)malloc(sizeof(struct SlotDir));
	vector<RID> rids;
	vector<int> recordLengths;
	int offset = 0;
	unsigned pageNumbers = fileHandle.getNumberOfPages();
	void *recordData = malloc(PAGE_SIZE * pageNumbers);
	for(unsigned i = 0; i < pageNumbers; i++)
	{
		fileHandle.readPage(i,buffer);
		int slotnumber;
		memcpy(&slotnumber,(char *)buffer + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
		for(int j = 0; j < slotnumber; j++)
		{
			memcpy(slotDir,(char *) buffer + PAGE_SIZE - 2 * sizeof(int) - (j + 1) * sizeof(struct SlotDir),sizeof(struct SlotDir));
			RID rid;
			rid.pageNum = i;
			rid.slotNum = slotDir->id;
			int recordLength;
			if(slotDir->iftombstone == 0)
				recordLength = slotDir->length;
			else
			{
				void * prid = malloc(sizeof(RID));
				memcpy(prid,(char *)buffer + slotDir->offset, slotDir->length);
				RID rid2 = *(RID *)prid;
				fileHandle.readPage(rid2.pageNum,buffer2);
				int slotnumber2;
				memcpy(&slotnumber2,(char *)buffer2 + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
				for(int a = 0; a < slotnumber2; a++)
				{
					memcpy(slotDir2,(char *) buffer + PAGE_SIZE - 2 * sizeof(int) - (j + 1) * sizeof(struct SlotDir),sizeof(struct SlotDir));
					if(slotDir2->id == (int)rid2.slotNum)
						recordLength = slotDir2->length;
				}
			}
			rids.push_back(rid);
			recordLengths.push_back(recordLength);
			readRecord(fileHandle,recordDescriptor,rid,(char *)recordData+offset);
			offset += recordLength;
		}
	}
	offset = 0;
	for(unsigned i = 0; i < rids.size(); i++)
	{
		deleteRecord(fileHandle,recordDescriptor,rids[i]);
		insertRecord(fileHandle,recordDescriptor,(char *)recordData+offset,rids[i]);
		offset += recordLengths[i];
	}
	return 0;
}
