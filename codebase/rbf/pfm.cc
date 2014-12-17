#include "pfm.h"

#include <iostream>

using namespace std;

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const char *fileName)
{
	FILE *pFile = NULL;
	pFile = fopen(fileName,"r");
	if(pFile != NULL)
	{
		return -1;
	}
	else
	{
		pFile = fopen(fileName,"wb+");
		if(pFile == NULL)
			return -1;
		fclose(pFile);
		return 0;
	}

}


RC PagedFileManager::destroyFile(const char *fileName)
{
	return remove(fileName);
}


RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)
{
	FILE *pFile = fopen(fileName,"rb+");
	if(pFile == NULL)
		return -1;
	if(fileHandle.setFile(pFile) != 0)
		return -1;
	fileHandle.setFileName(fileName);
    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	fclose(fileHandle.getFile());
    return 0;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}


FileHandle::~FileHandle()
{
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	if(pageNum >= getNumberOfPages())
		return -1;
	fseek(pFile,pageNum * PAGE_SIZE,SEEK_SET);
	fread(data,sizeof(char),PAGE_SIZE,pFile);
	//fflush(pFile);
        readPageCounter = readPageCounter + 1;
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	if(pageNum > getNumberOfPages())
		return -1;
	fseek(pFile,pageNum * PAGE_SIZE,SEEK_SET);
	fwrite(data,sizeof(char),PAGE_SIZE,pFile);
	fflush(pFile);
        writePageCounter = writePageCounter + 1;
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
    fseek(pFile,0,SEEK_END);
    fwrite(data,sizeof(char),PAGE_SIZE,pFile);
    fflush(pFile);
    appendPageCounter = appendPageCounter + 1;
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    fseek(pFile,0,SEEK_END);
    return (PageNum) (ftell(pFile) / PAGE_SIZE);
}

RC FileHandle::setFile(FILE * p_File)
{
	pFile = p_File;
	if(pFile != NULL)
		return 0;
	else
		return -1;
}

FILE * FileHandle::getFile()
{
	return pFile;
}

const char* FileHandle::getFileName()
{
	return filename;
}

RC FileHandle::setFileName(const char* filename)
{
	this->filename = filename;
	return 0;
}

