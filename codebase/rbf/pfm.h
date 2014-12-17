#ifndef _pfm_h_
#define _pfm_h_

#include<stdio.h>

typedef int RC;
typedef unsigned PageNum;

#define PAGE_SIZE 4096 

using namespace std;

class FileHandle;


class PagedFileManager
{
public:
    static PagedFileManager* instance();                     // Access to the _pf_manager instance

    RC createFile    (const char *fileName);                         // Create a new file
    RC destroyFile   (const char *fileName);                         // Destroy a file
    RC openFile      (const char *fileName, FileHandle &fileHandle); // Open a file
    RC closeFile     (FileHandle &fileHandle);                       // Close a file

protected:
    PagedFileManager();                                   // Constructor
    ~PagedFileManager();                                  // Destructor

private:
    static PagedFileManager *_pf_manager;
};


class FileHandle
{
public:
    FileHandle();                                                    // Default constructor
    ~FileHandle();                                                   // Destructor

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file

    RC setFile(FILE * pFile);
    FILE * getFile();

    const char* getFileName();
    RC setFileName(const char * filename);
public:
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
private:
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
private:
    FILE * pFile;
    const char* filename;
 };

 #endif
