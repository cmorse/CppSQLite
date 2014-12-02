/*
 * CppSQLite
 * Developed by Rob Groves <rob.groves@btinternet.com>
 * Maintained by NeoSmart Technologies <http://neosmart.net/>
 *
 * CppSQLite was originally developed by Rob Groves on CodeProject:
 * <http://www.codeproject.com/KB/database/CppSQLite.aspx>
 *
 * Maintenance and updates are Copyright (C) 2011 NeoSmart Technologies
 * <http://neosmart.net/>
 *
 * Original copyright information:
 * Copyright (C) 2004 Rob Groves. All Rights Reserved.
 * <rob.groves@btinternet.com>
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement, is hereby granted, provided that the above copyright notice,
 * this paragraph and the following two paragraphs appear in all copies,
 * modifications, and distributions.
 *
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT,
 * INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST
 * PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF THE AUTHOR HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE. THE SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF
 * ANY, PROVIDED HEREUNDER IS PROVIDED "AS IS". THE AUTHOR HAS NO OBLIGATION
 * TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
*/

#ifndef _CppSQLite3_H_
#define _CppSQLite3_H_

#include "sqlite3.h"
#include <cstring>
#include <string>
#include <inttypes.h>

#define CPPSQLITE_ERROR 10000

namespace {
  // Named constant for passing to CppSQLite3Exception when passing it a string
  // that cannot be deleted.
  static const bool DONT_DELETE_MSG=false;
}

class CppSQLite3Exception
{
  public:
    CppSQLite3Exception(const int nErrCode, const char *szErrMess, bool bDeleteMsg=true);

    CppSQLite3Exception(const CppSQLite3Exception &e);

    ~CppSQLite3Exception();

    const int errorCode() const;

    std::string errorMessage() const;

    static std::string errorCodeAsString(int nErrCode);

  private:
    const int mnErrCode;
    std::string mpszErrMess;
};


class CppSQLite3Buffer
{
  public:
    CppSQLite3Buffer();
    ~CppSQLite3Buffer();

    const char *format(const char *szFormat, ...);

    operator const char*() { return mpBuf; }

    void clear();

  private:
    char *mpBuf;
};

class CppSQLite3Binary
{
  public:
    CppSQLite3Binary();
    ~CppSQLite3Binary();

    void setBinary(const unsigned char *pBuf, int nLen);
    void setEncoded(const unsigned char *pBuf);

    const unsigned char *getEncoded();
    const unsigned char *getBinary();

    int getBinaryLength();

    unsigned char *allocBuffer(int nLen);

    void clear();

  private:

    unsigned char *mpBuf;
    int mnBinaryLen;
    int mnBufferLen;
    int mnEncodedLen;
    bool mbEncoded;
};


class CppSQLite3Query
{
  public:
    CppSQLite3Query();
    CppSQLite3Query(const CppSQLite3Query &rQuery);
    CppSQLite3Query(sqlite3 *pDB, sqlite3_stmt *pVM, bool bEof, bool bOwnVM=true);
    CppSQLite3Query &operator=(const CppSQLite3Query &rQuery);
    ~CppSQLite3Query();

    int numFields() const;

    int fieldIndex(const std::string &szField) const;
    std::string fieldName(int nCol) const;

    std::string fieldDeclType(int nCol) const;
    int fieldDataType(int nCol) const;

    std::string fieldValue(int nField) const;
    std::string fieldValue(const std::string &szField) const;

    int getIntField(int nField, int nNullValue=0) const;
    int getIntField(const std::string &szField, int nNullValue=0) const;

    int64_t getInt64Field(int nField, int64_t nNullValue=0) const;
    int64_t getInt64Field(const std::string &szField, int64_t nNullValue=0) const;

    double getFloatField(int nField, double fNullValue=0.0) const;
    double getFloatField(const std::string &szField, double fNullValue=0.0) const;

    std::string getStringField(int nField, const std::string &szNullValue="") const;
    std::string getStringField(const std::string &szField, const std::string &szNullValue="") const;

    const unsigned char *getBlobField(int nField, int &nLen) const;
    const unsigned char *getBlobField(const std::string &szField, int &nLen) const;

    bool fieldIsNull(int nField) const;
    bool fieldIsNull(const std::string &szField) const;

    bool eof() const;

    void nextRow();

    void finalize();

    void setMaxRetryCount(int nMaxRetryCount);

    void setRetryTimeUs(int nRetryTimeUs);

  private:
    void checkVM() const;

    void rollback() const;

    void checkFieldIndex(int nCol) const;

    sqlite3 *mpDB;
    sqlite3_stmt *mpVM;
    bool mbEof;
    int mnCols;
    bool mbOwnVM;

    // How many times to retry after an SQLITE_LOCKED
    int mnMaxRetryCount;

    // How many useconds to sleep for before retrying on SQLITE_LOCKED
    int mnRetryTimeUs;
};


class CppSQLite3Table
{
  public:
    CppSQLite3Table();
    CppSQLite3Table(const CppSQLite3Table &rTable);
    CppSQLite3Table(char **paszResults, int nRows, int nCols);
    ~CppSQLite3Table();

    CppSQLite3Table &operator=(const CppSQLite3Table &rTable);

    int numFields() const;

    int numRows() const;

    std::string fieldName(int nCol);

    std::string fieldValue(int nField) const;
    std::string fieldValue(const std::string &szField) const;

    int getIntField(int nField, int nNullValue=0) const;
    int getIntField(const std::string &szField, int nNullValue=0) const;

    double getFloatField(int nField, double fNullValue=0.0) const;
    double getFloatField(const std::string &szField, double fNullValue=0.0) const;

    const std::string getStringField(int nField, const std::string &szNullValue="") const;
    const std::string getStringField(const std::string &szField, const std::string &szNullValue="") const;

    bool fieldIsNull(int nField) const;
    bool fieldIsNull(const std::string &szField) const;

    void setRow(int nRow);

    void finalize();

  private:

    void checkResults() const;

    int mnRows;
    int mnCols;
    int mnCurrentRow;
    char **mpaszResults;
};


class CppSQLite3Statement
{
  public:
    CppSQLite3Statement();
    CppSQLite3Statement(const CppSQLite3Statement &rStatement);
    CppSQLite3Statement(sqlite3 *pDB, sqlite3_stmt *pVM);
    ~CppSQLite3Statement();

    CppSQLite3Statement &operator=(const CppSQLite3Statement &rStatement);

    int execDML() const;

    CppSQLite3Query execQuery() const;

    void bind(int nParam, const std::string &szValue);
    void bind(int nParam, const int nValue);
    void bind(int nParam, const int64_t nValue);
    void bind(int nParam, const double dwValue);
    void bind(int nParam, const unsigned char *blobValue, int nLen);
    void bindNull(int nParam);

    void reset();

    void finalize();

    void setMaxRetryCount(int nMaxRetryCount);

    void setRetryTimeUs(int nRetryTimeUs);

  private:
    void checkDB() const;
    void checkVM() const;

    sqlite3 *mpDB;
    sqlite3_stmt *mpVM;

    // How many times to retry after an SQLITE_LOCKED
    int mnMaxRetryCount;

    // How many useconds to sleep for before retrying on SQLITE_LOCKED
    int mnRetryTimeUs;
};


class CppSQLite3DB
{
  public:
    CppSQLite3DB();
    ~CppSQLite3DB();

    void open(const std::string &szFile);

    void close();

    bool tableExists(const std::string &szTable) const;

    int execDML(const std::string &szSQL);

    CppSQLite3Query execQuery(const std::string &szSQL) const;

    int execScalar(const std::string &szSQL) const;

    CppSQLite3Table getTable(const std::string &szSQL) const;

    CppSQLite3Statement compileStatement(const std::string &szSQL) const;

    sqlite_int64 lastRowId() const;

    void interrupt();

    void setBusyTimeout(int nMillisecs);

    void setMaxRetryCount(int nMaxRetryCount);

    void setRetryTimeUs(int nRetryTimeUs);

    static const char *SQLiteVersion() { return SQLITE_VERSION; }

    // Backup DB to the file target
    //
    // Contents of the file target are overwritten
    void backup(const std::string &target);

    // Restore from a backup DB in file target
    //
    // Contents of the local database mpDB are overwritten
    void restore(const std::string &target);

  private:
    CppSQLite3DB(const CppSQLite3DB &db);
    CppSQLite3DB &operator=(const CppSQLite3DB &db);

    sqlite3_stmt *compile(const std::string &szSQL) const;

    // Backup or restore the local DB to target.
    //
    // Copies all pages in the database with a single sqlite3_backup_step command
    // Supports backup to or from in-memory databases
    void backupOrRestore(const std::string &target, bool isBackup);

    void checkDB() const;

    void rollback() const;

    sqlite3 *mpDB;

    // How long before timing out most operations
    int mnBusyTimeoutMs;

    // How many times to retry after an SQLITE_LOCKED
    int mnMaxRetryCount;

    // How many useconds to sleep for before retrying on SQLITE_LOCKED
    int mnRetryTimeUs;
};

inline void CppSQLite3Query::checkVM() const
{
  if (mpVM == NULL) {
    throw CppSQLite3Exception(CPPSQLITE_ERROR, "Null Virtual Machine pointer", DONT_DELETE_MSG);
  }
}

inline void CppSQLite3Query::checkFieldIndex(int nCol) const
{
  if (nCol < 0 || nCol > mnCols - 1) {
    throw CppSQLite3Exception(CPPSQLITE_ERROR, "Invalid field index requested", DONT_DELETE_MSG);
  }
}

inline void CppSQLite3Table::checkResults() const
{
  if (mpaszResults == 0) {
    throw CppSQLite3Exception(CPPSQLITE_ERROR, "Null Results pointer", DONT_DELETE_MSG);
  }
}

inline void CppSQLite3Statement::checkDB() const
{
  if (mpDB == NULL) {
    throw CppSQLite3Exception(CPPSQLITE_ERROR, "Database not open", DONT_DELETE_MSG);
  }
}

inline void CppSQLite3Statement::checkVM() const
{
  if (mpVM == NULL) {
    throw CppSQLite3Exception(CPPSQLITE_ERROR, "Null Virtual Machine pointer", DONT_DELETE_MSG);
  }
}

inline void CppSQLite3DB::checkDB() const
{
  if (mpDB == NULL) {
    throw CppSQLite3Exception(CPPSQLITE_ERROR, "Database not open", DONT_DELETE_MSG);
  }
}

#endif
