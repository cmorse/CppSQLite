/*
 * CppSQLite
 * Developed by Rob Groves <rob.groves@btinternet.com>
 * Maintained by NeoSmart Technologies <http://neosmart.net/>
 * See LICENSE comment in CppSQLite3.h for copyright and license info
*/

#include "CppSQLite3.h"
#include <cstdlib>
#include <sstream>
#include <iostream>
#include <unistd.h>

using namespace std;

////////////////////////////////////////////////////////////////////////////////
// Prototypes for SQLite functions not included in SQLite DLL, but copied below
// from SQLite encode.c
////////////////////////////////////////////////////////////////////////////////
int sqlite3_encode_binary(const unsigned char *in, int n, unsigned char *out);
int sqlite3_decode_binary(const unsigned char *in, unsigned char *out);

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

CppSQLite3Exception::CppSQLite3Exception(const int nErrCode,
                                         const char* szErrMess,
                                         bool bDeleteMsg)
 : mnErrCode(nErrCode)
{
  mpszErrMess = (szErrMess ? szErrMess : "");

  if (bDeleteMsg && szErrMess) {
    sqlite3_free((void*)szErrMess);
  }
}

CppSQLite3Exception::CppSQLite3Exception(const CppSQLite3Exception &e)
 : mnErrCode(e.mnErrCode)
{
  mpszErrMess = e.mpszErrMess;
}

const int CppSQLite3Exception::errorCode() const
{
  return mnErrCode;
}

string CppSQLite3Exception::errorMessage() const
{
  ostringstream rio;
  rio << errorCodeAsString(mnErrCode) << "[" << mnErrCode << "]: " << mpszErrMess;
  return rio.str();
}

string CppSQLite3Exception::errorCodeAsString(int nErrCode)
{
  switch (nErrCode)
  {
    case SQLITE_OK                      : return "SQLITE_OK";
    case SQLITE_ERROR                   : return "SQLITE_ERROR";
    case SQLITE_INTERNAL                : return "SQLITE_INTERNAL";
    case SQLITE_PERM                    : return "SQLITE_PERM";
    case SQLITE_ABORT                   : return "SQLITE_ABORT";
    case SQLITE_BUSY                    : return "SQLITE_BUSY";
    case SQLITE_LOCKED                  : return "SQLITE_LOCKED";
    case SQLITE_NOMEM                   : return "SQLITE_NOMEM";
    case SQLITE_READONLY                : return "SQLITE_READONLY";
    case SQLITE_INTERRUPT               : return "SQLITE_INTERRUPT";
    case SQLITE_IOERR                   : return "SQLITE_IOERR";
    case SQLITE_CORRUPT                 : return "SQLITE_CORRUPT";
    case SQLITE_NOTFOUND                : return "SQLITE_NOTFOUND";
    case SQLITE_FULL                    : return "SQLITE_FULL";
    case SQLITE_CANTOPEN                : return "SQLITE_CANTOPEN";
    case SQLITE_PROTOCOL                : return "SQLITE_PROTOCOL";
    case SQLITE_EMPTY                   : return "SQLITE_EMPTY";
    case SQLITE_SCHEMA                  : return "SQLITE_SCHEMA";
    case SQLITE_TOOBIG                  : return "SQLITE_TOOBIG";
    case SQLITE_CONSTRAINT              : return "SQLITE_CONSTRAINT";
    case SQLITE_MISMATCH                : return "SQLITE_MISMATCH";
    case SQLITE_MISUSE                  : return "SQLITE_MISUSE";
    case SQLITE_NOLFS                   : return "SQLITE_NOLFS";
    case SQLITE_AUTH                    : return "SQLITE_AUTH";
    case SQLITE_FORMAT                  : return "SQLITE_FORMAT";
    case SQLITE_RANGE                   : return "SQLITE_RANGE";
    case SQLITE_ROW                     : return "SQLITE_ROW";
    case SQLITE_DONE                    : return "SQLITE_DONE";
    case SQLITE_IOERR_READ              : return "SQLITE_IOERR_READ";
    case SQLITE_IOERR_SHORT_READ        : return "SQLITE_IOERR_SHORT_READ";
    case SQLITE_IOERR_WRITE             : return "SQLITE_IOERR_WRITE";
    case SQLITE_IOERR_FSYNC             : return "SQLITE_IOERR_FSYNC";
    case SQLITE_IOERR_DIR_FSYNC         : return "SQLITE_IOERR_DIR_FSYNC";
    case SQLITE_IOERR_TRUNCATE          : return "SQLITE_IOERR_TRUNCATE";
    case SQLITE_IOERR_FSTAT             : return "SQLITE_IOERR_FSTAT";
    case SQLITE_IOERR_UNLOCK            : return "SQLITE_IOERR_UNLOCK";
    case SQLITE_IOERR_RDLOCK            : return "SQLITE_IOERR_RDLOCK";
    case SQLITE_IOERR_DELETE            : return "SQLITE_IOERR_DELETE";
    case SQLITE_IOERR_BLOCKED           : return "SQLITE_IOERR_BLOCKED";
    case SQLITE_IOERR_NOMEM             : return "SQLITE_IOERR_NOMEM";
    case SQLITE_IOERR_ACCESS            : return "SQLITE_IOERR_ACCESS";
    case SQLITE_IOERR_CHECKRESERVEDLOCK : return "SQLITE_IOERR_CHECKRESERVEDLOCK";
    case SQLITE_IOERR_LOCK              : return "SQLITE_IOERR_LOCK";
    case SQLITE_IOERR_CLOSE             : return "SQLITE_IOERR_CLOSE";
    case SQLITE_IOERR_DIR_CLOSE         : return "SQLITE_IOERR_DIR_CLOSE";
    case SQLITE_IOERR_SHMOPEN           : return "SQLITE_IOERR_SHMOPEN";
    case SQLITE_IOERR_SHMSIZE           : return "SQLITE_IOERR_SHMSIZE";
    case SQLITE_IOERR_SHMLOCK           : return "SQLITE_IOERR_SHMLOCK";
    case SQLITE_IOERR_SHMMAP            : return "SQLITE_IOERR_SHMMAP";
    case SQLITE_IOERR_SEEK              : return "SQLITE_IOERR_SEEK";
    case SQLITE_IOERR_DELETE_NOENT      : return "SQLITE_IOERR_DELETE_NOENT";
    case SQLITE_IOERR_MMAP              : return "SQLITE_IOERR_MMAP";
    case SQLITE_IOERR_GETTEMPPATH       : return "SQLITE_IOERR_GETTEMPPATH";
    case SQLITE_IOERR_CONVPATH          : return "SQLITE_IOERR_CONVPATH";
    case SQLITE_LOCKED_SHAREDCACHE      : return "SQLITE_LOCKED_SHAREDCACHE";
    case SQLITE_BUSY_RECOVERY           : return "SQLITE_BUSY_RECOVERY";
    case SQLITE_BUSY_SNAPSHOT           : return "SQLITE_BUSY_SNAPSHOT";
    case SQLITE_CANTOPEN_NOTEMPDIR      : return "SQLITE_CANTOPEN_NOTEMPDIR";
    case SQLITE_CANTOPEN_ISDIR          : return "SQLITE_CANTOPEN_ISDIR";
    case SQLITE_CANTOPEN_FULLPATH       : return "SQLITE_CANTOPEN_FULLPATH";
    case SQLITE_CANTOPEN_CONVPATH       : return "SQLITE_CANTOPEN_CONVPATH";
    case SQLITE_CORRUPT_VTAB            : return "SQLITE_CORRUPT_VTAB";
    case SQLITE_READONLY_RECOVERY       : return "SQLITE_READONLY_RECOVERY";
    case SQLITE_READONLY_CANTLOCK       : return "SQLITE_READONLY_CANTLOCK";
    case SQLITE_READONLY_ROLLBACK       : return "SQLITE_READONLY_ROLLBACK";
    case SQLITE_READONLY_DBMOVED        : return "SQLITE_READONLY_DBMOVED";
    case SQLITE_ABORT_ROLLBACK          : return "SQLITE_ABORT_ROLLBACK";
    case SQLITE_CONSTRAINT_CHECK        : return "SQLITE_CONSTRAINT_CHECK";
    case SQLITE_CONSTRAINT_COMMITHOOK   : return "SQLITE_CONSTRAINT_COMMITHOOK";
    case SQLITE_CONSTRAINT_FOREIGNKEY   : return "SQLITE_CONSTRAINT_FOREIGNKEY";
    case SQLITE_CONSTRAINT_FUNCTION     : return "SQLITE_CONSTRAINT_FUNCTION";
    case SQLITE_CONSTRAINT_NOTNULL      : return "SQLITE_CONSTRAINT_NOTNULL";
    case SQLITE_CONSTRAINT_PRIMARYKEY   : return "SQLITE_CONSTRAINT_PRIMARYKEY";
    case SQLITE_CONSTRAINT_TRIGGER      : return "SQLITE_CONSTRAINT_TRIGGER";
    case SQLITE_CONSTRAINT_UNIQUE       : return "SQLITE_CONSTRAINT_UNIQUE";
    case SQLITE_CONSTRAINT_VTAB         : return "SQLITE_CONSTRAINT_VTAB";
    case SQLITE_CONSTRAINT_ROWID        : return "SQLITE_CONSTRAINT_ROWID";
    case SQLITE_NOTICE_RECOVER_WAL      : return "SQLITE_NOTICE_RECOVER_WAL";
    case SQLITE_NOTICE_RECOVER_ROLLBACK : return "SQLITE_NOTICE_RECOVER_ROLLBACK";
    case SQLITE_WARNING_AUTOINDEX       : return "SQLITE_WARNING_AUTOINDEX";
    case SQLITE_AUTH_USER               : return "SQLITE_AUTH_USER";
    case CPPSQLITE_ERROR                : return "CPPSQLITE_ERROR";
    default: return "UNKNOWN_ERROR";
  }
}

CppSQLite3Exception::~CppSQLite3Exception()
{
}

////////////////////////////////////////////////////////////////////////////////

CppSQLite3Buffer::CppSQLite3Buffer()
{
  mpBuf = NULL;
}

CppSQLite3Buffer::~CppSQLite3Buffer()
{
  clear();
}

void CppSQLite3Buffer::clear()
{
  if (mpBuf) {
    sqlite3_free(mpBuf);
    mpBuf = NULL;
  }
}

const char *CppSQLite3Buffer::format(const char *szFormat, ...)
{
  clear();
  va_list va;
  va_start(va, szFormat);
  mpBuf = sqlite3_vmprintf(szFormat, va);
  va_end(va);
  return mpBuf;
}

////////////////////////////////////////////////////////////////////////////////

CppSQLite3Binary::CppSQLite3Binary()
 : mpBuf(NULL),
   mnBinaryLen(0),
   mnBufferLen(0),
   mnEncodedLen(0),
   mbEncoded(false)
{
}

CppSQLite3Binary::~CppSQLite3Binary()
{
  clear();
}

void CppSQLite3Binary::setBinary(const unsigned char *pBuf, int nLen)
{
  mpBuf = allocBuffer(nLen);
  memcpy(mpBuf, pBuf, nLen);
}

void CppSQLite3Binary::setEncoded(const unsigned char *pBuf)
{
  clear();

  mnEncodedLen = strlen(reinterpret_cast<const char*>(pBuf));
  mnBufferLen = mnEncodedLen + 1; // Allow for NULL terminator

  mpBuf = new unsigned char[mnBufferLen];

  if (!mpBuf) {
    throw CppSQLite3Exception(CPPSQLITE_ERROR, "Cannot allocate memory", DONT_DELETE_MSG);
  }

  memcpy(mpBuf, pBuf, mnBufferLen);
  mbEncoded = true;
}

const unsigned char *CppSQLite3Binary::getEncoded()
{
  if (!mbEncoded) {
    unsigned char *ptmp = new unsigned char[mnBinaryLen];
    memcpy(ptmp, mpBuf, mnBinaryLen);
    mnEncodedLen = sqlite3_encode_binary(ptmp, mnBinaryLen, mpBuf);
    delete[] ptmp;
    mbEncoded = true;
  }

  return mpBuf;
}

const unsigned char *CppSQLite3Binary::getBinary()
{
  if (mbEncoded) {
    // in/out buffers can be the same
    mnBinaryLen = sqlite3_decode_binary(mpBuf, mpBuf);

    if (mnBinaryLen == -1) {
      throw CppSQLite3Exception(CPPSQLITE_ERROR, "Cannot decode binary", DONT_DELETE_MSG);
    }

    mbEncoded = false;
  }

  return mpBuf;
}

int CppSQLite3Binary::getBinaryLength()
{
  getBinary();
  return mnBinaryLen;
}

unsigned char *CppSQLite3Binary::allocBuffer(int nLen)
{
  clear();

  // Allow extra space for encoded binary as per comments in
  // SQLite encode.c See bottom of this file for implementation
  // of SQLite functions use 3 instead of 2 just to be sure ;-)
  mnBinaryLen = nLen;
  mnBufferLen = 3 + (257 * nLen) / 254;

  mpBuf = new unsigned char[mnBufferLen];

  if (!mpBuf) {
    throw CppSQLite3Exception(CPPSQLITE_ERROR, "Cannot allocate memory", DONT_DELETE_MSG);
  }

  mbEncoded = false;

  return mpBuf;
}

void CppSQLite3Binary::clear()
{
  if (mpBuf) {
    mnBinaryLen = 0;
    mnBufferLen = 0;
    delete[] mpBuf;
    mpBuf = NULL;
  }
}

////////////////////////////////////////////////////////////////////////////////

CppSQLite3Query::CppSQLite3Query()
  : mpVM(NULL),
    mbEof(true),
    mnCols(0),
    mbOwnVM(false),
    mnMaxRetryCount(5),     // Retry 5 times on SQLITE_LOCKED
    mnRetryTimeUs(5000)     // Sleep for 0.005 seconds before retrying on SQLITE_LOCKED
{
}

CppSQLite3Query::CppSQLite3Query(const CppSQLite3Query &rQuery)
{
  mpVM = rQuery.mpVM;
  // Only one object can own the VM
  const_cast<CppSQLite3Query&>(rQuery).mpVM = NULL;
  mbEof = rQuery.mbEof;
  mnCols = rQuery.mnCols;
  mbOwnVM = rQuery.mbOwnVM;
  mnMaxRetryCount = rQuery.mnMaxRetryCount;
  mnRetryTimeUs = rQuery.mnRetryTimeUs;
}

CppSQLite3Query::CppSQLite3Query(sqlite3 *pDB, sqlite3_stmt *pVM, bool bEof, bool bOwnVM)
  : mpDB(pDB),
    mpVM(pVM),
    mbEof(bEof),
    mbOwnVM(bOwnVM),
    mnMaxRetryCount(5),     // Retry 5 times on SQLITE_LOCKED
    mnRetryTimeUs(5000)     // Sleep for 0.005 seconds before retrying on SQLITE_LOCKED
{
  mnCols = sqlite3_column_count(mpVM);
}

CppSQLite3Query::~CppSQLite3Query()
{
  try {
    finalize();
  } catch (...) {
  }
}

CppSQLite3Query &CppSQLite3Query::operator=(const CppSQLite3Query &rQuery)
{
  try {
    finalize();
  } catch (...) {
  }

  mpVM = rQuery.mpVM;
  // Only one object can own the VM
  const_cast<CppSQLite3Query&>(rQuery).mpVM = NULL;
  mbEof = rQuery.mbEof;
  mnCols = rQuery.mnCols;
  mbOwnVM = rQuery.mbOwnVM;
  mnMaxRetryCount = rQuery.mnMaxRetryCount;
  mnRetryTimeUs = rQuery.mnRetryTimeUs;
  return *this;
}

int CppSQLite3Query::numFields() const
{
  checkVM();
  return mnCols;
}

string CppSQLite3Query::fieldValue(int nField) const
{
  checkVM();

  if (nField < 0 || nField > mnCols - 1) {
    throw CppSQLite3Exception(CPPSQLITE_ERROR, "Invalid field index requested", DONT_DELETE_MSG);
  }

  if (fieldDataType(nField) == SQLITE_NULL) {
    return "";
  } else {
    return reinterpret_cast<const char *>(sqlite3_column_text(mpVM, nField));
  }
}

string CppSQLite3Query::fieldValue(const string &szField) const
{
  int nField = fieldIndex(szField);
  return reinterpret_cast<const char *>(sqlite3_column_text(mpVM, nField));
}

int CppSQLite3Query::getIntField(int nField, int nNullValue) const
{
  if (fieldDataType(nField) == SQLITE_NULL) {
    return nNullValue;
  } else {
    return sqlite3_column_int(mpVM, nField);
  }
}

int CppSQLite3Query::getIntField(const string &szField, int nNullValue) const
{
  int nField = fieldIndex(szField);
  return getIntField(nField, nNullValue);
}

int64_t CppSQLite3Query::getInt64Field(int nField, int64_t nNullValue) const
{
  if (fieldDataType(nField) == SQLITE_NULL) {
    return nNullValue;
  } else {
    return sqlite3_column_int64(mpVM, nField);
  }
}

int64_t CppSQLite3Query::getInt64Field(const string &szField, int64_t nNullValue) const
{
  int nField = fieldIndex(szField);
  return getIntField(nField, nNullValue);
}

double CppSQLite3Query::getFloatField(int nField, double fNullValue) const
{
  if (fieldDataType(nField) == SQLITE_NULL) {
    return fNullValue;
  } else {
    return sqlite3_column_double(mpVM, nField);
  }
}

double CppSQLite3Query::getFloatField(const string &szField, double fNullValue) const
{
  int nField = fieldIndex(szField);
  return getFloatField(nField, fNullValue);
}

string CppSQLite3Query::getStringField(int nField, const string &szNullValue) const
{
  if (fieldDataType(nField) == SQLITE_NULL) {
    return szNullValue;
  } else {
    return reinterpret_cast<const char*>(sqlite3_column_text(mpVM, nField));
  }
}

string CppSQLite3Query::getStringField(const string &szField, const string &szNullValue) const
{
  int nField = fieldIndex(szField);
  return getStringField(nField, szNullValue);
}

const unsigned char *CppSQLite3Query::getBlobField(int nField, int &nLen) const
{
  checkVM();

  if (nField < 0 || nField > mnCols - 1) {
    throw CppSQLite3Exception(CPPSQLITE_ERROR, "Invalid field index requested", DONT_DELETE_MSG);
  }

  nLen = sqlite3_column_bytes(mpVM, nField);
  return (const unsigned char*)sqlite3_column_blob(mpVM, nField);
}

const unsigned char *CppSQLite3Query::getBlobField(const string &szField, int &nLen) const
{
  int nField = fieldIndex(szField);
  return getBlobField(nField, nLen);
}

bool CppSQLite3Query::fieldIsNull(int nField) const
{
  return (fieldDataType(nField) == SQLITE_NULL);
}

bool CppSQLite3Query::fieldIsNull(const string &szField) const
{
  int nField = fieldIndex(szField);
  return (fieldDataType(nField) == SQLITE_NULL);
}

int CppSQLite3Query::fieldIndex(const string &szField) const
{
  checkVM();

  if (!szField.empty()) {
    string szTemp;
    for (int nField = 0; nField < mnCols; nField++) {
      szTemp = sqlite3_column_name(mpVM, nField);

      if (szField == szTemp) {
        return nField;
      }
    }
  }

  throw CppSQLite3Exception(CPPSQLITE_ERROR, "Invalid field name requested", DONT_DELETE_MSG);
}

string CppSQLite3Query::fieldName(int nCol) const
{
  checkVM();
  checkFieldIndex(nCol);

  return sqlite3_column_name(mpVM, nCol);
}

string CppSQLite3Query::fieldDeclType(int nCol) const
{
  checkVM();
  checkFieldIndex(nCol);

  return sqlite3_column_decltype(mpVM, nCol);
}

int CppSQLite3Query::fieldDataType(int nCol) const
{
  checkVM();
  checkFieldIndex(nCol);

  return sqlite3_column_type(mpVM, nCol);
}

bool CppSQLite3Query::eof() const
{
  checkVM();
  return mbEof;
}

void CppSQLite3Query::nextRow()
{
  checkVM();

  int tries = 0;

  while (true) {
    int nRet = sqlite3_step(mpVM);

    if (nRet == SQLITE_DONE) {
      // no rows
      mbEof = true;
      break;

    } else if (nRet == SQLITE_ROW) {
      // more rows, nothing to do
      break;

    } else if ((nRet == SQLITE_BUSY || nRet == SQLITE_LOCKED) && tries < mnMaxRetryCount) {
      // Database is locked, sleep for a bit
      cout << "Database is locked." << endl;

      if (nRet == SQLITE_BUSY) {
        rollback();
      }

      // Sleep fora bit to give thread holding the lock to finish
      usleep(mnMaxRetryCount);

      tries++;
      continue;

    } else {
      if (nRet == SQLITE_FULL || nRet == SQLITE_IOERR || nRet == SQLITE_NOMEM || nRet == SQLITE_BUSY || nRet == SQLITE_INTERRUPT) {
        rollback();
      }

      nRet = sqlite3_finalize(mpVM);
      mpVM = NULL;
      const char *szError = sqlite3_errmsg(mpDB);
      throw CppSQLite3Exception(nRet, szError, DONT_DELETE_MSG);
    }
  }
}

void CppSQLite3Query::finalize()
{
  if (mpVM && mbOwnVM) {
    int nRet = sqlite3_finalize(mpVM);
    mpVM = NULL;
    if (nRet != SQLITE_OK) {
      const char *szError = sqlite3_errmsg(mpDB);
      throw CppSQLite3Exception(nRet, szError, DONT_DELETE_MSG);
    }
  }
}

void CppSQLite3Query::setMaxRetryCount(int nMaxRetryCount)
{
  mnMaxRetryCount = nMaxRetryCount;
}

void CppSQLite3Query::setRetryTimeUs(int nRetryTimeUs)
{
  mnRetryTimeUs = nRetryTimeUs;
}

void CppSQLite3Query::rollback() const
{
  // Returns 0 when the given database connection
  // is in the middle of a manually-initiated transaction
  if (sqlite3_get_autocommit(mpDB) == 0) {
    cout << "Rolling back db transaction." << endl;

    // Rollback the transaction that failed
    char *szError = NULL;
    int nRet2 = sqlite3_exec(mpDB, "ROLLBACK", 0, 0, &szError);

    if (nRet2 != SQLITE_OK) {
      sqlite3_free(szError);
    }
  }
}

////////////////////////////////////////////////////////////////////////////////

CppSQLite3Table::CppSQLite3Table()
  : mnRows(0),
    mnCols(0),
    mnCurrentRow(0),
    mpaszResults(NULL)
{
}

CppSQLite3Table::CppSQLite3Table(const CppSQLite3Table &rTable)
{
  mpaszResults = rTable.mpaszResults;
  // Only one object can own the results
  const_cast<CppSQLite3Table&>(rTable).mpaszResults = NULL;
  mnRows = rTable.mnRows;
  mnCols = rTable.mnCols;
  mnCurrentRow = rTable.mnCurrentRow;
}

CppSQLite3Table::CppSQLite3Table(char **paszResults, int nRows, int nCols)
  : mnRows(nRows),
    mnCols(nCols),
    mnCurrentRow(0),
    mpaszResults(paszResults)
{
}

CppSQLite3Table::~CppSQLite3Table()
{
  try {
    finalize();
  } catch (...) {
  }
}

CppSQLite3Table &CppSQLite3Table::operator=(const CppSQLite3Table &rTable)
{
  try {
    finalize();
  } catch (...) {
  }
  mpaszResults = rTable.mpaszResults;
  // Only one object can own the results
  const_cast<CppSQLite3Table&>(rTable).mpaszResults = NULL;
  mnRows = rTable.mnRows;
  mnCols = rTable.mnCols;
  mnCurrentRow = rTable.mnCurrentRow;
  return *this;
}

void CppSQLite3Table::finalize()
{
  if (mpaszResults) {
    sqlite3_free_table(mpaszResults);
    mpaszResults = NULL;
  }
}

int CppSQLite3Table::numFields() const
{
  checkResults();
  return mnCols;
}

int CppSQLite3Table::numRows() const
{
  checkResults();
  return mnRows;
}

string CppSQLite3Table::fieldValue(int nField) const
{
  checkResults();

  if (nField < 0 || nField > mnCols - 1) {
    throw CppSQLite3Exception(CPPSQLITE_ERROR, "Invalid field index requested", DONT_DELETE_MSG);
  }

  int nIndex = (mnCurrentRow * mnCols) + mnCols + nField;
  return mpaszResults[nIndex];
}

string CppSQLite3Table::fieldValue(const string &szField) const
{
  checkResults();

  if (!szField.empty()) {
    for (int nField = 0; nField < mnCols; nField++) {
      if (szField == string(mpaszResults[nField])) {
        int nIndex = (mnCurrentRow * mnCols) + mnCols + nField;
        return mpaszResults[nIndex];
      }
    }
  }

  throw CppSQLite3Exception(CPPSQLITE_ERROR, "Invalid field name requested", DONT_DELETE_MSG);
}

int CppSQLite3Table::getIntField(int nField, int nNullValue) const
{
  if (fieldIsNull(nField)) {
    return nNullValue;
  } else {
    return atoi(fieldValue(nField).c_str());
  }
}

int CppSQLite3Table::getIntField(const string &szField, int nNullValue) const
{
  if (fieldIsNull(szField)) {
    return nNullValue;
  } else {
    return atoi(fieldValue(szField).c_str());
  }
}

double CppSQLite3Table::getFloatField(int nField, double fNullValue) const
{
  if (fieldIsNull(nField)) {
    return fNullValue;
  } else {
    return atof(fieldValue(nField).c_str());
  }
}

double CppSQLite3Table::getFloatField(const string &szField, double fNullValue) const
{
  if (fieldIsNull(szField)) {
    return fNullValue;
  } else {
    return atof(fieldValue(szField).c_str());
  }
}

const string CppSQLite3Table::getStringField(int nField, const string &szNullValue) const
{
  if (fieldIsNull(nField)) {
    return szNullValue;
  } else {
    return fieldValue(nField);
  }
}

const string CppSQLite3Table::getStringField(const string &szField, const string &szNullValue) const
{
  if (fieldIsNull(szField)) {
    return szNullValue;
  } else {
    return fieldValue(szField);
  }
}

bool CppSQLite3Table::fieldIsNull(int nField) const
{
  checkResults();
  return fieldValue(nField).empty();
}

bool CppSQLite3Table::fieldIsNull(const string &szField) const
{
  checkResults();
  return fieldValue(szField).empty();
}

string CppSQLite3Table::fieldName(int nCol)
{
  checkResults();

  if (nCol < 0 || nCol > mnCols - 1) {
    throw CppSQLite3Exception(CPPSQLITE_ERROR, "Invalid field index requested", DONT_DELETE_MSG);
  }

  return mpaszResults[nCol];
}

void CppSQLite3Table::setRow(int nRow)
{
  checkResults();

  if (nRow < 0 || nRow > mnRows - 1) {
    throw CppSQLite3Exception(CPPSQLITE_ERROR, "Invalid row index requested", DONT_DELETE_MSG);
  }

  mnCurrentRow = nRow;
}

////////////////////////////////////////////////////////////////////////////////

CppSQLite3Statement::CppSQLite3Statement()
  : mpDB(NULL),
    mpVM(NULL),
    mnMaxRetryCount(5),     // Retry 5 times on SQLITE_LOCKED
    mnRetryTimeUs(5000)     // Sleep for 0.005 seconds before retrying on SQLITE_LOCKED
{
}

CppSQLite3Statement::CppSQLite3Statement(const CppSQLite3Statement &rStatement)
{
  mpDB = rStatement.mpDB;
  mpVM = rStatement.mpVM;
  mnMaxRetryCount = rStatement.mnMaxRetryCount;
  mnRetryTimeUs = rStatement.mnRetryTimeUs;
  // Only one object can own VM
  const_cast<CppSQLite3Statement&>(rStatement).mpVM = NULL;
}

CppSQLite3Statement::CppSQLite3Statement(sqlite3 *pDB, sqlite3_stmt *pVM)
  : mpDB(pDB),
    mpVM(pVM),
    mnMaxRetryCount(5),     // Retry 5 times on SQLITE_LOCKED
    mnRetryTimeUs(5000)     // Sleep for 0.005 seconds before retrying on SQLITE_LOCKED
{
}

CppSQLite3Statement::~CppSQLite3Statement()
{
  try {
    finalize();
  } catch (...) {
  }
}

CppSQLite3Statement &CppSQLite3Statement::operator=(const CppSQLite3Statement &rStatement)
{
  mpDB = rStatement.mpDB;
  mpVM = rStatement.mpVM;
  // Only one object can own VM
  const_cast<CppSQLite3Statement&>(rStatement).mpVM = NULL;
  return *this;
}

int CppSQLite3Statement::execDML() const
{
  checkDB();
  checkVM();

  const char *szError = NULL;

  int nRet = sqlite3_step(mpVM);

  if (nRet == SQLITE_DONE) {
    int nRowsChanged = sqlite3_changes(mpDB);

    nRet = sqlite3_reset(mpVM);

    if (nRet != SQLITE_OK) {
      szError = sqlite3_errmsg(mpDB);
      throw CppSQLite3Exception(nRet, szError, DONT_DELETE_MSG);
    }

    return nRowsChanged;
  } else {
    nRet = sqlite3_reset(mpVM);
    szError = sqlite3_errmsg(mpDB);
    throw CppSQLite3Exception(nRet, szError, DONT_DELETE_MSG);
  }
}

CppSQLite3Query CppSQLite3Statement::execQuery() const
{
  checkDB();
  checkVM();

  int tries = 0;

  while (true) {
    int nRet = sqlite3_step(mpVM);

    if (nRet == SQLITE_DONE) {
      // no rows
      CppSQLite3Query query(mpDB, mpVM, true, false);
      query.setMaxRetryCount(mnMaxRetryCount);
      query.setRetryTimeUs(mnRetryTimeUs);
      return query;
    } else if (nRet == SQLITE_ROW) {
      // at least 1 row
      CppSQLite3Query query(mpDB, mpVM, false, false);
      query.setMaxRetryCount(mnMaxRetryCount);
      query.setRetryTimeUs(mnRetryTimeUs);
      return query;

    } else if ((nRet == SQLITE_BUSY || nRet == SQLITE_LOCKED) && tries < mnMaxRetryCount) {
      // Database is locked, sleep for a bit
      cout << "Database is locked." << endl;

      // Sleep fora bit to give thread holding the lock to finish
      usleep(mnRetryTimeUs);

      tries++;
      continue;

    } else {
      nRet = sqlite3_reset(mpVM);
      const char *szError = sqlite3_errmsg(mpDB);
      throw CppSQLite3Exception(nRet, szError, DONT_DELETE_MSG);
    }
  }
}

void CppSQLite3Statement::bind(int nParam, const string &szValue)
{
  checkVM();
  int nRes = sqlite3_bind_text(mpVM, nParam, szValue.c_str(), -1, SQLITE_TRANSIENT);

  if (nRes != SQLITE_OK) {
    throw CppSQLite3Exception(nRes, "Error binding string param", DONT_DELETE_MSG);
  }
}

void CppSQLite3Statement::bind(int nParam, const int nValue)
{
  checkVM();
  int nRes = sqlite3_bind_int(mpVM, nParam, nValue);

  if (nRes != SQLITE_OK) {
    throw CppSQLite3Exception(nRes, "Error binding int param", DONT_DELETE_MSG);
  }
}

void CppSQLite3Statement::bind(int nParam, const int64_t nValue)
{
  checkVM();
  int nRes = sqlite3_bind_int64(mpVM, nParam, nValue);

  if (nRes != SQLITE_OK) {
    throw CppSQLite3Exception(nRes, "Error binding int64 param", DONT_DELETE_MSG);
  }
}

void CppSQLite3Statement::bind(int nParam, const double dValue)
{
  checkVM();
  int nRes = sqlite3_bind_double(mpVM, nParam, dValue);

  if (nRes != SQLITE_OK) {
    throw CppSQLite3Exception(nRes, "Error binding double param", DONT_DELETE_MSG);
  }
}

void CppSQLite3Statement::bind(int nParam, const unsigned char *blobValue, int nLen)
{
  checkVM();
  int nRes = sqlite3_bind_blob(mpVM, nParam, (const void*)blobValue, nLen, SQLITE_TRANSIENT);

  if (nRes != SQLITE_OK) {
    throw CppSQLite3Exception(nRes, "Error binding blob param", DONT_DELETE_MSG);
  }
}

void CppSQLite3Statement::bindNull(int nParam)
{
  checkVM();
  int nRes = sqlite3_bind_null(mpVM, nParam);

  if (nRes != SQLITE_OK) {
    throw CppSQLite3Exception(nRes, "Error binding NULL param", DONT_DELETE_MSG);
  }
}

void CppSQLite3Statement::reset()
{
  if (mpVM) {
    int nRet = sqlite3_reset(mpVM);

    if (nRet != SQLITE_OK) {
      const char *szError = sqlite3_errmsg(mpDB);
      throw CppSQLite3Exception(nRet, szError, DONT_DELETE_MSG);
    }
  }
}

void CppSQLite3Statement::finalize()
{
  if (mpVM) {
    int nRet = sqlite3_finalize(mpVM);
    mpVM = NULL;

    if (nRet != SQLITE_OK) {
      const char *szError = sqlite3_errmsg(mpDB);
      throw CppSQLite3Exception(nRet, szError, DONT_DELETE_MSG);
    }
  }
}

void CppSQLite3Statement::setMaxRetryCount(int nMaxRetryCount)
{
  mnMaxRetryCount = nMaxRetryCount;
}

void CppSQLite3Statement::setRetryTimeUs(int nRetryTimeUs)
{
  mnRetryTimeUs = nRetryTimeUs;
}

////////////////////////////////////////////////////////////////////////////////

CppSQLite3DB::CppSQLite3DB()
  : mpDB(NULL),
    mnBusyTimeoutMs(1000), // 1 seconds
    mnMaxRetryCount(5),     // Retry 5 times on SQLITE_LOCKED
    mnRetryTimeUs(5000)     // Sleep for 0.005 seconds before retrying on SQLITE_LOCKED
{
}

CppSQLite3DB::CppSQLite3DB(const CppSQLite3DB &db)
  : mpDB(db.mpDB),
    mnBusyTimeoutMs(db.mnBusyTimeoutMs),
    mnMaxRetryCount(db.mnMaxRetryCount),
    mnRetryTimeUs(db.mnRetryTimeUs)
{
}

CppSQLite3DB::~CppSQLite3DB()
{
  close();
}

CppSQLite3DB &CppSQLite3DB::operator=(const CppSQLite3DB &db)
{
  mpDB = db.mpDB;
  mnBusyTimeoutMs = 1000; // 1 seconds
  return *this;
}

void CppSQLite3DB::open(const string &szFile)
{
  int nRet = sqlite3_open_v2(szFile.c_str(), &mpDB, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);

  if (nRet != SQLITE_OK) {
    const char *szError = sqlite3_errmsg(mpDB);

    close();

    throw CppSQLite3Exception(nRet, szError, DONT_DELETE_MSG);
  }

	// Enable extended error codes
	sqlite3_extended_result_codes(mpDB, 1);

  setBusyTimeout(mnBusyTimeoutMs);
}

void CppSQLite3DB::close()
{
  if (mpDB) {
    sqlite3_close_v2(mpDB);
    mpDB = NULL;
  }
}

CppSQLite3Statement CppSQLite3DB::compileStatement(const string &szSQL) const
{
  checkDB();

  sqlite3_stmt *pVM = compile(szSQL);
  return CppSQLite3Statement(mpDB, pVM);
}

bool CppSQLite3DB::tableExists(const string &szTable) const
{
  ostringstream sql;
  sql << "select count(*) from sqlite_master where type='table' and name='" << szTable << "'";
  int nRet = execScalar(sql.str());
  return (nRet > 0);
}

int CppSQLite3DB::execDML(const string &szSQL)
{
  checkDB();

  char *szError = NULL;

  int tries = 0;

  while (true) {
    int nRet = sqlite3_exec(mpDB, szSQL.c_str(), 0, 0, &szError);

    if (nRet == SQLITE_OK) {
      return sqlite3_changes(mpDB);

    } else if ((nRet == SQLITE_BUSY || nRet == SQLITE_LOCKED) && tries < mnMaxRetryCount) {
      // Database is locked, sleep for a bit
      cout << "Database is locked." << endl;

      sqlite3_free(szError);

      if (nRet == SQLITE_BUSY) {
        rollback();
      }

      // Sleep fora bit to give thread holding the lock to finish
      usleep(mnRetryTimeUs);

      tries++;
      continue;
    } else {
      if (nRet == SQLITE_FULL || nRet == SQLITE_IOERR || nRet == SQLITE_NOMEM || nRet == SQLITE_BUSY || nRet == SQLITE_INTERRUPT) {
        rollback();
      }

      throw CppSQLite3Exception(nRet, szError);
    }
  }
}

CppSQLite3Query CppSQLite3DB::execQuery(const std::string &szSQL) const
{
  checkDB();

  sqlite3_stmt *pVM = compile(szSQL);

  int tries = 0;

  while (true) {
    int nRet = sqlite3_step(pVM);

    if (nRet == SQLITE_DONE) {
      // no rows
      CppSQLite3Query query(mpDB, pVM, true);
      query.setMaxRetryCount(mnMaxRetryCount);
      query.setRetryTimeUs(mnRetryTimeUs);
      return query;
    } else if (nRet == SQLITE_ROW) {
      // at least 1 row
      CppSQLite3Query query(mpDB, pVM, false);
      query.setMaxRetryCount(mnMaxRetryCount);
      query.setRetryTimeUs(mnRetryTimeUs);
      return query;

    } else if ((nRet == SQLITE_BUSY || nRet == SQLITE_LOCKED) && tries < mnMaxRetryCount) {
      // Database is locked, sleep for a bit
      cout << "Database is locked." << endl;

      if (nRet == SQLITE_BUSY) {
        rollback();
      }

      // Sleep fora bit to give thread holding the lock to finish
      usleep(mnRetryTimeUs);

      tries++;
      continue;

    } else {
      if (nRet == SQLITE_FULL || nRet == SQLITE_IOERR || nRet == SQLITE_NOMEM || nRet == SQLITE_BUSY || nRet == SQLITE_INTERRUPT) {
        rollback();
      }

      nRet = sqlite3_finalize(pVM);
      const char *szError= sqlite3_errmsg(mpDB);
      throw CppSQLite3Exception(nRet, szError, DONT_DELETE_MSG);
    }
  }
}

int CppSQLite3DB::execScalar(const string &szSQL) const
{
  CppSQLite3Query q = execQuery(szSQL);

  if (q.eof() || q.numFields() < 1) {
    throw CppSQLite3Exception(CPPSQLITE_ERROR, "Invalid scalar query", DONT_DELETE_MSG);
  }

  return atoi(q.fieldValue(0).c_str());
}

CppSQLite3Table CppSQLite3DB::getTable(const string &szSQL) const
{
  checkDB();

  char *szError = NULL;
  char **paszResults = NULL;
  int nRet;
  int nRows = 0;
  int nCols = 0;

  nRet = sqlite3_get_table(mpDB, szSQL.c_str(), &paszResults, &nRows, &nCols, &szError);

  if (nRet == SQLITE_OK) {
    return CppSQLite3Table(paszResults, nRows, nCols);
  } else {
    throw CppSQLite3Exception(nRet, szError);
  }
}

sqlite_int64 CppSQLite3DB::lastRowId() const
{
  return sqlite3_last_insert_rowid(mpDB);
}

void CppSQLite3DB::interrupt()
{
  checkDB();
  sqlite3_interrupt(mpDB);
}

void CppSQLite3DB::backup(const std::string &target)
{
  backupOrRestore(target, true);
}

void CppSQLite3DB::restore(const std::string &target)
{
  backupOrRestore(target, false);
}

void CppSQLite3DB::backupOrRestore(const std::string &target, bool isBackup)
{
  checkDB();

  // Open the file that we are going to backup to ro restore from
  CppSQLite3DB backupDB;
  backupDB.open(target);

	if (isBackup) {
		backupDB.execDML("PRAGMA journal_mode = WAL");
	}

  sqlite3 *pTo   = (isBackup ? backupDB.mpDB : mpDB);
  sqlite3 *pFrom = (isBackup ? mpDB          : backupDB.mpDB);

  sqlite3_backup *pBackup = sqlite3_backup_init(pTo, "main", pFrom, "main");

  if (!pBackup) {
    int nRet = sqlite3_errcode(pTo);
    const char *szError = sqlite3_errmsg(pTo);
    throw CppSQLite3Exception(nRet, szError, DONT_DELETE_MSG);
  }

  int tries = 0;

  while (true) {
    // Copy every page from pFrom into backupDB
    int nRet = sqlite3_backup_step(pBackup, -1);

    if (nRet == SQLITE_DONE) {
      // Backup completed successfully
      sqlite3_backup_finish(pBackup);
      break;

    } else if (nRet == SQLITE_OK) {
      // Copying pages completed successfully, but there is still more work to do

    } else if ((nRet == SQLITE_BUSY || nRet == SQLITE_LOCKED) && tries < mnMaxRetryCount) {
      // Database is locked, sleep for a bit
      cout << "CppSQLite3DB::backupOrRestore: Database is locked." << endl;

      // Sleep fora bit to give thread holding the lock to finish
      usleep(mnRetryTimeUs);

      tries++;
      continue;

    } else {
      nRet = sqlite3_backup_finish(pBackup);
      const char *szError = sqlite3_errmsg(pTo);
      throw CppSQLite3Exception(nRet, szError, DONT_DELETE_MSG);
    }
  }
}

void CppSQLite3DB::setBusyTimeout(int nMillisecs)
{
  mnBusyTimeoutMs = nMillisecs;
  sqlite3_busy_timeout(mpDB, mnBusyTimeoutMs);
}

void CppSQLite3DB::setMaxRetryCount(int nMaxRetryCount)
{
  mnMaxRetryCount = nMaxRetryCount;
}

void CppSQLite3DB::setRetryTimeUs(int nRetryTimeUs)
{
  mnRetryTimeUs = nRetryTimeUs;
}

sqlite3_stmt *CppSQLite3DB::compile(const string &szSQL) const
{
  checkDB();

  const char *szTail = NULL;
  sqlite3_stmt *pVM;

  int nRet = sqlite3_prepare_v2(mpDB, szSQL.c_str(), -1, &pVM, &szTail);

  if (nRet != SQLITE_OK) {
    const char *szError = sqlite3_errmsg(mpDB);
    throw CppSQLite3Exception(nRet, szError, false);
  }

  return pVM;
}

void CppSQLite3DB::rollback() const
{
  // Returns 0 when the given database connection
  // is in the middle of a manually-initiated transaction
  if (sqlite3_get_autocommit(mpDB) == 0) {
    cout << "Rolling back db transaction." << endl;

    // Rollback the transaction that failed
    char *szError = NULL;
    int nRet2 = sqlite3_exec(mpDB, "ROLLBACK", 0, 0, &szError);

    if (nRet2 != SQLITE_OK) {
      sqlite3_free(szError);
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
// SQLite encode.c reproduced here, containing implementation notes and source
// for sqlite3_encode_binary() and sqlite3_decode_binary()
////////////////////////////////////////////////////////////////////////////////

/*
** 2002 April 25
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**  May you do good and not evil.
**  May you find forgiveness for yourself and forgive others.
**  May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains helper routines used to translate binary data into
** a null-terminated string (suitable for use in SQLite) and back again.
** These are convenience routines for use by people who want to store binary
** data in an SQLite database.  The code in this file is not used by any other
** part of the SQLite library.
**
** $Id: encode.c,v 1.10 2004/01/14 21:59:23 drh Exp $
*/

/*
** How This Encoder Works
**
** The output is allowed to contain any character except 0x27 (') and
** 0x00.  This is accomplished by using an escape character to encode
** 0x27 and 0x00 as a two-byte sequence.  The escape character is always
** 0x01.  An 0x00 is encoded as the two byte sequence 0x01 0x01.  The
** 0x27 character is encoded as the two byte sequence 0x01 0x03.  Finally,
** the escape character itself is encoded as the two-character sequence
** 0x01 0x02.
**
** To summarize, the encoder works by using an escape sequences as follows:
**
**     0x00  ->  0x01 0x01
**     0x01  ->  0x01 0x02
**     0x27  ->  0x01 0x03
**
** If that were all the encoder did, it would work, but in certain cases
** it could double the size of the encoded string.  For example, to
** encode a string of 100 0x27 characters would require 100 instances of
** the 0x01 0x03 escape sequence resulting in a 200-character output.
** We would prefer to keep the size of the encoded string smaller than
** this.
**
** To minimize the encoding size, we first add a fixed offset value to each
** byte in the sequence.  The addition is modulo 256.  (That is to say, if
** the sum of the original character value and the offset exceeds 256, then
** the higher order bits are truncated.)  The offset is chosen to minimize
** the number of characters in the string that need to be escaped.  For
** example, in the case above where the string was composed of 100 0x27
** characters, the offset might be 0x01.  Each of the 0x27 characters would
** then be converted into an 0x28 character which would not need to be
** escaped at all and so the 100 character input string would be converted
** into just 100 characters of output.  Actually 101 characters of output -
** we have to record the offset used as the first byte in the sequence so
** that the string can be decoded.  Since the offset value is stored as
** part of the output string and the output string is not allowed to contain
** characters 0x00 or 0x27, the offset cannot be 0x00 or 0x27.
**
** Here, then, are the encoding steps:
**
**   (1)   Choose an offset value and make it the first character of
**       output.
**
**   (2)   Copy each input character into the output buffer, one by
**       one, adding the offset value as you copy.
**
**   (3)   If the value of an input character plus offset is 0x00, replace
**       that one character by the two-character sequence 0x01 0x01.
**       If the sum is 0x01, replace it with 0x01 0x02.  If the sum
**       is 0x27, replace it with 0x01 0x03.
**
**   (4)   Put a 0x00 terminator at the end of the output.
**
** Decoding is obvious:
**
**   (5)   Copy encoded characters except the first into the decode
**       buffer.  Set the first encoded character aside for use as
**       the offset in step 7 below.
**
**   (6)   Convert each 0x01 0x01 sequence into a single character 0x00.
**       Convert 0x01 0x02 into 0x01.  Convert 0x01 0x03 into 0x27.
**
**   (7)   Subtract the offset value that was the first character of
**       the encoded buffer from all characters in the output buffer.
**
** The only tricky part is step (1) - how to compute an offset value to
** minimize the size of the output buffer.  This is accomplished by testing
** all offset values and picking the one that results in the fewest number
** of escapes.  To do that, we first scan the entire input and count the
** number of occurances of each character value in the input.  Suppose
** the number of 0x00 characters is N(0), the number of occurances of 0x01
** is N(1), and so forth up to the number of occurances of 0xff is N(255).
** An offset of 0 is not allowed so we don't have to test it.  The number
** of escapes required for an offset of 1 is N(1)+N(2)+N(40).  The number
** of escapes required for an offset of 2 is N(2)+N(3)+N(41).  And so forth.
** In this way we find the offset that gives the minimum number of escapes,
** and thus minimizes the length of the output string.
*/

/*
** Encode a binary buffer "in" of size n bytes so that it contains
** no instances of characters '\'' or '\000'.  The output is
** null-terminated and can be used as a string value in an INSERT
** or UPDATE statement.  Use sqlite3_decode_binary() to convert the
** string back into its original binary.
**
** The result is written into a preallocated output buffer "out".
** "out" must be able to hold at least 2 +(257*n)/254 bytes.
** In other words, the output will be expanded by as much as 3
** bytes for every 254 bytes of input plus 2 bytes of fixed overhead.
** (This is approximately 2 + 1.0118*n or about a 1.2% size increase.)
**
** The return value is the number of characters in the encoded
** string, excluding the "\000" terminator.
*/
int sqlite3_encode_binary(const unsigned char *in, int n, unsigned char *out){
  int i, j, e, m;
  int cnt[256];
  if( n<=0 ){
    out[0] = 'x';
    out[1] = 0;
    return 1;
  }
  memset(cnt, 0, sizeof(cnt));
  for(i=n-1; i>=0; i--){
    cnt[in[i]]++;
  }
  m = n;
  for(i=1; i<256; i++){
    int sum;
    if( i=='\'' ) continue;
    sum = cnt[i] + cnt[(i+1)&0xff] + cnt[(i+'\'')&0xff];
    if( sum<m ){
      m = sum;
      e = i;
      if( m==0 ) break;
    }
  }
  out[0] = e;
  j = 1;
  for(i=0; i<n; i++){
    int c = (in[i] - e)&0xff;
    if( c==0 ){
      out[j++] = 1;
      out[j++] = 1;
    }else if( c==1 ){
      out[j++] = 1;
      out[j++] = 2;
    }else if( c=='\'' ){
      out[j++] = 1;
      out[j++] = 3;
    }else{
      out[j++] = c;
    }
  }
  out[j] = 0;
  return j;
}

/*
** Decode the string "in" into binary data and write it into "out".
** This routine reverses the encoding created by sqlite3_encode_binary().
** The output will always be a few bytes less than the input.  The number
** of bytes of output is returned.  If the input is not a well-formed
** encoding, -1 is returned.
**
** The "in" and "out" parameters may point to the same buffer in order
** to decode a string in place.
*/
int sqlite3_decode_binary(const unsigned char *in, unsigned char *out){
  int i, c, e;
  e = *(in++);
  i = 0;
  while( (c = *(in++))!=0 ){
    if( c==1 ){
      c = *(in++);
      if( c==1 ){
        c = 0;
      }else if( c==2 ){
        c = 1;
      }else if( c==3 ){
        c = '\'';
      }else{
        return -1;
      }
    }
    out[i++] = (c + e)&0xff;
  }
  return i;
}
