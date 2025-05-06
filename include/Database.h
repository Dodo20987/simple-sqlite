#pragma once
#include <iostream>
#include <string>
#include <sstream>
#include <cstdint>
#include <fstream>
#include <vector>
#include <algorithm>
#include <iomanip>
#include "../include/SQLParser.h"
#include <unordered_map>
#include "./serialHelper.h"
const int HEADER_SIZE = 100;
const int PAGE_SIZE = 8;

class Database {
private:
    mutable std::ifstream database_file;
    int64_t parseVarint(const unsigned char* data, int& bytes_read) const;
    const unsigned short getPageSize() const;
    bool matchesWhereCondition(const std::string& value, const std::string& operation, const std::string& condition) const;
    bool evaluateWhere(const WhereClause& where, const std::unordered_map<std::string, std::string>& row) const;
    bool evaluateCondition(const WhereCondition& cond, const std::unordered_map<std::string, std::string>& row) const;
    void parseSQL(const std::string& query) const;
    void navigateToRows();
    std::vector<std::string> extractColumnValues(const std::vector<uint64_t>& serial_types) const;
    const unsigned short extractNumberOfRows() const;
    void computeSchemaSize(const char* record_header,unsigned short size, unsigned short& type_size, unsigned short& name_size, 
        unsigned short& tbl_name_size, unsigned short& root_size,unsigned short& sql_size) const;
public:
    Database(std::ifstream&& database_file) : database_file(std::move(database_file)) {}
    const std::streampos getFileSize() const;
    void printTables();
    void printDBInfo();
    void printRowCount(const std::string& query);
    void selectColumn(const std::string& query);
    void selectColumnWithWhere(const std::string &query);
    bool isCount(const std::string& query) const;
    bool hasWhereClause(const std::string& query) const;

};