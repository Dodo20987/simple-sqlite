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
#include "./BTreeNavigator.h"
#include <optional>
const int HEADER_SIZE = 100;
const int PAGE_SIZE = 8;

class BTreeNavigator;
struct schemaRecord {
    std::string type_name;
    std::string table_name;
    std::string tbl;
    std::string root;
    std::string sql;
    bool is_index = false;
};
class Database {
private:
    mutable std::ifstream database_file;
    mutable BTreeNavigator b_tree_nav;
    const unsigned short getPageSize() const;
    bool matchesWhereCondition(const std::string& value, const std::string& operation, const std::string& condition) const;
    bool evaluateCondition(const WhereCondition& cond, const std::unordered_map<std::string, std::string>& row) const;
    void parseSQL(const std::string& query) const;
    void navigateToRows();
    unsigned short extractNumberOfRows(const std::string& columns_def, 
        const std::unordered_map<std::string, std::vector<std::string>> tokens, const char* root, unsigned short page_size) const;
    void computeSchemaSize(const char* record_header,unsigned short size, unsigned short& type_size, unsigned short& name_size, 
        unsigned short& tbl_name_size, unsigned short& root_size,unsigned short& sql_size) const;
    std::optional<schemaRecord> containsIndexRecord(const std::unordered_map<std::string, schemaRecord>& records, const schemaRecord& record) const;
    std::vector<long> selectColumnIndex(const schemaRecord& index_record, SQLParser& string_parser);
    
public:
    Database(std::ifstream&& database_file, BTreeNavigator nav) : database_file(std::move(database_file)), b_tree_nav(nav) {}
    const std::streampos getFileSize() const;
    void printTables();
    void printDBInfo();
    void printRowCount(const std::string& query);
    void selectColumn(const std::string& query);
    void selectColumnWithWhere(const std::string &query);
    bool isCount(const std::string& query) const;
    bool hasWhereClause(const std::string& query) const;
    std::vector<std::string> extractColumnValues(const std::vector<uint64_t>& serial_types, uint64_t& rowid) const;
    std::unordered_map<std::string, schemaRecord> getRecords(unsigned short cell_count) const;
    bool isMatchingIndex(const schemaRecord& record, const std::string& table_name_string) const;
    int64_t parseVarint(const unsigned char* data, int& bytes_read) const;
    bool evaluateWhere(const WhereClause& where, const std::unordered_map<std::string, std::string>& row) const;
    std::vector<uint64_t> computeSerialTypes(uint32_t page_offset, char* buf, int index, uint64_t& rowid) const;
    std::vector<uint64_t> computeIndexSerialTypes(uint64_t page_offset, char* buf, int index, bool is_leaf) const;   
};