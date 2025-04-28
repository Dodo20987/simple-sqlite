#pragma once
#include <iostream>
#include <string>
#include <sstream>
#include <cstdint>
#include <fstream>
#include <vector>
#include <algorithm>

class Database {
private:
    mutable std::ifstream database_file;
    bool isCount(const std::string& query) const;
    unsigned int parse_varint(const unsigned char* data, int& bytes) const;
    unsigned short get_page_size() const;
public:
    Database(std::ifstream&& database_file) : database_file(std::move(database_file)) {}
    const std::streampos getFileSize() const;
    void printTables();
    void printDBInfo();
    void printRowCount(const std::string& query);
    void selectColumng(const std::string& query);

};