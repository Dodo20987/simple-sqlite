#pragma once
#include <iostream>
#include <string>
#include <sstream>
#include <cstdint>
#include <fstream>
#include <vector>
#include <algorithm>
#include <iomanip>

const int HEADER_SIZE = 100;
const int PAGE_SIZE = 8;

class Database {
private:
    mutable std::ifstream database_file;
    unsigned int parseVarint(const unsigned char* data, int& bytes_read) const;
    const unsigned short getPageSize() const;
public:
    Database(std::ifstream&& database_file) : database_file(std::move(database_file)) {}
    const std::streampos getFileSize() const;
    void printTables();
    void printDBInfo();
    void printRowCount(const std::string& query);
    void selectColumn(const std::string& query);
    bool isCount(const std::string& query) const;


};