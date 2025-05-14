#pragma once
#include <vector>
#include <string>
#include <sstream>
#include <cstdint>
#include <fstream>
#include <algorithm>
#include <iomanip>
#include "../include/SQLParser.h"

enum class TableB {
    leafCell = 0x0d,
    interiorCell = 0x05,
    unknown = 0
};
enum class IndexB {
    leafCell = 0x0a,
    interiorCell = 0x02,
    unknown = 0
};
class Database;
class BTreeNavigator {
private:
    TableB getPageTypeTableB(std::ifstream& database_file, uint32_t page_number, int page_size) const;
    IndexB getPageTypeIndexB(std::ifstream& database_file, uint32_t page_number, uint32_t page_offset) const;
public:
    BTreeNavigator() {}
    void traverseBTreePageTableB(std::ifstream& database_file, uint32_t page_number,int page_size, SQLParser& string_parser,
    std::vector<int>& col_indices, std::unordered_map<int, std::string>& index_to_name, Database& db);
    void traverseBTreePageIndexB(std::ifstream& database_file, uint32_t page_number, int page_size);
    void readInteriorTablePage(int page_offset);
    void readLeafTablePage(std::ifstream& database_file,uint32_t page_offset, SQLParser& string_parser, 
        std::vector<int>& col_indices, std::unordered_map<int, std::string>& index_to_name, Database& db);


};