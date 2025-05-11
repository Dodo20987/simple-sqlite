#pragma once
#include <vector>
#include <string>
#include <sstream>
#include <cstdint>
#include <fstream>
#include <algorithm>
#include <iomanip>
#include "../include/SQLParser.h"
#include "../include/Database.h"

class BTreeNavigator {
private:
    TableB getPageTypeTableB(std::ifstream& database_file, uint32_t page_number, int page_size) const;
    IndexB getPageTypeIndexB(std::ifstream& database_file, uint32_t page_number, int page_size) const;
public:
    BTreeNavigator() {}
    void traverseBTreePageTableB(std::ifstream& database_file, uint32_t page_number, int page_offset, int page_size);
    void traverseBTreePageIndexB(std::ifstream& database_file, uint32_t page_number, int page_size);
    void readInteriorTablePage(int page_offset);
    void readLeafTablePage(int page_offset);


};