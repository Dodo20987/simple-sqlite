#include <cstring>
#include <iostream>
#include <fstream>
#include <string>
#include <iomanip>
#include <vector>
#include <sstream>
#include <algorithm>
#include <cstdint>
#include <unordered_map>
#include "../include/Database.h"
#include "../include/SQLParser.h"

void string_parse_test(const std::string& query) {
    SQLParser sql(query);
    auto res = sql.selectQuery();
    std::cout << "col_names: " << std::endl;
    for (const auto& x : res["cols"]) {
        std::cout << x << std::endl;
    }
    std::cout << "table names: " << std::endl;

    for (const auto& x : res["tables"]) {
        std::cout << x << std::endl;
    }
}
//TODO: instead of parsing the sql query by position use regex on the query
int main(int argc, char* argv[]) {
    // Flush after every std::cout / std::cerr
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    std::cerr << "Logs from your program will appear here" << std::endl;

    if (argc != 3) {
        std::cerr << "Expected two arguments" << std::endl;
        return 1;
    }
    //std::string query = "SELECT name, color FROM apples";
    //string_parse_test(query);
    std::string database_file_path = argv[1];
    std::string command = argv[2];
    std::ifstream database_file(database_file_path, std::ios::binary);
    if (!database_file) {
        std::cerr << "Failed to open the database file" << std::endl;
        return 0;
    }

    Database d1(std::move(database_file));
    if (command == ".dbinfo") {
        d1.printDBInfo();
    }
    else if (command == ".tables") {
        d1.printTables();
    }
    else {
        if(d1.isCount(command)) {
            d1.printRowCount(command);
        }
        else {
            d1.selectColumn(command);
        }
    }

    return 0;
}

