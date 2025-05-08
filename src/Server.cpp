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
void accept_input() {
    std::cout << "sqlite> ";
}
//TODO: instead of parsing the sql query by position use regex on the query
int main(int argc, char* argv[]) {
    // Flush after every std::cout / std::cerr
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    std::cerr << "Logs from your program will appear here" << std::endl;

    if (argc != 2) {
        std::cerr << "Expected two arguments" << std::endl;
        return 1;
    }
    //std::string query = "SELECT name, color FROM apples";
    //string_parse_test(query);
    std::string database_file_path = argv[1];
    //std::string command = argv[2];
    std::string input;
    while(true) {
        accept_input();
        std::getline(std::cin, input);
        std::ifstream database_file(database_file_path, std::ios::binary);
        if (!database_file) {
            std::cerr << "Failed to open the database file" << std::endl;
            return 0;
        }

        Database d1(std::move(database_file));
        //SQLParser s1(command);
        if (input == ".dbinfo") {
            d1.printDBInfo();
        }
        else if (input == ".tables") {
            d1.printTables();
        }
        else if(input == ".exit") {
            database_file.close();
            break;
        }
        else {
            if(d1.isCount(input)) {
                d1.printRowCount(input);
            }
            else if(d1.hasWhereClause(input))  {
                d1.selectColumnWithWhere(input);
            }
            else {
                d1.selectColumn(input);
            }
        }
    }

    return 0;
}

