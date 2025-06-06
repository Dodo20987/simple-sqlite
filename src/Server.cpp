#include <cstring>
#include <iostream>
#include <fstream>
#include <string>
#include <iomanip>
#include <vector>
#include <sstream>
#include <algorithm>
#include <cstdint>
#include <chrono>
#include <unordered_map>
#include "../include/Database.h"
#include "../include/SQLParser.h"
#include "../include/BTreeNavigator.h"
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
    std::ifstream database_file(database_file_path, std::ios::binary | std::ios::ate);
    BTreeNavigator nav;
    Database d1(std::move(database_file),nav);
    while(true) {
        auto start = std::chrono::high_resolution_clock::now();
        accept_input();
        std::getline(std::cin, input);

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
        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> duration = end - start;
        std::cout << "Elapsed time: " << duration.count() << " seconds" << std::endl;
    }
    database_file.close();


    return 0;
}

