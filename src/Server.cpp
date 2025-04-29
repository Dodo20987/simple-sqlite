#include <cstring>
#include <iostream>
#include <fstream>
#include <string>
#include <iomanip>
#include <vector>
#include <sstream>
#include <algorithm>
#include <cstdint>
#include "../include/Database.h"

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

