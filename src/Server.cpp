#include <cstring>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
const int HEADER_SIZE = 100;
const int PAGE_SIZE = 8;
void printTables(std::string database_file_path) {
    std::ifstream database_file(database_file_path, std::ios::binary);
    if (!database_file) {
        std::cerr << "Failed to open the database file" << std::endl;
        return;
    }   
    char buf[2];
    database_file.seekg(HEADER_SIZE + 3);
    database_file.read(buf,2);
    unsigned short cell_count = (static_cast<unsigned char>(buf[1]) | (static_cast<unsigned char>(buf[0]) << 8));

    for (int i = 0; i < cell_count; i++) {
        database_file.seekg(HEADER_SIZE + PAGE_SIZE + (i * 2));
        database_file.read(buf,2);
        unsigned short offset = (static_cast<unsigned char>(buf[1]) | (static_cast<unsigned char>(buf[0]) << 8));
        database_file.seekg(offset);
        
        char record[3];
        database_file.read(record, 3);

        unsigned short cols = static_cast<unsigned short>(record[2]);
        char record_header[cols];

        // first entry contains the serial type for type, and entry 2
        // contains the serial type for name
        database_file.read(record_header, cols - 1); // -1 because the size of the header is alredy read previously

        unsigned short type_size = (static_cast<unsigned short>(record_header[0]) - 13) / 2;
        unsigned short name_size = (static_cast<unsigned short>(record_header[1]) - 13) / 2;
        char type_name[type_size];
        char table_name[name_size];
        database_file.read(type_name, type_size);
        database_file.read(table_name, name_size);

        std::cout << table_name << ((i == cell_count - 1) ? "\n" : " ");
        
    }
}
void printDBInfo(std::string database_file_path) {
    std::ifstream database_file(database_file_path, std::ios::binary);
    if (!database_file) {
        std::cerr << "Failed to open the database file" << std::endl;
        return;
    }

    // Uncomment this to pass the first stage
    database_file.seekg(16);  // Skip the first 16 bytes of the header
     
    char buffer[2];
    database_file.read(buffer, 2);
    unsigned short page_size = (static_cast<unsigned char>(buffer[1]) | (static_cast<unsigned char>(buffer[0]) << 8));
    database_file.seekg(HEADER_SIZE + 3);
    database_file.read(buffer,2);
    unsigned short table_size = (static_cast<unsigned char>(buffer[1]) | (static_cast<unsigned char>(buffer[0]) << 8));

    std::cout << "database page size: " << page_size << std::endl;
    std::cout << "number of tables: " << table_size << std::endl;
 
}

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

    if (command == ".dbinfo") {
        printDBInfo(database_file_path);
    }
    else if (command == ".tables") {
        printTables(database_file_path);
    }
    else {
        std::ifstream database_file(database_file_path, std::ios::binary);
        if (!database_file) {
            std::cerr << "Failed to open the database file" << std::endl;
            return 1;
        }
        std::istringstream iss(command);
        std::vector<std::string> tokens;
        std::string word;
        while (iss >> word) {
            tokens.push_back(word);
        }
        /*
        for (auto x : tokens) {
            std::cout << x << std::endl;
        }*/
        std::string table = tokens.back();
        char buf[2];
        database_file.seekg(HEADER_SIZE + 3);
        database_file.read(buf,2);
        unsigned short cell_count = (static_cast<unsigned char>(buf[1]) | (static_cast<unsigned char>(buf[0]) << 8));
        for (int i = 0; i < cell_count; i++) {
            database_file.seekg(HEADER_SIZE + PAGE_SIZE + (i * 2));
            database_file.read(buf,2);
            unsigned short offset = (static_cast<unsigned char>(buf[1]) | (static_cast<unsigned char>(buf[0]) << 8));
            database_file.seekg(offset);
            
            char record[3];
            database_file.read(record, 3);

            unsigned short cols = static_cast<unsigned short>(record[2]);
            char record_header[cols];

            // first entry contains the serial type for type, and entry 2
            // contains the serial type for name
            database_file.read(record_header, cols - 1); // -1 because the size of the header is alredy read previously

            unsigned short type_size = (static_cast<unsigned short>(record_header[0]) - 13) / 2;
            unsigned short name_size = (static_cast<unsigned short>(record_header[1]) - 13) / 2;
            unsigned short tbl_name_size = (static_cast<unsigned short>(record_header[2]) - 13) / 2;
            unsigned short root_size = static_cast<unsigned short>(record_header[3]);

            char type_name[type_size];
            char table_name[name_size];
            char tbl[tbl_name_size];
            char root[root_size];
            //std::cout << "root : " << root_size << std::endl;
            database_file.read(type_name, type_size);
            database_file.read(table_name, name_size);
            database_file.read(tbl, tbl_name_size);
            database_file.read(root, root_size);

            if (tbl == table) {
                std::cout << table_name << std::endl;
                unsigned short root_page = static_cast<unsigned short>(root[0]);
                database_file.seekg((root_page - 1) * 4096); 
                char buffer[5];
                database_file.read(buffer,5);
                unsigned short number_of_rows = (static_cast<unsigned char>(buffer[4]) | (static_cast<unsigned char>(buffer[3]) << 8));
                std::cout << number_of_rows << std::endl;

                // 4096 is the default page size in sqlite
            }
        }
    }

    return 0;
}

