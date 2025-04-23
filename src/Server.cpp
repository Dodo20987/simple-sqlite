#include <cstring>
#include <iostream>
#include <fstream>
const int HEADER_SIZE = 100;
const int PAGE_SIZE = 8;
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
        std::ifstream database_file(database_file_path, std::ios::binary);
        if (!database_file) {
            std::cerr << "Failed to open the database file" << std::endl;
            return 1;
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
    else if (command == ".tables") {
        std::ifstream database_file(database_file_path, std::ios::binary);
        if (!database_file) {
            std::cerr << "Failed to open the database file" << std::endl;
            return 1;
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

    return 0;
}

