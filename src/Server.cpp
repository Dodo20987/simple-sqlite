#include <cstring>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <algorithm>
const int HEADER_SIZE = 100;
const int PAGE_SIZE = 8;
// Function to parse a varint from a byte array
unsigned int parse_varint(const unsigned char* data, int& bytes_read) {
    unsigned int result = 0;
    bytes_read = 0;
    
    for (int i = 0; i < 9; ++i) {
        result = (result << 7) | (data[i] & 0x7F);
        ++bytes_read;
        if (!(data[i] & 0x80)) {
            break; // MSB not set = last byte
        }
    }
    return result;
}

bool is_count(const std::string& query) {
    std::string upper_query = query;
    std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);

    return upper_query.find("COUNT(") != std::string::npos;
}
unsigned short get_page_size(std::ifstream database_file) {
    database_file.seekg(16);
    char buf[2];
    database_file.read(buf,2);
    return (static_cast<unsigned char>(buf[1]) | (static_cast<unsigned char>(buf[0]) << 8));
}
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
void print_row_count(std::string database_file_path, std::string command) {
    std::ifstream database_file(database_file_path, std::ios::binary);
    if (!database_file) {
        std::cerr << "Failed to open the database file" << std::endl;
        return;
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
        unsigned short root_page = static_cast<unsigned short>(root[0]);
        //std::cout << "root : " << root_page << std::endl;
        database_file.read(type_name, type_size);
        database_file.read(table_name, name_size);
        database_file.read(tbl, tbl_name_size);
        database_file.read(root, root_size);
        std::cout << table_name << std::endl;
        if (table_name == table) {
            unsigned short root_page = static_cast<unsigned short>(root[0]);

            // 4096 is the default page size in sqlite
            database_file.seekg((root_page - 1) * 4096); 
            char buffer[5];
            database_file.read(buffer,5);
            unsigned short number_of_rows = (static_cast<unsigned char>(buffer[4]) | (static_cast<unsigned char>(buffer[3]) << 8));
            std::cout << number_of_rows << std::endl;
            break;

        }
    }
}

void select_columns(std::string database_file_path, std::string command) {
    std::ifstream database_file(database_file_path, std::ios::binary);
    if (!database_file) {
        std::cerr << "Failed to open the database file" << std::endl;
        return;
    }
    std::istringstream iss(command);
    std::vector<std::string> tokens;
    std::string word;
    while (iss >> word) {
        tokens.push_back(word);
    }
    std::string table = tokens.back();
    std::string column = tokens[1];
    std::cout << "query column: " << column << std::endl;
    char buf[2];
    database_file.seekg(HEADER_SIZE + 3);
    database_file.read(buf,2);
    unsigned short cell_count = (static_cast<unsigned char>(buf[1]) | (static_cast<unsigned char>(buf[0]) << 8));
    //unsigned short cell_count = parse_varint(buf);
    //unsigned short cell_count = parse_varint(reinterpret_cast<const unsigned char*>(buf));

    std::cout << cell_count << std::endl;
    for (int i = 0; i < cell_count; i++) {
        database_file.seekg(HEADER_SIZE + PAGE_SIZE + (i * 2));
        database_file.read(buf,2);
        unsigned short curr_offset = (static_cast<unsigned char>(buf[1]) | (static_cast<unsigned char>(buf[0]) << 8));
        database_file.seekg(curr_offset);
        
        char record[3];
        database_file.read(record, 3);

        unsigned short cols = static_cast<unsigned short>(record[2]);
        char record_header[cols];

        // first entry contains the serial type for type, and entry 2
        // contains the serial type for name
        database_file.read(record_header, cols - 1); // -1 because the size of the header is alredy read previously

        //TODO: the sizes of serial types are varints that range from 1 - 9 bytes.
        // currently orange is not being printed beacause every field is 1 byte except for sql_size which is 2 bytes
        // it's causing strange behaviour, need to not hardcode 1 byte. need to be able to parse varint
        // create a function parse_varint that correctly calcuates the serial types in order to fix sql_size bugging out 
        /*
        unsigned short type_size = (static_cast<unsigned short>(record_header[0]) - 13) / 2;
        unsigned short name_size = (static_cast<unsigned short>(record_header[1]) - 13) / 2;
        unsigned short tbl_name_size = (static_cast<unsigned short>(record_header[2]) - 13) / 2;
        unsigned short root_size = static_cast<unsigned short>(record_header[3]);
        unsigned short sql_size = (static_cast<unsigned short>(record_header[4]) - 13) / 2;
        
        std::cout << "cols: " << cols - 1 << std::endl;
        std::cout << "num: " << static_cast<unsigned short>(record_header[4]) << std::endl;*/
        std::vector<unsigned int> serial_types;
        int offset = 0;
        for (int j = 0; j < cols - 1; j++) {
            int bytes_read = 0;
            unsigned int serial_type = parse_varint(reinterpret_cast<const unsigned char*>(&record_header[offset]), bytes_read);
            serial_types.push_back(serial_type);
            offset += bytes_read;
        }
        unsigned short type_size = (static_cast<unsigned short>(serial_types[0]) - 13) / 2;
        unsigned short name_size = (static_cast<unsigned short>(serial_types[1]) - 13) / 2;
        unsigned short tbl_name_size = (static_cast<unsigned short>(serial_types[2]) - 13) / 2;
        unsigned short root_size = static_cast<unsigned short>(serial_types[3]);
        unsigned short sql_size = (static_cast<unsigned short>(serial_types[4]) - 13) / 2;
        
        char type_name[type_size];
        char table_name[name_size];
        char tbl[tbl_name_size];
        char root[root_size];
        char sql_text[sql_size];
        //unsigned short root_page = static_cast<unsigned short>(root[0]);
        database_file.read(type_name, type_size);
        database_file.read(table_name, name_size);
        database_file.read(tbl, tbl_name_size);
        database_file.read(root, root_size);
        database_file.read(sql_text, sql_size);
        std::cout << sql_text << std::endl;
        std::cout << "table name: " << table_name << std::endl;
        /*
        if (table_name == table) {
            unsigned short root_page = static_cast<unsigned short>(root[0]);

            // 4096 is the default page size in sqlite
            database_file.seekg((root_page - 1) * 4096); 
            char buffer[5];
            database_file.read(buffer,5);
            unsigned short number_of_rows = (static_cast<unsigned char>(buffer[4]) | (static_cast<unsigned char>(buffer[3]) << 8));
            std::cout << number_of_rows << std::endl;
            break;

        }*/
    }

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
        if(is_count(command)) {
            print_row_count(database_file_path,command);
        }
        else {
            select_columns(database_file_path, command);
        }
    }

    return 0;
}

