#include "../include/Database.h"

const std::streampos Database::getFileSize() const {
    std::streampos original_pos = database_file.tellg();
    
    database_file.seekg(0, std::ios::cur);
    std::streampos file_size = database_file.tellg();
    database_file.seekg(original_pos);

    return file_size;
}

bool Database::isCount(const std::string& query) const {
    std::string upper_query = query;
    std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);
    return upper_query.find("COUNT(") != std::string::npos;
}

unsigned int Database::parseVarint(const unsigned char* data, int& bytes_read) const {
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
bool Database::matchesWhereCondition(std::vector<std::string>& values, const std::string& operation, const std::string& condition) const {

}

const unsigned short Database::getPageSize() const {
    std::streampos original_position = database_file.tellg();
    database_file.seekg(16);
    char buf[2];
    database_file.read(buf,2);
    unsigned short page_size = (static_cast<unsigned char>(buf[1]) | (static_cast<unsigned char>(buf[0]) << 8));
    database_file.seekg(original_position);
    return page_size;

}

void Database::printTables() {
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

void Database::printDBInfo() {
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

void Database::printRowCount(const std::string& query) {
    std::istringstream iss(query);
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

void Database::selectColumn(const std::string& query) {
    auto page_size = this->getPageSize();
    SQLParser string_parser(query);
    std::unordered_map<std::string, std::vector<std::string>> tokens = string_parser.selectQuery();
   
    char buf[2];
    database_file.seekg(HEADER_SIZE + 3);
    database_file.read(buf,2);
    unsigned short cell_count = (static_cast<unsigned char>(buf[1]) | (static_cast<unsigned char>(buf[0]) << 8));

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
        std::vector<unsigned int> serial_types;
        int offset = 0;
        for (int j = 0; j < cols - 1; j++) {
            int bytes_read = 0;
            unsigned int serial_type = this->parseVarint(reinterpret_cast<const unsigned char*>(&record_header[offset]), bytes_read);
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
        std::vector<std::string> column_names;
        std::string sql(sql_text, sql_size);
        size_t start = sql.find('(') + 1;
        size_t end = sql.find(')');
        std::string columns_def = sql.substr(start, end - start);
        std::string table_name_string(table_name);
        if (std::find(tokens["tables"].begin(), tokens["tables"].end(), table_name_string) != tokens["tables"].end()) {
            std::istringstream col_stream(columns_def);
            std::string col;
            while(std::getline(col_stream, col, ',')) {
               std::istringstream col_word_stream(col);
               std::string col_name;
               col_word_stream >> col_name;
               column_names.push_back(col_name);
            }
            std::vector<std::vector<std::string>::iterator> matched_iterators;
            std::vector<int> col_indices;
            for (const auto& x : tokens["cols"]) {
                auto it = std::find(column_names.begin(), column_names.end(), x);   
                if (it != column_names.end()) {
                    matched_iterators.push_back(it);
                }
            }
            for (const auto& x : matched_iterators) {
                int col_index = std::distance(column_names.begin(), x);
                col_indices.push_back(col_index);
            }
            //int col_index = std::distance(column_names.begin(), it);
            int root_page = static_cast<unsigned char>(root[0]);
            int page_offset = (root_page - 1) * page_size;
            char buf[2];
            database_file.seekg(page_offset + 3);
            database_file.read(buf,2);
            unsigned short number_of_rows = (static_cast<unsigned char>(buf[0]) << 8) | static_cast<unsigned char>(buf[1]);
            for (int k = 0; k < number_of_rows; k++) {
                database_file.seekg(page_offset + PAGE_SIZE + (k * 2));
                database_file.read(buf,2);
                unsigned short cell_offset = (static_cast<unsigned char>(buf[1]) | (static_cast<unsigned char>(buf[0]) << 8));
                database_file.seekg(page_offset + cell_offset);
                unsigned char varint_buf[9];
                database_file.read(reinterpret_cast<char*>(varint_buf), 9); 

                int offset = 0;
                int bytes;
                uint64_t payload_size = this->parseVarint(varint_buf + offset, bytes);
                offset += bytes;
                uint64_t rowid = this->parseVarint(varint_buf + offset, bytes);
                offset += bytes;
     
                database_file.seekg(page_offset + offset + cell_offset);
                database_file.read(reinterpret_cast<char*>(varint_buf), 9);
                offset = 0;
                uint64_t header_size = this->parseVarint(varint_buf + offset, bytes);
                offset += bytes;
                std::vector<unsigned char> header(header_size - offset);
                database_file.seekg(-(9 - offset), std::ios::cur);
                // -1 because we're off by 1 when reading
                database_file.read(reinterpret_cast<char*>(header.data()), header_size - 1);

                std::vector<uint64_t> serial_types;
                int h_offset = 0;

                while(h_offset < header.size()) {
                    uint64_t serial_type = this->parseVarint(&header[h_offset], bytes);
                    serial_types.push_back(serial_type);
                    h_offset += bytes;
                }

                std::vector<std::string> column_values;
                for(size_t m = 0; m < serial_types.size(); ++m) {
                    uint64_t stype = serial_types[m];
                    int size = 0;
                    switch(stype) {
                        case 0:
                            size = 0;
                            break;
                        case 1:
                            size = 1;
                            break;
                        case 2:
                            size = 2;
                            break;
                        case 3:
                            size = 3;
                            break;
                        case 4:
                            size = 4;
                            break;
                        case 5:
                            size = 6;
                            break;
                        case 6:
                            size = 8;
                            break;
                        default:
                            if(stype >= 13 && stype % 2 == 1) size = (stype - 13) / 2;
                            else {
                                std::cerr << "Unsupported serial type: " << stype << std::endl;
                                return;
                            }
                            break;
                    }
                    std::vector<char> data(size);
                    database_file.read(data.data(), size);

                    if (stype >= 13) {
                        column_values.emplace_back(data.data(), size); // TEXT
                    } else {
                        std::stringstream ss;
                        for (int k = 0; k < size; ++k) {
                            ss << std::hex << std::setfill('0') << std::setw(2) << (0xff & static_cast<unsigned char>(data[k]));
                        }
                        column_values.push_back(ss.str()); // store raw bytes as hex string for debugging
                    }
                }
                bool is_first_iteration = true;
                for (int col_index : col_indices) {
                    if (col_index < column_values.size()) {
                        if (is_first_iteration) {
                            std::cout << column_values[col_index];
                            is_first_iteration = false;
                        }
                        else
                            std::cout << "|" << column_values[col_index];
                    } else {
                        std::cout << "Row " << k << ": [column index out of bounds]" << std::endl;
                    }
                }
                std::cout << std::endl;
            }
            break;
        }
        //std::cout << "table name: " << table_name << std::endl;
    }


}