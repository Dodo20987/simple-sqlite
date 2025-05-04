#include "../include/Database.h"

std::vector<std::string> Database::extractColumnValues(const std::vector<uint64_t>& serial_types) const {
    std::vector<std::string> column_values;
    for(size_t m = 0; m < serial_types.size(); ++m) {
        uint64_t stype = serial_types[m];
        //std::vector<char> data;
        int size = 0;
        //TODO: numbers are currently displayed as hexadecimal must convert to decimal
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
            case 8:
                size = 0;
                break;
            case 9:
                size = 0;
                break;
            default:
                if(stype >= 13 && stype % 2 == 1) size = (stype - 13) / 2;
                else {
                    std::cerr << "Unsupported serial type: " << stype << std::endl;
                    return {};
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
            if (stype >= 1 && stype <= 6) {
                column_values.emplace_back(handleInt(data,size));
            }
            else if (stype == 7) {
                column_values.emplace_back(handleFloat(data, size));
            }
            else if (stype == 8)  {
                column_values.emplace_back(handleSerial8(data));
            }
            else if (stype == 9) {
                column_values.emplace_back(handleSerial9(data));
            }
            else {
                column_values.push_back(ss.str());
            }
        }
    }

    return column_values;
}

const unsigned short Database::extractNumberOfRows() const {

}
void Database::computeSerialSize() const {

}
void Database::selectColumnWithWhere(const std::string& query) {
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
            std::unordered_map<int, std::string> index_to_name;
            std::vector<std::vector<std::string>::iterator> matched_iterators;
            std::vector<int> desired_col_indices;
            std::vector<int> col_indices;
            for (const auto& x : tokens["cols"]) {
                auto it = std::find(column_names.begin(), column_names.end(), x);   
                if (it != column_names.end()) {
                    matched_iterators.push_back(it);
                }
            }
            for (const auto& x : matched_iterators) {
                int col_index = std::distance(column_names.begin(), x);
                desired_col_indices.push_back(col_index);
            }
            for (size_t i = 0; i < column_names.size(); ++i) {
                col_indices.push_back(i);
                index_to_name[i] = column_names[i];
            }

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
                database_file.read(reinterpret_cast<char*>(header.data()), header_size - offset);

                std::vector<uint64_t> serial_types;
                int h_offset = 0;

                while(h_offset < header.size()) {
                    uint64_t serial_type = this->parseVarint(&header[h_offset], bytes);
                    serial_types.push_back(serial_type);
                    h_offset += bytes;
                }
                std::vector<std::string> column_values = this->extractColumnValues(serial_types);
                bool is_first_iteration = true;
                std::unordered_map<std::string, std::string> row;
                WhereClause where = string_parser.parseWhereClause();
                for (int col_index : col_indices) {
                    if (col_index < column_values.size()) {
                        row[index_to_name[col_index]] = column_values[col_index];
                    }
                }

                if(evaluateWhere(where, row)) {
                    for(const auto& val : row) {
                        if(is_first_iteration) {
                            std::cout << val.second;
                            is_first_iteration = false;
                        }
                        else std::cout << "|" << val.second;
                    }
                    std::cout << std::endl;
                }

            }
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
            //TODO: must extract all col names from the actual create table statement
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

                std::vector<std::string> column_values = this->extractColumnValues(serial_types);
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
    }
}