#include "../include/Database.h"
uint64_t decodeVarint(const char* data, size_t& offset) {
    uint64_t result = 0;
    int shift = 0;

    while(true) {
        unsigned char byte = data[offset++];
        result |= (static_cast<uint64_t>(byte & 0x7F) << shift);

        if ((byte & 0x80) == 0) {
            break;
        }
        shift += 7;
    }

    return result;
}
void Database::selectColumnWithWhere(const std::string& query) {
    auto page_size = this->getPageSize();
    SQLParser string_parser(query);
    std::unordered_map<std::string, std::vector<std::string>> tokens = string_parser.selectQuery();
   
    char buf[2];
    database_file.seekg(HEADER_SIZE);
    database_file.read(buf,1);
    uint32_t flag = static_cast<unsigned char>(buf[0]);
    //std::cout << "flag: " << flag << std::endl;
    database_file.seekg(HEADER_SIZE + 3);
    database_file.read(buf,2);
    unsigned short cell_count = (static_cast<unsigned char>(buf[1]) | (static_cast<unsigned char>(buf[0]) << 8));

    for (int i = 0; i < cell_count; i++) {
        database_file.seekg(HEADER_SIZE + PAGE_SIZE + (i * 2));
        database_file.read(buf,2);
        unsigned short curr_offset = (static_cast<unsigned char>(buf[1]) | (static_cast<unsigned char>(buf[0]) << 8));
        database_file.seekg(curr_offset);
        
        /*char header_buf[9];
        database_file.read(header_buf, 9);

        int header_bytes_read = 0;
        unsigned int header_size = this->parseVarint(reinterpret_cast<unsigned char*>(header_buf), header_bytes_read);
        std::cout << "sz: " << header_size << std::endl;
        exit(1);*/

        unsigned char record[9];
        //std::cout << "pos: " << database_file.tellg() << std::endl;
        database_file.read(reinterpret_cast<char*>(record),9);
        int header_bytes;
        int offset = 0;
        //unsigned int header_size = this->parseVarint(reinterpret_cast<unsigned char*>(record[1]), header_bytes);
        unsigned int header_size = this->parseVarint(record + offset, header_bytes);
        offset += header_bytes;
        //std::cout << "pos: " << database_file.tellg() << std::endl;
        unsigned int header2 = this->parseVarint(record + offset, header_bytes);
        offset += header_bytes;
        unsigned int record_header_size = this->parseVarint(record + offset, header_bytes);
        offset += header_bytes;


        database_file.seekg(-(9 - offset), std::ios::cur);
        char record_header[record_header_size];

        database_file.read(record_header, record_header_size - 1); // -1 because the size of the header is alredy read previously
        unsigned short type_size, name_size, tbl_name_size, root_size, sql_size; 
        this->computeSchemaSize(record_header, record_header_size - header_bytes, type_size, name_size, tbl_name_size, root_size, sql_size);
        //std::cout << cols << std::endl;
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
        std::string table_name_string(table_name, name_size);
        SQLParser check_index(sql);
        int root_page = static_cast<unsigned char>(root[0]);
        for(auto x: tokens["tables"]) {
            std::cout << "table name: " << x << std::endl;
        }

        if(check_index.isCreateIndex()) {
            std::cout << "is a create index: " << std::endl;
            std::cout << sql << std::endl;
        }
        else {
            std::cout << "not create index table" << std::endl;
        }
        if (std::find(tokens["tables"].begin(), tokens["tables"].end(), table_name_string) != tokens["tables"].end()) {
            exit(1);
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

            size_t offset = 0;
            //int root_page = decodeVarint(root, offset);
            int root_page = static_cast<unsigned char>(root[0]);

            int page_offset = (root_page - 1) * page_size;
            char buf[2];
            database_file.seekg(page_offset);
            database_file.read(buf,1);
            //TODO: must read the page type and check if it's an interior cell and traverse the B-Tree
            uint32_t flag = static_cast<uint32_t>(buf[0]);
            //std::cout << "page_type: " << flag << std::endl;
            b_tree_nav.traverseBTreePageTableB(database_file,root_page,page_size,string_parser,
            col_indices, index_to_name, *this);
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
        unsigned short type_size, name_size, tbl_name_size, root_size, sql_size; 
        this->computeSchemaSize(record_header, cols - 1, type_size, name_size, tbl_name_size, root_size, sql_size);
        
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
            int root_page = static_cast<unsigned char>(root[0]);
            int page_offset = (root_page - 1) * page_size;
            char buf[2];
            database_file.seekg(page_offset + 3);
            database_file.read(buf,2);
            unsigned short number_of_rows = (static_cast<unsigned char>(buf[0]) << 8) | static_cast<unsigned char>(buf[1]);
            for (int k = 0; k < number_of_rows; k++) {
                uint64_t rowid = 0;
                std::vector<uint64_t> serial_types = this->computeSerialTypes(page_offset,buf,k, rowid);

                std::vector<std::string> column_values = this->extractColumnValues(serial_types, rowid);
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