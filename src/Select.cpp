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
std::vector<unsigned long> Database::selectColumnIndex(const schemaRecord& index_record, SQLParser& string_parser) {
    auto page_size = this->getPageSize();
    int root_page = static_cast<unsigned char>(index_record.root[0]);
    char buf[2];
    std::vector<unsigned long> out_id;
    std::unordered_map<std::string, std::vector<std::string>> tokens = string_parser.selectQuery();
    WhereClause clause = string_parser.parseWhereClause();
    //std::cout << "clauses: " << clause.conditions.size() << std::endl;
    std::vector<std::string> key_values;
    std::vector<std::string> column_names;
    size_t start = index_record.sql.find("(") + 1;
    size_t end = index_record.sql.find(")");
    std::string columns_def = index_record.sql.substr(start, end - start);
    std::istringstream ss(columns_def);
    std::string col;
    SQLParser index_parser(index_record.sql);
    //std::cout << "sql: " << index_record.sql << std::endl;
    std::vector<std::string> indices = index_parser.extractColumnIndice();
    //std::cout << "conditions\n";
    for (const auto& x : clause.conditions) {
        if (std::find(indices.begin(), indices.end(), x.column) != indices.end()) {
            key_values.push_back(x.value);
        }
    }
    while(std::getline(ss, col, ',')) {
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
    int page_offset = (root_page - 1) * page_size;
    database_file.seekg(page_offset);
    database_file.read(buf,2);
    uint32_t flag = static_cast<unsigned char>(buf[0]);

    std::vector<std::string> targets = index_parser.extractColumnIndice();
    b_tree_nav.traverseBTreePageIndexB(database_file, root_page, page_size, index_parser, clause, *this, out_id, targets);

    return out_id;
}
void Database::selectColumnWithWhere(const std::string& query) {
    auto page_size = this->getPageSize();
    SQLParser string_parser(query);
    std::unordered_map<std::string, std::vector<std::string>> tokens = string_parser.selectQuery();
   
    char buf[2];
    database_file.seekg(HEADER_SIZE);
    database_file.read(buf,1);
    uint32_t flag = static_cast<unsigned char>(buf[0]);
    database_file.seekg(HEADER_SIZE + 3);
    database_file.read(buf,2);
    // maps the table name to record content
    unsigned short cell_count = (static_cast<unsigned char>(buf[1]) | (static_cast<unsigned char>(buf[0]) << 8));
    std::vector<std::string> column_names;
    std::unordered_map<std::string, schemaRecord> records = this->getRecords(cell_count);
    for(const auto& x : records) {
        SQLParser s1(x.second.sql);
        if (std::find(tokens["tables"].begin(), tokens["tables"].end(), x.second.table_name) != tokens["tables"].end()) {
            auto index_record = this->containsIndexRecord(records, x.second);

            std::vector<unsigned long> out_id;
            if (index_record.has_value()) {
                out_id = this->selectColumnIndex(index_record.value(), string_parser);
                std::sort(out_id.begin(), out_id.end());
                std::cout << "rows: " << out_id.size() << std::endl;
            }
            size_t start = x.second.sql.find('(') + 1;
            size_t end = x.second.sql.find(')');
            std::string columns_def = x.second.sql.substr(start, end - start);

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
                //std::cout << "col: " << x << std::endl;
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
            int root_page = static_cast<unsigned char>(x.second.root[0]);

            int page_offset = (root_page - 1) * page_size;
            char buf[2];
            database_file.seekg(page_offset);
            database_file.read(buf,1);
            uint32_t flag = static_cast<uint32_t>(buf[0]);
            b_tree_nav.traverseBTreePageTableB(database_file,root_page,page_size,string_parser,
            col_indices,desired_col_indices ,index_to_name, *this, out_id);
            break;
        }
    }
    
}
void Database::selectColumn(const std::string& query) {
    auto page_size = this->getPageSize();
    SQLParser string_parser(query);
    std::unordered_map<std::string, std::vector<std::string>> tokens = string_parser.selectQuery();
   
    char buf[2];
    database_file.seekg(HEADER_SIZE);
    database_file.read(buf,1);
    uint32_t flag = static_cast<unsigned char>(buf[0]);
    database_file.seekg(HEADER_SIZE + 3);
    database_file.read(buf,2);
    // maps the table name to record content
    unsigned short cell_count = (static_cast<unsigned char>(buf[1]) | (static_cast<unsigned char>(buf[0]) << 8));
    std::vector<std::string> column_names;
    std::unordered_map<std::string, schemaRecord> records = this->getRecords(cell_count);
    for(const auto& x : records) {
        SQLParser s1(x.second.sql);
        if (std::find(tokens["tables"].begin(), tokens["tables"].end(), x.second.table_name) != tokens["tables"].end()) {
            auto index_record = this->containsIndexRecord(records, x.second);

            size_t start = x.second.sql.find('(') + 1;
            size_t end = x.second.sql.find(')');
            std::string columns_def = x.second.sql.substr(start, end - start);

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
            int root_page = static_cast<unsigned char>(x.second.root[0]);

            int page_offset = (root_page - 1) * page_size;
            char buf[2];
            database_file.seekg(page_offset);
            database_file.read(buf,1);
            uint32_t flag = static_cast<uint32_t>(buf[0]);
            std::vector<unsigned long> out_id;
            b_tree_nav.traverseBTreePageTableB(database_file,root_page,page_size,string_parser,
            col_indices,desired_col_indices, index_to_name, *this, out_id);
            break;
        }
    }
}