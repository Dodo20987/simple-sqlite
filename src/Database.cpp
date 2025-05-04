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

bool Database::hasWhereClause(const std::string& query) const {
    std::regex where_pattern(R"(\bwhere\b)", std::regex_constants::icase);
    bool res = std::regex_search(query, where_pattern);
    return res;
}
int64_t Database::parseVarint(const unsigned char* data, int& bytes_read) const {
    int64_t result = 0;
    bytes_read = 0;

    for (int i = 0; i < 8; i++) {
        result = (result << 7) | (data[i] & 0x7F);
        bytes_read++;
        if ((data[i] & 0x80) == 0) {
            //std::cout << "i: " << i << std::endl;
            return result;
        }
    }

    // Special case: 9th byte is stored as full 8 bits
    result = (result << 8) | data[8];
    bytes_read = 9;
    std::cout << "res: " << std::endl;
    return result;

}
bool Database::evaluateCondition(const WhereCondition& cond, const std::unordered_map<std::string, std::string>& row) const {
    auto it = row.find(cond.column);
    if (it == row.end()) return false; // col does not exist

    const std::string& val = it->second;
    //std::cout << "val: " << val << std::endl;
    if (cond.operation == "=") return val == cond.value;
    if (cond.operation == "!=") return val != cond.value;
    if (cond.operation == "<") return std::stod(val) < std::stod(cond.value);
    if (cond.operation == "<=") return std::stod(val) <= std::stod(cond.value);
    if (cond.operation == ">") return std::stod(val) > std::stod(cond.value);
    if (cond.operation == ">=") return std::stod(val) >= std::stod(cond.value);

    return false;
}

// row contains something like {(name, val), (age, val) ...}
bool Database::evaluateWhere(const WhereClause& where, const std::unordered_map<std::string, std::string>& row) const {
    if (where.conditions.empty()) return true;
    
    bool result = evaluateCondition(where.conditions[0], row);
    //std::cout << "logic size" << where.logic.size() << std::endl;
    //std::cout << "cond: " << "{" << where.conditions[0].column << where.conditions[0].operation << where.conditions[0].value << "}" << std::endl;
    //std::cout << "res" << result << std::endl;
    for (size_t i = 0; i < where.logic.size(); i++) {
        //std::cout << "cond: " << "{" << where.conditions[i].column << where.conditions[i].operation << where.conditions[i].value << "}" << std::endl;
        bool next = evaluateCondition(where.conditions[i + 1], row);
        if (where.logic[i] == LogicalOp::AND) result = result && next;
        else result = result || next;
    }
    return result;
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