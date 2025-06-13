#include "../include/Database.h"

std::optional<schemaRecord> Database::containsIndexRecord(const std::unordered_map<std::string, schemaRecord>& records, const schemaRecord& record) const {
    for (const auto& x : records) {
        if (x.second.is_index) {
            SQLParser s1(x.second.sql);
            std::string str = s1.extractNameFromIndexTable();
            if(str == record.table_name) {
                return x.second;
            }
        }
    }

    return std::nullopt;
}
std::unordered_map<std::string, schemaRecord> Database::getRecords(unsigned short cell_count) const {
    std::unordered_map<std::string, schemaRecord> records;
    for (int i = 0; i < cell_count; i++) {
        char buf[2];
        database_file.seekg(HEADER_SIZE + PAGE_SIZE + (i * 2));
        database_file.read(buf,2);
        unsigned short curr_offset = ((static_cast<unsigned char>(buf[0]) << 8) | static_cast<unsigned char>(buf[1]));
        database_file.seekg(curr_offset);

        unsigned char record[9];
        database_file.read(reinterpret_cast<char*>(record), 9);
        int header_bytes;
        int offset = 0;

        unsigned int header_size = parseVarint(record + offset, header_bytes);
        offset += header_bytes;

        unsigned int header2 = parseVarint(record + offset, header_bytes);
        offset += header_bytes;

        unsigned int record_header_size = parseVarint(record + offset, header_bytes);
        offset += header_bytes;

        database_file.seekg(-(9 - offset), std::ios::cur);
        char record_header[record_header_size];
        
        database_file.read(record_header, record_header_size - 1);
        unsigned short type_size, name_size, tbl_name_size, root_size, sql_size; 
        this->computeSchemaSize(record_header, record_header_size - header_bytes, type_size, name_size, tbl_name_size, root_size, sql_size);

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
        std::string sql_string(sql_text, sql_size);
        size_t start = sql_string.find('(') + 1;
        size_t end = sql_string.find(')');
        std::string columns_def = sql_string.substr(start, end - start);
        std::string type_name_string(type_name, type_size);
        std::string table_name_string(table_name, name_size);
        std::string tbl_string(tbl, tbl_name_size);
        std::string root_string(root, root_size);
        
        SQLParser check_index(sql_string);
        schemaRecord table_record = {type_name_string,table_name_string, tbl_string, root_string,sql_string};
        if (check_index.isCreateIndex()) {
            table_record.is_index = true;
        }
        records[table_name_string] = table_record;
    }
    return records;
}
bool Database::isMatchingIndex(const schemaRecord& record, const std::string& table_name_string) const {
    std::string record_table_string = record.table_name;
    return record_table_string == table_name_string;
}
std::vector<std::string> Database::extractColumnValues(const std::vector<uint64_t>& serial_types, uint64_t& rowid) const {
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
            if (stype == 0) {
                column_values.push_back(std::to_string(rowid));
            }
            else if (stype >= 1 && stype <= 6) {
                column_values.emplace_back(handleInt(data,size));
            }
            else if (stype == 7) {
                column_values.emplace_back(handleFloat(data, size));
            }
            else if (stype == 8)  {
                column_values.emplace_back(handleSerial8());
            }
            else if (stype == 9) {
                column_values.emplace_back(handleSerial9());
            }
            else {
                column_values.push_back(ss.str());
            }
        }
    }

    return column_values;
}

std::vector<uint64_t> Database::computeIndexSerialTypes(uint64_t page_offset, char* buf, int index, bool is_leaf) const {
    if(!is_leaf)
        database_file.seekg(page_offset + 12 + (index * 2));
    else
        database_file.seekg(page_offset + 8 + (index * 2));
    database_file.read(buf,2);
    unsigned short cell_offset = (static_cast<unsigned char>(buf[1]) | (static_cast<unsigned char>(buf[0]) << 8));
    database_file.seekg(page_offset + cell_offset);
    unsigned char varint_buf[9];
    database_file.read(reinterpret_cast<char*>(varint_buf),9);
    int offset;
    if (!is_leaf)
        offset = 4;
    else
        offset = 0;
    int bytes;
    
    uint64_t payload_size = this->parseVarint(varint_buf + offset, bytes);
    offset += bytes;
    database_file.seekg(page_offset + offset + cell_offset);


    database_file.read(reinterpret_cast<char*>(varint_buf),9);
    offset = 0;
    uint64_t header_size = this->parseVarint(varint_buf + offset, bytes);
    offset += bytes;

    std::vector<unsigned char> header(header_size - offset);
    database_file.seekg(-(9 - offset), std::ios::cur);
    database_file.read(reinterpret_cast<char*>(header.data()),header_size - offset);
    
    std::vector<uint64_t> serial_types;
    int h_offset = 0;

    while(h_offset < header.size()) {
        uint64_t serial_type = this->parseVarint(&header[h_offset], bytes);
        serial_types.push_back(serial_type);
        h_offset += bytes;
    }
    
    return serial_types;
}
uint64_t Database::computeRowId(uint32_t page_offset, char* buf, int index) const {
    database_file.seekg(page_offset + 8 + (index * 2));
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
    return rowid;
}
std::vector<uint64_t> Database::computeSerialTypes(uint32_t page_offset, char* buf, int index, uint64_t& rowid) const {
    database_file.seekg(page_offset + 8 + (index * 2));
    database_file.read(buf,2);
    unsigned short cell_offset = (static_cast<unsigned char>(buf[1]) | (static_cast<unsigned char>(buf[0]) << 8));
    database_file.seekg(page_offset + cell_offset);
    unsigned char varint_buf[9];
    database_file.read(reinterpret_cast<char*>(varint_buf), 9); 

    int offset = 0;
    int bytes;
    uint64_t payload_size = this->parseVarint(varint_buf + offset, bytes);
    offset += bytes;
    rowid = this->parseVarint(varint_buf + offset, bytes);
    offset += bytes;
     
    database_file.seekg(page_offset + offset + cell_offset);
    database_file.read(reinterpret_cast<char*>(varint_buf), 9);
    offset = 0;
    uint64_t header_size = this->parseVarint(varint_buf + offset, bytes);
    offset += bytes;
    

    std::vector<unsigned char> header(header_size - offset);
    database_file.seekg(-(9 - offset), std::ios::cur);
    database_file.read(reinterpret_cast<char*>(header.data()), header_size - offset);

    std::vector<uint64_t> serial_types;
    int h_offset = 0;

    while(h_offset < header.size()) {
        uint64_t serial_type = this->parseVarint(&header[h_offset], bytes);
        serial_types.push_back(serial_type);
        h_offset += bytes;
    }

    return serial_types;
}

void Database::computeSchemaSize(const char* record_header,
    unsigned short size, 
    unsigned short &type_size,
    unsigned short &name_size, 
    unsigned short &tbl_name_size, 
    unsigned short &root_size, 
    unsigned short &sql_size ) const {
    std::vector<unsigned int> serial_types;
    int offset = 0;
    while(offset < size) {
        int bytes_read = 0;
        unsigned int serial_type = this->parseVarint(reinterpret_cast<const unsigned char*>(&record_header[offset]), bytes_read);
        serial_types.push_back(serial_type);
        offset += bytes_read;
        int display = (serial_type >= 13) ? ((static_cast<unsigned short>(serial_type) - 13) / 2) : -1;
    }
    type_size = (static_cast<unsigned short>(serial_types[0]) - 13) / 2;
    name_size = (static_cast<unsigned short>(serial_types[1]) - 13) / 2;
    tbl_name_size = (static_cast<unsigned short>(serial_types[2]) - 13) / 2;
    root_size = static_cast<unsigned short>(serial_types[3]);
    sql_size = (static_cast<unsigned short>(serial_types[4]) - 13) / 2;
}

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
            return result;
        }
    }

    result = (result << 8) | data[8];
    bytes_read = 9;
    return result;

}
bool Database::evaluateCondition(const WhereCondition& cond, const std::unordered_map<std::string, std::string>& row) const {
    auto it = row.find(cond.column);
    if (it == row.end()) return false; // col does not exist

    const std::string& val = it->second;
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
    
    for (size_t i = 0; i < where.logic.size(); i++) {
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