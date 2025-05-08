#include "../include/Database.h"

TableB Database::getPageType(uint32_t page_number) const {
    int page_size = this->getPageSize();
    database_file.seekg(page_number * page_size);

    unsigned char page_type_byte;
    database_file.read(reinterpret_cast<char*>(page_type_byte), 1);

    switch (page_type_byte) {
        case 0x0d:
            return TableB::leafCell;
        case 0x02:
            return TableB::interiorCell;
        default:
            return TableB::unknown;
    };
}
void Database::traverseBTreePage(uint32_t page_number) {
    TableB page_type = this->getPageType(page_number);
    switch(page_type) {
        case TableB::leafCell:
            // do something
            break;
        case TableB::interiorCell:
            // do something
            break;
        default:
            return;
    };

}

std::vector<std::string> Database::extractColumnValues(const std::vector<uint64_t>& serial_types) const {
    std::vector<std::string> column_values;
    for(size_t m = 0; m < serial_types.size(); ++m) {
        uint64_t stype = serial_types[m];
        //std::vector<char> data;
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
            if (stype >= 1 && stype <= 6) {
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

unsigned short Database::extractNumberOfRows(const std::string& columns_def, const std::unordered_map<std::string, std::vector<std::string>> tokens, const char* root, unsigned short page_size) const {

}
std::vector<uint64_t> Database::computeSerialTypes(unsigned short page_offset, char* buf, int index) const {
    database_file.seekg(page_offset + PAGE_SIZE + (index * 2));
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
    //std::cout << "header size : " << size << std::endl;
    /*
    for (int j = 0; j < size; j++) {
        int bytes_read = 0;
        unsigned int serial_type = this->parseVarint(reinterpret_cast<const unsigned char*>(&record_header[offset]), bytes_read);
        serial_types.push_back(serial_type);
        offset += bytes_read;
        std::cout << "t: " << bytes_read << " " << (static_cast<unsigned short>(serial_type) - 13) / 2 << std::endl;
    }*/
    while(offset < size) {
        int bytes_read = 0;
        unsigned int serial_type = this->parseVarint(reinterpret_cast<const unsigned char*>(&record_header[offset]), bytes_read);
        serial_types.push_back(serial_type);
        offset += bytes_read;
        int display = (serial_type >= 13) ? ((static_cast<unsigned short>(serial_type) - 13) / 2) : -1;
        std::cout << "raw serial_type: " << serial_type 
              << ", bytes_read: " << bytes_read 
              << ", size guess: " << ((serial_type >= 13) ? ((serial_type - 13) / 2) : -1) 
              << std::endl;
        std::cout << "t: " << bytes_read << " " << display << std::endl;
    }
    std::cout << "vec size: " << serial_types.size() << std::endl;
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