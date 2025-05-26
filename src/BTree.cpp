#include "../include/BTreeNavigator.h"
#include "../include/Database.h"

TableB BTreeNavigator::getPageTypeTableB(std::ifstream& database_file, uint32_t page_number, int page_size) const {
    database_file.seekg((page_number - 1) * page_size + (page_number == 1 ? 100 : 0));
    char page_type_byte[2];
    database_file.read(page_type_byte, 1);
    uint32_t flag = static_cast<uint32_t>(page_type_byte[0]);
    //std::cout << "flag: " << flag << std::endl;

    switch (flag) {
        case 0x0d:
            return TableB::leafCell;
        case 0x05:
            return TableB::interiorCell;
        default:
            return TableB::unknown;
    }
}
IndexB BTreeNavigator::getPageTypeIndexB(std::ifstream& database_file, uint32_t page_number, int page_size) const {
    char data[2];
    database_file.seekg((page_number - 1) * page_size);
    database_file.read(data, 1);
    uint32_t flag = static_cast<unsigned char>(data[0]);
    //std::cout << "index flag: " << flag << std::endl;
    switch(flag) {
        case 0x0a:
            return IndexB::leafCell;
        case 0x02:
            return IndexB::interiorCell;
        default:
            return IndexB::unknown;
    }

}
void BTreeNavigator::readLeafTablePage(std::ifstream& database_file, uint32_t page_offset, SQLParser& string_parser, std::vector<int>& col_indices,
std::unordered_map<int, std::string>& index_to_name, Database& db) {
    // number of cells
    char buf[2];
    database_file.seekg(page_offset + 3);
    database_file.read(buf,2);
    unsigned short number_of_rows = (static_cast<unsigned char>(buf[0]) << 8) | static_cast<unsigned char>(buf[1]);
    //TODO: the columns with null data is currently getting filled with the rowid
    // we do not want this behaviour, it should remain null
    for (int k = 0; k < number_of_rows; k++) {
        uint64_t rowid = 0;
        std::vector<uint64_t> serial_types = db.computeSerialTypes(page_offset,buf,k, rowid);
        std::vector<std::string> column_values = db.extractColumnValues(serial_types, rowid);
        bool is_first_iteration = true;
        std::unordered_map<std::string, std::string> row;
        WhereClause where = string_parser.parseWhereClause();
        for (int col_index : col_indices) {
            if (col_index < column_values.size()) {
                row[index_to_name[col_index]] = column_values[col_index];
            }
        }
        if(db.evaluateWhere(where, row)) {
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
}


void BTreeNavigator::traverseBTreePageTableB(std::ifstream& database_file, uint32_t page_number, int page_size, SQLParser& string_parser,
std::vector<int>& col_indices, std::unordered_map<int, std::string>& index_to_name, Database& db) {
    TableB page_type = this->getPageTypeTableB(database_file, page_number, page_size);
   //exit(1);
    switch(page_type) {
        case TableB::leafCell: {
            // extracting the records
            readLeafTablePage(database_file,(page_number - 1) * page_size, string_parser, col_indices, index_to_name, db);
            break;
        }
        case TableB::interiorCell: {
            // recurse on the child cells
            uint32_t page_offset = (page_number - 1) * page_size + (page_number == 1 ? 100 : 0);
            //std::cout << "case off" << page_offset << std::endl;
            char cell_bytes[2];
            database_file.seekg(page_offset + 3); // number of cells
            database_file.read(cell_bytes, 2);
            uint16_t cell_count = (static_cast<unsigned char>(cell_bytes[0]) << 8) | static_cast<unsigned char>(cell_bytes[1]);
            // reading the right most pointer
            char right_ptr_bytes[4];
            database_file.seekg(page_offset + 8);
            database_file.read(right_ptr_bytes, 4);
            // right child is guranteed to be 32 bits
            uint32_t right_child = (static_cast<unsigned char>(right_ptr_bytes[0]) << 24) | 
            (static_cast<unsigned char>(right_ptr_bytes[1]) << 16) | 
            (static_cast<unsigned char>(right_ptr_bytes[2]) << 8) | 
            static_cast<unsigned char>(right_ptr_bytes[3]);
            // cell content starts at offset 12
            std::vector<uint16_t> cell_offsets;
            for (int i = 0; i < cell_count; i++) {
                char cell_bytes[2];
                database_file.read(cell_bytes,2);
                uint16_t cell_offset = (static_cast<unsigned char>(cell_bytes[1]) | (static_cast<unsigned short>(cell_bytes[0]) << 8));
                cell_offsets.push_back(cell_offset);
            }
            std::vector<uint32_t> left_pointers;
            for (auto offset : cell_offsets) {
                database_file.seekg(page_offset + offset);
                char buffer[4];
                database_file.read(buffer, 4);
                uint32_t left_pointer = (static_cast<unsigned char>(buffer[3]) | (static_cast<unsigned char>(buffer[2]) << 8) |
                (static_cast<unsigned char>(buffer[1]) << 16) | (static_cast<unsigned char>(buffer[0]) << 24));
                left_pointers.push_back(left_pointer);
            }
            for (auto& page_num : left_pointers) {
                traverseBTreePageTableB(database_file, page_num, page_size, string_parser,col_indices, index_to_name, db);
            }
            traverseBTreePageTableB(database_file, right_child, page_size, string_parser, col_indices, index_to_name,db);
            
            break;
        }
        default:
            std::cout << "Unknown page type" << std::endl;
            exit(1);
            return;
    };

}
void BTreeNavigator::parseIndexPayload(std::ifstream&  database_file, uint32_t page_offset, 
    uint16_t cell_count, SQLParser& string_parser, Database& db, bool is_leaf) const {
    for(int i = 0; i < cell_count; i++) {
        char buf[2];
        uint64_t temp = 1;
        std::vector<uint64_t> serial_types = db.computeIndexSerialTypes(page_offset, buf, i, is_leaf);
        std::vector<std::string> column_values = db.extractColumnValues(serial_types,temp);
        bool is_first_iteration = true;
        std::unordered_map<std::string, std::string> row;
        WhereClause where = string_parser.parseWhereClause();
        for (const auto& val : column_values) {
            if(is_first_iteration) {
                std::cout << val;
                is_first_iteration = false;
            }
            else std::cout << "|" << val;
        }
        std::cout << std::endl;
    }
}
// NOTE: index b tree's all contain a payload, not just the leaf cells
void BTreeNavigator::traverseBTreePageIndexB(std::ifstream& database_file, uint32_t page_number, int page_size, SQLParser& string_parser, Database& db) {
    IndexB page_type = this->getPageTypeIndexB(database_file, page_number, page_size);
    uint32_t page_offset = (page_number - 1) * page_size + (page_number == 1 ? 100 : 0);
    char cell_bytes[2];
    database_file.seekg(page_offset + 3);
    database_file.read(cell_bytes,2);
    uint16_t cell_count = ((static_cast<unsigned char>(cell_bytes[0]) << 8) | static_cast<unsigned char>(cell_bytes[1]));
    switch(page_type) {
        case IndexB::leafCell: {
            this->parseIndexPayload(database_file, page_offset, cell_count, string_parser, db, true);           
            break;
        }
        case IndexB::interiorCell: {
            char right_ptr_bytes[4];
            database_file.seekg(page_offset + 8);
            database_file.read(right_ptr_bytes, 4);
            uint32_t right_child = (static_cast<unsigned char>(right_ptr_bytes[0]) << 24) | 
            (static_cast<unsigned char>(right_ptr_bytes[1]) << 16) | 
            (static_cast<unsigned char>(right_ptr_bytes[2]) << 8) | 
            static_cast<unsigned char>(right_ptr_bytes[3]);
            std::vector<uint16_t> cell_offsets;
            for(int i = 0; i < cell_count; i++) {
                char cell_bytes[2];
                database_file.read(cell_bytes,2);
                uint16_t cell_offset = ((static_cast<unsigned char>(cell_bytes[0]) << 8) | 
                static_cast<unsigned char>(cell_bytes[1]));
                cell_offsets.push_back(cell_offset);
            }
            std::vector<uint32_t> left_pointers;
            for (auto offset : cell_offsets) {
                database_file.seekg(page_offset + offset);
                char buffer[4];
                database_file.read(buffer, 4);
                uint32_t left_pointer = (static_cast<unsigned char>(buffer[3]) | (static_cast<unsigned char>(buffer[2]) << 8) |
                (static_cast<unsigned char>(buffer[1]) << 16) | (static_cast<unsigned char>(buffer[0]) << 24));
                left_pointers.push_back(left_pointer);
            }
            this->parseIndexPayload(database_file, page_offset, cell_count, string_parser, db, false);
            // traverse the left child nodes
            for (auto& page_num : left_pointers) {
                this->traverseBTreePageIndexB(database_file, page_num,page_size, string_parser,db);
            }

            // traversing the right child nodes
            this->traverseBTreePageIndexB(database_file, right_child, page_size, string_parser,db);

            break;
        }
        default:
            return;
    };
}

