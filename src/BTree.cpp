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



//TODO: need to traverse the b tree by looking at the out_id row id's and by looking at the first and last row id's of the page
// to determine if the page should be skipped or not 
void BTreeNavigator::traverseBTreePageTableB(std::ifstream& database_file, uint32_t page_number, int page_size, SQLParser& string_parser,
std::vector<int>& col_indices, std::unordered_map<int, std::string>& index_to_name, Database& db, const std::vector<long>& out_id) {
    TableB page_type = this->getPageTypeTableB(database_file, page_number, page_size);
    switch(page_type) {
        case TableB::leafCell: {
            // extracting the records
            readLeafTablePage(database_file,(page_number - 1) * page_size, string_parser, col_indices, index_to_name, db);
            break;
        }
        case TableB::interiorCell: {
            // recurse on the child cells
            uint32_t page_offset = (page_number - 1) * page_size + (page_number == 1 ? 100 : 0);

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
            std::vector<long> row_ids;
            for (auto offset : cell_offsets) {
                unsigned char varint_buf[9];
                uint64_t row_id = 0;
                int bytes_read = 0;
                database_file.seekg(page_offset + offset);
                char buffer[4];
                database_file.read(buffer, 4);
                uint32_t left_pointer = (static_cast<unsigned char>(buffer[3]) | (static_cast<unsigned char>(buffer[2]) << 8) |
                (static_cast<unsigned char>(buffer[1]) << 16) | (static_cast<unsigned char>(buffer[0]) << 24));
                database_file.read(reinterpret_cast<char*>(varint_buf), 9);
                left_pointers.push_back(left_pointer);
                row_id = db.parseVarint(varint_buf, bytes_read);
                row_ids.push_back(row_id);
            }
            for(size_t i = 0; i < 20; i++) { 
                std::cout << "collected id: " << out_id[i] << std::endl;
            }
            for(auto i : row_ids) {
                std::cout << "row id: " << i << std::endl;
            }
            exit(1);

            //checking if we need to go to left subtree
            for(size_t i = 0; i < left_pointers.size(); i++) {
                for(long id : out_id) {
                    if (id <= row_ids[i]) {
                        traverseBTreePageTableB(database_file, left_pointers[i], page_size, string_parser, col_indices,
                        index_to_name, db, out_id);
                        break;
                    }
                }
            }
            /*
            for (auto& page_num : left_pointers) {
                traverseBTreePageTableB(database_file, page_num, page_size, string_parser,col_indices, index_to_name, db, out_id);
            }*/
            // check if we need to go to the right child
            for (long id : out_id) {
                if (id > row_ids.back()) {
                    traverseBTreePageTableB(database_file, right_child, page_size, string_parser, col_indices, index_to_name,db, out_id);       
                }
            }
            
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
bool isDigits(const std::string& str) {
    return !str.empty() && std::all_of(str.begin(), str.end(), ::isdigit);
}
void BTreeNavigator::printRow(const std::unordered_map<std::string, std::string>& row) const {
    std::cout << "{";
    for (const auto& val : row) {
        std::cout << val.first << ": " << val.second << ", ";
    }
    std::cout << "}\n" ;
}
// NOTE: index b tree's all contain a payload, not just the leaf cells
void BTreeNavigator::traverseBTreePageIndexB(std::ifstream& database_file, uint32_t page_number, int page_size, SQLParser& string_parser,
    WhereClause& clause, Database& db, std::vector<long>& out_id) {
    IndexB page_type = this->getPageTypeIndexB(database_file, page_number, page_size);
    uint32_t page_offset = (page_number - 1) * page_size + (page_number == 1 ? 100 : 0);
    char cell_bytes[2];
    database_file.seekg(page_offset + 3);
    database_file.read(cell_bytes,2);
    uint16_t cell_count = ((static_cast<unsigned char>(cell_bytes[0]) << 8) | static_cast<unsigned char>(cell_bytes[1]));
    std::vector<std::string> targets = string_parser.extractColumnIndice();
    switch(page_type) {
        case IndexB::leafCell: {
            //this->parseIndexPayload(database_file, page_offset, cell_count, string_parser, db, true);           
            for (int i = 0; i < cell_count; i++) {
                char buf[2];
                uint64_t temp = 1;
                std::vector<uint64_t> serial_types = db.computeIndexSerialTypes(page_offset, buf, i, true);
                std::vector<std::string> column_values = db.extractColumnValues(serial_types, temp);

                // mapping the column values to the column names
                std::unordered_map<std::string, std::string> row;
                int index = 0;
                for (const auto& col_name : targets) {
                    row[col_name] = column_values[index];
                    index++;
                }
                row["rowid"] = column_values.back();

                for (const auto& x : clause.conditions) {
                    const std::string& key_val = row[x.column];
                    if (key_val == x.value) {
                        out_id.push_back(std::stol(row["rowid"]));
                        //printRow(row);
                        break;
                    }
                }
            }
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
            bool went_left = false;
            for(int i = 0; i < cell_count; i++) {
                ///////////////////////////////////////////////////
                // getting the left child pointer
                //////////////////////////////////////////////////
                auto offset = cell_offsets[i];
                database_file.seekg(page_offset + offset);
                char buffer[4];
                database_file.read(buffer, 4);
                uint32_t left_pointer = (static_cast<unsigned char>(buffer[3]) | (static_cast<unsigned char>(buffer[2]) << 8) |
                (static_cast<unsigned char>(buffer[1]) << 16) | (static_cast<unsigned char>(buffer[0]) << 24));

                char buf[2];
                uint64_t temp = 1;
                std::vector<uint64_t> serial_types = db.computeIndexSerialTypes(page_offset, buf, i, false);
                std::vector<std::string> column_values = db.extractColumnValues(serial_types, temp);
                // mapping the column values to the column names
                std::unordered_map<std::string, std::string> row;
                int index = 0;
                for (const auto& col_name : targets) {
                    row[col_name] = column_values[index];
                    index++;
                }
                row["rowid"] = column_values.back();
                bool go_left = false;
                for(const auto& x : clause.conditions) {
                    const std::string& key_val = row[x.column];
                    if (key_val > x.value) {
                        this->traverseBTreePageIndexB(database_file, left_pointer, page_size, string_parser, clause, db, out_id);
                        went_left = true;
                        break;
                    }
                    else if (key_val == x.value) {
                        //printRow(row);
                        out_id.push_back(std::stol(row["rowid"]));
                        this->traverseBTreePageIndexB(database_file, left_pointer, page_size, string_parser, clause, db, out_id);
                        went_left = true;
                        break;
                    }
                    //}
                }
            }

            if (!went_left)
                this->traverseBTreePageIndexB(database_file, right_child, page_size, string_parser, clause, db, out_id);
            break;
        }
        default:
            return;
    };
}

