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
std::vector<int>& col_indices, std::unordered_map<int, std::string>& index_to_name, Database& db, const std::vector<unsigned long>& out_id) {
    TableB page_type = this->getPageTypeTableB(database_file, page_number, page_size);
    switch(page_type) {
        case TableB::leafCell: {
            uint32_t page_offset = (page_number - 1) * page_size + (page_number == 1 ? 100 : 0);
            readLeafTablePage(database_file, page_offset, string_parser, col_indices, index_to_name, db);
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
            std::vector<unsigned long> row_ids;
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
            // If we have target rowids, only traverse pages that could contain them
            if (!out_id.empty()) {
                // Find the range of pages we need to visit
                size_t start_idx = 0;
                size_t end_idx = left_pointers.size();
                
                // Skip pages that are definitely too small
                while (start_idx < row_ids.size() && row_ids[start_idx] < out_id.front()) {
                    start_idx++;
                    // when we increment start_idx too much and is not in range of our row id
                    if (row_ids[start_idx] > out_id.front()) {
                        start_idx--;
                        break;
                    }
                }
                // Skip pages that are definitely too large
                while (end_idx > 0 && row_ids[end_idx - 1] > out_id.back()) {
                    end_idx--;
                    // if we decrement end_idx too much and is smaller than the row id
                    if(row_ids[end_idx - 1] < out_id.back()) {
                        end_idx++;
                        break;
                    }
                }
                // Traverse only the pages in our range
                for (size_t i = start_idx; i < end_idx; i++) {
                    traverseBTreePageTableB(database_file, left_pointers[i], page_size, string_parser,
                        col_indices, index_to_name, db, out_id);
                }
                
                // Also check the right child if it might contain our target
                if (end_idx == left_pointers.size() || row_ids[end_idx - 1] < out_id.back()) {
                    traverseBTreePageTableB(database_file, right_child, page_size, string_parser,
                        col_indices, index_to_name, db, out_id);
                }
            } else {
                // If no target rowids, traverse everything
                for (auto& page_num : left_pointers) {
                    traverseBTreePageTableB(database_file, page_num, page_size, string_parser,
                        col_indices, index_to_name, db, out_id);
                }
                traverseBTreePageTableB(database_file, right_child, page_size, string_parser,
                    col_indices, index_to_name, db, out_id);
            }
            break;
        }
        default:
            std::cout << "Unknown page type" << std::endl;
            exit(1);
    }
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
    WhereClause& clause, Database& db, std::vector<unsigned long>& out_id, const std::vector<std::string>& targets) {
    IndexB page_type = this->getPageTypeIndexB(database_file, page_number, page_size);
    uint32_t page_offset = (page_number - 1) * page_size + (page_number == 1 ? 100 : 0);
    char cell_bytes[2];
    database_file.seekg(page_offset + 3);
    database_file.read(cell_bytes,2);
    uint16_t cell_count = ((static_cast<unsigned char>(cell_bytes[0]) << 8) | static_cast<unsigned char>(cell_bytes[1]));
    if (cell_count == 0) {
        return;
    }
    switch(page_type) {
        case IndexB::leafCell: {
            // Find the first position where we might have a match
            int start_pos = 0;
            int end_pos = cell_count - 1;
            
            // Binary search to find the first potential match
            while (start_pos <= end_pos) {
                int mid = start_pos + (end_pos - start_pos) / 2;
                char buf[2];
                uint64_t temp = 1;
                std::vector<uint64_t> serial_types = db.computeIndexSerialTypes(page_offset, buf, mid, true);
                std::vector<std::string> column_values = db.extractColumnValues(serial_types, temp);
                
                std::unordered_map<std::string, std::string> row;
                int index = 0;
                for(const auto& col_name : targets) {
                    row[col_name] = column_values[index++];
                }
                bool should_go_left = false;
                for(const auto& x : clause.conditions) {
                    const std::string& key_val = row[x.column];
                    if(key_val >= x.value) {
                        should_go_left = true;
                        break;
                    }
                }
                if (should_go_left) {
                    end_pos = mid - 1;
                } else {
                    start_pos = mid + 1;
                }
            }
            
            // Now scan forward from start_pos until we find values greater than what we're looking for
            for (int i = start_pos; i < cell_count; i++) {
                char buf[2];
                uint64_t temp = 1;
                std::vector<uint64_t> serial_types = db.computeIndexSerialTypes(page_offset, buf, i, true);
                std::vector<std::string> column_values = db.extractColumnValues(serial_types, temp);
                
                std::unordered_map<std::string, std::string> row;
                int index = 0;
                for(const auto& col_name : targets) {
                    row[col_name] = column_values[index++];
                }
                row["rowid"] = column_values.back();
                
                bool matches_condition = false;
                bool should_continue = false;
                for(const auto& x : clause.conditions) {
                    const std::string& key_val = row[x.column];
                    if(key_val == x.value) {
                        matches_condition = true;
                    }
                    if(key_val > x.value) {
                        should_continue = true;
                        break;
                    }
                }
                
                if (matches_condition) {
                    out_id.push_back(static_cast<unsigned long>(std::stol(row["rowid"])));
                }
                if (should_continue) {
                    break;
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

            std::vector<uint16_t> cell_offsets(cell_count);
            for(int i = 0; i < cell_count; i++) {
                char cell_bytes[2];
                database_file.read(cell_bytes,2);
                cell_offsets[i] = ((static_cast<unsigned char>(cell_bytes[0]) << 8) | 
                static_cast<unsigned char>(cell_bytes[1]));
            }

            bool went_left = false;
            for(int i = 0; i < cell_count; i++) {
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
                for(const auto& x : clause.conditions) { const std::string& key_val = row[x.column];
                    if (key_val > x.value) { this->traverseBTreePageIndexB(database_file, left_pointer, page_size, string_parser, clause, db, out_id, targets);
                        went_left = true;
                        break;
                    }
                    else if (key_val == x.value) {
                        out_id.push_back(static_cast<unsigned long>(std::stol(row["rowid"])));
                        this->traverseBTreePageIndexB(database_file, left_pointer, page_size, string_parser, clause, db, out_id, targets);
                        std::cout << "here" << std::endl;
                        went_left = true;
                        break;
                    }
                }
            }
            if (!went_left)
                this->traverseBTreePageIndexB(database_file, right_child, page_size, string_parser, clause, db, out_id, targets);
            break;
        }
        default:
            return;
    };
}



