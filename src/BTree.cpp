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

//TODO: make out_id a deque for faster front removal
void BTreeNavigator::readLeafTablePage(std::ifstream& database_file, uint32_t page_offset, SQLParser& string_parser, std::vector<int>& col_indices,
std::unordered_map<int, std::string>& index_to_name, Database& db, std::vector<unsigned long>& out_id) {
    // number of cells
    char buf[2];
    database_file.seekg(page_offset + 3);
    database_file.read(buf,2);
    unsigned short number_of_rows = (static_cast<unsigned char>(buf[0]) << 8) | static_cast<unsigned char>(buf[1]);

    // If we have target rowids, use binary search to find the range of cells to process
    if (!out_id.empty()) {
        // Find the first cell that might contain our target rowids
        int start_pos = 0;
        int end_pos = number_of_rows - 1;
        std::vector<uint64_t> row_ids(number_of_rows);
        //std::cout << "leaf cell row id len: " << row_ids.size() << std::endl;
        // Binary search to find the first potential match
        while (start_pos <= end_pos) {
            int mid = start_pos + (end_pos - start_pos) / 2;
            uint64_t rowid = db.computeRowId(page_offset, buf, mid);
            //std::vector<uint64_t> serial_types = db.computeSerialTypes(page_offset, buf, mid, rowid);
            
            if (rowid >= out_id.front()) {
                end_pos = mid - 1;
            } else {
                start_pos = mid + 1;
            }
        }
        // Parse WHERE clause once outside the loop
        WhereClause where = string_parser.parseWhereClause();
        // Pre-allocate the row map with expected size
        std::unordered_map<std::string, std::string> row;
        row.reserve(col_indices.size());
        
        // Now process only cells that might contain our target rowids
        // TODO: looping here is not necessary cause we used binary search
        //for (int k = start_pos; k < number_of_rows; k++) {
            //uint64_t rowid = 0;
            // Get rowid from computeSerialTypes but don't use the serial types yet
            uint64_t rowid = db.computeRowId(page_offset, buf, start_pos);
            //std::vector<uint64_t> serial_types = db.computeSerialTypes(page_offset, buf, k, rowid);
            
            // If we've gone past our target range, stop processing
            
            // Only process if this rowid is in our target list
            if (std::binary_search(out_id.begin(), out_id.end(), rowid)) {
                // We already have the serial types, so just use them
                std::vector<uint64_t> serial_types = db.computeSerialTypes(page_offset, buf, start_pos, rowid);
                std::vector<std::string> column_values = db.extractColumnValues(serial_types, rowid);
                std::cout << "end: " << end_pos << std::endl;
                std::cout << "start: " << start_pos << std::endl;
                //std::cout << "found at idx: " << k << std::endl;
                std::cout << "num rows: " << number_of_rows << std::endl;
                std::cout << "front: " << out_id.front() << std::endl;
                // Clear the row map instead of creating a new one
                row.clear();
                bool is_first_iteration = true;
                
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
                            out_id.erase(out_id.begin());
                        }
                        else std::cout << "|" << val.second;
                    }
                    std::cout << std::endl;
                }
            }
            else {
                return;
            }
        //}
    } else {
        // Original behavior when no target rowids are provided
        for (int k = 0; k < number_of_rows; k++) {
            uint64_t rowid = 0;
            std::vector<uint64_t> serial_types = db.computeSerialTypes(page_offset, buf, k, rowid);
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
}

//TODO: need to traverse the b tree by looking at the out_id row id's and by looking at the first and last row id's of the page
// to determine if the page should be skipped or not 
void BTreeNavigator::traverseBTreePageTableB(std::ifstream& database_file, uint32_t page_number, int page_size, SQLParser& string_parser,
std::vector<int>& col_indices, std::unordered_map<int, std::string>& index_to_name, Database& db, std::vector<unsigned long>& out_id) {
    TableB page_type = this->getPageTypeTableB(database_file, page_number, page_size);
    switch(page_type) {
        case TableB::leafCell: {
            uint32_t page_offset = (page_number - 1) * page_size + (page_number == 1 ? 100 : 0);
            //std::cout << "leaf entered" << std::endl;
            readLeafTablePage(database_file, page_offset, string_parser, col_indices, index_to_name, db, out_id);
            break;
        }
        case TableB::interiorCell: {
            uint32_t page_offset = (page_number - 1) * page_size + (page_number == 1 ? 100 : 0);

            char cell_bytes[2];
            database_file.seekg(page_offset + 3); // number of cells
            database_file.read(cell_bytes, 2);
            uint16_t cell_count = (static_cast<unsigned char>(cell_bytes[0]) << 8) | static_cast<unsigned char>(cell_bytes[1]);
            char right_ptr_bytes[4];
            database_file.seekg(page_offset + 8);
            database_file.read(right_ptr_bytes, 4);
            uint32_t right_child = (static_cast<unsigned char>(right_ptr_bytes[0]) << 24) | 
                (static_cast<unsigned char>(right_ptr_bytes[1]) << 16) | 
                (static_cast<unsigned char>(right_ptr_bytes[2]) << 8) | 
                static_cast<unsigned char>(right_ptr_bytes[3]);

            // Get all cell offsets
            std::vector<uint16_t> cell_offsets;
            for (int i = 0; i < cell_count; i++) {
                char cell_bytes[2];
                database_file.read(cell_bytes,2);
                uint16_t cell_offset = (static_cast<unsigned char>(cell_bytes[1]) | (static_cast<unsigned short>(cell_bytes[0]) << 8));
                cell_offsets.push_back(cell_offset);
            }

            // Get all left pointers and rowids
            std::vector<uint32_t> left_pointers;
            std::vector<uint64_t> row_ids;
            for (auto offset : cell_offsets) {
                database_file.seekg(page_offset + offset);
                char buffer[4];
                database_file.read(buffer, 4);
                uint32_t left_pointer = (static_cast<unsigned char>(buffer[3]) | (static_cast<unsigned char>(buffer[2]) << 8) |
                    (static_cast<unsigned char>(buffer[1]) << 16) | (static_cast<unsigned char>(buffer[0]) << 24));
                left_pointers.push_back(left_pointer);
                unsigned char varint_buf[9];
                uint64_t rowid = 0;
                int bytes_read = 0;
                database_file.read(reinterpret_cast<char*>(varint_buf), 9);
                rowid = db.parseVarint(varint_buf, bytes_read);
                row_ids.push_back(rowid);
            }
            // If we have no target rowids or no cells, traverse everything
            if (out_id.empty() || cell_count == 0) {
                // Traverse all pages
                for (auto& page_num : left_pointers) {
                    traverseBTreePageTableB(database_file, page_num, page_size, string_parser,
                        col_indices, index_to_name, db, out_id);
                }
                traverseBTreePageTableB(database_file, right_child, page_size, string_parser,
                    col_indices, index_to_name, db, out_id);
                break;
            }
            else {
                // optimized version
                uint64_t min_rowid = out_id.front();
                uint64_t max_rowid = out_id.back();
                
                int start_idx = 0;
                int end_idx = cell_count - 1;

                // using binary search
                while(start_idx <= end_idx) {
                    int mid = start_idx + (end_idx - start_idx) / 2;
                    if (row_ids[mid] >= min_rowid) {
                        end_idx = mid - 1;
                    }
                    else {
                        start_idx = mid + 1;
                    }
                }

                for (int i = start_idx; i < cell_count; i++) {
                    // stop traversing if the page has gone past the max rowid
                    if (row_ids[i] > max_rowid) {
                        break;
                    }
                    // Traverse the left child page
                    traverseBTreePageTableB(database_file, left_pointers[i], page_size, string_parser,
                        col_indices, index_to_name, db, out_id);
                }

                // Check if we need to traverse the rightmost child
                if (start_idx < cell_count && row_ids[start_idx] <= max_rowid) {
                    traverseBTreePageTableB(database_file, right_child, page_size, string_parser,
                        col_indices, index_to_name, db, out_id);
                }
                break;

            }

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



