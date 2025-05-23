#include "../include/BTreeNavigator.h"
#include "../include/Database.h"

void BTreeNavigator::readInteriorTablePage(int page_offset) {

}

void BTreeNavigator::readLeafTablePage(std::ifstream& database_file, uint32_t page_offset, SQLParser& string_parser, std::vector<int>& col_indices,
std::unordered_map<int, std::string>& index_to_name, Database& db) {
    // number of cells
    //TODO: we have already written the logic inside the select functions so need to
    char buf[2];
    database_file.seekg(page_offset + 3);
    database_file.read(buf,2);
    //TODO: THE number of rows is not getting computed correctly for the superheroes.db
    unsigned short number_of_rows = (static_cast<unsigned char>(buf[0]) << 8) | static_cast<unsigned char>(buf[1]);
    /*
    std::cout << "num rows: " << number_of_rows << std::endl;
    */
    //exit(1);
    for (int k = 0; k < number_of_rows; k++) {
        //std::cout << "1" << std::endl;
        //std::cout << "page offset: " << page_offset << std::endl;
        uint64_t rowid = 0;
        std::vector<uint64_t> serial_types = db.computeSerialTypes(page_offset,buf,k, rowid);
        //std::cout << "2" << std::endl;
        std::vector<std::string> column_values = db.extractColumnValues(serial_types, rowid);
        //std::cout << "3" << std::endl;
        bool is_first_iteration = true;
        std::unordered_map<std::string, std::string> row;
        WhereClause where = string_parser.parseWhereClause();
        for (int col_index : col_indices) {
            if (col_index < column_values.size()) {
                row[index_to_name[col_index]] = column_values[col_index];
                //std::cout << "index: " << index_to_name[col_index] << " val: " << row[index_to_name[col_index]] << std::endl;
            }
        }
        /*
        for (const auto& x : row) {
            std::cout << "key: " << x.first << " val: " << x.second << std::endl;
        }*/
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
        //std::cout << "here" << std::endl;

    }
    //std::cout << "d1" << std::endl;
}

TableB BTreeNavigator::getPageTypeTableB(std::ifstream& database_file, uint32_t page_number, int page_size) const {
    database_file.seekg((page_number - 1) * page_size + (page_number == 1 ? 100 : 0));
    //std::cout << "in off: " << (page_number - 1) * page_size << std::endl;
    char page_type_byte[2];
    database_file.read(page_type_byte, 1);
    uint32_t flag = static_cast<uint32_t>(page_type_byte[0]);

    switch (flag) {
        case 0x0d:
            return TableB::leafCell;
        case 0x05:
            return TableB::interiorCell;
        default:
            return TableB::unknown;
    };
}
void BTreeNavigator::traverseBTreePageTableB(std::ifstream& database_file, uint32_t page_number, int page_size, SQLParser& string_parser,
std::vector<int>& col_indices, std::unordered_map<int, std::string>& index_to_name, Database& db) {
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
/*
void BTreeNavigator::traverseBTreePageIndexB(std::ifstream& database_file, uint32_t page_number, int page_size) {
    IndexB page_type = this->getPageTypeIndexB(database_file, page_number, page_size);
    switch(page_type) {
        case IndexB::leafCell:
            // do something
            break;
        case IndexB::interiorCell:
            // do something
            break;
        default:
            return;
    };
}*/

