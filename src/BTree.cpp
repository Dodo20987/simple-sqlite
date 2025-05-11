#include "../include/BTreeNavigator.h"
void BTreeNavigator::readInteriorTablePage(int page_offset) {

}
void BTreeNavigator::readLeafTablePage(int page_offset) {

}

TableB BTreeNavigator::getPageTypeTableB(std::ifstream& database_file, uint32_t page_number,int page_size) const {
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
IndexB BTreeNavigator::getPageTypeIndexB(std::ifstream& database_file, uint32_t page_number, int page_size) const {
    database_file.seekg(page_number * page_size);

    unsigned char page_type_byte;
    database_file.read(reinterpret_cast<char*>(page_type_byte), 1);

    switch (page_type_byte) {
        case 0x0d:
            return IndexB::leafCell;
        case 0x02:
            return IndexB::interiorCell;
        default:
            return IndexB::unknown;
    };
}
void BTreeNavigator::traverseBTreePageTableB(std::ifstream& database_file, uint32_t page_number, int page_offset, int page_size) {
    TableB page_type = this->getPageTypeTableB(database_file, page_number, page_size);
    switch(page_type) {
        case TableB::leafCell:
            // extracting the records
            readLeafTablePage(page_offset);
            break;
        case TableB::interiorCell:
            // recurse on the child cells
            readInteriorTablePage(page_offset);
            break;
        default:
            return;
    };

}

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
}

