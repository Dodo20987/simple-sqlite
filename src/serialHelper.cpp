#include "../include/serialHelper.h"

const std::string handleInt(const std::vector<char>& data, int size) {
    uint64_t int_val = 0;
    for (int i = 0; i < size; i++) {
        int_val = (int_val << 8) | static_cast<unsigned char>(data[i]);
    }
    return std::to_string(int_val);
}

const std::string handleFloat(const std::vector<char>& data, int size) {

}

const std::string handleSerial9(const std::vector<char>& data) {

}

const std::string handleSerial8(const std::vector<char>& data) {

}