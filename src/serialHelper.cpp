#include "../include/serialHelper.h"

const std::string handleInt(const std::vector<char>& data, int size) {
    uint64_t int_val = 0;
    for (int i = 0; i < size; i++) {
        int_val = (int_val << 8) | static_cast<unsigned char>(data[i]);
    }
    return std::to_string(int_val);
}

const std::string handleFloat(const std::vector<char>& data, int size) {
    uint64_t raw = 0;
    for (int i = 0; i < size; i++) {
        raw = (raw << 8) | static_cast<unsigned char>(data[i]);
    }
    
    double float_val;
    std::memcpy(&float_val, &raw, sizeof(double));
    
    return std::to_string(float_val);
}

const std::string handleSerial9() {
    return "1";
}

const std::string handleSerial8() {
    return "0";
}