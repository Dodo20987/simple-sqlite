#pragma once
#include <iostream>
#include <string>
#include <algorithm>
#include <cstdint>
#include <sstream>
#include <iomanip>
#include <vector>
#include <string>
#include <cstring>

const std::string handleInt(const std::vector<char>& data, int size);
const std::string handleFloat(const std::vector<char>& data, int size);
const std::string handleSerial9();
const std::string handleSerial8();