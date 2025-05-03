#pragma once
#include <iostream>
#include <string>
#include <algorithm>
#include <cstdint>
#include <sstream>
#include <iomanip>
#include <vector>

void handleInt(const std::vector<char>& data, int size);
void handleFloat(const std::vector<char>& data,int size);
void handleSerial9(const std::vector<char>& data);
void handleSerial8(const std::vector<char>& data);