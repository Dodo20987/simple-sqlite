#pragma once
#include <string>
#include <regex>
#include <algorithm>
#include <iostream>
#include <vector>
#include <sstream>
#include <unordered_map>
enum {
    COUNT = 0,
    SELECT = 1,
    INSERT = 2,
    
};
class SQLParser {
private:
    std::string query;
public:
    SQLParser() : query("") {}
    SQLParser(const std::string& query) : query(query) {}

    const std::string getQuery() const;
    void setQuery(const std::string& query);
    std::unordered_map<std::string, std::vector<std::string>> selectQuery();
    bool isSelect() const;
};