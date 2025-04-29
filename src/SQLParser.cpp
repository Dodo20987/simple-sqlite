#include "../include/SQLParser.h"

const std::string SQLParser::getQuery() const {
    return query;
}

void SQLParser::setQuery(const std::string& query) {
    this->query = query;
}

std::unordered_map<std::string, std::vector<std::string>> SQLParser::selectQuery() {
    std::regex regex(R"(select (.*?) from (\w+))", std::regex::icase);
    std::smatch match;
    std::unordered_map<std::string, std::vector<std::string>> mp;
    mp["cols"] = {};
    mp["tables"] = {};
    if(std::regex_match(query, match, regex)) {
        std::string column_str = match[1]; // args after select
        std::string table_name = match[2]; // table names after from 

        //std::cout << "col names: " << column_str << std::endl;

        // splitting cols by comma, and stripping white space
        std::vector<std::string> columns;
        std::istringstream ss(column_str);
        std::string col;

        while(std::getline(ss,col, ',')) {
            col.erase(0, col.find_first_not_of(" \t")); // left trim
            col.erase(col.find_last_not_of(" \t") + 1); // right trim
            columns.push_back(col);
        }
        // splitting tables by comma, and stripping white space
        std::vector<std::string> tables;
        std::istringstream ss2(table_name);
        while(std::getline(ss2,col, ',')) {
            col.erase(0, col.find_first_not_of(" \t"));
            col.erase(col.find_last_not_of(" \t") + 1);
            tables.push_back(col);
        }
        mp["cols"] = columns;
        mp["tables"] = tables;
    }

    return mp;
}

bool SQLParser::isSelect() const {
    std::string trimmed_query = query;
    trimmed_query.erase(0, trimmed_query.find_first_not_of(" \t\n\r"));
    
    return trimmed_query.size() >= 6 &&
       std::equal(trimmed_query.begin(), trimmed_query.begin() + 6, "SELECT",
                  [](char a, char b) { return std::toupper(a) == b; });
}