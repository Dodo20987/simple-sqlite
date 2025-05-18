#pragma once
#include <string>
#include <regex>
#include <algorithm>
#include <iostream>
#include <vector>
#include <sstream>
#include <unordered_map>
#include <cctype>
enum {
    COUNT = 0,
    SELECT = 1,
    INSERT = 2,
    
};
enum class LogicalOp { AND, OR };
struct WhereCondition {
    std::string column;
    std::string operation;
    std::string value;
};

// conditions = [cond0, cond1, ...] logic = [AND, OR, ....]
struct WhereClause {
    std::vector<WhereCondition> conditions;
    std::vector<LogicalOp> logic; // joins conditions[i] with conditions[i+1]
};

class SQLParser {
private:
    std::string query;
    std::string extractWhereClause() const;
    WhereCondition parseCondition(const std::string& expr) const;
public:
    SQLParser() : query("") {}
    SQLParser(const std::string& query) : query(query) {}
    bool isWhereClause() const;
    bool isCreateIndex() const;
    bool isCreateTable() const;
    bool matchIndexToTable(const std::string& table_name) const;
    std::vector<std::string> extractColumnIndice() const; 
    WhereClause parseWhereClause() const;
    const std::string getQuery() const;
    void setQuery(const std::string& query);
    std::unordered_map<std::string, std::vector<std::string>> selectQuery() const;
    bool isSelect() const;
};