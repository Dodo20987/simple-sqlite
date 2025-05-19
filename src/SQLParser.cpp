#include "../include/SQLParser.h"

std::vector<std::string> SQLParser::extractColumnIndice() const {
    std::string upper_query = query;
    std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);
    std::string keyword = "ON ";
    size_t pos = upper_query.find(keyword);
    std::vector<std::string> res;
    // TODO: using ',' as as the delim, iterate through the string and push into the res array
    if (pos != std::string::npos) {
        pos += keyword.length();
        size_t begin = upper_query.find('(', pos) + 1;
        size_t open_paren = upper_query.find("(", pos);
        size_t close_paren = upper_query.find(")", open_paren);
        char delim = ',';
        if(open_paren != std::string::npos && close_paren != std::string::npos) {
            std::string cols = query.substr(open_paren + 1, close_paren - open_paren - 1);
            std::stringstream ss(cols);
            std::string col;
            while(std::getline(ss, col, delim)) {
                col.erase(std::remove_if(col.begin(), col.end(), ::isspace), col.end());
                res.push_back(col);
            }
        }
       
    }
    return res;
}
bool SQLParser::isCreateIndex() const {
    std::string upper_query = query;
    std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);
    bool res = upper_query.find("CREATE INDEX") != std::string::npos;
    return res;
}

bool SQLParser::isCreateTable() const {
    std::string upper_query = query;
    std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);
    bool res = upper_query.find("CREATE TABLE") != std::string::npos;
    return res;
}
bool SQLParser::matchIndexToTable(const std::string& table_name) const {
    bool res = false;
    std::string upper_query = query;
    std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);
    std::string keyword = "ON ";
    size_t pos = upper_query.find(keyword);
    if(pos != std::string::npos) {
        pos += keyword.length();
        size_t end = upper_query.find(' ', pos);
        std::string actual_name = query.substr(pos, end - pos);
        res = actual_name == table_name;
    }
    return res;
}
const std::string SQLParser::getQuery() const {
    return query;
}


void SQLParser::setQuery(const std::string& query) {
    this->query = query;
}

std::string SQLParser::extractWhereClause() const {
    std::string upper_query = query;
    std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);
    size_t pos = upper_query.find("WHERE");
    if(pos == std::string::npos) {
        return "";
    }
    return query.substr(pos + 5);
}
WhereCondition SQLParser::parseCondition(const std::string& expr) const {
    static const std::vector<std::string> operators = { ">=", "<=", "!=", "=", "<", ">" };
    for (const auto& op : operators) {
        size_t pos = expr.find(op);
        if(pos != std::string::npos) {
            std::string col = expr.substr(0,pos);
            std::string val = expr.substr(pos + op.size());
             // Trim whitespace
            auto trim = [](std::string& s) {
                size_t start = s.find_first_not_of(" \t\n");
                size_t end = s.find_last_not_of(" \t\n");
                s = (start == std::string::npos) ? "" : s.substr(start, end - start + 1);
            };
            trim(col);
            trim(val);
            // Remove quotes from value if present
            if (!val.empty() && val.front() == '\'' && val.back() == '\'') {
                val = val.substr(1, val.size() - 2);
            }

            return { col, op, val };
        }
    }
    return {"","",""};
}
WhereClause SQLParser::parseWhereClause() const {
    std::string where_statement = this->extractWhereClause();
    WhereClause clause;
    std::istringstream iss(where_statement);
    std::string token;
    std::string current;

    while(iss >> token) {
        std::string upper_token = token;
        std::transform(upper_token.begin(), upper_token.end(), upper_token.begin(), ::toupper);
        if (upper_token == "AND" || upper_token == "OR") {
            clause.conditions.push_back(parseCondition(current));
            clause.logic.push_back(upper_token == "AND" ? LogicalOp::AND : LogicalOp::OR);
            current.clear();
        }
        else {
            if(!current.empty()) current += " ";
            current += token;
        }
    }
    if(!current.empty()) {
        clause.conditions.push_back(parseCondition(current));
    }
    return clause;
}
std::unordered_map<std::string, std::vector<std::string>> SQLParser::selectQuery() const {
    //std::regex regex(R"(select (.*?) from (\w+))", std::regex::icase);
    //std::regex regex(R"(select\s+(.*?)\s+from\s+(\w+))", std::regex::icase);
    //std::regex regex(R"(select\s+(.*?)\s+from\s+([^;]+))", std::regex::icase);
    //std::regex regex(R"(select\s+(.*?)\s+from\s+([^\s]+(?:\s*,\s*[^\s]+)*))", std::regex::icase);
    std::regex regex(R"(select\s+(.*?)\s+from\s+(.+?)(?:\s+where|\s*$))", std::regex::icase);
    std::smatch match;
    std::unordered_map<std::string, std::vector<std::string>> mp;
    mp["cols"] = {};
    mp["tables"] = {};
    if(std::regex_search(query, match, regex)) {
        std::string column_str = match[1]; // args after select
        std::string table_name = match[2]; // table names after from 

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
        std::string tbl;
        while(std::getline(ss2,tbl, ',')) {
            tbl.erase(0, tbl.find_first_not_of(" \t"));
            tbl.erase(tbl.find_last_not_of(" \t") + 1);
            tables.push_back(tbl);
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

bool SQLParser::isWhereClause() const {
    std::cout << "checking" << std::endl;
    std::string upper_query = query;
    std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);
    bool result = upper_query.find("WHERE") != std::string::npos;
    std::cout << "res : " << result << std::endl;
    return result;
}