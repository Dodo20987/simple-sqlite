#include "../include/Database.h";

const std::streampos Database::getFileSize() const {
    std::streampos original_pos = database_file.tellg();
    
    database_file.seekg(0, std::ios::cur);
    std::streampos file_size = database_file.tellg();
    database_file.seekg(original_pos);

    return file_size;
}

bool Database::isCount(const std::string& query) const {
    std::string upper_query = query;
    std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);
    return upper_query.find("COUNT(") != std::string::npos;
}