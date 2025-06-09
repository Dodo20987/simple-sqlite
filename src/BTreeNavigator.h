class BTreeNavigator {
private:
    // Add a simple LRU cache for pages
    struct PageCacheEntry {
        uint32_t page_number;
        std::vector<char> page_data;
        PageCacheEntry(uint32_t pn, const std::vector<char>& data) : page_number(pn), page_data(data) {}
    };
    
    static const size_t CACHE_SIZE = 100; // Adjust based on available memory
    std::list<PageCacheEntry> page_cache;
    std::unordered_map<uint32_t, std::list<PageCacheEntry>::iterator> page_cache_map;
    
    // Helper to get page data with caching
    std::vector<char> getPageData(std::ifstream& database_file, uint32_t page_number, int page_size) {
        auto it = page_cache_map.find(page_number);
        if (it != page_cache_map.end()) {
            // Move to front of LRU cache
            page_cache.splice(page_cache.begin(), page_cache, it->second);
            return it->second->page_data;
        }
        
        // Read page from disk
        std::vector<char> page_data(page_size);
        database_file.seekg((page_number - 1) * page_size + (page_number == 1 ? 100 : 0));
        database_file.read(page_data.data(), page_size);
        
        // Add to cache
        if (page_cache.size() >= CACHE_SIZE) {
            page_cache_map.erase(page_cache.back().page_number);
            page_cache.pop_back();
        }
        page_cache.emplace_front(page_number, page_data);
        page_cache_map[page_number] = page_cache.begin();
        
        return page_data;
    }
}; 