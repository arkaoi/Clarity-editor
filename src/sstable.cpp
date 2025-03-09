#include "database.hpp"

namespace DB {
SSTable::SSTable(const std::string &file) : filename(file) {
}

bool SSTable::compare_keys(
    const std::pair<std::string, std::string> &a,
    const std::pair<std::string, std::string> &b
) {
    return a.first < b.first;
}

void SSTable::write(const std::map<std::string, std::string> &data) {
    std::ofstream out((filename));
    if (!out) {
        return;
    }

    std::vector<std::pair<std::string, std::string>> sorted_data(
        data.begin(), data.end()
    );
    std::sort(sorted_data.begin(), sorted_data.end(), compare_keys);

    for (size_t i = 0; i < sorted_data.size(); ++i) {
        out << sorted_data[i].first << " " << sorted_data[i].second << "\n";
    }
}

const std::string SSTable::read(const std::string &key) {
    std::ifstream file(filename);
    if (!file) {
        return "";
    }

    std::string line;
    while (std::getline(file, line)) {
        size_t space_pos = line.find(' ');

        if (space_pos != std::string::npos) {
            std::string current_key = line.substr(0, space_pos);
            if (current_key == key) {
                return line.substr(space_pos + 1);
            }
        }
    }

    return "";
}

}  // namespace DB
