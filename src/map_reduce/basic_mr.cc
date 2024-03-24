#include <string>
#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>
#include <sstream>

#include "map_reduce/protocol.h"

namespace mapReduce {
    //
    // The map function is called once for each file of input. The first
    // argument is the name of the input file, and the second is the
    // file's complete contents. You should ignore the input file name,
    // and look only at the contents argument. The return value is a slice
    // of key/value pairs.
    //
    std::vector<KeyVal> Map(const std::string &content) {
        // Your code goes here
        // Hints: split contents into an array of words.

        std::vector<KeyVal> res;
        std::stringstream stream(content);
        std::string word;
        while (stream >> word) {
            std::size_t prev = 0, pos;
            if (word.find_first_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRETUVWXYZ") == std::string::npos)
                continue;
            while ((pos = word.find_first_of("1234567890 ,.\'\"\t;:!?-_()/\\@$*$%#^&+=|{}[]<>\r\n\b\f\v", prev)) != std::string::npos) {
                if (pos > prev)
                    res.emplace_back(word.substr(prev, pos - prev), "1");
                prev = pos + 1;
            }
            if (prev < word.length())
                res.emplace_back(word.substr(prev, std::string::npos), "1");
        }
        return res;
    }

    //
    // The reduce function is called once for each key generated by the
    // map tasks, with a list of all the values created for that key by
    // any map task.
    //
    std::string Reduce(const std::string &key, const std::vector<std::string> &values) {
        // Your code goes here
        // Hints: return the number of occurrences of the word.

        long sum = 0;
        for (auto &&i : values)
            sum += std::strtol(i.c_str(), nullptr, 10);
        return std::to_string(sum);
    }
}
