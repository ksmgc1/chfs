#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "map_reduce/protocol.h"

namespace mapReduce {
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
    }

    void SequentialMapReduce::doWork() {
        // Your code goes here

        std::vector<std::vector<KeyVal>> all_mapped;
        for (auto &&filename : files) {
            // size_t pos = 0;
            // chfs::inode_id_t inode_id = 1;
            // do {
            //     auto end_pos = filename.find_first_of('/', pos);
            //     auto path = filename.substr(pos, end_pos - pos);
            //     auto lookup_res = chfs_client->lookup(inode_id, path);
            //     if (lookup_res.is_err())
            //         return;
            //     inode_id = lookup_res.unwrap();
            //     pos = end_pos + 1;
            // }
            // while (pos != std::string::npos + 1);
            auto inode_id = chfs_client->lookup(1, filename).unwrap();
            auto file_attr = chfs_client->get_type_attr(inode_id).unwrap();
            auto read_res = chfs_client->read_file(inode_id, 0, file_attr.second.size).unwrap();
            auto data = std::string(read_res.begin(), read_res.end());
            auto map_res = Map(data);
            all_mapped.push_back(map_res);
        }
        std::map<std::string, std::vector<std::string>> map_res;
        for (auto i : all_mapped)
            for (auto j : i)
                map_res[j.key].push_back(j.val);
        for (auto &i : map_res)
            std::sort(i.second.begin(), i.second.end());
        std::map<std::string, std::string> reduce_res;
        for (auto key_vals : map_res) {
            auto reduce_r = Reduce(key_vals.first, key_vals.second);
            reduce_res[key_vals.first] = reduce_r;
        }
        std::stringstream ss;
        for (auto kv : reduce_res)
            ss << kv.first << ' ' << kv.second << std::endl;
        auto res = ss.str();
        std::vector<chfs::u8> output_data(res.begin(), res.end());
        chfs::inode_id_t output_id;
        auto lookup_res = chfs_client->lookup(1, outPutFile);
        if (lookup_res.is_err()) {
            auto mk_res = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, outPutFile);
            output_id = mk_res.unwrap();
        }
        else output_id = lookup_res.unwrap();
        chfs_client->write_file(output_id, 0, output_data);
    }
}
