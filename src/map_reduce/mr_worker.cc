#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <unordered_map>

#include "map_reduce/protocol.h"

namespace mapReduce {
    Worker::Worker(MR_CoordinatorConfig config) {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
        // Lab4: Your code goes here (Optional).
    }

    void Worker::doMap(int index, const std::string &filename) {
        // Lab4: Your code goes here.
        auto inodeId = chfs_client->lookup(1, filename).unwrap();
        auto fileAttr = chfs_client->get_type_attr(inodeId).unwrap();
        auto read_res = chfs_client->read_file(inodeId, 0, fileAttr.second.size).unwrap();
        auto data = std::string(read_res.begin(), read_res.end());
        auto mapResult = Map(data);
        std::stringstream ss;
        for (auto &&kv : mapResult)
            ss << kv.key << ' ' << kv.val << std::endl;
        auto resultStr = ss.str();
        chfs::inode_id_t resultInodeId = 0;
        auto mkRes = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-" + std::to_string(index));
        if (mkRes.is_err()) {
            auto lookupRes = chfs_client->lookup(1, "mr-" + std::to_string(index));
            resultInodeId = lookupRes.unwrap();
        }
        else
            resultInodeId = mkRes.unwrap();
        chfs_client->write_file(resultInodeId, 0, {resultStr.begin(), resultStr.end()});
    }

    void Worker::doReduce(int index, int firstFile, int nfiles) {
        // Lab4: Your code goes here.
        std::vector<std::string> files;
        std::string outputFilename;
        if (index == -1) {
            outputFilename = outPutFile;
            for (int i = 0; i < nfiles; ++i)
                files.emplace_back("mr-r-" + std::to_string(firstFile + i));
        }
        else {
            outputFilename = "mr-r-" + std::to_string(index);
            for (int i = 0; i < nfiles; ++i)
                files.emplace_back("mr-" + std::to_string(firstFile + i));
        }
        std::map<std::string, std::vector<std::string>> mapRes;
        for (const auto &file : files) {
            auto fileInodeId = chfs_client->lookup(1, file).unwrap();
            auto fileAttr = chfs_client->get_type_attr(fileInodeId).unwrap().second;
            auto fileContent = chfs_client->read_file(fileInodeId, 0, fileAttr.size).unwrap();
            std::stringstream ss({fileContent.begin(), fileContent.end()});
            std::string key, val;
            while (ss >> key >> val)
                mapRes[key].emplace_back(val);
        }
        // std::map<std::string, std::string> reduceRes;
        std::vector<KeyVal> reduceRes;
        std::stringstream ss;
        for (const auto &[k ,vs] : mapRes)
            ss << k << ' ' << Reduce(k, vs) << std::endl;
        auto resultStr = ss.str();
        chfs::inode_id_t resultInodeId = 0;
        auto lookupRes = chfs_client->lookup(1, outputFilename);
        if (lookupRes.is_err()) {
            auto mkRes = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, outputFilename);
            resultInodeId = mkRes.unwrap();
        }
        else
            resultInodeId = lookupRes.unwrap();
        chfs_client->write_file(resultInodeId, 0, {resultStr.begin(), resultStr.end()});
    }

    void Worker::doSubmit(mr_tasktype taskType, int index) {
        // Lab4: Your code goes here.
        auto taskTypeInt = static_cast<int>(taskType);
        mr_client->call(SUBMIT_TASK, taskTypeInt, index);
    }

    void Worker::stop() {
        shouldStop = true;
        work_thread->join();
    }

    void Worker::doWork() {
        while (!shouldStop) {
            // Lab4: Your code goes here.
            auto res = mr_client->call(ASK_TASK, 0);
            if (res.is_err()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                continue;
            }
            auto taskRes = res.unwrap()->as<std::tuple<int, int, std::string, int, int>>();
            auto [intType, id, filename, firstFile, fileNum] = taskRes;
            auto type = static_cast<mr_tasktype>(intType);
            if (type == NONE) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                continue;
            }
            if (type == MAP) {
                doMap(id, filename);
                doSubmit(MAP, id);
            }
            else {
                doReduce(id, firstFile, fileNum);
                doSubmit(REDUCE, id);
            }
        }
    }
}
