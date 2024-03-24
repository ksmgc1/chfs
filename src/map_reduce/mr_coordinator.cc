#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mutex>

#include "map_reduce/protocol.h"

namespace mapReduce {
    std::tuple<int, int, std::string, int, int> Coordinator::askTask(int) {
        // Lab4 : Your code goes here.
        // Free to change the type of return value.

        std::unique_lock<std::mutex> lock(mtx);
        if (!mapFinished) {
            int firstNotDist = -1;
            for (auto &job : mapJob)
                if (!job.second.first) {
                    job.second.first = true;
                    firstNotDist = job.first;
                    break;
                }
            if (firstNotDist != -1)
                return {static_cast<int>(mr_tasktype::MAP), firstNotDist, files[firstNotDist], -1, -1};
        }
        else if (!reduceFinished) {
            int firstNotDist = -1;
            int firstFile, fileNum;
            bool hasDist, hasDone;
            for (auto &job : reduceJob) {
                std::tie(firstFile, fileNum, hasDist, hasDone) = job.second;
                if (!hasDist) {
                    job.second = {firstFile, fileNum, true, hasDone};
                    firstNotDist = job.first;
                    break;
                }
            }
            if (firstNotDist != -1)
                return {static_cast<int>(mr_tasktype::REDUCE), firstNotDist, "", firstFile, fileNum};
        }
        else if (!isFinished && !lastReduceDistributed) {
            lastReduceDistributed = true;
            return {static_cast<int>(mr_tasktype::REDUCE), -1, "", 0, reduceJob.size()};
        }
        return {static_cast<int>(mr_tasktype::NONE), -1, "", -1, -1};
    }

    int Coordinator::submitTask(int taskType, int index) {
        // Lab4 : Your code goes here.

        std::unique_lock<std::mutex> lock(mtx);
        auto type = static_cast<mr_tasktype>(taskType);
        if (type == mr_tasktype::MAP) {
            mapJob[index] = {true, true};
            bool mFinished = true;
            for (auto &&job : mapJob)
                if (!job.second.second) {
                    mFinished = false;
                    break;
                }
            mapFinished = mFinished;
            return 0;
        }
        if (type == mr_tasktype::REDUCE) {
            if (index == -1) {
                isFinished = true;
                return 0;
            }
            std::get<3>(reduceJob.find(index)->second) = true;
            bool rFinished = true;
            for (auto &&job : reduceJob)
                if (!std::get<3>(job.second)) {
                    rFinished = false;
                    break;
                }
            reduceFinished = rFinished;
            return 0;
        }
        return -1;
    }

    // mr_coordinator calls Done() periodically to find out
    // if the entire job has finished.
    bool Coordinator::Done() {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        return this->isFinished;
    }

    // create a Coordinator.
    // nReduce is the number of reduce tasks to use.
    Coordinator::Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce) {
        this->files = files;
        this->isFinished = false;
        // Lab4: Your code goes here (Optional).

        for (int i = 0; i < files.size(); ++i)
            mapJob[i] = {false, false};
        int baseNum = files.size() / (nReduce - 1);
        int remainNum = files.size() % (nReduce - 1);
        for (int i = 0, fileIdx = 0; i < nReduce - 1; ++i) {
            int fileNum = baseNum + (i < remainNum ? 1 : 0);
            reduceJob[i] = {fileIdx, fileNum, false, false};
            fileIdx += fileNum;
        }
        mapFinished = false;
        reduceFinished = false;

        rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
        rpc_server->bind(ASK_TASK, [this](int i) { return this->askTask(i); });
        rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index) { return this->submitTask(taskType, index); });
        rpc_server->run(true, 1);
    }
}
