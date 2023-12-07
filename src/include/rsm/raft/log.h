#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include <mutex>
#include <vector>
#include <cstring>
#include <map>

namespace chfs {

/** 
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command>
class RaftLog {
public:
    RaftLog(std::shared_ptr<BlockManager> bm);
    ~RaftLog();

    /* Lab3: Your code here */

    int size();

    void append_log(int term, Command entry);

    void discard_log(int start_index);
    // index, term
    std::pair<int, int> get_log_stat(int index);
    // term, log
    std::pair<int, Command> get_log(int index);

    int get_term();

    int get_voted();

    void set_term(int term);

    void set_voted(int voted_for);

    void save_snapshot(std::vector<u8> data, int last_index, int last_term);
    // index, term
    std::pair<int, int> get_snapshot_stat();

    std::vector<u8> get_snapshot();

private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    /* Lab3: Your code here */

    struct StorageState {
        int current_term;
        int voted_for;
        int log_size;
        // snapshot
        int last_included_index;
        int last_included_term;
        usize snapshot_size;
        // block_id_t snapshot_block_id;
    };

    struct EntryStorage {
        usize entry_size;
        int term;
    };

    const block_id_t STATE_BLOCK = 1;
    const block_id_t SNAPSHOT_START_BLOCK = 2;

    StorageState storage_state;
    block_id_t cur_block_id;
    usize cur_offset;
    std::map<int, std::tuple<int, Command, block_id_t, usize>> log_map; // index -> term, command, block id, offset
    std::vector<u8> snapshot;

    inline void disk_append_log(int index, int term, Command entry);
    inline void flush_state();
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) : bm_(bm)
{
    /* Lab3: Your code here */
    log_map.clear();
    std::vector<u8> buf(bm_->block_size());
    auto state_p = reinterpret_cast<StorageState *>(buf.data());
    bm_->read_block(STATE_BLOCK, buf.data());
    storage_state = *state_p;
    cur_block_id = SNAPSHOT_START_BLOCK;
    snapshot.clear();
    if (storage_state.last_included_index > 0) {    // read snapshot
        usize sz = storage_state.snapshot_size;
        std::vector<u8> buf(bm_->block_size());
        // auto block_num = sz % bm_->block_size() == 0 ? sz / bm_->block_size() + 1 : sz / bm_->block_size();
        auto read_sz = 0;
        while (read_sz < sz) {
            auto remain_sz = sz - read_sz;
            auto len = remain_sz > bm_->block_size() ? bm_->block_size() : remain_sz;
            if (len == bm_->block_size()) {
                bm_->read_block(cur_block_id, buf.data());
                snapshot.insert(snapshot.end(), buf.begin(), buf.end());
            } else {
                bm_->read_block(cur_block_id, buf.data());
                snapshot.insert(snapshot.end(), buf.begin(), buf.begin() + len);
            }
            ++cur_block_id;
            read_sz += len;
        }
        if (storage_state.snapshot_size != snapshot.size())
            std::cerr << "Unexpected behavior: read snapshot size is not equal with stored snapshot size.\n";
    }
    cur_offset = 0;
    bm_->read_block(cur_block_id, buf.data());
    std::vector<u8> cmd_data;
    for (int idx = storage_state.last_included_index + 1; idx <= storage_state.log_size; ++idx) {
        EntryStorage *entry_p = reinterpret_cast<EntryStorage *>(buf.data() + cur_offset);
        usize cmd_size = entry_p->entry_size - sizeof(EntryStorage);
        cmd_data.clear();
        cmd_data.insert(cmd_data.end(), buf.begin() + cur_offset + sizeof(EntryStorage),
            buf.begin() + cur_offset + entry_p->entry_size);
        Command cmd;
        cmd.deserialize(cmd_data, cmd_size);
        log_map[idx] = {entry_p->term, cmd, cur_block_id, cur_offset};
        cur_offset += entry_p->entry_size;
        if (cur_offset >= bm_->block_size()) {
            ++cur_block_id;
            cur_offset = 0;
            bm_->read_block(cur_block_id, buf.data());
        }
    }
}

template <typename Command>
RaftLog<Command>::~RaftLog()
{
    /* Lab3: Your code here */
    bm_->flush();
}

/* Lab3: Your code here */

template <typename Command>
int RaftLog<Command>::size() {
    std::unique_lock<std::mutex> lock(mtx);
    // if (log_map.size() == 0)
    //     return 0;
    // return log_map.crbegin()->first;
    return storage_state.log_size;
}

template <typename Command>
void RaftLog<Command>::append_log(int term, Command entry) {
    std::unique_lock<std::mutex> lock(mtx);
    int before_sz = storage_state.log_size;
    disk_append_log(before_sz + 1, term, entry);
    ++storage_state.log_size;
    flush_state();
}

template <typename Command>
void RaftLog<Command>::discard_log(int start_index) {
    std::unique_lock<std::mutex> lock(mtx);
    auto it = log_map.find(start_index);
    if (it != log_map.end()) {
        cur_block_id = std::get<2>(it->second);
        cur_offset = std::get<3>(it->second);
        while (it != log_map.end()) {
            log_map.erase(it++);
        }
    }
    storage_state.log_size = start_index - 1;
    flush_state();
}

template <typename Command>
std::pair<int, int> RaftLog<Command>::get_log_stat(int index) {
    std::unique_lock<std::mutex> lock(mtx);
    if (index == 0)
        return {0, 0};
    if (index == storage_state.last_included_index)
        return {storage_state.last_included_index, storage_state.last_included_term};
    if (log_map.find(index) != log_map.end())
        return {index, std::get<0>(log_map[index])};
    else
        return {-1, -1};
}

template <typename Command>
std::pair<int, Command> RaftLog<Command>::get_log(int index) {
    std::unique_lock<std::mutex> lock(mtx);
    if (index == 0)
        return {0, Command()};
    if (log_map.find(index) != log_map.end()) {
        auto res = log_map[index];
        return {std::get<0>(res), std::get<1>(res)};
    }
    else
        return {-1, Command()};
}

template <typename Command>
inline void RaftLog<Command>::disk_append_log(int index, int term, Command entry) {
    usize entry_sz = sizeof(EntryStorage) + entry.size();
    if (cur_offset + entry_sz > bm_->block_size()) {
        ++cur_block_id;
        bm_->true_zero_block(cur_block_id);
        cur_offset = 0;
    }
    log_map[index] = {term, entry, cur_block_id, cur_offset};
    std::vector<u8> buf(sizeof(EntryStorage));
    EntryStorage * bufp = reinterpret_cast<EntryStorage *>(buf.data());
    bufp->entry_size = entry_sz;
    bufp->term = term;
    auto entry_data = entry.serialize(entry.size());
    buf.insert(buf.end(), entry_data.begin(), entry_data.end());
    bm_->true_write_partial_block(cur_block_id, buf.data(), cur_offset, buf.size(), false);
    cur_offset += entry_sz;
}

template <typename Command>
inline void RaftLog<Command>::flush_state() {
    std::vector<u8> buf(bm_->block_size());
    auto state_p = reinterpret_cast<StorageState *>(buf.data());
    *state_p = storage_state;
    bm_->true_write_block(STATE_BLOCK, buf.data(), false);
}

template <typename Command>
int RaftLog<Command>::get_term() {
    std::unique_lock<std::mutex> lock(mtx);
    return storage_state.current_term;
}

template <typename Command>
int RaftLog<Command>::get_voted() {
    std::unique_lock<std::mutex> lock(mtx);
    return storage_state.voted_for;
}

template <typename Command>
void RaftLog<Command>::set_term(int term) {
    std::unique_lock<std::mutex> lock(mtx);
    storage_state.current_term = term;
    flush_state();
}

template <typename Command>
void RaftLog<Command>::set_voted(int voted_for) {
    std::unique_lock<std::mutex> lock(mtx);
    storage_state.voted_for = voted_for;
    flush_state();
}

template <typename Command>
void RaftLog<Command>::save_snapshot(std::vector<u8> data, int last_index, int last_term) {
    if (last_index <= 0)
        return;
    std::unique_lock<std::mutex> lock(mtx);
    snapshot = data;
    usize sz = data.size();
    // auto block_num = sz % bm_->block_size() == 0 ? sz / bm_->block_size() + 1 : sz / bm_->block_size();
    block_id_t sn_block_id = SNAPSHOT_START_BLOCK;
    auto write_sz = 0;
    while (write_sz < sz) {
        auto remain_sz = sz - write_sz;
        auto len = remain_sz > bm_->block_size() ? bm_->block_size() : remain_sz;
        if (len == bm_->block_size()) {
            bm_->true_write_block(sn_block_id, data.data() + write_sz, false);
        } else {
            bm_->true_zero_block(sn_block_id);
            bm_->true_write_partial_block(sn_block_id, data.data() + write_sz, 0, len, false);
        }
        ++sn_block_id;
        write_sz += len;
    }
    storage_state.snapshot_size = sz;
    storage_state.last_included_index = last_index;
    storage_state.last_included_term = last_term;
    storage_state.log_size = std::max(last_index, storage_state.log_size);
    flush_state();
    for (auto it = log_map.begin(); it != log_map.end() && it->first <= last_index;) {
        log_map.erase(it++);
    }
    cur_block_id = sn_block_id;
    cur_offset = 0;
    for (auto it = log_map.find(last_index + 1); it != log_map.end(); ++it) {
        disk_append_log(it->first, std::get<0>(it->second), std::get<1>(it->second));
    }
}

template <typename Command>
std::pair<int, int> RaftLog<Command>::get_snapshot_stat() {
    std::unique_lock<std::mutex> lock(mtx);
    return {storage_state.last_included_index, storage_state.last_included_term};
}

template <typename Command>
std::vector<u8> RaftLog<Command>::get_snapshot() {
    std::unique_lock<std::mutex> lock(mtx);
    return snapshot;
}

} /* namespace chfs */
