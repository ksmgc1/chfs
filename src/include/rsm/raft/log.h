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

    void set_term(int term);

    void set_voted(int voted_for);

private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    /* Lab3: Your code here */

    struct StorageState {
        int current_term;
        int voted_for;
        int log_size;
    };

    struct EntryStorage {
        usize entry_size;
        int term;
    };

    constexpr block_id_t STATE_BLOCK = 1;

    StorageState storage_state;
    block_id_t cur_block_id;
    usize cur_offset;
    std::map<int, std::pair<int, Command>> log_map; // index -> term, command

    inline void disk_append_log(int term, Command entry);
    inline void flush_state();
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) : bm_(bm)
{
    /* Lab3: Your code here */
    log_map.clear();
}

template <typename Command>
RaftLog<Command>::~RaftLog()
{
    /* Lab3: Your code here */
}

/* Lab3: Your code here */

template <typename Command>
int RaftLog<Command>::size() {
    std::unique_lock<std::mutex> lock(mtx);
    if (log_map.size() == 0)
        return 0;
    return log_map.crbegin()->first;
}

template <typename Command>
void RaftLog<Command>::append_log(int term, Command entry) {
    std::unique_lock<std::mutex> lock(mtx);
    int before_sz = size();
    disk_append_log(term, entry);
    log_map.insert_or_assign(before_sz + 1, std::make_pair(term, entry));
    ++storage_state.log_size;
}

template <typename Command>
void RaftLog<Command>::discard_log(int start_index) {
    std::unique_lock<std::mutex> lock(mtx);
    auto it = log_map.find(start_index);
    if (it != log_map.end())
        while (it != log_map.end()) {
            log_map.erase(it++);
        }
}

template <typename Command>
std::pair<int, int> RaftLog<Command>::get_log_stat(int index) {
    std::unique_lock<std::mutex> lock(mtx);
    if (index == 0)
        return {0, 0};
    if (log_map.find(index) != log_map.end())
        return {index, log_map[index].first};
    else
        return {-1, -1};
}

template <typename Command>
std::pair<int, Command> RaftLog<Command>::get_log(int index) {
    std::unique_lock<std::mutex> lock(mtx);
    if (index == 0)
        return {0, Command()};
    if (log_map.find(index) != log_map.end())
        return log_map[index];
    else
        return {-1, Command()};
}

template <typename Command>
inline void RaftLog<Command>::disk_append_log(int term, Command entry) {
    auto entry_sz = sizeof(cur_offset) + sizeof(term) + entry.size();
    if (cur_offset + entry_sz > bm_->block_size()) {
        ++cur_block_id;
        bm_->true_zero_block(cur_block_id);
        cur_offset = 0;
    }
    std::vector<u8> buf(sizeof(EntryStorage));
    auto bufp = reinterpret_cast<EntryStorage *>(buf.data());
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
void RaftLog<Command>::set_term(int term) {
    storage_state.current_term = term;
    flush_state();
}

template <typename Command>
void RaftLog<Command>::set_voted(int voted_for) {
    storage_state.voted_for = voted_for;
    flush_state();
}

} /* namespace chfs */
