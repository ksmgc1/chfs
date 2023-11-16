#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs {
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm) {
  log_block_written_ = 0;
  cur_log_block_id_ = 0;
  cur_log_offset_ = 0;
  for (auto i = 0; i < log_block_num_; ++i)
    log_block_allocated_map_[i] = false;
}

CommitLog::~CommitLog() {}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize {
  // TODO: Implement this function.
  
  return log_block_written_ * log_entries_per_block + cur_log_offset_;
}

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id,
                           std::vector<std::shared_ptr<BlockOperation>> ops)
    -> void {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
}

// {Your code here}
auto CommitLog::recover() -> void {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
}
}; // namespace chfs