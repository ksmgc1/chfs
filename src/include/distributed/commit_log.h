//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// commit_log.h
//
// Identification: src/include/distributed/commit_log.h
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "block/manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "filesystem/operations.h"
#include <atomic>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace chfs {
/**
 * `BlockOperation` is an entry indicates an old block state and
 * a new block state. It's used to redo the operation when
 * the system is crashed.
 */
class BlockOperation {
public:
  explicit BlockOperation(block_id_t block_id, std::vector<u8> new_block_state)
      : block_id_(block_id), new_block_state_(new_block_state) {
    CHFS_ASSERT(new_block_state.size() == DiskBlockSize, "invalid block state");
  }

  block_id_t block_id_;
  std::vector<u8> new_block_state_;
};

/**
 * `CommitLog` is a class that records the block edits into the
 * commit log. It's used to redo the operation when the system
 * is crashed.
 */
class CommitLog {
public:
  explicit CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled);
  ~CommitLog();
  auto append_log(txn_id_t txn_id,
                  std::vector<std::shared_ptr<BlockOperation>> ops) -> void;
  auto commit_log(txn_id_t txn_id) -> void;
  auto checkpoint() -> void;
  auto recover() -> void;
  auto get_log_entry_num() -> usize;

  bool is_checkpoint_enabled_;
  std::shared_ptr<BlockManager> bm_;
  /**
   * {Append anything if you need}
   */
  struct LogEntry {
    txn_id_t txn_id;
    block_id_t block_id;
    block_id_t storage_block_id;
  };

  struct EntryBlock {
    block_id_t next_entry_block_id;
    LogEntry entry[0];
  };

  block_id_t log_start_block_ = KInvalidBlockID;
  static const block_id_t log_block_num_ = 1024;
  static const usize log_entries_per_block = (DiskBlockSize - sizeof(EntryBlock)) / sizeof(LogEntry);

private:
  block_id_t log_block_written_;
  block_id_t cur_log_block_id_;
  usize cur_log_offset_;
  bool log_block_allocated_map_[log_block_num_];

  auto allocate_log_block() -> ChfsResult<block_id_t> {
    for (auto i = 0; i < log_block_num_; ++i)
      if (log_block_allocated_map_[i] == false) {
        log_block_allocated_map_[i] = true;
        return log_start_block_ + i;
      }
    return ErrorType::OUT_OF_RESOURCE;
  }
};

} // namespace chfs