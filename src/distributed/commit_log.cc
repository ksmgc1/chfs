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
  log_start_block_ = KDefaultBlockCnt - log_block_num_;
  cur_log_block_id_ = log_start_block_;
  cur_log_offset_ = 0;
  for (auto i = 0; i < log_block_num_; ++i)
    log_block_allocated_map_[i] = false;
  log_block_allocated_map_[0] = true;
  bm_->true_zero_block(log_start_block_);
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
  
  bm_->set_log_enabled(false);
  auto log_size = ops.size();
  if (log_size == 0) return;
  std::vector<u8> entry_block(bm_->block_size());
  auto entry_block_p = reinterpret_cast<EntryBlock *>(entry_block.data());
  std::vector<block_id_t> sync_list;

  bm_->read_block(cur_log_block_id_, entry_block.data());
  sync_list.push_back(cur_log_block_id_);
  for (auto i = 0; i < log_size; ++i) {
    auto storage_block_id = allocate_log_block().unwrap();
    bm_->true_write_block(storage_block_id, ops[i]->new_block_state_.data(), false);
    sync_list.push_back(storage_block_id);
    entry_block_p->entry[cur_log_offset_] = {txn_id, ops[i]->block_id_, storage_block_id};
    ++entry_block_p->entry_num;
    ++cur_log_offset_;
    if (cur_log_offset_ == log_entries_per_block) {
      auto next_entry_block_id = allocate_log_block().unwrap();
      entry_block_p->next_entry_block_id = next_entry_block_id;
      bm_->true_write_block(cur_log_block_id_, entry_block.data(), false);
      bm_->true_zero_block(next_entry_block_id);
      bm_->read_block(next_entry_block_id, entry_block.data());
      ++log_block_written_;
      cur_log_block_id_ = next_entry_block_id;
      cur_log_offset_ = 0;
      sync_list.push_back(next_entry_block_id);
    }
    if (is_checkpoint_enabled_ && get_log_entry_num() == kMaxLogSize) {
      bm_->true_write_block(cur_log_block_id_, entry_block.data(), false);
      checkpoint();
      bm_->read_block(cur_log_block_id_, entry_block.data());
    }
  }

  bm_->true_write_block(cur_log_block_id_, entry_block.data(), false);
  for (auto &i : sync_list)
    bm_->sync(i);

}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  // TODO: Implement this function.
  
  bm_->set_log_enabled(false);
  std::vector<u8> entry_block(bm_->block_size());
  auto entry_block_p = reinterpret_cast<EntryBlock *>(entry_block.data());
  bm_->read_block(cur_log_block_id_, entry_block.data());
  entry_block_p->entry[cur_log_offset_] = {txn_id, 0, 0};
  ++entry_block_p->entry_num;
  ++cur_log_offset_;
  if (cur_log_offset_ == log_entries_per_block) {
    auto next_entry_block_id = allocate_log_block().unwrap();
    entry_block_p->next_entry_block_id = next_entry_block_id;
    bm_->true_write_block(cur_log_block_id_, entry_block.data(), false);
    bm_->sync(cur_log_block_id_);
    bm_->true_zero_block(next_entry_block_id);
    ++log_block_written_;
    cur_log_block_id_ = next_entry_block_id;
    cur_log_offset_ = 0;
  } else {
    bm_->true_write_block(cur_log_block_id_, entry_block.data(), false);
    bm_->sync(cur_log_block_id_);
  }

}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  // TODO: Implement this function.

  bm_->flush();
  
  auto checking_block_id = log_start_block_;
  std::map<txn_id_t, std::vector<LogEntry>, std::less<txn_id_t>> txn_ops;
  std::vector<u8> entry_block(bm_->block_size());
  auto entry_block_p = reinterpret_cast<EntryBlock *>(entry_block.data());
  std::vector<LogEntry> not_finished;
  std::vector<block_id_t> zero_set;

  do {
    bm_->read_block(checking_block_id, entry_block.data());
    for (auto i = 0; i < entry_block_p->entry_num; ++i) {
      auto it = txn_ops.find(entry_block_p->entry[i].txn_id);
      if (it != txn_ops.end())
        it->second.emplace_back(entry_block_p->entry[i]);
      else {
        std::vector<LogEntry> v;
        v.emplace_back(entry_block_p->entry[i]);
        txn_ops.insert({entry_block_p->entry[i].txn_id, v});
      }
    }
    checking_block_id = entry_block_p->next_entry_block_id;
  } while (checking_block_id != KInvalidBlockID);

  for (auto i = txn_ops.begin(); i != txn_ops.end(); ++i) {
    auto ops = i->second;
    if (ops.back().block_id != 0 || ops.back().storage_block_id != 0) {  // uncommited transaction, remain
      not_finished.insert(not_finished.end(), ops.begin(), ops.end());
      continue;
    }
    for (auto j = ops.begin(); j != ops.end(); ++j) {
      if (j->block_id == 0 || j->storage_block_id == 0)
        continue;
      zero_set.emplace_back(j->storage_block_id);
    }
  }

  for (auto &i : zero_set) {
    bm_->true_zero_block(i);
    deallocate_log_block(i);
  }

  log_block_written_ = 0;
  cur_log_block_id_ = log_start_block_;
  cur_log_offset_ = 0;
  std::vector<u8> new_log_block(bm_->block_size());
  auto new_block_p = reinterpret_cast<EntryBlock *>(new_log_block.data());
  new_block_p->entry_num = 0;
  new_block_p->next_entry_block_id =0;
  for (auto i = 0; i < not_finished.size(); ++i) {
    new_block_p->entry[cur_log_offset_] = not_finished[i];
    ++new_block_p->entry_num;
    ++cur_log_offset_;
    if (cur_log_offset_ == log_entries_per_block) {
      auto next_entry_block_id = allocate_log_block().unwrap();
      new_block_p->next_entry_block_id = next_entry_block_id;
      bm_->true_write_block(cur_log_block_id_, new_log_block.data(), false);
      bm_->sync(cur_log_block_id_);
      bm_->true_zero_block(next_entry_block_id);
      bm_->read_block(next_entry_block_id, new_log_block.data());
      ++log_block_written_;
      cur_log_block_id_ = next_entry_block_id;
      cur_log_offset_ = 0;
    }
  }

  bm_->true_write_block(cur_log_block_id_, new_log_block.data(), false);
  bm_->sync(cur_log_block_id_);

}

// {Your code here}
auto CommitLog::recover() -> void {
  // TODO: Implement this function.
  
  bm_->clear_ops();
  bm_->set_log_enabled(false);

  auto recover_block_id = log_start_block_;
  std::map<txn_id_t, std::vector<LogEntry>, std::less<txn_id_t>> txn_ops;
  std::vector<u8> entry_block(bm_->block_size());
  auto entry_block_p = reinterpret_cast<EntryBlock *>(entry_block.data());
  std::vector<block_id_t> zero_set;

  do {
    zero_set.emplace_back(recover_block_id);
    bm_->read_block(recover_block_id, entry_block.data());
    for (auto i = 0; i < entry_block_p->entry_num; ++i) {
      auto it = txn_ops.find(entry_block_p->entry[i].txn_id);
      if (it != txn_ops.end())
        it->second.emplace_back(entry_block_p->entry[i]);
      else {
        std::vector<LogEntry> v;
        v.emplace_back(entry_block_p->entry[i]);
        txn_ops.insert({entry_block_p->entry[i].txn_id, v});
      }
      if (entry_block_p->entry[i].storage_block_id != 0)
        zero_set.emplace_back(entry_block_p->entry[i].storage_block_id);
    }
    recover_block_id = entry_block_p->next_entry_block_id;
  } while (recover_block_id != KInvalidBlockID);

  std::vector<u8> buffer(bm_->block_size());

  for (auto i = txn_ops.begin(); i != txn_ops.end(); ++i) {
    auto ops = i->second;
    if (ops.back().block_id != 0 || ops.back().storage_block_id != 0)  // uncommited transaction, discard
      continue;
    for (auto j = ops.begin(); j != ops.end(); ++j) {
      if (j->block_id == 0 || j->storage_block_id == 0)
        break;
      bm_->read_block(j->storage_block_id, buffer.data());
      bm_->true_write_block(j->block_id, buffer.data(), false);
    }
  }

  auto res = bm_->flush();
  if (res.is_err()) {
    std::cerr << "Flush page cache failed when recovering.";
    exit(1);
  }

  for (auto &i : zero_set) {
    bm_->true_zero_block(i);
    deallocate_log_block(i);
  }
  cur_log_block_id_ = KDefaultBlockCnt - log_block_num_;
  cur_log_offset_ = 0;

}
}; // namespace chfs