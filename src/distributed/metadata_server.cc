#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
    cur_txn_id = 0;
    // // allocate blocks for log
    // block_id_t first_log_block;
    // block_id_t last_log_block;
    // for (auto i = 0; i < commit_log->log_block_num_; ++i) {
    //   auto log_block_res = operation_->block_allocator_->allocate();
    //   if (log_block_res.is_err()) {
    //     std::cerr << "Cannot allocate block for log." << std::endl;
    //     exit(1);
    //   }
    //   if (i == 0)
    //     first_log_block = log_block_res.unwrap(); 
    //   else if (i == commit_log->log_block_num_ - 1)
    //     last_log_block = log_block_res.unwrap();
    // }
    // std::cout << "first log block: " << first_log_block << std::endl;
    // CHFS_ASSERT(last_log_block - first_log_block == commit_log->log_block_num_ - 1, "Blocks for log are not continuous.");

    // commit_log->log_start_block_ = first_log_block;
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);

  // init mutex
  auto total_inode_num = operation_->inode_manager_->get_max_inode_supported();
  inode_mutex_ = std::make_unique<std::vector<std::shared_mutex>>(total_inode_num + 1);

  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);

  // init mutex
  auto total_inode_num = operation_->inode_manager_->get_max_inode_supported();
  inode_mutex_ = std::make_unique<std::vector<std::shared_mutex>>(total_inode_num + 1);

  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.

  txn_id_t txn_id;
  if (is_log_enabled_) {
    bm_mutex_.lock();
    // std::cout << "mknode locked name = " << name << std::endl;
    txn_id = ++cur_txn_id;
    operation_->block_manager_->set_log_enabled(true);
  }

  if (inode_mutex_->size() <= parent)
    return 0;
  std::unique_lock<std::shared_mutex> p_lock((*inode_mutex_)[parent]);
  std::lock_guard<std::mutex> lock(allocator_mutex);

  ChfsResult<inode_id_t> res(0);
  if (type == DirectoryType) {
    res = operation_->mkdir(parent, name.c_str());
  } else if (type == RegularFileType) {
    res = operation_->mkfile(parent, name.c_str());
  }

  if (res.is_ok()) {
    if (is_log_enabled_) {
      auto ops = operation_->block_manager_->get_ops();
      std::vector<std::shared_ptr<BlockOperation>> block_ops;
      for (auto &i : ops)
        block_ops.emplace_back(std::make_shared<BlockOperation>(i.first, i.second));
      commit_log->append_log(txn_id, block_ops);
      commit_log->commit_log(txn_id);
      // do block operations after commit
      auto lgres = operation_->block_manager_->write_and_clear_ops();
      if (lgres.is_err())
        return 0;
      bm_mutex_.unlock();
      operation_->block_manager_->set_log_enabled(false);
    }
    // std::cout << res.unwrap() << std::endl;
    return res.unwrap();
  }
  // std::cout << "erorr: " << (int)res.unwrap_error() << std::endl;

  return 0;
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  // TODO: Implement this function.
  txn_id_t txn_id;
  if (is_log_enabled_) {
    bm_mutex_.lock();
    txn_id = ++cur_txn_id;
    operation_->block_manager_->set_log_enabled(true);
  }

  // acquire parent lock
  if (inode_mutex_->size() <= parent)
    return false;
  std::shared_lock<std::shared_mutex> p_lock((*inode_mutex_)[parent]);

  std::list<DirectoryEntry> list;
  read_directory(operation_.get(), parent, list);
  inode_id_t id = KInvalidInodeID;
  for (auto &i : list)
    if (i.name.compare(name) == 0)
      id = i.id;
  if (id == KInvalidInodeID)
    return false;
  
  std::vector<u8> inode(DiskBlockSize);
  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto ino_bid_res = operation_->inode_manager_->get(id);
  if (ino_bid_res.is_err())
    return false;
  auto ino_bid = ino_bid_res.unwrap();

  // acquire locks
  if (inode_mutex_->size() <= id)
    return false;
  std::shared_lock<std::shared_mutex> lock((*inode_mutex_)[id]);

  if (inode_p->get_type() == InodeType::FILE) {
    auto block_mac_ids = reinterpret_cast<std::tuple<block_id_t, mac_id_t, version_t> *>(inode_p->blocks);
    auto file_sz = inode_p->get_size();
    auto block_sz = DiskBlockSize;
    auto block_num = (file_sz % block_sz) ? (file_sz / block_sz + 1) : (file_sz / block_sz);
    for (auto i = 0; i < block_num; ++i) {
      auto entry = block_mac_ids[i];
      clients_[std::get<1>(entry)]->async_call("free_block", std::get<0>(entry));
    }
    std::unique_lock<std::mutex> allo_lock(allocator_mutex);
    auto deallo_res = operation_->block_allocator_->deallocate(ino_bid);
    if (deallo_res.is_err())
      return false;
    auto free_res = operation_->inode_manager_->free_inode(id);
    if (free_res.is_err())
      return false;
    allo_lock.unlock();
  } else {
    std::unique_lock<std::mutex> allo_lock(allocator_mutex);
    auto rm_res = operation_->remove_file(id);
    if (rm_res.is_err())
      return false;
    allo_lock.unlock();
  }

  lock.unlock();
  p_lock.unlock();
  std::unique_lock<std::shared_mutex> p_w_lock((*inode_mutex_)[parent]);

  //change parent time
  auto par_ino_bid_res = operation_->inode_manager_->get(parent);
  if (par_ino_bid_res.is_err())
    return false;
  auto par_ino_bid = par_ino_bid_res.unwrap();
  operation_->block_manager_->read_block(par_ino_bid, inode.data());
  inode_p->inner_attr.set_mtime(time(0));
  operation_->block_manager_->write_block(par_ino_bid, inode.data());

  auto read_res = operation_->read_file(parent);
    if (read_res.is_err())
      return false;
    auto buffer = read_res.unwrap();
    auto src = std::string(buffer.begin(), buffer.end());
    src = rm_from_directory(src, std::string(name));
    buffer = std::vector<u8>(src.begin(), src.end());
    operation_->write_file(parent, buffer);

  if (is_log_enabled_) {
    auto ops = operation_->block_manager_->get_ops();
    std::vector<std::shared_ptr<BlockOperation>> block_ops;
    for (auto &i : ops)
      block_ops.emplace_back(std::make_shared<BlockOperation>(i.first, i.second));
    commit_log->append_log(txn_id, block_ops);
    commit_log->commit_log(txn_id);
    // do block operations after commit
    auto lgres = operation_->block_manager_->write_and_clear_ops();
    if (lgres.is_err())
      return false;
    bm_mutex_.unlock();
    operation_->block_manager_->set_log_enabled(false);
  }
  
  return true;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.

  // acquire parent lock
  if (inode_mutex_->size() <= parent)
    return 0;
  std::shared_lock<std::shared_mutex> p_lock((*inode_mutex_)[parent]);

  auto res = operation_->lookup(parent, name.c_str());
  if (res.is_ok())
    return res.unwrap();

  return 0;
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  // TODO: Implement this function.

  // acquire lock
  if (inode_mutex_->size() <= id)
    return {};
  std::shared_lock<std::shared_mutex> p_lock((*inode_mutex_)[id]);

  auto bid_res = operation_->inode_manager_->get(id);
  if (bid_res.is_err())
    return {};
  auto inode_data = std::vector<u8>(DiskBlockSize);
  operation_->block_manager_->read_block(bid_res.unwrap(), inode_data.data());
  auto inode_p = reinterpret_cast<Inode *>(inode_data.data());
  auto file_sz = inode_p->get_size();
  auto block_sz = DiskBlockSize;
  auto block_num = (file_sz % block_sz) ? (file_sz / block_sz + 1) : (file_sz / block_sz);

  // is this SHIT?
  auto block_mac_ids = reinterpret_cast<std::tuple<block_id_t, mac_id_t, version_t> *>(inode_p->blocks);
  std::vector<BlockInfo> info;
  for (auto i = 0; i < block_num; ++i)
    info.emplace_back(std::get<0>(block_mac_ids[i]), std::get<1>(block_mac_ids[i]), std::get<2>(block_mac_ids[i]));

  return info;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // TODO: Implement this function.

  auto mac_id = generator.rand(1, num_data_servers);
  auto machine = clients_[mac_id];
  auto call_res = machine->call("alloc_block");
  if (call_res.is_err())
    return {};
  auto res = call_res.unwrap()->as<std::pair<block_id_t, version_t>>();

  // acquire lock
  if (inode_mutex_->size() <= id)
    return {};
  std::unique_lock<std::shared_mutex> lock((*inode_mutex_)[id]);

  std::vector<u8> inode(DiskBlockSize);
  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto ino_bid = operation_->inode_manager_->get(id);
  operation_->block_manager_->read_block(ino_bid.unwrap(), inode.data());
  auto block_mac_ids = reinterpret_cast<std::tuple<block_id_t, mac_id_t, version_t> *>(inode_p->blocks);
  auto file_sz = inode_p->get_size();
  auto block_sz = DiskBlockSize;
  auto block_num = (file_sz % block_sz) ? (file_sz / block_sz + 1) : (file_sz / block_sz);
  auto max_block_num = (block_sz - sizeof(Inode)) / sizeof(std::tuple<block_id_t, mac_id_t, version_t>);
  if (max_block_num < block_num + 1) {
    clients_[mac_id]->call("free_block", res.first);
    return {};
  }

  block_mac_ids[block_num] = {res.first, mac_id, res.second};
  inode_p->inner_attr.size += block_sz;
  
  operation_->block_manager_->write_block(ino_bid.unwrap(), inode.data());

  return {res.first, mac_id, res.second};
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  // TODO: Implement this function.
 
  if (machine_id <= 0 || machine_id > num_data_servers)
    return false;

  // acquire lock
  if (inode_mutex_->size() <= id)
    return false;
  std::unique_lock<std::shared_mutex> lock((*inode_mutex_)[id]);
  
  std::vector<u8> inode(DiskBlockSize);
  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto ino_bid = operation_->inode_manager_->get(id);
  operation_->block_manager_->read_block(ino_bid.unwrap(), inode.data());
  auto block_mac_ids = reinterpret_cast<std::tuple<block_id_t, mac_id_t, version_t> *>(inode_p->blocks);
  auto file_sz = inode_p->get_size();
  auto block_sz = DiskBlockSize;
  auto block_num = (file_sz % block_sz) ? (file_sz / block_sz + 1) : (file_sz / block_sz);
  std::tuple<block_id_t, mac_id_t, version_t> *ptr = nullptr;
  auto index = -1;
  for (auto i = 0; i < block_num; ++i)
    if (std::get<0>(block_mac_ids[i]) == block_id && std::get<1>(block_mac_ids[i]) == machine_id) {
      ptr = &block_mac_ids[i];
      index = i;
      break;
    }
  
  if (index == -1)
    return false;

  auto machine = clients_[machine_id];
  auto call_res = machine->call("free_block", block_id);
  if (call_res.is_err())
    return false;
  auto res = call_res.unwrap()->as<bool>();
  if (!res)
    return false;

  *ptr = {KInvalidBlockID, 0, 0};
  for (auto i = index; i < block_num - 1; ++i)
    block_mac_ids[i] = block_mac_ids[i + 1];
  block_mac_ids[block_num - 1] = {KInvalidBlockID, 0, 0};
  inode_p->inner_attr.size -= block_sz;
  operation_->block_manager_->write_block(ino_bid.unwrap(), inode.data());

  return true;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  // TODO: Implement this function.

  // acquire lock
  if (inode_mutex_->size() <= node)
    return {};
  std::shared_lock<std::shared_mutex> lock((*inode_mutex_)[node]);
  
  auto list = std::list<DirectoryEntry>();
  auto res = read_directory(operation_.get(), node, list);
  if (res.is_err())
    return {};
  auto vec = std::vector<std::pair<std::string, inode_id_t>>();
  for (auto &i : list)
    vec.emplace_back(i.name, i.id);

  return vec;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  // TODO: Implement this function.

  // acquire lock
  if (inode_mutex_->size() <= id)
    return {};
  std::shared_lock<std::shared_mutex> lock((*inode_mutex_)[id]);

  auto res = operation_->get_type_attr(id);
  if (res.is_ok()) {
    auto type_attr = res.unwrap();
    return {type_attr.second.size, type_attr.second.atime, type_attr.second.mtime, type_attr.second.ctime, type_attr.first == InodeType::FILE ? RegularFileType : DirectoryType};
  }

  return {};
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs