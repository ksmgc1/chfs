#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, 0, false);
  } else {
    // We need to reserve some blocks for storing the version of each block

    auto blk_sz = bm->block_size();
    auto versions_per_blk = blk_sz / sizeof(version_t);
    auto total_blks = bm->total_blocks();
    auto version_block_num = total_blks % versions_per_blk ? total_blks / versions_per_blk + 1 : total_blks / versions_per_blk;
    for (auto i = 0; i < version_block_num; ++i)
      bm->zero_block(i);

    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, version_block_num, true));

    // init mutex
    block_mutex_.clear();

  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// added a helper function to get/increase the version of a block
inline auto get_version(std::shared_ptr<BlockManager> bm, block_id_t block_id) -> version_t {
  auto blk_sz = bm->block_size();
  auto versions_per_blk = blk_sz / sizeof(version_t);
  auto version_blk_id = block_id / versions_per_blk;
  auto version_blk_offs = block_id % versions_per_blk;
  std::vector<u8> version_blk(bm->block_size());
  bm->read_block(version_blk_id, version_blk.data());
  auto version_p = reinterpret_cast<version_t *>(version_blk.data());
  return version_p[version_blk_offs];
}

inline auto increase_version(std::shared_ptr<BlockManager> bm, block_id_t block_id) -> version_t {
  auto blk_sz = bm->block_size();
  auto versions_per_blk = blk_sz / sizeof(version_t);
  auto version_blk_id = block_id / versions_per_blk;
  auto version_blk_offs = block_id % versions_per_blk;
  std::vector<u8> version_blk(bm->block_size());
  bm->read_block(version_blk_id, version_blk.data());
  auto version_p = reinterpret_cast<version_t *>(version_blk.data());
  version_p[version_blk_offs] = version_p[version_blk_offs] + 1;
  bm->write_block(version_blk_id, version_blk.data());
  return version_p[version_blk_offs];
}

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  // TODO: Implement this function.

  // acuqire read lock
  std::shared_lock<std::shared_mutex> lock(*block_mutex_[block_id]);

  auto act_version = get_version(block_allocator_->bm, block_id);
  if (act_version != version)
    return {};

  const auto block_sz = block_allocator_->bm->block_size();
  if (offset + len > block_sz)
    return {};
  std::vector<u8> buffer(block_sz);
  block_allocator_->bm->read_block(block_id, buffer.data());

  return {buffer.begin() + offset, buffer.begin() + offset + len};
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  // TODO: Implement this function.

  // acquire write lock
  std::unique_lock<std::shared_mutex> lock(*block_mutex_[block_id]);

  auto res = block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, buffer.size());  

  if (res.is_ok())
    return true;
  return false;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.
  
  // acquire allocator mutex
  std::lock_guard<std::mutex> lock(allocator_mutex_);

  auto res = block_allocator_->allocate();
  if (res.is_ok()) {
    auto version = increase_version(block_allocator_->bm, res.unwrap());
    // insert mutex
    block_mutex_.emplace(res.unwrap(), std::make_shared<std::shared_mutex>());
    return {res.unwrap(), version};
  }
  
  return {};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // TODO: Implement this function.
  
  auto res = block_allocator_->deallocate(block_id);
  if (res.is_ok()) {
    increase_version(block_allocator_->bm, block_id);
    // remove mutex
    block_mutex_.erase(block_id);
    return true;
  } 

  return false;
}
} // namespace chfs