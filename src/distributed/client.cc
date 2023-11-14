#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  
  auto res = metadata_server_->call("mknode", static_cast<u8>(type), parent, name);
  if (res.is_err())
    return ChfsResult<inode_id_t>(res.unwrap_error());

  auto id = res.unwrap()->as<inode_id_t>();
  if (id == 0)
    return ErrorType::AlreadyExist;

  return ChfsResult<inode_id_t>(id);
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  // TODO: Implement this function.

  auto look_res = lookup(parent, name);
  if (look_res.is_err())
    return look_res.unwrap_error();
  
  auto res = metadata_server_->call("unlink", parent, name);
  if (res.is_err())
    return ChfsNullResult(res.unwrap_error());

  auto succ = res.unwrap()->as<bool>();
  if (!succ)
    return ErrorType::NotEmpty;

  return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  
  auto res = metadata_server_->call("lookup", parent, name);
  if (res.is_err())
    return ChfsResult<inode_id_t>(res.unwrap_error());

  auto id = res.unwrap()->as<inode_id_t>();
  if (id == 0)
    return ErrorType::NotExist;

  return ChfsResult<inode_id_t>(id);
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  // TODO: Implement this function.
  
  auto res = metadata_server_->call("readdir", id);
  if (res.is_err())
    return res.unwrap_error();

  return res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  // TODO: Implement this function.
  
  auto res = metadata_server_->call("get_type_attr", id);
  if (res.is_err())
    return ChfsResult<std::pair<InodeType, FileAttr>>(res.unwrap_error());

  auto tup = res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  if (std::get<4>(tup) == 0)
    return ErrorType::NotExist;

  FileAttr attr;
  attr.size = std::get<0>(tup);
  attr.atime = std::get<1>(tup);
  attr.mtime = std::get<2>(tup);
  attr.ctime = std::get<3>(tup);

  return ChfsResult<std::pair<InodeType, FileAttr>>({std::get<4>(tup) == static_cast<u8>(FileType::REGULAR) ? InodeType::FILE : InodeType::Directory, attr});
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  // TODO: Implement this function.
  
  auto blk_map_res = metadata_server_->call("get_block_map", id);
  if (blk_map_res.is_err())
    return ChfsResult<std::vector<u8>>(blk_map_res.unwrap_error());
  auto blk_map = blk_map_res.unwrap()->as<std::vector<BlockInfo>>();

  auto start_blk = offset / DiskBlockSize;
  auto end_blk = (offset + size - 1) / DiskBlockSize;
  if (end_blk >= blk_map.size())
    return ChfsResult<std::vector<u8>>(ErrorType::INVALID_ARG);
  std::vector<u8> file(size);
  auto read_len = 0;
  for (auto i = start_blk; i <= end_blk; ++i) {
    auto len = DiskBlockSize;
    auto offs = 0;
    if (i == start_blk && i == end_blk) {
      len = size;
      offs = offset % DiskBlockSize;
    }
    else if (i == start_blk) {
      len = DiskBlockSize - (offset % DiskBlockSize);
      offs = offset % DiskBlockSize;
    }
    else if (i == end_blk)
      len = (size - (DiskBlockSize - (offset % DiskBlockSize))) % DiskBlockSize;
    
    auto read_res = data_servers_[std::get<1>(blk_map[i])]->async_call("read_data", std::get<0>(blk_map[i]), offs, len, std::get<2>(blk_map[i]));
    if (read_res.is_err())
      return ChfsResult<std::vector<u8>>(read_res.unwrap_error());
    auto res_buf = read_res.unwrap()->get().as<std::vector<u8>>();
    if (res_buf.size() < len)
      return ChfsResult<std::vector<u8>>(ErrorType::NotPermitted);

    std::memcpy(&(file.data()[read_len]), res_buf.data(), len);
    read_len += len;

  }

  return ChfsResult<std::vector<u8>>(file);
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  // TODO: Implement this function.
  
  auto size = data.size();
  auto blk_map_res = metadata_server_->call("get_block_map", id);
  if (blk_map_res.is_err())
    return ChfsNullResult(blk_map_res.unwrap_error());
  auto blk_map = blk_map_res.unwrap()->as<std::vector<BlockInfo>>();

  auto start_blk = offset / DiskBlockSize;
  auto end_blk = (offset + size - 1) / DiskBlockSize;
  if (end_blk >= blk_map.size()) 
    for (auto i = blk_map.size(); i <= end_blk; ++i) {
      auto alloc_res = metadata_server_->call("alloc_block", id);
      if (alloc_res.is_err())
        return ChfsNullResult(alloc_res.unwrap_error());
      auto alloc_info = alloc_res.unwrap()->as<BlockInfo>();
      blk_map.emplace_back(alloc_info);
    }

  auto write_len = 0;
  for (auto i = start_blk; i <= end_blk; ++i) {
    auto len = DiskBlockSize;
    auto offs = 0;
    if (i == start_blk && i == end_blk) {
      len = size;
      offs = offset % DiskBlockSize;
    }
    else if (i == start_blk) {
      len = DiskBlockSize - (offset % DiskBlockSize);
      offs = offset % DiskBlockSize;
    }
    else if (i == end_blk)
      len = (size - (DiskBlockSize - (offset % DiskBlockSize))) % DiskBlockSize;
    
    std::vector<u8> write_data(data.begin() + write_len, data.begin() + write_len + len);
    write_len += len;

    auto write_res = data_servers_[std::get<1>(blk_map[i])]->async_call("write_data", std::get<0>(blk_map[i]), offs, write_data);
    if (write_res.is_err())
      return ChfsNullResult(write_res.unwrap_error());
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  // TODO: Implement this function.
  
  auto res = metadata_server_->call("free_block", id, block_id, mac_id);
  if (res.is_err())
    return ChfsNullResult(res.unwrap_error());
  
  if (!res.unwrap()->as<bool>())
    return ChfsNullResult(ErrorType::INVALID_ARG);

  return KNullOk;
}

} // namespace chfs