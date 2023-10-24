#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // TODO: Implement this function.
  //       Append the new directory entry to `src`.

  if (!src.empty() && *(src.end() - 1) != '/')
    src.append("/");
  src += filename + ':' + inode_id_to_string(id);
  
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  // TODO: Implement this function.

  std::list<DirectoryEntry> res;
  auto p2 = 0;
  if (!src.empty() && *(src.end() - 1) != '/')
    src.append("/");
  auto size = src.size();
  for (auto p1 = 0; p1 < size; ++p1) {
    p2 = src.find('/', p1);
    if (p2 < size) {
      auto entry_str = src.substr(p1, p2 - p1);
      auto p = entry_str.find(':');
      DirectoryEntry entry;
      entry.name = entry_str.substr(0, p);
      auto id_str = entry_str.substr(p + 1);
      entry.id = string_to_inode_id(id_str);
      list.push_back(entry);
      p1 = p2;
    } else break;
  }
}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // TODO: Implement this function.
  //       Remove the directory entry from `src`.
  
  std::list<DirectoryEntry> list;
  parse_directory(src, list);
  for (auto &i : list) {
    if (i.name == filename)
      continue;
    res = append_to_directory(res, i.name, i.id);
  }

  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  // TODO: Implement this function.
  
  auto res = fs->read_file(id);
  if (res.is_err())
    return ChfsNullResult(res.unwrap_error());
  auto res_vec = res.unwrap();
  auto src = std::string(res_vec.begin(), res_vec.end());
  parse_directory(src, list);

  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  
  read_directory(this, id, list);
  for (auto &i : list)
    if (i.name.compare(name) == 0)
      return ChfsResult<inode_id_t>(i.id);

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.

  std::list<DirectoryEntry> list;
  read_directory(this, id, list);
  for (auto &i : list)
    if (i.name.compare(name) == 0)
      return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
  auto allo_inode_res = alloc_inode(type);
  if (allo_inode_res.is_err())
    return ChfsResult<inode_id_t>(allo_inode_res.unwrap_error());

  auto read_res = read_file(id);
  if (read_res.is_err())
    return ChfsResult<inode_id_t>(read_res.unwrap_error());
  auto buffer = read_res.unwrap();
  auto src = std::string(buffer.begin(), buffer.end());
  src = append_to_directory(src, std::string(name), allo_inode_res.unwrap());
  buffer = std::vector<u8>(src.begin(), src.end());
  write_file(id, buffer);

  return allo_inode_res;
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // TODO: 
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  
  std::list<DirectoryEntry> list;
  read_directory(this, parent, list);
  inode_id_t id = KInvalidInodeID;
  for (auto &i : list)
    if (i.name.compare(name) == 0)
      id = i.id;
  if (id == KInvalidInodeID)
    return ChfsNullResult(ErrorType::NotExist);
  
  //change parent time
  std::vector<u8> inode(block_manager_->block_size());
  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto ino_res = inode_manager_->read_inode(parent, inode);
  if (ino_res.is_err())
    return ChfsNullResult(ino_res.unwrap_error());
  inode_p->inner_attr.set_all_time(time(0));
  block_manager_->write_block(ino_res.unwrap(), inode.data());

  auto rm_res = remove_file(id);
  if (rm_res.is_err())
    return ChfsNullResult(rm_res.unwrap_error());
  
  auto read_res = read_file(parent);
  if (read_res.is_err())
    return ChfsNullResult(read_res.unwrap_error());
  auto buffer = read_res.unwrap();
  auto src = std::string(buffer.begin(), buffer.end());
  src = rm_from_directory(src, std::string(name));
  buffer = std::vector<u8>(src.begin(), src.end());
  write_file(parent, buffer);
  
  return KNullOk;
}

} // namespace chfs
