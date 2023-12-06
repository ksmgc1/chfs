#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
    /* Lab3: Your code here */
    
    int term;
    int candidate_id;
    int last_log_index;
    int last_log_term;
    MSGPACK_DEFINE(
        term,
        candidate_id,
        last_log_index,
        last_log_term
    )
};

struct RequestVoteReply {
    /* Lab3: Your code here */

    int term;
    bool vote_granted;
    MSGPACK_DEFINE(
        term,
        vote_granted
    )
};

template <typename Command>
struct AppendEntriesArgs {
    /* Lab3: Your code here */

    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    std::vector<std::pair<int, Command>> entries;
    int leader_commit;
};

struct RpcAppendEntriesArgs {
    /* Lab3: Your code here */

    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    int log_size;
    std::vector<int> entries_terms;
    std::vector<std::vector<u8>> entries_logs;
    int leader_commit;
    MSGPACK_DEFINE(
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        log_size,
        entries_terms,
        entries_logs,
        leader_commit
    )
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    /* Lab3: Your code here */

    RpcAppendEntriesArgs res;
    res.term = arg.term;
    res.leader_id = arg.leader_id;
    res.prev_log_index = arg.prev_log_index;
    res.prev_log_term = arg.prev_log_term;
    res.log_size = arg.entries.size();
    std::vector<int> entries_terms;
    std::vector<std::vector<u8>> entries_logs;
    for (auto &i: arg.entries) {
        entries_terms.emplace_back(i.first);
        entries_logs.emplace_back(i.second.serialize(i.second.size()));
    }
    res.entries_logs = std::move(entries_logs);
    res.entries_terms = std::move(entries_terms);
    res.leader_commit = arg.leader_commit;
    return res;
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    /* Lab3: Your code here */

    AppendEntriesArgs<Command> res;
    res.term = rpc_arg.term;
    res.leader_id = rpc_arg.leader_id;
    res.prev_log_index = rpc_arg.prev_log_index;
    res.prev_log_term = rpc_arg.prev_log_term;
    std::vector<std::pair<int, Command>> entries;
    for (auto i = 0; i < rpc_arg.log_size; ++i) {
        Command command;
        command.deserialize(rpc_arg.entries_logs[i], rpc_arg.entries_logs[i].size());
        entries.emplace_back(rpc_arg.entries_terms[i], command);
    }
    res.entries = std::move(entries);
    res.leader_commit = rpc_arg.leader_commit;
    return res;
}

struct AppendEntriesReply {
    /* Lab3: Your code here */

    int term;
    bool success;
    MSGPACK_DEFINE(
        term,
        success
    )
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */

    int term;
    int leader_id;
    int last_included_index;
    int last_included_term;
    std::vector<u8> data;
    MSGPACK_DEFINE(
        term,
        leader_id,
        last_included_index,
        last_included_term,
        data
    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */

    int term;
    MSGPACK_DEFINE(
        term
    )
};

} /* namespace chfs */