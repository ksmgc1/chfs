#pragma once

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdarg.h>
#include <unistd.h>
#include <filesystem>
#include <random>

#include "rsm/state_machine.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "utils/thread_pool.h"
#include "librpc/server.h"
#include "librpc/client.h"
#include "block/manager.h"

namespace chfs {

enum class RaftRole {
    Follower,
    Candidate,
    Leader
};

struct RaftNodeConfig {
    int node_id;
    uint16_t port;
    std::string ip_address;
};

template <typename StateMachine, typename Command>
class RaftNode {

#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        char buf[512];                                                                                      \
        sprintf(buf,"[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf;} );                                         \
    } while (0);

public:
    RaftNode (int node_id, std::vector<RaftNodeConfig> node_configs);
    ~RaftNode();

    /* interfaces for test */
    void set_network(std::map<int, bool> &network_availablility);
    void set_reliable(bool flag);
    int get_list_state_log_num();
    int rpc_count();
    std::vector<u8> get_snapshot_direct();

private:
    /* 
     * Start the raft node.
     * Please make sure all of the rpc request handlers have been registered before this method.
     */
    auto start() -> int;

    /*
     * Stop the raft node.
     */
    auto stop() -> int;
    
    /* Returns whether this node is the leader, you should also return the current term. */
    auto is_leader() -> std::tuple<bool, int>;

    /* Checks whether the node is stopped */
    auto is_stopped() -> bool;

    /* 
     * Send a new command to the raft nodes.
     * The returned tuple of the method contains three values:
     * 1. bool:  True if this raft node is the leader that successfully appends the log,
     *      false If this node is not the leader.
     * 2. int: Current term.
     * 3. int: Log index.
     */
    auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

    /* Save a snapshot of the state machine and compact the log. */
    auto save_snapshot() -> bool;

    /* Get a snapshot of the state machine */
    auto get_snapshot() -> std::vector<u8>;


    /* Internal RPC handlers */
    auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
    auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
    auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

    /* RPC helpers */
    void send_request_vote(int target, RequestVoteArgs arg);
    void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

    void send_append_entries(int target, AppendEntriesArgs<Command> arg);
    void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

    void send_install_snapshot(int target, InstallSnapshotArgs arg);
    void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

    /* background workers */
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();


    /* Data structures */
    bool network_stat;          /* for test */

    std::mutex mtx;                             /* A big lock to protect the whole data structure. */
    std::mutex clients_mtx;                     /* A lock to protect RpcClient pointers */
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<RaftLog<Command>> log_storage;     /* To persist the raft log. */
    std::unique_ptr<StateMachine> state;  /*  The state machine that applies the raft log, e.g. a kv store. */

    std::unique_ptr<RpcServer> rpc_server;      /* RPC server to recieve and handle the RPC requests. */
    std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map;  /* RPC clients of all raft nodes including this node. */
    std::vector<RaftNodeConfig> node_configs;   /* Configuration for all nodes */ 
    int my_id;                                  /* The index of this node in rpc_clients, start from 0. */

    std::atomic_bool stopped;

    RaftRole role;
    int current_term;
    int leader_id;

    std::unique_ptr<std::thread> background_election;
    std::unique_ptr<std::thread> background_ping;
    std::unique_ptr<std::thread> background_commit;
    std::unique_ptr<std::thread> background_apply;

    /* Lab3: Your code here */
    // random
    std::mt19937 random_generator;
    int64_t random_time(int64_t start, int64_t max) {
        std::uniform_int_distribution<> distrib(0, max);
        return distrib(random_generator) + start;
    }
    // election
    int64_t last_time;
    int64_t election_timeout;
    int voted_for;
    // int last_log_index;
    // int last_log_term;
    int granted_num;
    // log
    int commit_index;
    int last_applied;
    // leader
    int64_t last_ping_time;
    std::map<int, int> next_index;
    std::map<int, int> match_index;
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs):
    network_stat(true),
    node_configs(configs),
    my_id(node_id),
    stopped(true),
    role(RaftRole::Follower),
    current_term(0),
    leader_id(-1)
{
    auto my_config = node_configs[my_id];

    /* launch RPC server */
    rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

    /* Register the RPCs. */
    rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
    rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
    rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
    rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
    rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
    rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
    rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

    rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
    rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
    rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

   /* Lab3: Your code here */ 

    std::random_device rd;
    random_generator = std::mt19937(rd());

    std::string filename("/tmp/raft_log/node" + std::to_string(my_id) + ".log");
    auto bm = std::make_shared<BlockManager>(filename);
    log_storage = std::make_unique<RaftLog<Command>>(bm);
    state = std::make_unique<StateMachine>();

    commit_index = 0;
    last_applied = 0;
    auto stat = log_storage->get_snapshot_stat();
    if (stat.first > 0) {
        state->apply_snapshot(log_storage->get_snapshot());
        commit_index = stat.first;
        last_applied = stat.first;
    }

    // persist state

    current_term = log_storage->get_term();
    voted_for = log_storage->get_voted();

    rpc_server->run(true, configs.size()); 
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode()
{
    stop();

    thread_pool.reset();
    rpc_server.reset();
    state.reset();
    log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int
{
    /* Lab3: Your code here */

    role = RaftRole::Follower;
    leader_id = -1;
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    election_timeout = random_time(1000, 500);
    last_time = now;
    last_ping_time = now;
    thread_pool = std::make_unique<ThreadPool>(8);

    rpc_clients_map.clear();
    for (auto &i: node_configs) {
        rpc_clients_map[i.node_id] = std::make_unique<RpcClient>(i.ip_address, i.port, true);
    }

    stopped = false;
    background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int
{
    /* Lab3: Your code here */
    
    stopped = true;
    background_election->join();
    background_ping->join();
    background_commit->join();
    background_apply->join();
    thread_pool.reset();
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
{
    /* Lab3: Your code here */

    std::unique_lock<std::mutex> lock(mtx);
    if (role == RaftRole::Leader)
        return std::make_tuple(true, current_term);
    else return std::make_tuple(false, current_term);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool
{
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
{
    /* Lab3: Your code here */

    std::unique_lock<std::mutex> lock(mtx);
    if (role != RaftRole::Leader)
        return std::make_tuple(false, current_term, -1);
    Command command;
    command.deserialize(cmd_data, cmd_size);
    log_storage->append_log(current_term, command);
    return {true, current_term, log_storage->size()};
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
{
    /* Lab3: Your code here */ 

    std::unique_lock<std::mutex> lock(mtx);
    if (log_storage->size() == 0)   // cannot snapshot without a log
        return false;
    int applied_idx = last_applied;
    int applied_term = log_storage->get_log_stat(applied_idx).second;
    auto snapshot = state->snapshot();
    log_storage->save_snapshot(snapshot, applied_idx, applied_term);
    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    /* Lab3: Your code here */
    return state->snapshot();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
{
    /* Lab3: Your code here */

    std::unique_lock<std::mutex> lock(mtx);
    RequestVoteReply reply;
    if (args.term < current_term) {
        reply.vote_granted = false;
        reply.term = current_term;
        return reply;
    }
    else {
        if (args.term > current_term) {
            current_term = args.term;
            log_storage->set_term(current_term);
            role = RaftRole::Follower;
            voted_for = -1;
            log_storage->set_voted(voted_for);
        }

        reply.term = current_term;

        int sz = log_storage->size();
        int tm = log_storage->get_log_stat(sz).second;
        if (voted_for != -1 && voted_for != args.candidate_id) {
            reply.vote_granted = false;
        } else if (args.last_log_term < tm) {
            reply.vote_granted = false;
        } else if (args.last_log_term == tm && args.last_log_index < sz) {
            reply.vote_granted = false;
        } else {
            reply.vote_granted = true;
            voted_for = args.candidate_id;
            log_storage->set_voted(voted_for);
            last_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        }
    }
    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
{
    /* Lab3: Your code here */

    std::unique_lock<std::mutex> lock(mtx);
    if (reply.term < current_term)
        return;
    if (reply.term > current_term) {
        current_term = reply.term;
        log_storage->set_term(current_term);
        role = RaftRole::Follower;
        last_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        return;
    }
    if (role == RaftRole::Follower || role == RaftRole::Leader)
        return;
    if (reply.vote_granted) {
        ++granted_num;
        if (granted_num > node_configs.size() / 2) {
            granted_num = 0;
            role = RaftRole::Leader;
            leader_id = my_id;
            next_index.clear();
            match_index.clear();
            for (auto &i: node_configs) {
                if (i.node_id == my_id)
                    continue;
                next_index[i.node_id] = log_storage->size() + 1;
                match_index[i.node_id] = 0;
            }
        }
    }

    return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
{
    /* Lab3: Your code here */

    std::unique_lock<std::mutex> lock(mtx);

    if (rpc_arg.term >= current_term) {
        current_term = rpc_arg.term;
        log_storage->set_term(current_term);
        leader_id = rpc_arg.leader_id;
        role = RaftRole::Follower;
        last_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }
    AppendEntriesReply reply;
    reply.term = current_term;
    if (rpc_arg.term < current_term) {
        reply.success = false;
    } else {
        leader_id = rpc_arg.leader_id;
        last_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        // check if i can append the log
        AppendEntriesArgs<Command> arg = transform_rpc_append_entries_args<Command>(rpc_arg);
        if (log_storage->get_log_stat(arg.prev_log_index).second != arg.prev_log_term)
            reply.success = false;
        else {  // apply log
            int sz = arg.entries.size();
            log_storage->discard_log(arg.prev_log_index + 1);
            for (int i = 0; i < sz; ++i) {
                log_storage->append_log(arg.entries[i].first, arg.entries[i].second);
            }
            // commit
            commit_index = log_storage->size() < rpc_arg.leader_commit ? log_storage->size() : rpc_arg.leader_commit;

            reply.success = true;
        }
    }
    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
{
    /* Lab3: Your code here */

    std::unique_lock<std::mutex> lock(mtx);
    if (reply.term > current_term) {
        current_term = reply.term;
        log_storage->set_term(current_term);
        role = RaftRole::Follower;
        return;
    }
    if (reply.success) {
        int match_idx = arg.prev_log_index + arg.entries.size();
        next_index[node_id] = match_idx + 1;
        match_index[node_id] = match_idx;
        if (commit_index < match_idx) {
            int match_num = 1;
            for (auto &i: match_index)
                if (i.second >= match_idx)
                    ++match_num;
            if (match_num > node_configs.size() / 2)
                commit_index = match_idx;
        }

    } else {
        auto sn_stat = log_storage->get_snapshot_stat();
        if (sn_stat.first > 0 && next_index[node_id] - 1 == sn_stat.first) {
            InstallSnapshotArgs args;
            args.term = current_term;
            args.leader_id = my_id;
            args.last_included_index = sn_stat.first;
            args.last_included_term = sn_stat.second;
            args.data = log_storage->get_snapshot();
            if (rpc_clients_map[node_id] != nullptr)
                thread_pool->enqueue(&RaftNode::send_install_snapshot, this, node_id, args);
        } else
            --next_index[node_id];
    }
    return;
}


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{
    /* Lab3: Your code here */

    std::unique_lock<std::mutex> lock(mtx);
    InstallSnapshotReply reply;
    if (args.term < current_term) {
        reply.term = current_term;
        return reply;
    }
    if (args.term > current_term) {
        current_term = args.term;
        log_storage->set_term(current_term);
        leader_id = args.leader_id;
        role = RaftRole::Follower;
    }
    reply.term = current_term;
    log_storage->save_snapshot(args.data, args.last_included_index, args.last_included_term);
    if (commit_index < args.last_included_index)
        commit_index = args.last_included_index;
    if (last_applied < args.last_included_index) {
        state->apply_snapshot(args.data);
        last_applied = args.last_included_index;
    }
    return reply;
}


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
{
    /* Lab3: Your code here */

    std::unique_lock<std::mutex> lock(mtx);
    if (reply.term > current_term) {
        current_term = reply.term;
        log_storage->set_term(current_term);
        role = RaftRole::Follower;
        return;
    }
    match_index[node_id] = std::max(arg.last_included_index, match_index[node_id]);
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr 
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
    clients_lock.unlock();
    if (res.is_ok()) { 
        handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
    } else {
        // RPC fails
    }
}


/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            std::unique_lock<std::mutex> lock(mtx);
            if (role == RaftRole::Leader) {
                lock.unlock();
                std::this_thread::yield();
                continue;
            }
            auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            
            if (now - last_time < election_timeout) {
                lock.unlock();
                std::this_thread::yield();
                continue;
            } else {    // become candidate, start voting
                role = RaftRole::Candidate;
                ++current_term;
                log_storage->set_term(current_term);
                voted_for = my_id;
                log_storage->set_voted(voted_for);
                granted_num = 1;
                last_time = now;
                election_timeout = random_time(1000, 500);
                RequestVoteArgs args;
                args.term = current_term;
                args.candidate_id = my_id;
                int sz = log_storage->size();
                args.last_log_index = sz;
                args.last_log_term = log_storage->get_log_stat(sz).second;
                for (auto &i: rpc_clients_map) {
                    if (i.first == my_id || i.second == nullptr)
                        continue;
                    thread_pool->enqueue(&RaftNode::send_request_vote, this, i.first, args);
                }
                lock.unlock();
                std::this_thread::yield();
            }
        }
    }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            std::unique_lock<std::mutex> lock(mtx);
            if (role != RaftRole::Leader) {
                lock.unlock();
                std::this_thread::yield();
                continue;
            }

            AppendEntriesArgs<Command> args;
            args.term = current_term;
            args.leader_id = my_id;
            args.leader_commit = commit_index;
            for (auto &i: rpc_clients_map) {
                if (i.first == my_id || i.second == nullptr)
                    continue;
                int sz = log_storage->size();
                if (match_index[i.first] == sz)
                    continue;
                int prev_idx = next_index[i.first] - 1;
                args.prev_log_index = prev_idx;
                args.prev_log_term = log_storage->get_log_stat(prev_idx).second;
                std::vector<std::pair<int, Command>> entries;
                for (int i = prev_idx + 1; i <= sz; ++i) {
                    entries.push_back(log_storage->get_log(i));
                }
                args.entries = entries;
                thread_pool->enqueue(&RaftNode::send_append_entries, this, i.first, args);
            }
            lock.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */

            std::unique_lock<std::mutex> lock(mtx);
            if (last_applied == commit_index) {
                lock.unlock();
                std::this_thread::yield();
                continue;
            }
            while (last_applied < commit_index) {
                int idx = last_applied + 1;
                Command cmd = log_storage->get_log(idx).second;
                state->apply_log(cmd);
                ++last_applied;
            }
        }
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            std::unique_lock<std::mutex> lock(mtx);
            if (role != RaftRole::Leader) {
                lock.unlock();
                std::this_thread::yield();
                continue;
            }

            auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            if (now - last_ping_time < 100) {
                lock.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(50 - (now - last_ping_time)));
                continue;
            } else {
                last_ping_time = now;
                AppendEntriesArgs<Command> arg;
                arg.term = current_term;
                arg.leader_id = my_id;
                arg.entries = std::vector<std::pair<int, Command>>();
                arg.leader_commit = commit_index;
                for (auto &i: rpc_clients_map) {
                    if (i.first == my_id || i.second == nullptr)
                        continue;
                    int prev_idx = next_index[i.first] - 1;
                    arg.prev_log_index = prev_idx;
                    arg.prev_log_term = log_storage->get_log_stat(prev_idx).second;
                    thread_pool->enqueue(&RaftNode::send_append_entries, this, i.first, arg);
                }
            }

        }
    }

    return;
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    /* turn off network */
    if (!network_availability[my_id]) {
        for (auto &&client: rpc_clients_map) {
            if (client.second != nullptr)
                client.second.reset();
        }

        return;
    }

    for (auto node_network: network_availability) {
        int node_id = node_network.first;
        bool node_status = node_network.second;

        if (node_status && rpc_clients_map[node_id] == nullptr) {
            RaftNodeConfig target_config;
            for (auto config: node_configs) {
                if (config.node_id == node_id) 
                    target_config = config;
            }

            rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
        }

        if (!node_status && rpc_clients_map[node_id] != nullptr) {
            rpc_clients_map[node_id].reset();
        }
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            client.second->set_reliable(flag);
        }
    }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num()
{
    /* only applied to ListStateMachine*/
    std::unique_lock<std::mutex> lock(mtx);

    return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count()
{
    int sum = 0;
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            sum += client.second->count();
        }
    }
    
    return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
{
    if (is_stopped()) {
        return std::vector<u8>();
    }

    std::unique_lock<std::mutex> lock(mtx);

    return state->snapshot(); 
}

}