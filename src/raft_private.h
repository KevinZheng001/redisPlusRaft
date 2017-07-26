#ifndef RAFT_PRIVATE_H_
#define RAFT_PRIVATE_H_

enum {
    RAFT_NODE_STATUS_DISCONNECTED,
    RAFT_NODE_STATUS_CONNECTED,
    RAFT_NODE_STATUS_CONNECTING,
    RAFT_NODE_STATUS_DISCONNECTING
};

/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. 
 *
 * @file
 * @author Willem Thiart himself@willemthiart.com
 */
/* raft_server_private_t 该结构体是Raft在实现中的抽象体，保存了Raft协议
 * 运行过程中状态和需要的所有数据。*/
typedef struct {
    /* Persistent state: */
    /* 所有服务器比较固定的状态: */

    /* the server's best guess of what the current term is
     * starts at zero */
    /* 服务器最后一次知道的任期号（初始化为 0，持续递增） */
    int current_term;

    /* The candidate the server voted for in its current term,
     * or Nil if it hasn't voted for any.  */
    /* 记录在当前分期内给哪个Candidate投过票 */
    int voted_for;

    /* the log which is replicated */
    /* 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号 */
    void* log;

    /* Volatile state: */
    /* 变动比较频繁的变量: */

    /* idx of highest log entry known to be committed */
    /* 已知的最大的已经被提交的日志条目的索引值 */
    int commit_idx;

    /* idx of highest log entry applied to state machine */
    /* 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增） */
    int last_applied_idx;

    /* follower/leader/candidate indicator */
    /* 三种状态：follower/leader/candidate */
    int state;

    /* amount of time left till timeout */
    /* 计时器，周期函数每次执行时会递增改值 */
    int timeout_elapsed;

    raft_node_t* nodes;
    int num_nodes;

    /* Follower检测选举计时器是否超时时间 */
    int election_timeout;

    /* leader 发送心跳信号的间隔时间 */
    int request_timeout;

    /* what this node thinks is the node ID of the current leader, or -1 if
     * there isn't a known current leader. */
    /* 保存Leader的信息，没有Leader时为NULL */
    raft_node_t* current_leader;

    /* callbacks */
    /* callbacks，由调用该raft实现的调用者来实现，网络IO和持久存储
     * 都由调用者在callback中实现 */
    raft_cbs_t cb;
    void* udata;

    /* my node ID */
    /* 自己的信息 */
    raft_node_t* raft_myself;

    /* the log which has a voting cfg change, otherwise -1 */
    /* 该raft实现每次只进行一个服务器的配置更改，该变量记录raft server
     * 是否正在进行配置更改*/
    int voting_cfg_change_log_idx;

    /* our membership with the cluster is confirmed (ie. configuration log was
     * committed) */
    int connected;
} raft_server_private_t;

void raft_election_start(raft_server_t* me);

void raft_become_candidate(raft_server_t* me);

void raft_become_follower(raft_server_t* me);

void raft_vote(raft_server_t* me, raft_node_t* node);

void raft_set_current_term(raft_server_t* me,int term);

/**
 * @return 0 on error */
int raft_send_requestvote(raft_server_t* me, raft_node_t* node);

int raft_send_appendentries(raft_server_t* me, raft_node_t* node);

int raft_send_appendentries_all(raft_server_t* me_);

/**
 * Apply entry at lastApplied + 1. Entry becomes 'committed'.
 * @return 1 if entry committed, 0 otherwise */
int raft_apply_entry(raft_server_t* me_);

/**
 * Appends entry using the current term.
 * Note: we make the assumption that current term is up-to-date
 * @return 0 if unsuccessful */
int raft_append_entry(raft_server_t* me_, raft_entry_t* c);

void raft_set_last_applied_idx(raft_server_t* me, int idx);

void raft_set_state(raft_server_t* me_, int state);

int raft_get_state(raft_server_t* me_);

raft_node_t* raft_node_new(void* udata, int id);

void raft_node_set_next_idx(raft_node_t* node, int nextIdx);

void raft_node_set_match_idx(raft_node_t* node, int matchIdx);

int raft_node_get_match_idx(raft_node_t* me_);

void raft_node_vote_for_me(raft_node_t* me_, const int vote);

int raft_node_has_vote_for_me(raft_node_t* me_);

void raft_node_set_has_sufficient_logs(raft_node_t* me_);

int raft_node_has_sufficient_logs(raft_node_t* me_);

int raft_votes_is_majority(const int nnodes, const int nvotes);

void raft_pop_log(raft_server_t* me_, raft_entry_t* ety, const int idx);

void raft_offer_log(raft_server_t* me_, raft_entry_t* ety, const int idx);

#endif /* RAFT_PRIVATE_H_ */
