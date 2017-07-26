/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief Implementation of a Raft server
 * @author Willem Thiart himself@willemthiart.com
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

/* for varags */
#include <stdarg.h>

#include "raft.h"
#include "raft_log.h"
#include "raft_private.h"

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

#ifndef max
#define max(a, b) ((a) < (b) ? (b) : (a))
#endif

static void __log(raft_server_t *me_, raft_node_t* node, const char *fmt, ...)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    char buf[1024];
    va_list args;

    va_start(args, fmt);
    vsprintf(buf, fmt, args);

    if (me->cb.log)
        me->cb.log(me_, node, me->udata, buf);
}

raft_server_t* raft_new()
{
    raft_server_private_t* me = (raft_server_private_t*)calloc(1, sizeof(raft_server_private_t));
    if (!me)
        return NULL;
    me->current_term = 0;
    me->voted_for = -1;
    me->timeout_elapsed = 0;
    me->request_timeout = 200;
    me->election_timeout = 2000;
    me->log = log_new();
    me->voting_cfg_change_log_idx = -1;
    raft_set_state((raft_server_t*)me, RAFT_STATE_FOLLOWER);
    me->current_leader = NULL;
    return (raft_server_t*)me;
}

void raft_set_callbacks(raft_server_t* me_, raft_cbs_t* funcs, void* udata)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    memcpy(&me->cb, funcs, sizeof(raft_cbs_t));
    me->udata = udata;
    log_set_callbacks(me->log, &me->cb, me_);
}

void raft_free(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    log_free(me->log);
    free(me_);
}

void raft_clear(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    me->current_term = 0;
    me->voted_for = -1;
    me->timeout_elapsed = 0;
    me->voting_cfg_change_log_idx = -1;
    raft_set_state((raft_server_t*)me, RAFT_STATE_FOLLOWER);
    me->current_leader = NULL;
    me->commit_idx = 0;
    me->last_applied_idx = 0;
    me->num_nodes = 0;
    me->raft_myself = NULL;
    me->voting_cfg_change_log_idx = 0;
    log_clear(me->log);
}

void raft_delete_entry_from_idx(raft_server_t* me_, int idx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    assert(me->commit_idx < idx);

    if (idx <= me->voting_cfg_change_log_idx)
        me->voting_cfg_change_log_idx = -1;

    log_delete(me->log, idx);
}

void raft_election_start(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    __log(me_, NULL, "election starting: %d %d, term: %d ci: %d",
          me->election_timeout, me->timeout_elapsed, me->current_term,
          raft_get_current_idx(me_));

    raft_become_candidate(me_);
}

void raft_become_leader(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    __log(me_, NULL, "becoming leader term:%d", raft_get_current_term(me_));
    printf("becoming leader term:%d\n", raft_get_current_term(me_));

    raft_set_state(me_, RAFT_STATE_LEADER);
    for (i = 0; i < me->num_nodes; i++)
    {
        if (me->raft_myself == me->nodes[i])
            continue;

        raft_node_t* node = me->nodes[i];
        raft_node_set_next_idx(node, raft_get_current_idx(me_) + 1);
        raft_node_set_match_idx(node, 0);
        raft_send_appendentries(me_, node);
    }
}

/** 成为竞选者Candidate 集群中每个服务器都有一个竞选计时器，当一个服务器在
 *  计时器超时时间内都没有收到来自Leader的心跳，则认为集群中不存在Leader或者
 *  是Leader挂了，该服务器就会变成Candidate，进而发起投票去竞选Leader，
 *  下面raft_become_candidate函数就是服务器变成Candidate的函数，函数中主要做这几件事情：
 *    自增当前的任期号（currentTerm）
 *    给自己投票
 *    重置选举超时计时器
 *    发送请求投票的 RPC 给其他所有服务器 **/
/* Follower成为Candidate执行的函数 */
void raft_become_candidate(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    __log(me_, NULL, "becoming candidate");
    printf("becoming candidate\n");

    /*自增当前的任期号；给自己投票，设置自己的状态为CANDIDATE*/
    raft_set_current_term(me_, raft_get_current_term(me_) + 1);
    for (i = 0; i < me->num_nodes; i++)
        raft_node_vote_for_me(me->nodes[i], 0);

    raft_vote(me_, me->raft_myself);
    me->current_leader = NULL;
    raft_set_state(me_, RAFT_STATE_CANDIDATE);

    /* We need a random factor here to prevent simultaneous candidates.
     * If the randomness is always positive it's possible that a fast node
     * would deadlock the cluster by always gaining a headstart. To prevent
     * this, we allow a negative randomness as a potential handicap. */
    /* 我们需要一个随机因素来防止同时出现的候选人。
     * 如果随机性始终是正的，那么可能是一个快速节点。
     * 将僵局的集群总是获得领先。为了防止
     * 这，我们允许消极的随机性作为一个潜在的障碍。 */
    /* 重置选举超时计时器。为了防止多个Candidate竞争，将下一次发起投票的时间间隔设置成随机值*/
    me->timeout_elapsed = me->election_timeout - 2 * (rand() % me->election_timeout);

    /*发送请求投票的 RPC 给其他所有服务器*/
    for (i = 0; i < me->num_nodes; i++)
        if (me->raft_myself != me->nodes[i] && raft_node_is_voting(me->nodes[i]))
            raft_send_requestvote(me_, me->nodes[i]);
}

void raft_become_follower(raft_server_t* me_)
{
    __log(me_, NULL, "becoming follower");
    printf("becoming follower\n");
    raft_set_state(me_, RAFT_STATE_FOLLOWER);
}

/** 周期函数 Raft需要周期性地做一些事情，比如Leader需要周期性地给其它服务器append日志，
 *  以让日志落后的服务器有机会追上来；所有服务器需要周期性地将已经确认提交的日志应用到
 *  状态机中去等等。
 *
 *  raft_periodic函数是该raft实现中被周期性调用的函数，调用周期是1000ms。机器在不同状态下
 *  会在这个函数中做不同的事情。Leader周期性地向Follower同步日志。而Follower周期性地检测
 *  是否在特定的时间内没有收到过来自Leader的心跳包，如果是的话就变成Candidate开始发起投票
 *  竞选Leader。不管是Leader还是Follower，都会周期性地将已经提交的日志commit到状态机FSM中去。
 */
/** raft周期性执行的函数,实现raft中的定时器以及定期应用日志到状态机
 *  msec_since_last_period 必须小于 me_->election_timeout, me_->request_timeout;
 *   */
int raft_periodic(raft_server_t* me_, int msec_since_last_period)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    /* 选举计时器；Follower每次收到Leader的心跳后会重置清0，Leader每次发送日志也会清0 */
    me->timeout_elapsed += msec_since_last_period;

    /* Only one voting node means it's safe for us to become the leader */
    if (1 == raft_get_num_voting_nodes(me_) &&
        raft_node_is_voting(raft_get_my_node((void*)me)) && !raft_is_leader(me_)) {
        raft_become_leader(me_);
    }

    if (me->state == RAFT_STATE_LEADER)
    {
        /* Leader周期性地向Follower同步日志 */
        if (me->timeout_elapsed >= me->request_timeout) {
            raft_send_appendentries_all(me_);
        }
    }
    /* Follower检测选举计时器是否超时 */
    else if (me->timeout_elapsed >= me->election_timeout)
    {
        if (1 < raft_get_num_voting_nodes(me_) && raft_node_is_voting(raft_get_my_node(me_))) {
            raft_election_start(me_);
        }
    }

    /* 周期性地将已经确认commit的日志应用到状态机FSM */
    if (me->last_applied_idx < me->commit_idx)
        return raft_apply_entry(me_);

    return 0;
}

raft_entry_t* raft_get_entry_from_idx(raft_server_t* me_, int etyidx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    return log_get_at_idx(me->log, etyidx);
}

int raft_voting_change_is_in_progress(raft_server_t* me_)
{
    return ((raft_server_private_t*)me_)->voting_cfg_change_log_idx != -1;
}

/* 
 * 处理添加日志请求回复 Leader收到添加日志回复后，可以知道下面这些信息：
 * 自己是不是已经过时(current_term < response->term即为过时)
 * follower是否成功添加日志，如果添加失败，则减小发给follower的日志索引
 * nextIndex再重试；如果添加成功则更新本地记录的follower日志信息，
 * 并检查日志是否最新，如果不是最新则继续发送添加日志请求。
 * 新机器的日志添加，详见3.4节-- 成员变更
 **/
/** 处理添加日志请求回复
 *   */
int raft_recv_appendentries_response(raft_server_t* me_,
                                     raft_node_t* node,
                                     msg_appendentries_response_t* ae_response)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    __log(me_, node,
          "received appendentries response %s current idx:%d response current idx:%d 1st idx:%d",
          ae_response->success == 1 ? "SUCCESS" : "fail",
          raft_get_current_idx(me_),
          ae_response->current_idx,
          ae_response->first_idx);

    if (!node)
        return -1;

    /* Stale response -- ignore */
    /* 过时的回复 -- 忽略 */
    if (ae_response->current_idx != 0 && ae_response->current_idx <= raft_node_get_match_idx(node))
        return 0;

    /* oh~我不是Leader */
    if (!raft_is_leader(me_))
        return -1;

    /* If response contains term T > currentTerm: set currentTerm = T
       and convert to follower (§5.3) */
    /* 回复中的term比自己的要大，说明自己是一个过时的Leader，无条件转为Follower */
    if (me->current_term < ae_response->term)
    {
        raft_set_current_term(me_, ae_response->term);
        raft_become_follower(me_);
        return 0;
    }
    /* 过时的回复，网络状况不好时会出现 */
    else if (me->current_term != ae_response->term)
        return 0;

    /* 由于日志不一致导致添加日志不成功*/
    if (0 == ae_response->success)
    {
        /* If AppendEntries fails because of log inconsistency:
           decrement nextIndex and retry (§5.3) */
        /* 将nextIdex减*/
        int next_idx = raft_node_get_next_idx(node);
        assert(0 <= next_idx);
        /* Follower的日志数量还远远少于Leader，将nextIdex设为回复中的current_idx+1和Leader
         * 当前索引中较小的一个，一般回复中的current_idx+1会比较小*/
        if (ae_response->current_idx < next_idx - 1)
            raft_node_set_next_idx(node, min(ae_response->current_idx + 1, raft_get_current_idx(me_)));
        /* Follower的日志数量和Leader差不多，但是比对前一条日志时失败，这种情况将next_idx减1
         * 重试*/
        else
            raft_node_set_next_idx(node, next_idx - 1);

        /* retry */
        /* 使用更新后的nextIdx重新发送添加日志请求 */
        raft_send_appendentries(me_, node);
        return 0;
    }

    assert(ae_response->current_idx <= raft_get_current_idx(me_));

    /* 下面处理添加日志请求的情况 */
    /* 更新本地记录的Follower的日志情况 */
    raft_node_set_next_idx(node, ae_response->current_idx + 1);
    raft_node_set_match_idx(node, ae_response->current_idx);

    /* 如果是新加入的机器，则判断它的日志是否是最新，如果达到了最新，则赋予它投票权，
     * 这里逻辑的详细解释在第3.4节 -- 成员变更*/
    if (!raft_node_is_voting(node) &&
        !raft_voting_change_is_in_progress(me_) &&
        raft_get_current_idx(me_) <= ae_response->current_idx + 1 &&
        me->cb.node_has_sufficient_logs &&
        0 == raft_node_has_sufficient_logs(node)
        )
    {
        int e = me->cb.node_has_sufficient_logs(me_, me->udata, node);
        if (0 == e)
            raft_node_set_has_sufficient_logs(node);
    }

    /* Update commit idx */
    int point = ae_response->current_idx;
    if (point)
    {
        raft_entry_t* ety = raft_get_entry_from_idx(me_, point);
        if (raft_get_commit_idx(me_) < point && ety->term == me->current_term)
        {
            /* 如果一条日志回复成功的数量超过一半，则将日志提交commit，即允许应用到状态机 */
            int i, votes = 1;
            for (i = 0; i < me->num_nodes; i++)
            {
                /*如果follower已经添加了索引大于等于ae_response->current_idx的日志，则vote加1*/
                if (me->raft_myself != me->nodes[i] &&
                    raft_node_is_voting(me->nodes[i]) &&
                    point <= raft_node_get_match_idx(me->nodes[i]))
                {
                    votes++;
                }
            }

            /* 投票数大于所有服务器的一半，则将日志提交 */
            if (votes > (raft_get_num_voting_nodes(me_)/2))
                raft_set_commit_idx(me_, point);
        }
    }

    /* Aggressively send remaining entries */
    /* 如果follower的日志还没有最新，那么继续发送添加日志请求 */
    if (raft_get_entry_from_idx(me_, raft_node_get_next_idx(node)))
        raft_send_appendentries(me_, node);

    /* periodic applies committed entries lazily */

    return 0;
/* 3.3 成员变更
 *
 * 成员的变更都是以日志的形式下发的。添加的新成员分两阶段进行，
 * 第一阶段中新成员没有有投票权，但是有接收日志的权力；
 * 当它的日志同步到最新后就进入到第二阶段，由Leader赋予投票权，
 * 从而成为集群中完整的一员。删除成员相对比较简单，所有服务器收到删除成员的日志后，
 * 立马将该成员的信息从本地抹除。
 *
 * 添加成员过程
 * 1. 管理员向Leader发送添加成员命令
 * 2. Leader添加一条 RAFT_LOGTYPE_ADD_NONVOTING_NODE日志，即添加没有投票权的服务器。
 *    该日志与其它普通日志一样同步给集群中其它服务器。收到该日志的服务器在本地保存该新成员的信息。
 * 3. 当新成员的日志同步到最新后，Leader添加一条 RAFT_LOGTYPE_ADD_NODE日志，
 *    即有投票权的服务器，同样地，该日志与其它普通日志一样同步给集群中其它服务器。
 *    收到该日志的服务器在本地保存该新成员的信息，以后的投票活动会将新成员考虑进去。
 *
 * 删除成员过程
 * 1. 管理员向Leader发送删除成员命令。
 * 2. Leader添加一条 RAFT_LOGTYPE_REMOVE_NODE 日志，并跟普通日志一样同步给其它服务器。
 *    收到该日志的服务器立即将被成员信息从本地删除。
 **/
}

/* 处理添加日志请求 所有的服务器都有可能收到添加日志请求，
 * 比如过时的Leader和Candidate以及正常运行的Follower。
 * 处理添加日志请求的过程主要就是验证请求中的日志是否比本地日志新的过程。*/
/*
 * 1. 处理任期号的三种情况(大于等于和小于)
 * 2. 处理prev log不一致的情况，返回包中告诉Leader自己目前的log情况
 * 3. 处理添加日志成功的情况-- 保存新日志并更新current_idx和commit_idx
 * */
int raft_recv_appendentries(
    raft_server_t* me_,
    raft_node_t* node,
    msg_appendentries_t* ae,
    msg_appendentries_response_t *ae_response
    )
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    me->timeout_elapsed = 0;

//    if (0 < ae->n_entries)
        __log(me_, node, "recvd appendentries term:%d ci:%d leader_commit:%d pre_log_idx:%d pre_log_term:%d #%d",
              ae->term,
              raft_get_current_idx(me_),
              ae->leader_commit,
              ae->prev_log_idx,
              ae->prev_log_term,
              ae->n_entries);
    /* 下面for循环跳过请求中已经在本地添加过的日志*/
    for (int i = 0; i < ae->n_entries; i++)
    {
        raft_entry_t* ety = &ae->entries[i];
        printf(" --entries[%d].term = %d\n", i, ety->term);
        printf(" --entries[%d].id = %d\n", i, ety->id);
        printf(" --entries[%d].type = %d\n", i, ety->type);
        printf(" --entries[%d].data.len = %d\n", i, ety->data.len);
        printf(" --entries[%d].data.buf = %s\n", i, ety->data.buf);
    }

    ae_response->term = me->current_term;

    /* 处理任期号 */
    /* currentTerm == ae->term,当自己是Candidate时收到term与自己相等的请求，
     * 说明已经有其它Candidate成为了Leader,自己无条件变成Follower*/
    if (raft_is_candidate(me_) && me->current_term == ae->term)
    {
        raft_become_follower(me_);
    }
    /* currentTerm < ae->term. 自己的任期号已经落后Leader，无条件成为Follower，并且更新自己的term*/
    else if (me->current_term < ae->term)
    {
        raft_set_current_term(me_, ae->term);
        ae_response->term = ae->term;
        raft_become_follower(me_);
    }
    /* currentTerm > ae->term. 说明收到一个过时Leader的请求，直接回包告诉它最新的term */
    else if (ae->term < me->current_term)
    {
        /* 1. Reply false if term < currentTerm (§5.1) */
        __log(me_, node, "AE term %d is less than current term %d", ae->term, me->current_term);
        goto fail_with_current_idx;
    }

    /* Not the first appendentries we've received */
    /* NOTE: the log starts at 1 */
    /* 检查请求中prev_log_idx日志的term与本地对应索引的term是否一致 */
    if (0 < ae->prev_log_idx)
    {
        raft_entry_t* e = raft_get_entry_from_idx(me_, ae->prev_log_idx);
        /* 本地在prev_log_idx位置还不存在日志，说明日志已经落后Leader了，返回false
         * 并告诉leader自己当前日志的位置，这样Leader知道下一次该发哪条日志过来了*/
        if (!e)
        {
            __log(me_, node, "AE no log at prev_idx %d", ae->prev_log_idx);
            goto fail_with_current_idx;
        }

        /* 2. Reply false if log doesn't contain an entry at prevLogIndex
           whose term matches prevLogTerm (§5.3) */
        if (raft_get_current_idx(me_) < ae->prev_log_idx)
            goto fail_with_current_idx;

        /* 本地在prev_log_idx位置的日志的term与请求中的prev_log_term不一致，
         * 此时本地无条件删除本地与请求不一致的日志，并向Leader返回删除后的日志位置*/
        if (e->term != ae->prev_log_term)
        {
            __log(me_, node, "AE term doesn't match prev_term (ie. %d vs %d) ci:%d pli:%d",
                  e->term, ae->prev_log_term, raft_get_current_idx(me_), ae->prev_log_idx);
            /* Delete all the following log entries because they don't match */
            raft_delete_entry_from_idx(me_, ae->prev_log_idx);
            ae_response->current_idx = ae->prev_log_idx - 1;
            goto fail;
        }
    }

    /* 3. If an existing entry conflicts with a new one (same index
       but different terms), delete the existing entry and all that
       follow it (§5.3) */
    /* 本地的日志比Leader要多。当本地服务器曾经是Leader，收到了很多客户端请求
     * 并还没来得及同步时会出现这种情况。这时本地无条件删除比Leader多的日志 */
    if (0 < ae->prev_log_idx && ae->prev_log_idx + 1 < raft_get_current_idx(me_))
    {
        /* Heartbeats shouldn't cause logs to be deleted. Heartbeats might be 
         * sent before the leader received the last appendentries response */
        if (ae->n_entries != 0 &&
                /* this is an old out-of-order appendentry message */
                me->commit_idx < ae->prev_log_idx + 1)
            raft_delete_entry_from_idx(me_, ae->prev_log_idx + 1);
    }

    ae_response->current_idx = ae->prev_log_idx;

    int i;
    /* 下面for循环跳过请求中已经在本地添加过的日志*/
    for (i = 0; i < ae->n_entries; i++)
    {
        raft_entry_t* ety = &ae->entries[i];
        int ety_index = ae->prev_log_idx + 1 + i;
        raft_entry_t* existing_ety = raft_get_entry_from_idx(me_, ety_index);
        ae_response->current_idx = ety_index;
        if (existing_ety && existing_ety->term != ety->term && me->commit_idx < ety_index)
        {
            raft_delete_entry_from_idx(me_, ety_index);
            break;
        }
        else if (!existing_ety)
            break;
    }

    /* Pick up remainder in case of mismatch or missing entry */
    /* 下面for循环将请求中确认的新日志添加到本地 */
    for (; i < ae->n_entries; i++)
    {
        int e = raft_append_entry(me_, &ae->entries[i]);
        if (-1 == e)
            goto fail_with_current_idx;
        else if (RAFT_ERR_SHUTDOWN == e)
        {
            ae_response->success = 0;
            ae_response->first_idx = 0;
            return RAFT_ERR_SHUTDOWN;
        }
        ae_response->current_idx = ae->prev_log_idx + 1 + i;
    }

    /* 4. If leaderCommit > commitIndex, set commitIndex =
        min(leaderCommit, index of most recent entry) */
    /* 4. 请求中携带了Leader已经提交到状态机的日志索引，本地同样也更新这个索引，将其
     *    设置为本地最大日志索引和leader_commit中的较小者*/
    if (raft_get_commit_idx(me_) < ae->leader_commit)
    {
        int last_log_idx = max(raft_get_current_idx(me_), 1);
        raft_set_commit_idx(me_, min(last_log_idx, ae->leader_commit));
    }

    /* update current leader because we accepted appendentries from it */
    /* 更新Leader信息 */
    me->current_leader = node;

    ae_response->success = 1;
    ae_response->first_idx = ae->prev_log_idx + 1;
    return 0;

fail_with_current_idx:
    ae_response->current_idx = raft_get_current_idx(me_);
fail:
    ae_response->success = 0;
    ae_response->first_idx = 0;
    return -1;
}

int raft_already_voted(raft_server_t* me_)
{
    return ((raft_server_private_t*)me_)->voted_for != -1;
}

/** 检查是否满足投票的条件 
 * */
static int __should_grant_vote(raft_server_private_t* me, msg_requestvote_t* vote_request)
{
    /* TODO: 4.2.3 Raft Dissertation:
     * if a server receives a RequestVote request within the minimum election
     * timeout of hearing from a current leader, it does not update its term or
     * grant its vote */

    if (!raft_node_is_voting(raft_get_my_node((void*)me)))
        return 0;

    /**请求中的任期号term比本地term要小，不给投票*/
    if (vote_request->term < raft_get_current_term((void*)me))
        return 0;

    /* TODO: if voted for is candiate return 1 (if below checks pass) */
    /*如果已经投过票了，返回false*/
    if (raft_already_voted((void*)me))
        return 0;

    /* Below we check if log is more up-to-date... */

    /* 下面代码检查请求中日志信息是否比本地日志新*/

    /* 获取本地最新的日志索引 */
    int current_idx = raft_get_current_idx((void*)me);

    /* Our log is definitely not more up-to-date if it's empty! */
    /* 本地日志为空，请求中的日志信息绝对比本地要新，返回true */
    if (0 == current_idx)
        return 1;

    raft_entry_t* e = raft_get_entry_from_idx((void*)me, current_idx);
    /* 如果本地最新日志中的任期号比请求中的last_log_term要小，则返回true */
    if (e->term < vote_request->last_log_term)
        return 1;

    /* 本地最新日志中的任期号与请求中的last_log_term相等，则比较日志索引，
     * 索引比较大的说明日志比较新*/
    if (vote_request->last_log_term == e->term && current_idx <= vote_request->last_log_idx)
        return 1;

    /*果本地最新日志中的任期号比请求中的last_log_term要大，则返回false */
    return 0;
}

/* 
 * 处理投票请求 处理投票请求的逻辑主要就是判断是否要同意投票，判断的依据就是
 * 请求中的任期号和日志信息的新旧程度，还有就是自己是否给其它相同任期号的服务器
 * 投过票，如果投过就不能再投，每人只有一票投票权。
 *
 * 如果term > currentTerm, 则转为Follower模式。
 * 这里收到投票请求的服务器有可能是一个网络状况不佳的Leader或者是一个还没来得及
 * 发出投票请求的Candidate，他们收到任期号比自己要新的请求后，都要无条件变成Follower，
 * 以保证只有一个Leader存在
 *
 * 如果term < currentTerm返回false。请求中的term比自己的term还要小，说明是一个过时
 * 的请求，则不给它投票返回false。
 *
 * 如果 term == currentTerm，请求中的日志信息不比本地日志旧，并且尚未给其它
 * Candidate投过票，那么就投票给他
 **/
int raft_recv_requestvote(raft_server_t* me_,
                          raft_node_t* node,
                          msg_requestvote_t* vote_request,
                          msg_requestvote_response_t *response)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!node)
        node = raft_get_node(me_, vote_request->candidate_id);

    /*如果请求中term > 本地currentTerm, 则转为Follower模式*/
    if ( vote_request->term > raft_get_current_term(me_))
    {
        raft_set_current_term(me_, vote_request->term);
        raft_become_follower(me_);
    }

    /*如果需要投票，则回复true，即将r->vote_granted = 1;*/
    if (__should_grant_vote(me, vote_request))
    {
        /* It shouldn't be possible for a leader or candidate to grant a vote
         * Both states would have voted for themselves */
        assert(!(raft_is_leader(me_) || raft_is_candidate(me_)));

        /*同意投票--本地记录给哪个服务器投了票，并设置response中的vote_granted为1*/
        raft_vote_for_nodeid(me_, vote_request->candidate_id);
        response->vote_granted = RAFT_REQUESTVOTE_ERR_GRANTED;

        /* there must be in an election. */
        me->current_leader = NULL;

        me->timeout_elapsed = 0;
    }
    else
    {
        /* It's possible the candidate node has been removed from the cluster but
         * hasn't received the appendentries that confirms the removal. Therefore
         * the node is partitioned and still thinks its part of the cluster. It
         * will eventually send a requestvote. This is error response tells the
         * node that it might be removed. */
        if (!node)
        {
            response->vote_granted = RAFT_REQUESTVOTE_ERR_UNKNOWN_NODE;
            goto done;
        }
        else
            response->vote_granted = RAFT_REQUESTVOTE_ERR_NOT_GRANTED;
    }

done:
    __log(me_, node, "node requested vote: %d replying: %s",
          node,
          response->vote_granted == 1 ? "granted" :
          response->vote_granted == 0 ? "not granted" : "unknown");

    /*更新本地保存的任期号，与请求中的保持一致*/
    response->term = raft_get_current_term(me_);
    return 0;
}

int raft_votes_is_majority(const int num_nodes, const int nvotes)
{
    if (num_nodes < nvotes)
        return 0;
    int half = num_nodes / 2;
    return half + 1 <= nvotes;
}

/* 收到投票回复 Candidate收到投票回复后，检查是否给自己投了票，
 * 如果投了票则统计当前收到的投票总数，超过一半则成为Leader */
int raft_recv_requestvote_response(raft_server_t* me_,
                                   raft_node_t* node,
                                   msg_requestvote_response_t* v_response)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    __log(me_, node, "node responded to requestvote status: %s",
          v_response->vote_granted == 1 ? "granted" :
          v_response->vote_granted == 0 ? "not granted" : "unknown");

    /* Oh~我不是Candidate，直接返回 */
    if (!raft_is_candidate(me_))
    {
        return 0;
    }
    /* response中的任期号比自己的大，说明自己的term已经过时，无条件转为Follower */
    else if (v_response->term > raft_get_current_term(me_))
    {
        raft_set_current_term(me_, v_response->term);
        raft_become_follower(me_);
        return 0;
    }
    /* response中的任期号比自己小，说明收到了一个过时的response，忽略即可。
     * 当网络比较差的时候容易出现这种情况 */
    else if (raft_get_current_term(me_) != v_response->term)
    {
        /* The node who voted for us would have obtained our term.
         * Therefore this is an old message we should ignore.
         * This happens if the network is pretty choppy. */
        return 0;
    }

    __log(me_, node, "node responded to requestvote status:%s ct:%d rt:%d",
          v_response->vote_granted == 1 ? "granted" :
          v_response->vote_granted == 0 ? "not granted" : "unknown",
          me->current_term,
          v_response->term);

    switch (v_response->vote_granted)
    {
        case RAFT_REQUESTVOTE_ERR_GRANTED:
        /* Yeah~给我投票了 */
            if (node) {
                /* 记录给自己投票的服务器信息 */
                raft_node_vote_for_me(node, 1);
            }
            int votes = raft_get_nvotes_for_me(me_);
            /* 如果给自己投票的服务器超过了总数的一般，则成为Leader */
            if (raft_votes_is_majority(raft_get_num_voting_nodes(me_), votes)) {
                raft_become_leader(me_);
            }
            break;

        case RAFT_REQUESTVOTE_ERR_NOT_GRANTED:
            break;

        case RAFT_REQUESTVOTE_ERR_UNKNOWN_NODE:
            if (raft_node_is_voting(raft_get_my_node(me_)) &&
                me->connected == RAFT_NODE_STATUS_DISCONNECTING)
                return RAFT_ERR_SHUTDOWN;
            break;

        default:
            assert(0);
    }

    return 0;
}

int raft_recv_entry_from_client(raft_server_t* me_, raft_entry_t* e, msg_entry_response_t *r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    /* Only one voting cfg change at a time */
    if (raft_entry_is_voting_cfg_change(e))
        if (raft_voting_change_is_in_progress(me_))
            return RAFT_ERR_ONE_VOTING_CHANGE_ONLY;

    if (!raft_is_leader(me_))
        return RAFT_ERR_NOT_LEADER;

    __log(me_, NULL, "received entry t:%d id: %d idx: %d",
          me->current_term, e->id, raft_get_current_idx(me_) + 1);

    raft_entry_t ety;
    ety.term = me->current_term;
    ety.id = e->id;
    ety.type = e->type;
    memcpy(&ety.data, &e->data, sizeof(raft_entry_data_t));
    raft_append_entry(me_, &ety);
    for (i = 0; i < me->num_nodes; i++)
    {
        if (me->raft_myself == me->nodes[i] || !me->nodes[i] ||
            !raft_node_is_voting(me->nodes[i]))
            continue;

        /* Only send new entries.
         * Don't send the entry to peers who are behind, to prevent them from
         * becoming congested. */
        int next_idx = raft_node_get_next_idx(me->nodes[i]);
        if (next_idx == raft_get_current_idx(me_))
            raft_send_appendentries(me_, me->nodes[i]);
    }

    /* if we're the only node, we can consider the entry committed */
    if (1 == raft_get_num_voting_nodes(me_))
        raft_set_commit_idx(me_, raft_get_current_idx(me_));

    r->id = e->id;
    r->idx = raft_get_current_idx(me_);
    r->term = me->current_term;

    if (raft_entry_is_voting_cfg_change(e))
        me->voting_cfg_change_log_idx = raft_get_current_idx(me_);

    return 0;
}

int raft_send_requestvote(raft_server_t* me_, raft_node_t* node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    msg_requestvote_t rv;

    assert(node);
    assert(node != me->raft_myself);

    __log(me_, node, "sending requestvote to: %d", raft_node_get_id(node));

    rv.term = me->current_term;
    rv.last_log_idx = raft_get_current_idx(me_);
    rv.last_log_term = raft_get_last_log_term(me_);
    rv.candidate_id = raft_get_nodeid(me_);
    assert(me->cb.send_requestvote);
    return me->cb.send_requestvote(me_, me->udata, node, &rv);
}

int raft_append_entry(raft_server_t* me_, raft_entry_t* ety)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (raft_entry_is_voting_cfg_change(ety))
        me->voting_cfg_change_log_idx = raft_get_current_idx(me_);

    return log_append_entry(me->log, ety);
}

int raft_apply_entry(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    /* Don't apply after the commit_idx */
    if (me->last_applied_idx == me->commit_idx)
        return -1;

    int log_idx = me->last_applied_idx + 1;

    raft_entry_t* ety = raft_get_entry_from_idx(me_, log_idx);
    if (!ety)
        return -1;

    __log(me_, NULL, "applying log: %d, id: %d size: %d",
          me->last_applied_idx, ety->id, ety->data.len);

    me->last_applied_idx++;
    assert(me->cb.applylog);
    int e = me->cb.applylog(me_, me->udata, ety, me->last_applied_idx - 1);
    if (RAFT_ERR_SHUTDOWN == e)
        return RAFT_ERR_SHUTDOWN;

    /* Membership Change: confirm connection with cluster */
    if (RAFT_LOGTYPE_ADD_NODE == ety->type)
    {
        int node_id = me->cb.log_get_node_id(me_, raft_get_udata(me_), ety, log_idx);
        raft_node_set_has_sufficient_logs(raft_get_node(me_, node_id));
        if (node_id == raft_get_nodeid(me_))
            me->connected = RAFT_NODE_STATUS_CONNECTED;
    }

    /* voting cfg change is now complete */
    if (log_idx == me->voting_cfg_change_log_idx)
        me->voting_cfg_change_log_idx = -1;

    return 0;
}

raft_entry_t* raft_get_entries_from_idx(raft_server_t* me_, int idx, int* n_etys)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    return log_get_from_idx(me->log, idx, n_etys);
}

/* 添加日志请求 Leader除了在收到客户端请求后会发起添加日志请求，还会在周期函数
 * raft_periodic中发起添加日志请求。Leader维护了所有Follower的日志情况，
 * 如果Follower的日志比较旧，就会周期性地给它发送添加日志请求。关于日志怎么同步
 * 和保持一致性的原理，可以阅读raft论文5.3节--日志复制。简单地说就是，
 * Leader在给Follower发送一条日志N时，会顺带将前一条日志M的信息也带过去。
 * Follower会检查请求中前一条日志M的信息与本地相同索引的日志是否吻合，
 * 如果吻合说明本地在M以前的所有日志都是和Leader一致的(raft论文中使用递归法证明，
 * 因为所有日志都是按照同样的规则添加的)。*/
int raft_send_appendentries(raft_server_t* me_, raft_node_t* node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    assert(node);
    assert(node != me->raft_myself);

    /* 初始化请求的参数-- 当前任期号、最新日志索引 */
    msg_appendentries_t ae = {};
    ae.term = me->current_term;
    ae.leader_commit = raft_get_commit_idx(me_);
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;

    /* 根据记录的Follower的日志信息，获取要发给Follower的下一条日志索引 */
    int next_idx = raft_node_get_next_idx(node);

    /* 添加下一条日志的内容*/
    ae.entries = raft_get_entries_from_idx(me_, next_idx, &ae.n_entries);
    //sqj
    //raft_entry_t ret;
    //ret.term = 111;
    //ret.type = 222;
    //ret.id = 333;
    //ret.data.len = 13;
    //ret.data.buf = (void *)"sunqijiang888";
    //ae.entries = &ret;
    //ae.n_entries = 1;

    /* previous log is the log just before the new logs */
    /* 添加要添加日志的前一条日志信息，用来做日志一致性检查，关于怎么保证
     * Leader和Follower日志的一致性，可参看raft论文第5.3节--日志复制*/
    if (1 < next_idx)
    {
        raft_entry_t* prev_ety = raft_get_entry_from_idx(me_, next_idx - 1);
        ae.prev_log_idx = next_idx - 1;
        if (prev_ety)
            ae.prev_log_term = prev_ety->term;
    }

    __log(me_, node, "sending appendentries node: ci:%d comi:%d t:%d lc:%d pli:%d plt:%d #%d",
          raft_get_current_idx(me_),
          raft_get_commit_idx(me_),
          ae.term,
          ae.leader_commit,
          ae.prev_log_idx,
          ae.prev_log_term,
          ae.n_entries);

    /* callback函数，实现网络发送功能，由使用该raft实现的调用者实现网络IO功能*/
    /* 调用callback发送请求，callback由该raft实现的调用者来实现*/
    assert(me->cb.send_appendentries);
    return me->cb.send_appendentries(me_, me->udata, node, &ae);
}

int raft_send_appendentries_all(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i, e;

    me->timeout_elapsed = 0;
    for (i = 0; i < me->num_nodes; i++)
    {
        if (me->raft_myself != me->nodes[i])
        {
            e = raft_send_appendentries(me_, me->nodes[i]);
            if (0 != e)
                return e;
        }
    }

    return 0;
}

raft_node_t* raft_add_node(raft_server_t* me_, void* udata, int id, int is_self)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    __log(me, NULL, "node count = %d, add id = [%d]\n", me->num_nodes, id);

    /* set to voting if node already exists */
    raft_node_t* node = raft_get_node(me_, id);
    if (node)
    {
        if (!raft_node_is_voting(node))
        {
            raft_node_set_voting(node, 1);
            return node;
        }
        else
        {
            /* we shouldn't add a node twice */
            __log(me, NULL, "we shouldn't add a node twice");
            return NULL;
        }
    }

    me->num_nodes++;
    me->nodes = (raft_node_t*)realloc(me->nodes, sizeof(void*) * me->num_nodes);
    me->nodes[me->num_nodes - 1] = raft_node_new(udata, id);

    assert(me->nodes[me->num_nodes - 1]);

    if (is_self)
        me->raft_myself = me->nodes[me->num_nodes - 1];

    return me->nodes[me->num_nodes - 1];
}

raft_node_t* raft_add_non_voting_node(raft_server_t* me_, void* udata, int id, int is_self)
{
    if (raft_get_node(me_, id))
        return NULL;

    raft_node_t* node = raft_add_node(me_, udata, id, is_self);
    if (!node)
        return NULL;

    raft_node_set_voting(node, 0);
    return node;
}

void raft_remove_node(raft_server_t* me_, raft_node_t* node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    raft_node_t* new_array, *new_nodes;
    new_array = (raft_node_t*)calloc((me->num_nodes - 1), sizeof(void*));
    new_nodes = new_array;

    int i, found = 0;
    for (i = 0; i<me->num_nodes; i++)
    {
        if (me->nodes[i] == node)
        {
            found = 1;
            continue;
        }
        *new_nodes = me->nodes[i];
        new_nodes++;
    }

    assert(found);

    me->num_nodes--;
    free(me->nodes);
    me->nodes = new_array;

    free(node);
}

int raft_get_nvotes_for_me(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i, votes;

    for (i = 0, votes = 0; i < me->num_nodes; i++) 
    {
        if (me->raft_myself != me->nodes[i] && raft_node_is_voting(me->nodes[i])) 
        {
            if (raft_node_has_vote_for_me(me->nodes[i])) 
            {
                votes += 1;
            }
        }
    }

    if (me->voted_for == raft_get_nodeid(me_))
        votes += 1;

    return votes;
}

void raft_vote(raft_server_t* me_, raft_node_t* node)
{
    raft_vote_for_nodeid(me_, node ? raft_node_get_id(node) : -1);
}

void raft_vote_for_nodeid(raft_server_t* me_, const int nodeid)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    me->voted_for = nodeid;
    assert(me->cb.persist_vote);
    me->cb.persist_vote(me_, me->udata, nodeid);
}

int raft_msg_entry_response_committed(raft_server_t* me_, const msg_entry_response_t* r)
{
    raft_entry_t* ety = raft_get_entry_from_idx(me_, r->idx);
    if (!ety)
        return 0;

    /* entry from another leader has invalidated this entry message */
    if (r->term != ety->term)
        return -1;
    return r->idx <= raft_get_commit_idx(me_);
}

int raft_apply_all(raft_server_t* me_)
{
    while (raft_get_last_applied_idx(me_) < raft_get_commit_idx(me_))
    {
        int e = raft_apply_entry(me_);
        if (0 != e)
            return e;
    }

    return 0;
}

int raft_entry_is_voting_cfg_change(raft_entry_t* ety)
{
    return RAFT_LOGTYPE_ADD_NODE == ety->type || RAFT_LOGTYPE_DEMOTE_NODE == ety->type;
}

int raft_entry_is_cfg_change(raft_entry_t* ety)
{
    return (
        RAFT_LOGTYPE_ADD_NODE == ety->type ||
        RAFT_LOGTYPE_ADD_NONVOTING_NODE == ety->type ||
        RAFT_LOGTYPE_DEMOTE_NODE == ety->type ||
        RAFT_LOGTYPE_REMOVE_NODE == ety->type);
}

void raft_offer_log(raft_server_t* me_, raft_entry_t* ety, const int idx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!raft_entry_is_cfg_change(ety))
        return;

    int node_id = me->cb.log_get_node_id(me_, raft_get_udata(me_), ety, idx);
    raft_node_t* node = raft_get_node(me_, node_id);
    int is_self = node_id == raft_get_nodeid(me_);

    switch (ety->type)
    {
        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            if (!is_self)
            {
                raft_node_t* node = raft_add_non_voting_node(me_, NULL, node_id, is_self);
                assert(node);
            }
            break;

        case RAFT_LOGTYPE_ADD_NODE:
            node = raft_add_node(me_, NULL, node_id, is_self);
            assert(node);
            assert(raft_node_is_voting(node));
            break;

        case RAFT_LOGTYPE_DEMOTE_NODE:
            raft_node_set_voting(node, 0);
            break;

        case RAFT_LOGTYPE_REMOVE_NODE:
            if (node)
                raft_remove_node(me_, node);
            break;

        default:
            assert(0);
    }
}

void raft_pop_log(raft_server_t* me_, raft_entry_t* ety, const int idx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!raft_entry_is_cfg_change(ety))
        return;

    int node_id = me->cb.log_get_node_id(me_, raft_get_udata(me_), ety, idx);

    switch (ety->type)
    {
        case RAFT_LOGTYPE_DEMOTE_NODE:
            {
            raft_node_t* node = raft_get_node(me_, node_id);
            raft_node_set_voting(node, 1);
            }
            break;

        case RAFT_LOGTYPE_REMOVE_NODE:
            {
            int is_self = node_id == raft_get_nodeid(me_);
            raft_node_t* node = raft_add_non_voting_node(me_, NULL, node_id, is_self);
            assert(node);
            }
            break;

        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            {
            int is_self = node_id == raft_get_nodeid(me_);
            raft_node_t* node = raft_get_node(me_, node_id);
            raft_remove_node(me_, node);
            if (is_self)
                assert(0);
            }
            break;

        case RAFT_LOGTYPE_ADD_NODE:
            {
            raft_node_t* node = raft_get_node(me_, node_id);
            raft_node_set_voting(node, 0);
            }
            break;

        default:
            assert(0);
            break;
    }
}
