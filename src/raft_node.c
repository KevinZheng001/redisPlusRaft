/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief Representation of a peer
 * @author Willem Thiart himself@willemthiart.com
 * @version 0.1
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include "raft.h"

#define RAFT_NODE_VOTED_FOR_ME       1
#define RAFT_NODE_VOTING             (1 << 1)
#define RAFT_NODE_HAS_SUFFICIENT_LOG (1 << 2)

/* raft_node_private_t 集群中机器节点的抽象体，包含了raft协议运行过
 * 程中需要保存的其它机器上的信息 */
typedef struct
{
    /*一般保存与其它机器的连接信息，由使用者决定怎么实现连接*/
    void* node_link;

    /*对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）*/
    int next_idx;
    /*对于每一个服务器，已经复制给他的日志的最高索引值*/
    int match_idx;

    /*有三种取值，是相或的关系 1:该机器有给我投票 2:该机器有投票权  3: 该机器有最新的日志*/
    int flags;

    /*机器对应的id值，这个每台机器在全局都是唯一的*/
    int id;
} raft_node_private_t;

raft_node_t* raft_node_new(void* udata, int id)
{
    raft_node_private_t* node_t;
    node_t = (raft_node_private_t*)calloc(1, sizeof(raft_node_private_t));
    if (!node_t)
        return NULL;
    node_t->node_link = udata;
    node_t->next_idx = 1;
    node_t->match_idx = 0;
    node_t->id = id;
    node_t->flags = RAFT_NODE_VOTING;
    return (raft_node_t*)node_t;
}

int raft_node_get_next_idx(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return me->next_idx;
}

void raft_node_set_next_idx(raft_node_t* me_, int nextIdx)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    /* log index begins at 1 */
    me->next_idx = nextIdx < 1 ? 1 : nextIdx;
}

int raft_node_get_match_idx(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return me->match_idx;
}

void raft_node_set_match_idx(raft_node_t* me_, int matchIdx)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    me->match_idx = matchIdx;
}

void* raft_node_get_link(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return me->node_link;
}

void raft_node_set_link(raft_node_t* me_, void* udata)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    me->node_link = udata;
}

void raft_node_vote_for_me(raft_node_t* me_, const int vote)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    if (vote)
        me->flags |= RAFT_NODE_VOTED_FOR_ME;
    else
        me->flags &= ~RAFT_NODE_VOTED_FOR_ME;
}

int raft_node_has_vote_for_me(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return (me->flags & RAFT_NODE_VOTED_FOR_ME) != 0;
}

void raft_node_set_voting(raft_node_t* me_, int voting)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    if (voting)
        me->flags |= RAFT_NODE_VOTING;
    else
        me->flags &= ~RAFT_NODE_VOTING;
}

int raft_node_is_voting(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return (me->flags & RAFT_NODE_VOTING) != 0;
}

void raft_node_set_has_sufficient_logs(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    me->flags |= RAFT_NODE_HAS_SUFFICIENT_LOG;
}

int raft_node_has_sufficient_logs(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return (me->flags & RAFT_NODE_HAS_SUFFICIENT_LOG) != 0;
}

int raft_node_get_id(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return me->id;
}
