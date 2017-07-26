#ifndef _RAFT_CUSTER_H
#define _RAFT_CUSTER_H
#include <stdio.h>
#include <string.h>

#include "raft.h"  
#include "raft_log.h"
#include "raft_private.h"

#include "lmdb.h"
#include "lmdb_helpers.h"

#define RAFT_PORT_INCR 30000 /* RAFT port = baseport + PORT_INCR */
#define PERIOD_MSEC 1000
//#define IP_STR_LEN strlen("111.111.111.111")
#define RAFT_BUFLEN 512 
#define IP_STR_LEN 78 

void raft_v();
void raftCron(void);
void raftInit(void);

typedef enum {
    HANDSHAKE_FAILURE,
    HANDSHAKE_SUCCESS,
} handshake_state_e;

/** Message types used for peer to peer traffic
 *  * These values are used to identify message types during deserialization */
typedef enum
{
    /** Handshake is a special non-raft message type
 *      * We send a handshake so that we can identify ourselves to our peers */
    MSG_HANDSHAKE,
    /** Successful responses mean we can start the Raft periodic callback */
    MSG_HANDSHAKE_RESPONSE,
    /** Tell leader we want to leave the cluster */
    /* When instance is ctrl-c'd we have to gracefuly disconnect */
    MSG_LEAVE,
    /* Receiving a leave response means we can shutdown */
    MSG_LEAVE_RESPONSE,
    MSG_REQUESTVOTE,
    MSG_REQUESTVOTE_RESPONSE,
    MSG_APPENDENTRIES,
    MSG_APPENDENTRIES_RESPONSE,
} raft_message_type_e;

/** Peer protocol handshake
 ** Send handshake after connecting so that our peer can identify us */
typedef struct
{
    int raft_port;
//    int http_port;
    int node_id;
} msg_handshake_t;

typedef struct
{
    int success;

    /* leader's Raft port */
    int leader_port;

    /* the responding node's HTTP port */
    //int http_port;

    /* my Raft node ID.
     ** Sometimes we don't know who we did the handshake with */
    int node_id;

    char leader_host[IP_STR_LEN];
} msg_handshake_response_t;

/** Add/remove Raft peer */
typedef struct
{
    int raft_port;
//    int http_port;
    int node_id;
    char host[IP_STR_LEN];
} entry_cfg_change_t;

union raft_clusterMsgData {
    msg_handshake_t hs;
    msg_handshake_response_t hsr;
    msg_requestvote_t rv;
    msg_requestvote_response_t rvr;
    msg_appendentries_t ae;
    msg_appendentries_response_t aer;
};

typedef struct
{
    char sig[4]; /* Siganture "Raft" */
    uint32_t totlen;  /* Total length of this message */
    uint32_t type;
    uint32_t padding[100];
    union raft_clusterMsgData data;
} raft_clusterMsg;

#define RAFT_CLUSTERMSG_MIN_LEN (sizeof(raft_clusterMsg)-sizeof(union raft_clusterMsgData))

typedef enum
{
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
} conn_status_e;

typedef struct peer_connection_s raft_link_t;

struct peer_connection_s
{
    /* peer's address */
    char peer_ip[NET_IP_STR_LEN];
    int peer_port;
    int raft_port;

    int fd;                     /* TCP socket file descriptor */

    sds sndbuf;                 /* Packet send buffer */
    sds rcvbuf;                 /* Packet reception buffer */

    /* tell if we need to connect or not */
    conn_status_e connection_status;

    /* number of entries currently expected.
     * this counts down as we consume entries */
    int n_expected_entries;

    /* remember most recent append entries msg, we refer to this msg when we
     * finish reading the log entries.
     * used in tandem with n_expected_entries */
    raft_clusterMsg ae;

    /* peer's raft node_idx */
    raft_node_t* node;

    raft_link_t *next;
    int wtype;
};

//typedef struct clusterLink {
//    mstime_t ctime;             /* Link creation time */
//    int fd;                     /* TCP socket file descriptor */
//    sds sndbuf;                 /* Packet send buffer */
//    sds rcvbuf;                 /* Packet reception buffer */
//    struct clusterNode *node;   /* Node related to this link if any, or NULL */
//} clusterLink;
typedef struct {
    /* the server's node ID */
    int node_id;

    /* Set of tickets that have been issued
     * We store unsigned ints in here */
    MDB_dbi tickets;

    /* Persistent state for voted_for and term
     * We store string keys (eg. "term") with int values */
    MDB_dbi state;

    /* Entries that have been appended to our log
     * For each log entry we store two things next to each other:
     *  - TPL serialized raft_entry_t
     *  - raft_entry_data_t */
    MDB_dbi entries;

    /* LMDB database environment */
    MDB_env *db_env;

    raft_server_t* raft;

    raft_link_t* conns;

    long long stats_bus_messages_sent;  /* Num of msg sent via cluster bus. */
    long long stats_bus_messages_received; /* Num of msg rcvd via cluster bus.*/

} raft_cluster_state;

#endif
