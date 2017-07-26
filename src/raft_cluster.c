#include "server.h" // must include at first line
#include "raft_cluster.h"
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <math.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <syslog.h>
#include <netinet/in.h>
#include <lua.h>

#include "endianconv.h"

static const char * type2string(int type) {
    switch(type) {
    case MSG_HANDSHAKE:              return "MSG_HANDSHAKE";               
    case MSG_HANDSHAKE_RESPONSE:     return "MSG_HANDSHAKE_RESPONSE";
    case MSG_LEAVE:                  return "MSG_LEAVE";
    case MSG_LEAVE_RESPONSE:         return "MSG_LEAVE_RESPONSE";
    case MSG_REQUESTVOTE:            return "MSG_REQUESTVOTE";
    case MSG_REQUESTVOTE_RESPONSE:   return "MSG_REQUESTVOTE_RESPONSE";
    case MSG_APPENDENTRIES:          return "MSG_APPENDENTRIES";
    case MSG_APPENDENTRIES_RESPONSE: return "MSG_APPENDENTRIES_RESPONSE";
    default: return "unknown";
    }
}

const char raft_cluster_version[] = PACKAGE_NAME " version " PACKAGE_VERSION " built at " BUILT_DATE;

void _raft_on_tcpserver_accept_handler(aeEventLoop *el, int fd, void *privdata, int mask);
void __raft_on_netprotocol_read_handler(aeEventLoop *el, int fd, void *privdata, int mask);

raft_cluster_state raft_server;
raft_cluster_state *raftsv = &raft_server;

void raft_v() {
    printf("%s\n", raft_cluster_version);
}

const char *connectionStatus[] = {"DISCONNECTED","CONNECTING","CONNECTED"};

void __new_db(raft_cluster_state* raftsv)
{
    char n[100];
    sprintf(n,"store-%d.mdb",server.port);
    mdb_db_env_create(&raftsv->db_env, 0, n, 1000);
    mdb_db_create(&raftsv->entries, raftsv->db_env, "entries");
    mdb_db_create(&raftsv->tickets, raftsv->db_env, "docs");
    mdb_db_create(&raftsv->state, raftsv->db_env, "state");
}   

void print_connection(raft_link_t *conn) {
    if (conn) {
        printf("--fd = %d\n",conn->fd); 
        printf("--ip:port = %.*s:%d\n",NET_IP_STR_LEN,conn->peer_ip,conn->peer_port); 
        printf("--connection_status = %s\n",connectionStatus[conn->connection_status]);
        printf("--raft_port = %d\n",conn->raft_port);
    }
}

/** Raft callback for saving term field to disk.
 *  * This only returns when change has been made to disk. */
int __raft_persist_term(
    raft_server_t* raft,
    void *udata,
    const int current_term
    )
{
    printf("__raft_persist_term  current_term = %d\n", current_term);
    return mdb_puts_int_commit(raftsv->db_env, raftsv->state, "term", current_term);
}

/** Raft callback for saving voted_for field to disk.
 *  * This only returns when change has been made to disk. */
int __raft_persist_vote(
    raft_server_t* raft,
    void *udata,
    const int voted_for
    )
{
    printf("__raft_persist_vote voted_for = %d\n", voted_for);
    return mdb_puts_int_commit(raftsv->db_env, raftsv->state, "voted_for", voted_for);
}

/** Load voted_for and term raft fields */
void __load_persistent_state(raft_cluster_state* raftsv)
{
    int val = -1;

    mdb_gets_int(raftsv->db_env, raftsv->state, "voted_for", &val);
    raft_vote_for_nodeid(raftsv->raft, val);
    mdb_gets_int(raftsv->db_env, raftsv->state, "term", &val);
    raft_set_current_term(raftsv->raft, val);
}

/** Raft callback for applying an entry to the finite state machine */
int __raft_applylog(raft_server_t* raft, void *udata, raft_entry_t *ety)
{
    return 0;
}
raft_link_t* __find_connection(raft_cluster_state* raftsv, const char* host, int raft_port)
{
    raft_link_t* conn;
    for (conn = raftsv->conns;
         conn && (0 != strcmp(host, conn->peer_ip) || conn->raft_port != raft_port);
         conn = conn->next)
        ;
    return conn;
}

void __raft_log(raft_server_t* raft, raft_node_t* node, void *udata, const char *buf)
{
//    if (opts.debug)
    //printf("raft: %s\n", buf);
}

/** Raft callback for sending request vote message */
int __raft_send_requestvote(
    raft_server_t* raft,
    void *user_data,
    raft_node_t *node,
    msg_requestvote_t* m
    )
{
//    printf("__raft_send_requestvote\n");
    raft_link_t* conn = raft_node_get_link(node);

    if (NULL == conn) {
        printf("conn is NULL\n");
        return -1;
    }
    int e = __connect_if_needed(conn);
    if (-1 == e) {
//        printf("__connect_if_needed failed\n");
        return 0;
    }

    unsigned char buf[sizeof(raft_clusterMsg)];
    raft_clusterMsg *msg = (raft_clusterMsg*) buf;

    raftBuildMessageHdr(msg, MSG_REQUESTVOTE);
    msg->data.rv = *m;
    msg->totlen = htonl(sizeof(*msg));
    raftClusterSendMessage(conn,buf,ntohl(msg->totlen));
    return 0;
}

/** Raft callback for sending appendentries message */
int __raft_send_appendentries(
    raft_server_t* raft,
    void *user_data,
    raft_node_t *node,
    msg_appendentries_t* m
    )
{
    raft_link_t* conn = raft_node_get_link(node);

    if (NULL == conn) {
        printf("conn is NULL\n");
        return -1;
    }

    int e = __connect_if_needed(conn);
    if (-1 == e)
        return 0;

    //char buf[RAFT_BUFLEN], *ptr = buf;
    //raft_clusterMsg msg = {};
    //msg.type = MSG_APPENDENTRIES;
    //msg.ae.term = m->term;
    //msg.ae.prev_log_idx   = m->prev_log_idx;
    //msg.ae.prev_log_term = m->prev_log_term;
    //msg.ae.leader_commit = m->leader_commit;
    //msg.ae.n_entries = m->n_entries;
    //ptr += __peer_msg_serialize(tpl_map("S(I$(IIIII))", &msg), bufs, ptr);

    unsigned char buf[sizeof(raft_clusterMsg)*2];
    raft_clusterMsg *msg = (raft_clusterMsg*) buf;

    raftBuildMessageHdr(msg, MSG_APPENDENTRIES);
    msg->data.ae.term = m->term;
    msg->data.ae.prev_log_idx   = m->prev_log_idx;
    msg->data.ae.prev_log_term = m->prev_log_term;
    msg->data.ae.leader_commit = m->leader_commit;
    msg->data.ae.n_entries = m->n_entries;
    int datalen = 0;
    if (m->n_entries > 0) {
            raft_entry_t *f =  m->entries;
            datalen = f->data.len;
            printf("nnnnnn%d,%d,%d,%d,%s\n",f->term,
            f->type,
            f->id,
            f->data.len,
            f->data.buf);

          msg->data.ae.entries = (raft_entry_t*) (&(msg->data.ae.n_entries)+sizeof(int));
          msg->data.ae.entries->term = f->term;
          msg->data.ae.entries->type = f->type;
          msg->data.ae.entries->id = f->id;
          msg->data.ae.entries->data.len = datalen;
          printf("dl = %d\n",msg->data.ae.entries->data.len);
//        memcpy(msg->data.ae.entries,m->entries,sizeof(raft_entry_t));
          msg->data.ae.entries->data.buf = &(msg->data.ae.entries->data.len)+sizeof(unsigned int); 
          memcpy(msg->data.ae.entries->data.buf,m->entries->data.buf,datalen);
          printf("buffff = %.*s\n",datalen,msg->data.ae.entries->data.buf);
    }
    if (datalen > sizeof(void*)) 
        datalen -= sizeof(void*);
    msg->totlen = htonl(sizeof(*msg)+datalen);
    raftClusterSendMessage(conn,buf,ntohl(msg->totlen));
    /* appendentries with payload */
    //if (0 < m->n_entries)
    //{
    //    tpl_bin tb = {
    //        .sz   = m->entries[0].data.len,
    //        .addr = m->entries[0].data.buf
    //    };

    //    /* list of entries */
    //    tpl_node *tn = tpl_map("IIIB",
    //            &m->entries[0].id,
    //            &m->entries[0].term,
    //            &m->entries[0].type,
    //            &tb);
    //    size_t sz;
    //    tpl_pack(tn, 0);
    //    tpl_dump(tn, TPL_GETSIZE, &sz);
    //    e = tpl_dump(tn, TPL_MEM | TPL_PREALLOCD, ptr, RAFT_BUFLEN);
    //    assert(0 == e);
    //    bufs[1].len = sz;
    //    bufs[1].base = ptr;
    //    e = uv_try_write(conn->stream, bufs, 2);
    //    if (e < 0)
    //        uv_fatal(e);

    //    tpl_free(tn);
    //}
    //else
    //{
    //    /* keep alive appendentries only */
    //    e = uv_try_write(conn->stream, bufs, 1);
    //    if (e < 0)
    //        uv_fatal(e);
    //}

    return 0;
}

/* Send data. This is handled using a trivial send buffer that gets
 * consumed by write(). We don't try to optimize this for speed too much
 * as this is a very low traffic channel. */
void raftClusterWriteHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    raft_link_t *conn = (raft_link_t*) privdata;
    char bufip[NET_IP_STR_LEN];
    int port;
    ssize_t nwritten;
    UNUSED(el);
    UNUSED(mask);

    nwritten = write(fd, conn->sndbuf, sdslen(conn->sndbuf));
    if (nwritten <= 0) {
        serverLog(LL_DEBUG,"I/O error writing to node conn: %s", strerror(errno));
        handleConnectionIOError(conn);
        return;
    } 
    else {
        //serverLog(LL_DEBUG,"send data  %d\n",nwritten);
    }
    sdsrange(conn->sndbuf,nwritten,-1);
    if (sdslen(conn->sndbuf) == 0)
        aeDeleteFileEvent(server.el, conn->fd, AE_WRITABLE);
    anetPeerToString(fd, bufip, NET_IP_STR_LEN, &port);
    serverLog(LL_DEBUG,"--->> write [%s] to conn [%s:%d remote port-(%d)]", type2string(conn->wtype), bufip, conn->raft_port, port);
}

/* Put stuff into the send buffer.
 *  *
 *   * It is guaranteed that this function will never have as a side effect
 *    * the link to be invalidated, so it is safe to call this function
 *     * from event handlers that will do stuff with the same link later. */
void raftClusterSendMessage(raft_link_t *conn, unsigned char *msg, size_t msglen) {
    if (sdslen(conn->sndbuf) == 0 && msglen != 0) {
        raft_clusterMsg *hdr = (raft_clusterMsg*) msg;
//        printf("send message -->>>%s\n", type2string(ntohl(hdr->type)));
        conn->wtype = ntohl(hdr->type);
        aeCreateFileEvent(server.el,conn->fd, AE_WRITABLE,raftClusterWriteHandler,conn);
    }

    conn->sndbuf = sdscatlen(conn->sndbuf, msg, msglen);
    raft_server.stats_bus_messages_sent++;
}

/* Build the message header. hdr must point to a buffer at least
 *  * sizeof(clusterMsg) in bytes. */
void raftBuildMessageHdr(raft_clusterMsg *hdr, int type) {
    int totlen = 0;
    uint64_t offset;

    memset(hdr,0,sizeof(*hdr));
    //hdr->ver = htons(CLUSTER_PROTO_VER);
    hdr->sig[0] = 'R';
    hdr->sig[1] = 'a';
    hdr->sig[2] = 'f';
    hdr->sig[3] = 't';
    hdr->type = htonl(type);
}

void __send_leave(raft_link_t* conn)
{
    unsigned char buf[sizeof(raft_clusterMsg)];
    raft_clusterMsg *msg = (raft_clusterMsg*) buf;
    //uv_buf_t bufs[1];
    //char buf[RAFT_BUFLEN];
    //raft_clusterMsg msg = {};
    raftBuildMessageHdr(msg, MSG_LEAVE);
    //msg->type = MSG_LEAVE;
    msg->totlen = htonl(sizeof(msg));
    raftClusterSendMessage(conn,buf,ntohl(msg->totlen));
    //__peer_msg_send(conn->stream, tpl_map("S(I)", &msg), &bufs[0], buf);
}

int __send_handshake_response(raft_link_t* conn, handshake_state_e success, raft_node_t* leader)
{
    unsigned char buf[sizeof(raft_clusterMsg)];
    raft_clusterMsg *msg = (raft_clusterMsg*) buf;

    raftBuildMessageHdr(msg, MSG_HANDSHAKE_RESPONSE);

    msg->data.hsr.success = htonl(success);
    msg->data.hsr.node_id = htonl(raftsv->node_id);
//    msg->data.hsr.leader_port = htonl(1987);
//    strcpy(msg->data.hsr.leader_host, "192.168.1.113");

    /* allow the peer to redirect to the leader */
    if (leader)
    {
        raft_link_t* leader_conn = raft_node_get_link(leader);
        if (NULL == leader_conn) {
            printf("conn is NULL\n");
            return -1;
        }
        if (leader_conn)
        {
            msg->data.hsr.leader_port = htonl(leader_conn->raft_port);
            snprintf(msg->data.hsr.leader_host, IP_STR_LEN, "%s", leader_conn->peer_ip);
        }
    }

//    msg.hsr.http_port = atoi(opts.http_port);
    msg->totlen = htonl(sizeof(*msg));
    raftClusterSendMessage(conn,buf,ntohl(msg->totlen));
}

void __send_handshake(raft_link_t* conn) {
    unsigned char buf[sizeof(raft_clusterMsg)];
    raft_clusterMsg *msg = (raft_clusterMsg*) buf;

    //char bufs[1];
    //char buf[RAFT_BUFLEN];
    //raft_clusterMsg msg = {};
    raftBuildMessageHdr(msg, MSG_HANDSHAKE);
    //msg->type = MSG_HANDSHAKE;
    msg->data.hs.raft_port = htonl(server.port+RAFT_PORT_INCR);
//    msg.hs.http_port = atoi(opts.http_port);
    msg->data.hs.node_id = htonl(raftsv->node_id);
    msg->totlen = htonl(sizeof(*msg));
    raftClusterSendMessage(conn,buf,ntohl(msg->totlen));
//    __peer_msg_send(conn->stream, tpl_map("S(I$(IIII))", &msg), &bufs[0], buf);
}

void __on_connection_accepted_by_peer(aeEventLoop *el, int fd, void *privdata, int mask) {
    int sockerr = 0, psync_result;
    raft_link_t *conn = (raft_link_t *) privdata;
    socklen_t errlen = sizeof(sockerr);

    /* Check for errors in the socket. */
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &sockerr, &errlen) == -1) {
        serverLog(LL_WARNING,"failed in getsockopt: %s", strerror(sockerr));
        sockerr = errno;
    }
    //printf("errno = %d\n",errno);
    if (sockerr) {
        serverLog(LL_WARNING,"Error condition on socket for __on_connection_accepted_by_peer: %s", strerror(sockerr));
        goto error;
    }

    if (conn->connection_status == CONNECTING) {
        aeDeleteFileEvent(server.el, fd, AE_WRITABLE|AE_READABLE);

        __send_handshake(conn);
        ///* start reading from peer */
        conn->connection_status = CONNECTED;
        aeCreateFileEvent(server.el, fd, AE_READABLE, __raft_on_netprotocol_read_handler, conn);
        return;
    }
    return;

error:
    aeDeleteFileEvent(server.el, fd, AE_READABLE|AE_WRITABLE);
    close(fd);
    return;
}

void __connection_set_peer(raft_link_t* conn, char* host, int port)
{
    conn->raft_port = port;
    printf("Connecting to %s:%d\n", host, port);
    memcpy(conn->peer_ip, host, sizeof(conn->peer_ip));
}

int __connect_to_peer(raft_link_t* conn)
{
    int fd;
    fd = anetTcpNonBlockBestEffortBindConnect(NULL, conn->peer_ip, conn->raft_port, NULL);
    if (fd == -1) {
        serverLog(LL_WARNING, "Unable to connect to raft node: %s", strerror(errno));
        return C_ERR;
    }
    else {
//        serverLog(LL_WARNING, "create fd : %d", fd);
    }

    if (aeCreateFileEvent(server.el, fd, AE_READABLE | AE_WRITABLE, __on_connection_accepted_by_peer, conn) == AE_ERR)
    {
        close(fd);
        serverLog(LL_WARNING,"Can't create writable event for connect_to_peer");
        return C_ERR;
    }
    else
    {
//        serverLog(LL_WARNING,"create AE_READABLE | AE_WRITABLE event for fd = %d", fd);
    }

    conn->connection_status = CONNECTING;
    conn->fd = fd;
//    print_connection(conn);
    return C_OK;
}

void __connect_to_peer_at_host(raft_link_t* conn, char* host, int port)
{
    __connection_set_peer(conn, host, port);
    __connect_to_peer(conn);
}

/** Initiate connection if we are disconnected */
int __connect_if_needed(raft_link_t* conn)
{
    if (CONNECTED != conn->connection_status) {
        if (DISCONNECTED == conn->connection_status) {
            __connect_to_peer(conn);
        }
        return -1;
    }
    return 0;
}

raft_link_t* __new_connection(raft_cluster_state* sv)
{
    //printf("__new_connection\n");
    raft_link_t* conn = calloc(1, sizeof(raft_link_t));

    conn->sndbuf = sdsempty();
    conn->rcvbuf = sdsempty();
    conn->node = NULL;
    conn->fd = -1;

    //conn->loop = &sv->peer_loop;
    conn->next = sv->conns;
    sv->conns = conn;
    return conn;
}

//int nodelist[6] = {8000, 8001, 8002, 8003, 8004, 8005};
int nodelist[3] = {8000, 8001, 8002};
#define MAX_CLUSTER_ACCEPTS_PER_CALL 1000
void _raft_on_tcpserver_accept_handler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int raftport, raftfd;
    int max = MAX_CLUSTER_ACCEPTS_PER_CALL;
    char raftip[NET_IP_STR_LEN];
    //raft_link_t *link;
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    ///* If the server is starting up, don't accept cluster connections:
    // * UPDATE messages may interact with the database content. */
    //if (server.masterhost == NULL && server.loading) return;

    while(max--) {
        raftfd = anetTcpAccept(server.neterr, fd, raftip, sizeof(raftip), &raftport);
        if (raftfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_VERBOSE, "Error accepting raft node: %s", server.neterr);
            return;
        }
        anetNonBlock(NULL,raftfd);
        anetEnableTcpNoDelay(NULL,raftfd);
    //    // anetTcpAccept anetSockName之后的本地监听就是服务器的监听端口

    //    /* Use non-blocking I/O for cluster messages. */
        serverLog(LL_WARNING,"New connection, Accepted raft node from %s:%d", raftip, raftport);
    //    /* 创建一个链接对象，我们用来处理连接。当数据可用时，它被传递给可读的处理器。
    //     * 初始时link->node指针被设置为空，因为我们不知道是哪个节点，但一旦我们知道节点的身份*/
    //    /* Create a link object we use to handle the connection.
    //     * It gets passed to the readable handler when data is available.
    //     * Initiallly the link->node pointer is set to NULL as we don't know
    //     * which node is, but the right node is references once we know the
    //     * node identity. */
    //    link = createClusterLink(NULL);
    //    link->fd = raftfd;
        //aeCreateFileEvent(server.el, raftfd, AE_READABLE, __raft_on_netprotocol_read_handler, link);
        raft_link_t* conn = __new_connection(raftsv);
        conn->node = NULL;
        conn->fd = raftfd;

        int port;
        anetPeerToString(raftfd, conn->peer_ip, NET_IP_STR_LEN, &conn->peer_port);

//        printf("111111111\n");
//        print_connection(conn);

        if (aeCreateFileEvent(server.el, raftfd, AE_READABLE, __raft_on_netprotocol_read_handler, conn) == AE_ERR) {
            serverPanic("Unrecoverable error creating file event.");
        }
        //aeCreateFileEvent(server.el, raftfd, AE_READABLE, __raft_on_netprotocol_read_handler, (void*)raftfd);
    }
}

int __append_cfg_change(raft_cluster_state* raftsv,
                               raft_logtype_e change_type,
                               char* host,
                               int raft_port, int http_port,
                               int node_id)
{
    entry_cfg_change_t *change = calloc(1, sizeof(*change));
    change->raft_port = raft_port;
    //change->http_port = http_port;
    change->node_id = node_id;
    strcpy(change->host, host);
    change->host[IP_STR_LEN-1] = 0;

    raft_entry_t entry;
    entry.id = rand();
    entry.type = change_type;
    entry.data.buf = (void*)change;
    entry.data.len = sizeof(*change);

    msg_entry_response_t r;

    int e = raft_recv_entry_from_client(raftsv->raft, &entry, &r);
    if (0 != e)
        return -1;
    return 0;
}

/** Non-voting node now has enough logs to be able to vote.
 *  * Append a finalization cfg log entry. */
void __raft_node_has_sufficient_logs(
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node)
{
    raft_link_t* conn = raft_node_get_link(node);
    __append_cfg_change(raftsv, RAFT_LOGTYPE_ADD_NODE,
                        conn->peer_ip,
                        conn->raft_port,
                        0,//conn->http_port,
                        raft_node_get_id(conn->node));
}

/** Raft callback for removing the first entry from the log
 *  * @note this is provided to support log compaction in the future */
int __raft_logentry_poll(
    raft_server_t* raft,
    void *udata,
    raft_entry_t *entry,
    int ety_idx
    )
{
    MDB_val k, v;

    mdb_poll(raftsv->db_env, raftsv->entries, &k, &v);

    return 0;
}

/** Raft callback for deleting the most recent entry from the log.
 *  * This happens when an invalid leader finds a valid leader and has to delete
 *   * superseded log entries. */
int __raft_logentry_pop(
    raft_server_t* raft,
    void *udata,
    raft_entry_t *entry,
    int ety_idx
    )
{
    MDB_val k, v;

    mdb_pop(raftsv->db_env, raftsv->entries, &k, &v);

    return 0;
}

raft_cbs_t raft_funcs = {
    .send_requestvote            = __raft_send_requestvote,
    .send_appendentries          = __raft_send_appendentries,
    .applylog                    = __raft_applylog,
    .persist_vote                = __raft_persist_vote,
    .persist_term                = __raft_persist_term,
    //.log_offer                   = __raft_logentry_offer,
    .log_poll                    = __raft_logentry_poll,
    .log_pop                     = __raft_logentry_pop,
    .node_has_sufficient_logs    = __raft_node_has_sufficient_logs,
    .log                         = __raft_log
};

/* When this function is called, there is a packet to process starting
 * at node->rcvbuf. Releasing the buffer is up to the caller, so this
 * function should just handle the higher level stuff of processing the
 * packet, modifying the cluster state if needed.
 *
 * The function returns 1 if the link is still valid after the packet
 * was processed, otherwise 0 if the link was freed since the packet
 * processing lead to some inconsistency error (for instance a PONG
 * received from the wrong sender ID). */
int raft_clusterProcessPacket(raft_link_t *conn) {
 //   printf("raft_clusterProcessPacket\n");
    raft_clusterMsg *hdr = (raft_clusterMsg*) conn->rcvbuf;
    uint32_t totlen = ntohl(hdr->totlen);
    uint32_t type = ntohl(hdr->type);

    raftsv->stats_bus_messages_received++;

    char peerip[NET_IP_STR_LEN];
    int peerport;

    anetPeerToString(conn->fd, peerip, NET_IP_STR_LEN, &peerport);

    serverLog(LL_DEBUG,"<<--- receiving [%s] (%d), %lu bytes from %.*s:%d",
              type2string(type), type, (unsigned long) totlen, NET_IP_STR_LEN, peerip, peerport);

//    printf("totlen = %d, type = %d\n",totlen,type);
    switch (type)
    {
        case MSG_HANDSHAKE:
             {
                 int rport = ntohl(hdr->data.hs.raft_port);
                 int nodeid = ntohl(hdr->data.hs.node_id);
//                 printf("raft_port = %d, node_id = %d\n", rport, nodeid);

                 raft_link_t* nconn = __find_connection(raftsv, conn->peer_ip, rport);
                 if (nconn && conn != nconn)
                     __delete_connection(raftsv, nconn);

                 conn->connection_status = CONNECTED;
                 printf("node %.*s:%d +ON\n", NET_IP_STR_LEN, peerip, rport);
                 //conn->http_port = m.hs.http_port;
                 conn->raft_port = rport;

                 raft_node_t* leader = raft_get_current_leader_node(raftsv->raft);

                 /* Is this peer in our configuration already? */
                 raft_node_t* node = raft_get_node(raftsv->raft, nodeid);
                 if (node)
                 {
                     raft_node_set_link(node, conn);
                     conn->node = node;
                 }

                 if (!leader)
                 {
                     return __send_handshake_response(conn, HANDSHAKE_FAILURE, NULL);
                 }
                 else if (raft_node_get_id(leader) != raftsv->node_id)
                 {
                     return __send_handshake_response(conn, HANDSHAKE_FAILURE, leader);
                 }
                 else if (node)
                 {
                     return __send_handshake_response(conn, HANDSHAKE_SUCCESS, NULL);
                 }
                 else
                 {
                     int e = __append_cfg_change(raftsv, 
                                                 RAFT_LOGTYPE_ADD_NONVOTING_NODE,
                                                 conn->peer_ip,
                                                 rport, 
                                                 //m.hs.http_port,
                                                 0,
                                                 nodeid);
                     if (0 != e)
                         return __send_handshake_response(conn, HANDSHAKE_FAILURE, NULL);
                     return __send_handshake_response(conn, HANDSHAKE_SUCCESS, NULL);
                 }
             }
             break;
        case MSG_HANDSHAKE_RESPONSE:
             {
                 int success = ntohl(hdr->data.hsr.success);
                 int lport = ntohl(hdr->data.hsr.leader_port);
                 int nodeid = ntohl(hdr->data.hsr.node_id);
   //              printf("success = %d, nodeid = %d, leader_addr = %.*s:%d\n",
   //                      success, nodeid, IP_STR_LEN, hdr->data.hsr.leader_host, lport); 
                if (0 == success)
                {
                    //conn->http_port = m.hsr.http_port;

                    /* We're being redirected to the leader */
                    if (lport)
                    {
                        raft_link_t* nconn =
                            __find_connection(raftsv, hdr->data.hsr.leader_host, lport);
                        if (!nconn)
                        {
                            nconn = __new_connection(raftsv);
                            printf("Redirecting to %s:%d...\n",
                                hdr->data.hsr.leader_host, lport);
                            __connect_to_peer_at_host(nconn, hdr->data.hsr.leader_host, lport);
                        }
                    }
                }
                else {
                    printf("Connected to leader: %s:%d\n",conn->peer_ip, conn->raft_port);
                    if (!conn->node)
                        conn->node = raft_get_node(raftsv->raft, nodeid);
                }
             }
             break;
        case MSG_REQUESTVOTE:
            {
                unsigned char buf[sizeof(raft_clusterMsg)];
                raft_clusterMsg *msg = (raft_clusterMsg*) buf;
                raftBuildMessageHdr(msg, MSG_REQUESTVOTE_RESPONSE);
                printf("requestvote term = %d, candidate_id = %d, last_log_idx = %d, last_log_term = %d\n", 
                       hdr->data.rv.term, 
                       hdr->data.rv.candidate_id, 
                       hdr->data.rv.last_log_idx, 
                       hdr->data.rv.last_log_term);

                int e = raft_recv_requestvote(raftsv->raft, conn->node, &hdr->data.rv, &msg->data.rvr);
                msg->totlen = htonl(sizeof(*msg));
                printf("vote response term = %d, vote_granted = %d\n",
                        msg->data.rvr.term, msg->data.rvr.vote_granted);
                raftClusterSendMessage(conn,buf,ntohl(msg->totlen));
//                __peer_msg_send(conn->stream, tpl_map("S(I$(IIII))", &msg), &bufs[0], buf);
            }
            break;
        case MSG_REQUESTVOTE_RESPONSE:
            {
               int e = raft_recv_requestvote_response(raftsv->raft, conn->node, &hdr->data.rvr);
            }
            break;
        case MSG_APPENDENTRIES:
            {
                /* special case: get ready to handle appendentries payload */
                if (0 < hdr->data.ae.n_entries)
                {
                    printf("rrrrrr\n");
                    raft_entry_t *ret = (raft_entry_t *)(&(hdr->data.ae.n_entries)+
                                         sizeof(hdr->data.ae.n_entries));
                    void *buf = (void *)(&ret->data.len + sizeof(ret->data.len));
                    printf("lll = %d\n",ret->data.len);
                    printf("  term = %d\n",ret->term);
                    printf("  id = %d\n",ret->id);
                    printf("  type = %d\n",ret->type);
                    printf(" buf = %.*s\n",ret->data.len, buf);
                    //printf("len = %d\n",
                    //    hdr->data.ae.entries->data.len);
                    //printf("sss = %.*s\n",
                    //    hdr->data.ae.entries->data.len,
                    //    hdr->data.ae.entries->data.buf);
                    conn->n_expected_entries = hdr->data.ae.n_entries;
                    //memcpy(&conn->ae, hdr, sizeof(raft_clusterMsg));
                    //memcpy(&conn->ae, hdr, totlen);
                    printf("ooooooo\n");
                    return 0;
                }

                /* this is a keep alive message */
                unsigned char buf[sizeof(raft_clusterMsg)];
                raft_clusterMsg *msg = (raft_clusterMsg*) buf;
                raftBuildMessageHdr(msg, MSG_APPENDENTRIES_RESPONSE);
                msg->totlen = htonl(sizeof(*msg));
                int e = raft_recv_appendentries(raftsv->raft, conn->node, &hdr->data.ae, &msg->data.aer);
                raftClusterSendMessage(conn,buf,ntohl(msg->totlen));
            }
            break;
        case MSG_APPENDENTRIES_RESPONSE:
            {
                int e = raft_recv_appendentries_response(raftsv->raft, conn->node, &hdr->data.aer);
                //uv_cond_signal(&raftsv->appendentries_received);
            }
            break;

        default:
            printf("unknown msg\n");

    }
    return 1;
}

void __delete_connection(raft_cluster_state* raftsv, raft_link_t* conn)
{
    //printf("delete_connection %p\n",conn);
    raft_link_t* prev = NULL;
    if (raftsv->conns == conn)
        raftsv->conns = conn->next;
    else if (raftsv->conns != conn)
    {
        for (prev = raftsv->conns; prev->next != conn; prev = prev->next);
        prev->next = conn->next;
    }
    else
        assert(0);

    if (conn->node)
        raft_node_set_link(conn->node, NULL);

    // TODO: make sure all resources are freed
    free(conn);
}

void freeRaftClusterConnection(raft_link_t *link) {
    if (link->fd != -1) {
        aeDeleteFileEvent(server.el, link->fd, AE_WRITABLE);
        aeDeleteFileEvent(server.el, link->fd, AE_READABLE);
    }
    link->connection_status = DISCONNECTED;
    sdsfree(link->sndbuf);
    sdsfree(link->rcvbuf);
    //if (link->node) {
    //    // set to NULL 保证下次能够去重新连接服务器
    //    link->node->link = NULL;
    //}
    close(link->fd);
    //__delete_connection(raftsv, link);
}

/* This function is called when we detect the link with this node is lost.
   We set the node as no longer connected. The Cluster Cron will detect
   this connection and will try to get it connected again.

   Instead if the node is a temporary node used to accept a query, we
   completely free the node on error. */
void handleConnectionIOError(raft_link_t *link) {
    freeRaftClusterConnection(link);
}

/* Read data. Try to read the first field of the header first to check the
 * full length of the packet. When a whole packet is in memory this function
 * will call the function to process the packet. And so forth. */
void __raft_on_netprotocol_read_handler(aeEventLoop *el, int fd, void *privdata, int mask) {
    char buf[sizeof(raft_clusterMsg)];
    ssize_t nread;
    raft_clusterMsg *hdr;
    raft_link_t *link = (raft_link_t*) privdata;
    unsigned int readlen, rcvbuflen;
    UNUSED(el);
    UNUSED(mask);

    while(1) { /* Read as long as there is data to read. */
        rcvbuflen = sdslen(link->rcvbuf);
//        printf("rcvbufflen = %d\n",rcvbuflen);
        
        if (rcvbuflen < 8) {
            /* First, obtain the first 8 bytes to get the full message
             * length. */
            readlen = 8 - rcvbuflen;
        } 
        else {
            /* Finally read the full message. */
            hdr = (raft_clusterMsg*) link->rcvbuf;
            if (rcvbuflen == 8) {
                /* Perform some sanity check on the message signature
                 * and length. */
                if (memcmp(hdr->sig,"Raft",4) != 0 || ntohl(hdr->totlen) < RAFT_CLUSTERMSG_MIN_LEN)
                {
                    serverLog(LL_WARNING,
                        "Bad message length or signature received "
                        "from Cluster bus.");
                    handleConnectionIOError(link);
                    return;
                }
            }
 //           printf("totlen = %d\n",ntohl(hdr->totlen));
            readlen = ntohl(hdr->totlen) - rcvbuflen;
            if (readlen > sizeof(buf)) readlen = sizeof(buf);
        }

        nread = read(fd,buf,readlen);
  //      printf("%d, read[%.*s]\n", nread, nread, buf);

        if (nread == -1 && errno == EAGAIN) return; /* No more data ready. */

        if (nread <= 0) {
            /* I/O error... */
            serverLog(LL_DEBUG,"I/O error reading from node link: %s",
                (nread == 0) ? "connection closed" : strerror(errno));
            if (nread == 0) {
                printf("node %.*s:%d +DOWN\n", NET_IP_STR_LEN, link->peer_ip, link->raft_port);
            }
            handleConnectionIOError(link);
            return;
        } else {
            /* Read data and recast the pointer to the new buffer. */
            link->rcvbuf = sdscatlen(link->rcvbuf,buf,nread);
            hdr = (raft_clusterMsg*) link->rcvbuf;
            rcvbuflen += nread;
        }

        /* Total length obtained? Process this packet. */
        if (rcvbuflen >= 8 && rcvbuflen == ntohl(hdr->totlen)) {
            if (raft_clusterProcessPacket(link)) {
                sdsfree(link->rcvbuf);
                link->rcvbuf = sdsempty();
            } else {
                return; /* Link no longer valid. */
            }
        }
    }
}

void raftCron(void) {
    
    raft_link_t* conn = raftsv->conns;
    int i = 0;
    //while (conn)
    //{
    //    print_connection(conn);
    //    if (conn->connection_status == CONNECTED) {
//  //          __send_handshake(conn);
    //    }
    //    else if (conn->connection_status == DISCONNECTED) {
 // //          __connect_to_peer(conn);
    //    }
    //    conn = conn->next;
    //    i++;
    //}
    //printf("***count = %d******\n",i);
    raft_periodic(raftsv->raft, PERIOD_MSEC);
    raft_apply_all(raftsv->raft);
}

void raftInit(void) {

    memset(raftsv, 0, sizeof(raft_cluster_state));

    raftsv->raft = raft_new();

    raftsv->stats_bus_messages_sent = 0;
    raftsv->stats_bus_messages_received = 0;
    raftsv->node_id = server.port;

    raft_set_callbacks(raftsv->raft, &raft_funcs, raftsv);

    srand(time(NULL));


    __new_db(raftsv);

    /* add self */
    raft_add_node(raftsv->raft, NULL, raftsv->node_id, 1);

    if (listenToPort(server.port+RAFT_PORT_INCR,server.raftfd,&server.raftfd_count) == C_ERR)
    {
        exit(1);
    } else {
        serverLog(LL_NOTICE, "raft listenToPort %d", server.port+RAFT_PORT_INCR);
        int j;

        for (j = 0; j < server.raftfd_count; j++) {
            if (aeCreateFileEvent(server.el, server.raftfd[j], AE_READABLE,
                _raft_on_tcpserver_accept_handler, NULL) == AE_ERR)
                    serverPanic("Unrecoverable error creating Redis RAFT file event.");
        }
    }

    for(int i = 0; i < sizeof(nodelist)/sizeof(nodelist[0]); ++i)
    {
        int raftport = nodelist[i]+RAFT_PORT_INCR;
        if (nodelist[i] == raftsv->node_id) continue;
        raft_link_t* conn = __new_connection(raftsv);
        //printf("init connnew = %p\n",conn);
        __connection_set_peer(conn, "127.0.0.1", raftport);
        raft_add_node(raftsv->raft, conn, nodelist[i], 0);
    }
    //raft_link_t* conn = __new_connection(raftsv);
    //__connect_to_peer_at_host(conn, "127.0.0.1", 38001);
   // __connection_set_peer(conn, "127.0.0.1", 38000);
    //__new_connection(raftsv);
    //__connection_set_peer(conn, "127.0.0.1", 38001);
    //__new_connection(raftsv);
    //__connection_set_peer(conn, "127.0.0.1", 38002);
}

