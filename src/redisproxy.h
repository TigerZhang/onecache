/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

#ifndef REDISPROXY_H
#define REDISPROXY_H

//Application version
#define APP_VERSION "3.0"
#define APP_NAME "OneCache"
#define APP_EXIT_KEY 10

#include "util/tcpserver.h"
#include "util/locker.h"

#include "command.h"
#include "redisproto.h"
#include "redisservantgroup.h"
#include "proxymanager.h"
#include "redisservant.h"
#include "redis-proxy-config.h"

class RedisConnection;
class RedisServant;
class RedisProxy;
//typedef std::shared_ptr<ClientPacket> ClientPacketPtr;
class ClientPacket : public Context
{
public:
    enum State {
        Unknown = 0,
        ProtoError = 1,
        ProtoNotSupport = 2,
        WrongNumberOfArguments = 3,
        RequestError = 4,
        RequestFinished = 5
    };

    ClientPacket(void);
    ~ClientPacket(void);

    void setFinishedState(State state);
    RedisProxy* proxy(void) const { return (RedisProxy*)server; }
    RedisProto::ParseState parseRecvBuffer(void);
    RedisProto::ParseState parseSendBuffer(void);
    bool isRecvParseEnd(void) const
    { return (recvBufferOffset == recvBuff.size()); }

    static void defaultFinishedHandler(ClientPacket *packet, void*);

    int finishedState;                              //Finished state
    void* finished_arg;                             //Finished function arg
    void (*finished_func)(ClientPacket*, void*);    //Finished notify function
    int commandType;                                //Current command type
    int recvBufferOffset;                           //Current request buffer offset
    int sendBufferOffset;                           //Current reply buffer offset
    RedisProtoParseResult recvParseResult;          //Request parse result
    RedisProtoParseResult sendParseResult;          //Reply parse result
    int sendToRedisBytes;                           //Send to redis bytes
    RedisServant* requestServant;                   //Object of request
    RedisConnection* redisSocket;                   //Redis socket

    // Create a new ClientPacket using contexts of a reference packet
    static ClientPacketPtr MakeAPacket(int commandType, ClientPacketPtr refPacket, char *command);
    static ClientPacketPtr Construct() { return new ClientPacket; }
};

class Monitor
{
public:
    Monitor(void) {}
    virtual ~Monitor(void) {}

    virtual void proxyStarted(RedisProxy*) {}
    virtual void clientConnected(ClientPacket*) {}
    virtual void clientDisconnected(ClientPacket*) {}
    virtual void replyClientFinished(ClientPacket*) {}
};

class RedisProxy : public TcpServer
{
public:
    enum {
        DefaultPort = 8221,

        MaxHashValue = 1024,
        DefaultMaxHashValue = 128,
    };

    RedisProxy(void);
    ~RedisProxy(void);

    RedisServantGroup * findNextMigration(int *pInt);

public:
    void setEventLoopThreadPool(EventLoopThreadPool* pool)
    { m_eventLoopThreadPool = pool; }

    EventLoopThreadPool* eventLoopThreadPool(void)
    { return m_eventLoopThreadPool; }

    bool vipEnabled(void) const { return m_vipEnabled; }
    const char* vipName(void) const { return m_vipName; }
    const char* vipAddress(void) const { return m_vipAddress; }
    void setVipName(const char* name) { strcpy(m_vipName, name); }
    void setVipAddress(const char* address) { strcpy(m_vipAddress, address); }
    void setVipEnabled(bool b) { m_vipEnabled = b; }
    void setGroupRetryTime(int seconds) { m_groupRetryTime = seconds; }
    void setAutoEjectGroupEnabled(bool b) { m_autoEjectGroup = b; }
    void setEjectAfterRestoreEnabled(bool b)
    { m_ejectAfterRestoreEnabled = b; }

    void setMonitor(Monitor* monitor) { m_monitor = monitor; }
    Monitor* monitor(void) const { return m_monitor; }

    bool run(const HostAddress &addr);
    void stop(void);

    void addRedisGroup(RedisServantGroup* group);
    bool setGroupMappingValue(int hashValue, RedisServantGroup* group);
    void setHashFunction(HashFunc func) { m_hashFunc = func; }
    void setMaxHashValue(int value) { m_maxHashValue = value; }

    HashFunc hashFunction(void) const { return m_hashFunc; }
    int maxHashValue(void) const { return m_maxHashValue; }
    RedisServantGroup* SlotNumToRedisServerGroup(int slotNum) const;

    int groupCount(void) const { return m_groups.size(); }
    RedisServantGroup* group(int index) const { return m_groups.at(index); }
    RedisServantGroup* group(const char* name) const;
    RedisServantGroup* mapToGroup(const char* key, int len);
    unsigned int KeyToIndex(const char* key, int len);
    void handleClientPacket(const char *key, int len, ClientPacketPtr packet);

    bool addGroupKeyMapping(const char* key, int len, RedisServantGroup* group);
    void removeGroupKeyMapping(const char* key, int len);

    StringMap<RedisServantGroup*>& keyMapping(void) { return m_keyMapping; }

    virtual Context* createContextObject(void);
    virtual void destroyContextObject(Context* c);
    virtual void closeConnection(Context* c);
    virtual void clientConnected(Context* c);
    virtual ReadStatus readingRequest(Context* c);
    virtual void readRequestFinished(Context* c);
    virtual void writeReply(Context* c);
    virtual void writeReplyFinished(Context* c);

    //
    // redis MIGRATE command will create a temporary connection to the target redis server, then close the connection
    // after migration done. The target redis server will have a lot of connections in TIME_WAIT state.
    // The target redis server will deny new connection when there too many connections hung in TIME_WAIT state,
    // migration will fail in this case. So the following system options may be needed.
    // net.ipv4.tcp_fin_timeout = 5
    // net.ipv4.tcp_tw_reuse = 1
    //
    // TODO: implement StartSlotMigration/SlotMigrationDone
    //
    // implement StartSlotMigration method
    // - set slot in migrating status. i.e., create a migration target and update the slot migration map.
    // - start a timer to check if migration is done by checking if source server is empty.
    // implement SlotMigrationDone callback
    // - update slot mapping. i.e., set slot -> target server.
    // - remove slot from migration map.
    // - update configration file.
    void StartSlotMigration(int slot, RedisServantGroup *sg);
    RedisServantGroup * GetSlotMigration(int slot) { return m_hashSlotMigrating[slot%MaxHashValue]; }
    ClientPacketPtr MakeMigratePacket(const char *key, int len, ClientPacketPtr origPacket,
                                                    RedisServantGroup *origGroup,
                                                    RedisServantGroup *migrationTargetServantGroup);

    static RedisServantGroup *CreateMigrationTarget(RedisProxy *context, int slotNum, std::string hostname, int port);

    // exposed for unit test ONLY
    void SetSlotMigrating(int slot, RedisServantGroup *sg) { m_hashSlotMigrating[slot%MaxHashValue] = sg; }

private:
    static void vipHandler(socket_t, short, void*);
    static std::map<std::string, RedisServantGroup *> migrationTargets; //<hostname:port, *>

private:
    Monitor* m_monitor;
    HashFunc m_hashFunc;
    int m_maxHashValue;
    RedisServantGroup* m_hashMapping[MaxHashValue];
    RedisServantGroup *m_hashSlotMigrating[MaxHashValue];
    Vector<RedisServantGroup*> m_groups;
    TcpSocket m_vipSocket;
    char m_vipName[256];
    char m_vipAddress[256];
    Event m_vipEvent;
    bool m_vipEnabled;
    int  m_groupRetryTime;
    bool m_autoEjectGroup;
    bool m_ejectAfterRestoreEnabled;
    StringMap<RedisServantGroup*> m_keyMapping;
    unsigned int m_threadPoolRefCount;
    EventLoopThreadPool* m_eventLoopThreadPool;
    Mutex m_groupMutex;
    ProxyManager m_proxyManager;

    Event e;
    static int migrationPos;

private:
    RedisProxy(const RedisProxy&);
    RedisProxy& operator =(const RedisProxy&);

    static void checkMigrationStat(int, short, void *arg);
};

class MigrationPair
{
public:
    RedisServantGroup *source;
    RedisServantGroup *target;
};

typedef MigrationPair* MigrationPairPtr;

#endif
