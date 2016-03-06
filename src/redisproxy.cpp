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

#include "util/logger.h"
#include "command.h"
#include "cmdhandler.h"
#include "non-portable.h"
#include "redisservant.h"
#include "redisproxy.h"
#include "redis-proxy-config.h"

#include <resp/all.hpp>

std::map<std::string, RedisServantGroup *> RedisProxy::migrationTargets;

ClientPacket::ClientPacket(void)
{
    commandType = -1;
    recvBufferOffset = 0;
    sendBufferOffset = 0;
    finishedState = 0;
    sendToRedisBytes = 0;
    requestServant = NULL;
    redisSocket = NULL;
    finished_func = defaultFinishedHandler;
}

ClientPacket::~ClientPacket(void)
{
}

void ClientPacket::setFinishedState(ClientPacket::State state)
{
    finishedState = state;
    finished_func(this, finished_arg);
}

RedisProto::ParseState ClientPacket::parseRecvBuffer(void)
{
    recvParseResult.reset();
    RedisProto::ParseState state = RedisProto::parse(recvBuff.data() + recvBufferOffset,
                                                     recvBuff.size() - recvBufferOffset,
                                                     &recvParseResult);
    if (state == RedisProto::ProtoOK) {
        recvBufferOffset += recvParseResult.protoBuffLen;
    }
    return state;
}

RedisProto::ParseState ClientPacket::parseSendBuffer(void)
{
    sendParseResult.reset();
    RedisProto::ParseState state = RedisProto::parse(sendBuff.data() + sendBufferOffset,
                                                     sendBuff.size() - sendBufferOffset,
                                                     &sendParseResult);

    LOG(Logger::VVVERBOSE, "parse send buf %d", state);

    if (state == RedisProto::ProtoOK) {
        sendBufferOffset += sendParseResult.protoBuffLen;
    }
    return state;
}

void ClientPacket::defaultFinishedHandler(ClientPacket *packet, void *)
{
    switch (packet->finishedState) {
    case ClientPacket::Unknown:
        packet->sendBuff.append("-Unknown state\r\n");
        break;
    case ClientPacket::ProtoError:
        packet->sendBuff.append("-Proto error\r\n");
        break;
    case ClientPacket::ProtoNotSupport:
        packet->sendBuff.append("-Proto not support\r\n");
        break;
    case ClientPacket::WrongNumberOfArguments:
        packet->sendBuff.append("-Wrong number of arguments\r\n");
        break;
    case ClientPacket::RequestError:
        packet->sendBuff.append("-Request error\r\n");
        break;
    case ClientPacket::RequestFinished:
        break;
    default:
        break;
    }
    packet->server->writeReply(packet);
}

struct MigrateCommandContext {
    ClientPacketPtr originPacket;
    RedisServant *targetRedisServant;
    RedisServantGroup* origGroup;
};

void MigrateFinishedHandler(ClientPacketPtr migPacket, void* arg)
{
    MigrateCommandContext*migContext = (MigrateCommandContext*)arg;
    ClientPacketPtr origPacket = migContext->originPacket;
    RedisServantGroup* origGroup = migContext->origGroup;
    RedisServant *targetRedisServant = migContext->targetRedisServant;

    // swallow the response of MIGRATE command

    LOG(Logger::VVVERBOSE, "Migrate finish stat %d", migPacket->finishedState);

    if (migPacket->finishedState == ClientPacket::RequestFinished) {
        // forward command to target server
        if (targetRedisServant) {
            targetRedisServant->handle(origPacket);
        }
    } else {
        LOG(Logger::Error, "Migrate failed.\n recv buf %s \n send buf %s",
                    migPacket->recvBuff.data(), migPacket->sendBuff.data());

        // fail occurred, response error message to origin request
        origPacket->sendBuff.append("-Migrate failed\r\n");
        origPacket->setFinishedState(ClientPacket::RequestError);
        origPacket->server->writeReply(origPacket);
    }

    delete migContext;
    delete migPacket;
}

ClientPacketPtr
ClientPacket::MakeAPacket(int commandType, ClientPacketPtr refPacket, char *command) {

    ClientPacketPtr pack = ClientPacket::Construct();
    pack->eventLoop = refPacket->eventLoop;
    pack->finished_func = defaultFinishedHandler;
    pack->finished_arg = NULL;
    pack->commandType = -1;
    pack->eventLoop = refPacket->eventLoop;

    return pack;
}

static Monitor dummy;

int RedisProxy::migrationPos = 0;

RedisProxy::RedisProxy(void)
{
    m_monitor = &dummy;
    m_hashFunc = hashForBytes;
    m_maxHashValue = DefaultMaxHashValue;
    for (int i = 0; i < MaxHashValue; ++i) {
        m_hashMapping[i] = NULL;
        m_hashSlotMigrating[i] = NULL;
    }
    m_vipAddress[0] = 0;
    m_vipName[0] = 0;
    m_vipEnabled = false;
    m_groupRetryTime = 30;
    m_autoEjectGroup = false;
    m_ejectAfterRestoreEnabled = false;
    m_threadPoolRefCount = 0;
    m_proxyManager.setProxy(this);
}

RedisProxy::~RedisProxy(void)
{
    for (int i = 0; i < m_groups.size(); ++i) {
        delete m_groups.at(i);
    }
}

void checkMigrationState(evutil_socket_t, short, void *arg) {
    RedisProxy *p = (RedisProxy *)arg;

    int index;
    RedisServantGroup *sg = p->findNextMigration(&index);
    if (sg) {
        LOG(Logger::INFO, "migrating n %s s %d", sg->groupName(), index);
    }

//    LOG(Logger::INFO, "timer checkMigrationState");
}

bool RedisProxy::run(const HostAddress& addr)
{
    e.setTimerPersist(eventLoop(), checkMigrationState, this);
    e.active(100);

    if (isRunning()) {
        return false;
    }

    if (m_vipEnabled) {
        TcpSocket sock = TcpSocket::createTcpSocket();
        LOG(Logger::Message, "connect to vip address(%s:%d)...", m_vipAddress, addr.port());
        if (!sock.connect(HostAddress(m_vipAddress, addr.port()))) {
            LOG(Logger::Message, "set VIP [%s,%s]...", m_vipName, m_vipAddress);
            int ret = NonPortable::setVipAddress(m_vipName, m_vipAddress, 0);
            LOG(Logger::Message, "set_vip_address return %d", ret);
        } else {
            m_vipSocket = sock;
            m_vipEvent.set(eventLoop(), sock.socket(), EV_READ, vipHandler, this);
            m_vipEvent.active();
        }
    }


    m_monitor->proxyStarted(this);
    LOG(Logger::Message, "Start the %s on port %d", APP_NAME, addr.port());

    RedisCommand cmds[] = {
        {"HASHMAPPING", 11, -1, onHashMapping, NULL},
        {"ADDKEYMAPPING", 13, -1, onAddKeyMapping, NULL},
        {"DELKEYMAPPING", 13, -1, onDelKeyMapping, NULL},
        {"SHOWMAPPING", 11, -1, onShowMapping, NULL},
        {"POOLINFO", 8, -1, onPoolInfo, NULL},

        // YMIGRATE <slot_num> <target_server_ip_address> <target_server_port>
        {"YMIGRATE", 8, -1, MigrateServer, this},
        {"MIGSTAT", 7, -1, ShowMigrateStatus, this},
        {"SHUTDOWN", 8, -1, onShutDown, this},
        {"LOG", 3, -1, SetLogLevel, this},
    };
    RedisCommandTable::instance()->registerCommand(cmds, sizeof(cmds)/sizeof(RedisCommand));

    return TcpServer::run(addr);
}

void RedisProxy::stop(void)
{
    TcpServer::stop();
    if (m_vipEnabled) {
        if (!m_vipSocket.isNull()) {
            LOG(Logger::Message, "delete vip...");
            int ret = NonPortable::setVipAddress(m_vipName, m_vipAddress, 1);
            LOG(Logger::Message, "delete vip return %d", ret);

            e.remove();
            m_vipEvent.remove();
            m_vipSocket.close();
        }
    }
    LOG(Logger::Message, "%s has stopped", APP_NAME);
}

void RedisProxy::addRedisGroup(RedisServantGroup *group)
{
    if (group) {
        group->setGroupId(m_groups.size());
        m_groups.append(group);
    }
}

bool RedisProxy::setGroupMappingValue(int hashValue, RedisServantGroup *group)
{
    if (hashValue >= 0 && hashValue < MaxHashValue) {
        LOG(Logger::Message, "set group[%d] group %s", hashValue, group ? group->groupName() : "NULL");
        m_hashMapping[hashValue] = group;
        return true;
    }
    return false;
}

RedisServantGroup *RedisProxy::SlotNumToRedisServerGroup(int slotNum) const
{
    if (slotNum >= 0 && slotNum < m_maxHashValue) {
        return m_hashMapping[slotNum];
    }
    return NULL;
}

RedisServantGroup *RedisProxy::group(const char *name) const
{
    for (int i = 0; i < groupCount(); ++i) {
        RedisServantGroup* p = group(i);
        if (strcmp(name, p->groupName()) == 0) {
            return p;
        }
    }
    return NULL;
}

unsigned int RedisProxy::KeyToIndex(const char* key, int len)
{
    unsigned int hash_val = m_hashFunc(key, len);
    unsigned int idx = hash_val % m_maxHashValue;
    return idx;
}

RedisServantGroup *RedisProxy::mapToGroup(const char* key, int len)
{
    if (!m_keyMapping.empty()) {
        String _key(key, len, false);
        StringMap<RedisServantGroup*>::iterator it = m_keyMapping.find(_key);
        if (it != m_keyMapping.end()) {
            return it->second;
        }
    }

    unsigned int hash_val = m_hashFunc(key, len);
    unsigned int idx = hash_val % m_maxHashValue;
    return m_hashMapping[idx];
}

ClientPacketPtr
RedisProxy::MakeMigratePacket(const char *key, int len, ClientPacketPtr origPacket,
                              RedisServantGroup *origGroup,
                              RedisServantGroup *migrationTargetServantGroup) {
    // 1. send migrate command to origin redis server
    RedisServant *rs = migrationTargetServantGroup->findUsableServant(origPacket);
    HostAddress addr = rs->connectionPool()->redisAddress();

    ClientPacketPtr migPacket = ClientPacket::MakeAPacket(-1, origPacket, NULL);
    resp::encoder<resp::buffer> enc;
    // MIGRATE host port key|"" destination-db timeout
    char port[16];
    snprintf(port, 16, "%d", addr.port());
    resp::buffer keyBuf(key, len);
    std::vector<resp::buffer> buffers = enc.encode("MIGRATE", addr.ip(), port, keyBuf, "0", "3000");
    std::string command = std::string(buffers.front().data(), buffers.front().size());

    MigrateCommandContext * migCmdCxt = new MigrateCommandContext;
    migCmdCxt->originPacket = origPacket;
    migCmdCxt->targetRedisServant = rs;
    migCmdCxt->origGroup = origGroup;

    migPacket->recvBuff.appendFormatString(command.c_str());
    migPacket->finished_func = MigrateFinishedHandler;
    migPacket->finished_arg = migCmdCxt;
    migPacket->parseRecvBuffer();

    return migPacket;

    // 2. after migration done, send original request to target redis server
}

void RedisProxy::handleClientPacket(const char *key, int len, ClientPacketPtr packet)
{
    uint index = KeyToIndex(key, len);

    RedisServantGroup* group = m_hashMapping[index];
    if (!group) {
        packet->setFinishedState(ClientPacket::RequestError);
        return;
    }

    RedisProxy *context = (RedisProxy *)packet->server;
    RedisServantGroup *migrationTargetServantGroup = context->GetSlotMigration(index);
    LOG(Logger::VVVERBOSE, "slot %d mig %d", index, migrationTargetServantGroup);
    if (migrationTargetServantGroup != NULL) {
        LOG(Logger::VVERBOSE, "slot %d is migrating to %s", index, migrationTargetServantGroup->groupName());

        packet = MakeMigratePacket(key/*origin*/, len/*origin*/, packet/*origin*/, group, migrationTargetServantGroup);
    }

    RedisServant* servant = group->findUsableServant(packet);
    if (servant) {
        servant->handle(packet);
    } else {
        if (m_autoEjectGroup) {
            m_groupMutex.lock();
            m_proxyManager.setGroupTTL(group, m_groupRetryTime, m_ejectAfterRestoreEnabled);
            m_groupMutex.unlock();
        }
        packet->setFinishedState(ClientPacket::RequestError);
    }
}

bool RedisProxy::addGroupKeyMapping(const char *key, int len, RedisServantGroup *group)
{
    if (key && len > 0 && group) {
        m_keyMapping.insert(StringMap<RedisServantGroup*>::value_type(String(key, len, true), group));
        return true;
    }
    return false;
}

void RedisProxy::removeGroupKeyMapping(const char *key, int len)
{
    if (key && len > 0) {
        m_keyMapping.erase(String(key, len));
    }
}


Context *RedisProxy::createContextObject(void)
{
    ClientPacket* packet = new ClientPacket;

    EventLoop* loop;
    if (m_eventLoopThreadPool) {
        int threadCount = m_eventLoopThreadPool->size();
        EventLoopThread* loopThread = m_eventLoopThreadPool->thread(m_threadPoolRefCount % threadCount);
        ++m_threadPoolRefCount;
        loop = loopThread->eventLoop();
    } else {
        loop = eventLoop();
    }

    packet->eventLoop = loop;
    return packet;
}

void RedisProxy::destroyContextObject(Context *c)
{
    delete c;
}

void RedisProxy::closeConnection(Context* c)
{
    ClientPacket* packet = (ClientPacket*)c;
    m_monitor->clientDisconnected(packet);
    TcpServer::closeConnection(c);
}

void RedisProxy::clientConnected(Context* c)
{
    ClientPacket* packet = (ClientPacket*)c;
    m_monitor->clientConnected(packet);
}

TcpServer::ReadStatus RedisProxy::readingRequest(Context *c)
{
    ClientPacket* packet = (ClientPacket*)c;
    switch (packet->parseRecvBuffer()) {
    case RedisProto::ProtoError:
        return ReadError;
    case RedisProto::ProtoIncomplete:
        return ReadIncomplete;
    case RedisProto::ProtoOK:
        return ReadFinished;
    default:
        return ReadError;
    }
}

void RedisProxy::readRequestFinished(Context *c)
{
    ClientPacket* packet = (ClientPacket*)c;
    RedisProtoParseResult& r = packet->recvParseResult;
    char* cmd = r.tokens[0].s;
    int len = r.tokens[0].len;

    RedisCommandTable* cmdtable = RedisCommandTable::instance();
    cmdtable->execCommand(cmd, len, packet);
}

void RedisProxy::writeReply(Context *c)
{
    ClientPacket* packet = (ClientPacket*)c;
    if (!packet->isRecvParseEnd()) {
        switch (packet->parseRecvBuffer()) {
        case RedisProto::ProtoError:
            closeConnection(c);
            break;
        case RedisProto::ProtoIncomplete:
            waitRequest(c);
            break;
        case RedisProto::ProtoOK:
            readRequestFinished(c);
            break;
        default:
            break;
        }
    } else {
        TcpServer::writeReply(c);
    }
}

void RedisProxy::writeReplyFinished(Context *c)
{
    ClientPacket* packet = (ClientPacket*)c;
    m_monitor->replyClientFinished(packet);
    packet->finishedState = ClientPacket::Unknown;
    packet->commandType = -1;
    packet->sendBuff.clear();
    packet->recvBuff.clear();
    packet->sendBytes = 0;
    packet->recvBytes = 0;
    packet->sendToRedisBytes = 0;
    packet->requestServant = NULL;
    packet->redisSocket = NULL;
    packet->recvBufferOffset = 0;
    packet->sendBufferOffset = 0;
    packet->sendParseResult.reset();
    packet->recvParseResult.reset();
    waitRequest(c);
}

void RedisProxy::vipHandler(socket_t sock, short, void* arg)
{
    char buff[64];
    RedisProxy* proxy = (RedisProxy*)arg;
    int ret = ::recv(sock, buff, 64, 0);
    if (ret == 0) {
        LOG(Logger::Message, "disconnected from VIP. change vip address...");
        int ret = NonPortable::setVipAddress(proxy->m_vipName, proxy->m_vipAddress, 0);
        LOG(Logger::Message, "set_vip_address return %d", ret);
        proxy->m_vipSocket.close();
    }
}

void
RedisProxy::checkMigrationStat(int, short, void *arg)
{
    MigrationPairPtr mpp = (MigrationPairPtr)arg;
    LOG(Logger::INFO, "checkMigrationStat");
    // TODO: implement checkMigrationStat
    // if migration done (keyspace of source server is empty
    //   1. update the configuration file
    //     - update slot -> server
    //     - delete migration config
    delete mpp;

    // if migration is still ongoing
    //   1. restart the timer and check the status later
}

void
RedisProxy::StartSlotMigration(int slot, RedisServantGroup *sg) {
    SetSlotMigrating(slot, sg);

    // Start a timer for this slot, checking migration state
    RedisServantGroup *origSg = SlotNumToRedisServerGroup(slot);
    RedisServant *serv = origSg->master(0);

//    MigrationPairPtr mpp = new MigrationPair;
//    mpp->source = origSg;
//    mpp->target = sg;

//    Event migrationCheckEvent;
//    migrationCheckEvent.setTimer(eventLoop(), checkMigrationStat, mpp);
//    int ret = migrationCheckEvent.active(500);
//    LOG(Logger::INFO, "StartSlotMigration start timer %d", ret);
}

static RedisServant *CreateRedisServant(EventLoop *ev_loop, string hostname, int port)
{
    LOG(Logger::VVVERBOSE, "create redis connection %s:%d", hostname.c_str(), port);

    RedisServant* servant = new RedisServant;
    RedisServant::Option opt;
    strcpy(opt.name, hostname.c_str());
    opt.poolSize = 50;
    opt.reconnInterval = 1;
    opt.maxReconnCount = 100;
    servant->setOption(opt);
    servant->setRedisAddress(HostAddress(hostname.c_str(), port));
    servant->setEventLoop(ev_loop);

    return servant;
}

static RedisServantGroup * CreateGroup(RedisProxy *context, string groupName, string hostname, int port)
{
    RedisServantGroup* group = new RedisServantGroup;
    RedisServantGroupPolicy* policy = RedisServantGroupPolicy::createPolicy("master_only");
    group->setGroupName(groupName.c_str());
    group->setPolicy(policy);

    RedisServant *servant = CreateRedisServant(context->eventLoop(), hostname, port);
    group->addMasterRedisServant(servant);

    group->setEnabled(true);

    return group;
}

RedisServantGroup *RedisProxy::CreateMigrationTarget(RedisProxy *context, int slotNum, std::string hostname, int port) {
    char addr[256];
    // check if connection existed by address
    snprintf(addr, 256, "%s:%d", hostname.c_str(), port);
    auto it = RedisProxy::migrationTargets.find(addr);
    if (it != RedisProxy::migrationTargets.end()) {
        LOG(Logger::DEBUG, "Found saved target %s", addr);
        return RedisProxy::migrationTargets[addr];
    }

    char name[256];
    // create connection
    snprintf(name, 256, "group-mig-%d", slotNum);
    LOG(Logger::DEBUG, "Create new target n %s s %d", addr, slotNum);
    RedisServantGroup *group = CreateGroup(context, name, hostname, port);
    if (group == NULL) {
        return NULL;
    }

    // save the connection
    RedisProxy::migrationTargets[addr] = group;

    return group;
}

RedisServantGroup *
RedisProxy::findNextMigration(int *pInt) {
    RedisServantGroup *ret = NULL;

    while (migrationPos < MaxHashValue) {
        if (m_hashSlotMigrating[migrationPos%MaxHashValue]) {
            *pInt = migrationPos;
            ret = m_hashSlotMigrating[migrationPos%MaxHashValue];

            // enumerate the next element next time
            migrationPos++;
            break;
        }
        migrationPos++;
    }

    migrationPos = migrationPos % MaxHashValue;
    return ret;
}
