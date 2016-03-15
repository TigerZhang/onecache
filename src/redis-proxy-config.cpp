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

#include <time.h>

#include "redis-proxy-config.h"
#include "redis-servant-select.h"

#ifdef WIN32
#define strcasecmp stricmp
#define strncasecmp strnicmp
#endif


COperateXml::COperateXml() {
    m_docPointer = new TiXmlDocument;
}

COperateXml::~COperateXml() {
    delete m_docPointer;
}

bool COperateXml::xml_open(const char* xml_path) {
    if (!m_docPointer->LoadFile(xml_path)) {
        return false;
    }
    m_rootElement = m_docPointer->RootElement();
    return true;
}

void COperateXml::xml_print() {
    m_docPointer->Print();
}

const TiXmlElement* COperateXml::getRootElement() {
    return m_rootElement;
}

// host info
CHostInfo::CHostInfo() {
    ip = "";
    host_name = "";
    port = 0;
    master = false;
    priority = 0;
    policy = 0;
}
CHostInfo::~CHostInfo(){}
string CHostInfo::get_ip()const
{
    return ip;
}

string CHostInfo::get_hostName()const
{
    return host_name;
}
int CHostInfo::get_port()const        { return port;}
bool CHostInfo::get_master()const     { return master;}
int CHostInfo::get_policy()const      { return policy;}
int CHostInfo::get_priority()const    { return priority;}
int CHostInfo::get_connectionNum()const    { return connection_num;}

void CHostInfo::set_ip(string& s)        { ip = s;}
void CHostInfo::set_hostName(string& s)  { host_name = s;}
void CHostInfo::set_port(int p)          { port = p;}
void CHostInfo::set_master(bool m)       { master = m;}
void CHostInfo::set_policy(int p)        { policy = p;}
void CHostInfo::set_priority(int p)      { priority = p;}
void CHostInfo::set_connectionNum(int p) { connection_num = p;}

CGroupInfo::CGroupInfo() : m_hashMin(-1), m_hashMax(-1)
{
    memset(m_groupName, '\0', sizeof(m_groupName));
    memset(m_groupPolicy, '\0', sizeof(m_groupPolicy));
    m_hosts.clear();
}

CGroupInfo::CGroupInfo(const CGroupInfo &info)
        : m_hashMin(info.m_hashMin), m_hashMax(info.m_hashMax) {
    memcpy(m_groupName, info.m_groupName, sizeof(m_groupName)/sizeof(m_groupName[0]));
    memcpy(m_groupPolicy, info.m_groupPolicy, sizeof(m_groupPolicy)/sizeof(m_groupPolicy[0]));
    m_hosts = info.m_hosts;
}

CGroupInfo::~CGroupInfo() {}

RedisProxyCfg::RedisProxyCfg() {
    m_operateXmlPointer = new COperateXml;
    m_hashInfo.hash_value_max = RedisProxyCfgChecker::REDIS_PROXY_HASH_MAX;
    m_threadNum = 0;
    m_port = 0;
    memset(m_vip.if_alias_name, '\0', sizeof(m_vip.if_alias_name));
    memset(m_vip.vip_address, '\0', sizeof(m_vip.vip_address));
    m_vip.enable = false;
    memset(m_logFile, '\0', sizeof(m_logFile));
    m_daemonize = false;
    m_guard = false;
    m_topKeyEnable = false;
    m_hashMappingList = new HashMappingList;
    m_keyMappingList = new KeyMappingList;
    m_groupInfo = new GroupInfoList;
}

RedisProxyCfg::~RedisProxyCfg() {
    if(NULL != m_operateXmlPointer) delete m_operateXmlPointer;
    delete m_hashMappingList;
    delete m_keyMappingList;
    delete m_groupInfo;
}

RedisProxyCfg *RedisProxyCfg::instance()
{
    static RedisProxyCfg * cfg = NULL;
    if (cfg == NULL) {
        cfg = new RedisProxyCfg;
    }
    return cfg;
}

void RedisProxyCfg::set_groupName(CGroupInfo& group, const char* name) {
    int size = strlen(name);
    memcpy(group.m_groupName, name, size+1);
}

void RedisProxyCfg::set_hashMin(CGroupInfo& group, int num) {
    group.m_hashMin = num;
}

void RedisProxyCfg::set_hashMax(CGroupInfo& group, int num) {
    group.m_hashMax = num;
}

void RedisProxyCfg::addHost(CGroupInfo& group, CHostInfo& p) {
    group.m_hosts.push_back(p);
}

void RedisProxyCfg::set_groupAttribute(TiXmlAttribute *groupAttr, CGroupInfo& pGroup) {
    for (; groupAttr != NULL; groupAttr = groupAttr->Next()) {
        const char* name = groupAttr->Name();
        const char* value = groupAttr->Value();
        if (value == NULL) value = "";
        if (0 == strcasecmp(name, "hash_min")) {
            set_hashMin(pGroup, atoi(value));
            continue;
        }
        if (0 == strcasecmp(name, "hash_max")) {
            set_hashMax(pGroup, atoi(value));
            continue;
        }
        if (0 == strcasecmp(name, "name")) {
            set_groupName(pGroup, value);
            continue;
        }
        if (0 == strcasecmp(name, "policy")) {
            memcpy(pGroup.m_groupPolicy, value, strlen(value)+1);
            continue;
        }
    }
}

void RedisProxyCfg::set_hostAttribute(TiXmlAttribute *addrAttr, CHostInfo& pHostInfo) {
     for (; addrAttr != NULL; addrAttr = addrAttr->Next()) {
        const char* name = addrAttr->Name();
        const char* value = addrAttr->Value();
        if (value == NULL) value = "";
        if (0 == strcasecmp(name, "host_name")) {
            string s = value;
            pHostInfo.set_hostName(s);
            continue;
        }
        if (0 == strcasecmp(name, "ip")) {
            string s = value;
            pHostInfo.set_ip(s);
            continue;
        }
        if (0 == strcasecmp(name, "connection_num")) {
            pHostInfo.set_connectionNum(atoi(value));
            continue;
        }
        if (0 == strcasecmp(name, "port")) {
            pHostInfo.set_port(atoi(value));
            continue;
        }
        if (0 == strcasecmp(name, "policy")) {
            pHostInfo.set_policy(atoi(value));
            continue;
        }
        if (0 == strcasecmp(name, "priority")) {
            pHostInfo.set_priority(atoi(value));
            continue;
        }
        if (0 == strcasecmp(name, "master")) {
            pHostInfo.set_master((atoi(value) != 0));
            continue;
        }
    }

}

void RedisProxyCfg::set_hostEle(TiXmlElement* hostContactEle, CHostInfo& hostInfo) {
    for (; hostContactEle != NULL; hostContactEle = hostContactEle->NextSiblingElement()) {
        const char* strValue;
        strValue = hostContactEle->Value();
        if (strValue == NULL) strValue = "";

        const char* strText;
        strText = hostContactEle->GetText();
        if (strText == 0) strText = "";

        if (0 == strcasecmp(strValue, "ip")) {
            string s = strText;
            hostInfo.set_ip(s);
            continue;
        }
        if (0 == strcasecmp(strValue, "connection_num")) {
            if (hostContactEle->GetText() != NULL)
                hostInfo.set_connectionNum(atoi(strText));
            continue;
        }
        if (0 == strcasecmp(strValue, "port")) {
            hostInfo.set_port(atoi(strText));
            continue;
        }
        if (0 == strcasecmp(strValue, "policy")) {
            hostInfo.set_policy(atoi(strText));
            continue;
        }
        if (0 == strcasecmp(strValue, "host_name")) {
            string s = strText;
            hostInfo.set_hostName(s);
            continue;
        }
        if (0 == strcasecmp(strValue, "priority")) {
            hostInfo.set_priority(atoi(strText));
            continue;
        }
        if (0 == strcasecmp(strValue, "master")) {
            hostInfo.set_master((atoi(strText) != 0));
            continue;
        }
    }
}

void RedisProxyCfg::getRootAttr(const TiXmlElement* pRootNode) {
    TiXmlAttribute *addrAttr = (TiXmlAttribute *)pRootNode->FirstAttribute();
    for (; addrAttr != NULL; addrAttr = addrAttr->Next()) {
        const char* name = addrAttr->Name();
        const char* value = addrAttr->Value();
        if (value == NULL) value = "";
        if (0 == strcasecmp(name, "thread_num")) {
            m_threadNum = atoi(value);
            continue;
        }
        if (0 == strcasecmp(name, "port")) {
            m_port = atoi(value);
            continue;
        }
//        if (0 == strcasecmp(name, "hash_value_max")) {
//            m_hashInfo.hash_value_max = atoi(value);
//            continue;
//        }
        if (0 == strcasecmp(name, "log_file")) {
            strcpy(m_logFile, value);
            continue;
        }
        if (0 == strcasecmp(name, "daemonize")) {
            if(strcasecmp(value, "0") != 0 && strcasecmp(value, "") != 0 ) {
                m_daemonize = true;
            }
            continue;
        }
        if (0 == strcasecmp(name, "guard")) {
            if(strcasecmp(value, "0") != 0 && strcasecmp(value, "") != 0) {
                m_guard = true;
            }
            continue;
        }
    }
}

void RedisProxyCfg::getVipAttr(const TiXmlElement* vidNode) {
    TiXmlAttribute *addrAttr = (TiXmlAttribute *)vidNode->FirstAttribute();
    for (; addrAttr != NULL; addrAttr = addrAttr->Next()) {
        const char* name = addrAttr->Name();
        const char* value = addrAttr->Value();
        if (value == NULL) value = "";
        if (0 == strcasecmp(name, "if_alias_name")) {
            strcpy(m_vip.if_alias_name, value);
            continue;
        }
        if (0 == strcasecmp(name, "vip_address")) {
            strcpy(m_vip.vip_address, value);
            continue;
        }
        if (0 == strcasecmp(name, "enable")) {
            if(strcasecmp(value, "0") != 0 && strcasecmp(value, "") != 0 ) {
                m_vip.enable = true;
            }
        }
    }
}

void RedisProxyCfg::setHashMappingNode(TiXmlElement* pNode) {
    TiXmlElement* pNext = pNode->FirstChildElement();
    for (; pNext != NULL; pNext = pNext->NextSiblingElement()) {
        HashMapping hashMap;
        if (0 == strcasecmp(pNext->Value(), "hash")) {
            TiXmlAttribute *addrAttr = pNext->FirstAttribute();
            for (; addrAttr != NULL; addrAttr = addrAttr->Next()) {
                const char* name = addrAttr->Name();
                const char* value = addrAttr->Value();
                if (value == NULL) value = "";
                if (0 == strcasecmp(name, "value")) {
                    hashMap.hash_value = atoi(value);
                    continue;
                }
                if (0 == strcasecmp(name, "group_name")) {
                    strcpy(hashMap.group_name, value);
                }
            }
            m_hashMappingList->push_back(hashMap);
        }
    }
}


void RedisProxyCfg::setGroupOption(const TiXmlElement* vidNode) {
    TiXmlAttribute *addrAttr = (TiXmlAttribute *)vidNode->FirstAttribute();
    for (; addrAttr != NULL; addrAttr = addrAttr->Next()) {
        const char* name = addrAttr->Name();
        const char* value = addrAttr->Value();
        if (value == NULL) value = "";
        if (0 == strcasecmp(name, "backend_retry_interval")) {
            m_groupOption.backend_retry_interval = atoi(value);
            continue;
        }
        if (0 == strcasecmp(name, "backend_retry_limit")) {
            m_groupOption.backend_retry_limit = atoi(value);
            continue;
        }
        if (0 == strcasecmp(name, "group_retry_time")) {
            m_groupOption.group_retry_time = atoi(value);
            continue;
        }

        if (0 == strcasecmp(name, "auto_eject_group")) {
            if(strcasecmp(value, "0") != 0 && strcasecmp(value, "") != 0 ) {
                m_groupOption.auto_eject_group = true;
                continue;
            }
        }

        if (0 == strcasecmp(name, "eject_after_restore")) {
            if(strcasecmp(value, "0") != 0 && strcasecmp(value, "") != 0 ) {
                m_groupOption.eject_after_restore = true;
            }
        }
    }
}



void RedisProxyCfg::setKeyMappingNode(TiXmlElement* pNode) {
    TiXmlElement* pNext = pNode->FirstChildElement();
    for (; pNext != NULL; pNext = pNext->NextSiblingElement()) {
        KeyMapping hashMap;
        if (0 == strcasecmp(pNext->Value(), "key")) {
            TiXmlAttribute *addrAttr = pNext->FirstAttribute();
            for (; addrAttr != NULL; addrAttr = addrAttr->Next()) {
                const char* name = addrAttr->Name();
                const char* value = addrAttr->Value();
                if (value == NULL) value = "";
                if (0 == strcasecmp(name, "key_name")) {
                    strcpy(hashMap.key, value);
                    continue;
                }
                if (0 == strcasecmp(name, "group_name")) {
                    strcpy(hashMap.group_name, value);
                }
            }
            m_keyMappingList->push_back(hashMap);
        }
    }
}

void RedisProxyCfg::LoadMigrationSlots(TiXmlElement *pNode) {
    TiXmlElement *pNext = pNode->FirstChildElement();
    for (; pNext != NULL; pNext = pNext->NextSiblingElement()) {
        if (strcasecmp(pNext->Value(), "migrate") == 0) {
            LOG(Logger::INFO, "migrate");
            TiXmlAttribute *attr = pNext->FirstAttribute();

            MigrationOption migration;
            for (; attr != NULL; attr = attr->Next()) {
                const char *name = attr->Name();
                const char *value = attr->Value();

                LOG(Logger::INFO, "%s: %s", name, value);

                if (strcasecmp(name, "slot") == 0) {
                    migration.slotNum = atoi(value);
                } else if (strcasecmp(name, "server_address") == 0) {
                    migration.addr = std::string(value);
                } else if (strcasecmp(name, "port") == 0) {
                    migration.port = atoi(value);
                }
            }

            migrationOptions.push_back(migration);
        }
    }
}

void RedisProxyCfg::getGroupNode(TiXmlElement* pNode) {
    CGroupInfo groupTmp;
    set_groupAttribute(pNode->FirstAttribute(), groupTmp);
    TiXmlElement* pNext = pNode->FirstChildElement();
    for (; pNext != NULL; pNext = pNext->NextSiblingElement()) {
        if (0 == strcasecmp(pNext->Value(), "hash_min")) {
            if (NULL == pNext->GetText()) {
                set_hashMin(groupTmp, 0);
                continue;
            }
            set_hashMin(groupTmp, atoi(pNext->GetText()));
            continue;
        }
        if (0 == strcasecmp(pNext->Value(), "hash_max")) {
            if (NULL == pNext->GetText()) {
                set_hashMax(groupTmp, 0);
                continue;
            }
            set_hashMax(groupTmp, atoi(pNext->GetText()));
            continue;
        }
        if (0 == strcasecmp(pNext->Value(), "name")) {
            set_groupName(groupTmp, pNext->GetText());
            continue;
        }

        if (0 == strcasecmp(pNext->Value(), "host")) {
            CHostInfo hostInfo;
            set_hostAttribute(pNext->FirstAttribute(), hostInfo);
            TiXmlElement* hostContactEle = (TiXmlElement* )pNext->FirstChildElement();
            set_hostEle(hostContactEle, hostInfo);
            addHost(groupTmp, hostInfo);
        }
    }
//    CGroupInfoPtr group = std::make_shared<CGroupInfo>(CGroupInfo(groupTmp));
    m_groupInfo->push_back(groupTmp);
}

bool GetNodePointerByName(
    TiXmlElement* pRootEle,
    const std::string &strNodeName,
    TiXmlElement* &Node)
{
    if (strNodeName==pRootEle->Value()) {
        Node = pRootEle;
        return true;
    }
    TiXmlElement* pEle = pRootEle;
    for (pEle = pRootEle->FirstChildElement(); pEle; pEle = pEle->NextSiblingElement()) {
        if(GetNodePointerByName(pEle,strNodeName,Node))
            return true;
    }

    return false;
}


bool RedisProxyCfg::rewriteConfig(RedisProxy *proxy) {
    TiXmlElement* pRootNode = (TiXmlElement*) m_operateXmlPointer->getRootElement();
    // TODO: rewrite the group list regarding RedisProxy::m_hashMapping
    rewriteGroups(proxy, pRootNode);
    rewriteSlotMap(proxy, pRootNode);
    rewriteKeyMap(proxy, pRootNode);
    rewriteMigrationSlots(proxy, pRootNode);

    // add time
    time_t now = time(NULL);
    char fileName[512];
    struct tm* current_time = localtime(&now);
    sprintf(fileName,"onecache%d%02d%02d%02d.xml",
        current_time->tm_year + 1900,
        current_time->tm_mon + 1,
        current_time->tm_mday,
        current_time->tm_hour);

    int rc = std::rename(configFile, fileName);
    if (rc) {
        LOG(Logger::Error, "rename config file failed %d", rc);
    }
    bool ok = m_operateXmlPointer->m_docPointer->SaveFile(configFile);
    return ok;
}

void RedisProxyCfg::rewriteMigrationSlots(RedisProxy *proxy, TiXmlElement *pRootNode) const {// migration slots
    string migrations = "migration_slots";
    TiXmlElement* p;
    if (GetNodePointerByName(pRootNode, migrations, p)) {
        pRootNode->RemoveChild(p);
    }
    TiXmlElement migrationSlots(migrations.c_str());
    for (int i = 0; i < proxy->maxHashValue(); i++) {
        RedisServantGroup *sg = proxy->GetSlotMigration(i);
        if (sg) {
            RedisServant *rs = sg->master(0);
            HostAddress addr("0.0.0.0", 0);
            if (rs) {
                addr = rs->redisAddress();
            }

            TiXmlElement migrateNode("migrate");
            migrateNode.SetAttribute("slot", i);
            migrateNode.SetAttribute("server_address", addr.ip());
            migrateNode.SetAttribute("port", addr.port());
            migrationSlots.InsertEndChild(migrateNode);
        }
    }
    pRootNode->InsertEndChild(migrationSlots);
}

void RedisProxyCfg::rewriteKeyMap(RedisProxy *proxy, TiXmlElement *pRootNode) const {// key mapping
    string nodeKey = "key_mapping";
    TiXmlElement* pKey;
    if (GetNodePointerByName(pRootNode, nodeKey, pKey)) {
        pRootNode->RemoveChild(pKey);
    }
    TiXmlElement keyMappingNode(nodeKey.c_str());

    StringMap<RedisServantGroup*>& keyMapping = proxy->keyMapping();
    StringMap<RedisServantGroup*>::iterator it = keyMapping.begin();
    for (; it != keyMapping.end(); ++it) {
        String key = it->first;
        TiXmlElement keyNode("key");
        keyNode.SetAttribute("key_name", key.data());
        RedisServantGroup* group = it->second;
        if (group != NULL) {
            keyNode.SetAttribute("group_name", group->groupName());
        }
        keyMappingNode.InsertEndChild(keyNode);
    }
    pRootNode->InsertEndChild(keyMappingNode);
}

void RedisProxyCfg::rewriteSlotMap(const RedisProxy *proxy, TiXmlElement *pRootNode) const {
    return;

//    string nodeHash = "hash_mapping";
    TiXmlElement* pOldHashMap;
    if (GetNodePointerByName(pRootNode, hashMapName, pOldHashMap)) {
        pRootNode->RemoveChild(pOldHashMap);
    }
    TiXmlElement hashMappingNode(hashMapName.c_str());
    for (int i = 0; i < proxy->maxHashValue(); ++i) {
        TiXmlElement hashNode("hash");
        hashNode.SetAttribute("value", i);
        hashNode.SetAttribute("group_name", proxy->slotNumToRedisServerGroup(i)->groupName());
        hashMappingNode.InsertEndChild(hashNode);
    }
    pRootNode->InsertEndChild(hashMappingNode);
}


bool RedisProxyCfg::loadCfg(const char* file) {
    configFile = file;

    if (!m_operateXmlPointer->xml_open(file)) return false;

    const TiXmlElement* pRootNode = m_operateXmlPointer->getRootElement();
    getRootAttr(pRootNode);
    TiXmlElement* pNode = (TiXmlElement*)pRootNode->FirstChildElement();
    for (; pNode != NULL; pNode = pNode->NextSiblingElement()) {
        if (0 == strcasecmp(pNode->Value(), "thread_num")) {
            if (pNode->GetText() != NULL) m_threadNum = atoi(pNode->GetText());
            continue;
        }
        if (0 == strcasecmp(pNode->Value(), "port")) {
            if (pNode->GetText() != NULL) m_port = atoi(pNode->GetText());
            continue;
        }
        if (0 == strcasecmp(pNode->Value(), "vip")) {
            getVipAttr(pNode);
            continue;
        }

        if (0 == strcasecmp(pNode->Value(), "top_key")) {
            TiXmlAttribute *addrAttr = (TiXmlAttribute *)pNode->FirstAttribute();
            for (; addrAttr != NULL; addrAttr = addrAttr->Next()) {
                const char* name = addrAttr->Name();
                const char* value = addrAttr->Value();
                if (0 == strcasecmp(name, "enable")) {
                    if(strcasecmp(value, "0") != 0 && strcasecmp(value, "") != 0 ) {
                        m_topKeyEnable = true;
                    }
                }
            }
            continue;
        }

//        if (0 == strcasecmp(pNode->Value(), "hash")) {
//            TiXmlElement* pNext = pNode->FirstChildElement();
//            if (NULL == pNext) continue;
//            for (; pNext != NULL; pNext = pNext->NextSiblingElement()) {
//                if (0 == strcasecmp(pNext->Value(), "hash_value_max")) {
//                    if (pNext->GetText() == NULL) {
//                        m_hashInfo.hash_value_max = 0;
//                        continue;
//                    }
//                    m_hashInfo.hash_value_max = atoi(pNext->GetText());
//                }
//            }
//            continue;
//        }
        if (0 == strcasecmp(pNode->Value(), groupName.c_str())) {
            getGroupNode(pNode);
            continue;
        }
        if (0 == strcasecmp(pNode->Value(), hashMapName.c_str())) {
            setHashMappingNode(pNode);
            continue;
        }
        if (0 == strcasecmp(pNode->Value(), "group_option")) {
            setGroupOption(pNode);
            continue;
        }
        if (0 == strcasecmp(pNode->Value(), "key_mapping")) {
            setKeyMappingNode(pNode);
            continue;
        }
        if (0 == strcasecmp(pNode->Value(), "migration_slots")) {
            LoadMigrationSlots(pNode);
            continue;
        }
    }

    return true;
}


bool RedisProxyCfgChecker::isValid(RedisProxyCfg *pCfg, char *errMsg, int len)
{
    const int hash_value_max = pCfg->hashInfo()->hash_value_max;
    if (hash_value_max > REDIS_PROXY_HASH_MAX) {
        snprintf(errMsg, len, "hash_value_max is not greater than 1024");
        return false;
    }

    int port = pCfg->port();
    if (port <= 0 || port > 65535) {
        snprintf(errMsg, len, "port is invalid");
        return false;
    }

    int thread_num = pCfg->threadNum();
    if (thread_num <= 0) {
        snprintf(errMsg, len, "thread_num is invalid");
        return false;
    }

    bool barray[REDIS_PROXY_HASH_MAX] = {0};
    string groupNameBuf[512];
    int groupCnt_ = pCfg->groupCnt();
    for (int i = 0; i < groupCnt_; ++i) {
        const CGroupInfoPtr group = pCfg->group(i);
        if (0 == strlen(group->groupName())) {
            snprintf(errMsg, len, "group name cannot be empty");
            return false;
        }
        bool empty = false;
        if (0 == strlen(group->groupPolicy())) {
            empty = true;
        }
        if (!empty) {
            if (0 != strcasecmp(group->groupPolicy(), POLICY_READ_BALANCE) &&
                0 != strcasecmp(group->groupPolicy(), POLICY_MASTER_ONLY))
            {
                snprintf(errMsg, len, "invalid policy, it should be read_balance or master_only");
                return false;
            }
        }

        groupNameBuf[i] = group->groupName();
        for (int j = 0; j < i; ++j) {
            if (groupNameBuf[i] == groupNameBuf[j]) {
                snprintf(errMsg, len, "group name exsited");
                return false;
            }
        }

        if (group->hashMin() > group->hashMax()) {
            snprintf(errMsg, len, "hash_min > hash_max");
            return false;
        }
        for (int j = group->hashMin(); j <= group->hashMax(); ++j) {
            if (j < 0 || j > REDIS_PROXY_HASH_MAX) {
                snprintf(errMsg, len, "hash value is invalid");
                return false;
            }
            if (j >= hash_value_max) {
                snprintf(errMsg, len, "hash value is out of range");
                return false;
            }

            if (barray[j]) {
                snprintf(errMsg, len, "hash range error");
                return false;
            }
            barray[j] = true;
        }
    }
    for (int i = 0; i < hash_value_max; ++i) {
        if (!barray[i]) {
            snprintf(errMsg, len, "hash values are not complete");
            return false;
        }
    }

    const GroupOption* groupOp = pCfg->groupOption();
    if (groupOp->backend_retry_interval <= 0) {
        snprintf(errMsg, len, "backend_retry_interval invalid");
        return false;
    }

    if (groupOp->backend_retry_limit <= 0) {
        snprintf(errMsg, len, "backend_retry_limit invalid");
        return false;
    }

    if (groupOp->auto_eject_group) {
        if (groupOp->group_retry_time <= 0) {
            snprintf(errMsg, len, "group_retry_time invalid");
            return false;
        }
    }

    int hashMapCnt = pCfg->hashMapCnt();
    for (int i = 0; i < hashMapCnt; ++i) {
        const HashMapping * p = pCfg->hashMapping(i);
        if (p->hash_value < 0) {
            snprintf(errMsg, len, "hash_mapping value < 0");
            return false;
        }
        bool exist = false;
        for (int j = 0; j < groupCnt_; ++j) {
            if (groupNameBuf[j] == p->group_name) {
                exist = true;
                break;
            }
        }
        if (!exist) {
            snprintf(errMsg, len, "group name [%s] of hash_mapping doesn't exist", p->group_name);
            return false;
        }
    }

    int keyMapCnt = pCfg->keyMapCnt();
    for (int i = 0; i < keyMapCnt; ++i) {
        const KeyMapping * p = pCfg->keyMapping(i);
        std::string groupName = p->group_name;
        bool exist = false;
        for (int j = 0; j < groupCnt_; ++j) {
            if (groupNameBuf[j] == groupName) {
                exist = true;
                break;
            }
        }
        if (!exist) {
            snprintf(errMsg, len, "group name [%s] of key_mapping doesn't exist", p->group_name);
            return false;
        }
    }

    return true;
}

// TODO: rewriteGroups test cases
bool RedisProxyCfg::rewriteGroups(RedisProxy *proxy, TiXmlElement *pRootNode) {
    //
    // group: | g1 | g1 | g1 | g2 | g2 | g3 | g3 | g3 |
    // index:   0    1    2    3    4    5    6    7
    //                         ^                   ^
    //                         |                   |
    // previous group: g1, current group: g2       |
    // hash min: 0, hash max: 2                    |
    //                                             |
    //                                  previous group: g3, current group: g3
    //                                  hash min: 5, hash max: 7
    std::vector<std::shared_ptr<CGroupInfo>> newGroupInfoList;
//typedef std::shared_ptr<CGroupInfo> CGroupInfoPtr;
//    GroupInfoList *newGroupInfoList = new GroupInfoList;
    RedisServantGroup *preSg = NULL;
    int hashMin = 0, hashMax = 0;
    int i;
    for (i=0; i < proxy->maxHashValue(); i++) {
        RedisServantGroup *sg = proxy->slotNumToRedisServerGroup(i);
        if (preSg == NULL) {
            hashMin = i;
            preSg = sg;
        } else {
            if (preSg != sg) {
                // if the current group is a new one, than the hash max of
                // previous group should be (current index - 1)
                hashMax = i - 1;
                {
                    std::shared_ptr<CGroupInfo> groupInfo = std::make_shared<CGroupInfo>();
                    if (foundAGroup(groupInfo, hashMin, hashMax, i, preSg)) {
                        newGroupInfoList.push_back(groupInfo);
                    }
                }
                preSg = sg;
                hashMax = hashMin = i;
            }
        }
    }
    {
        std::shared_ptr<CGroupInfo> groupInfo = std::make_shared<CGroupInfo>();
        if (foundAGroup(groupInfo, hashMin, i-1, i, preSg)) {
            newGroupInfoList.push_back(groupInfo);
        }
    }

    // remove current groups
    TiXmlElement *oldGroup;
    while (GetNodePointerByName(pRootNode, groupName, oldGroup)) {
        pRootNode->RemoveChild(oldGroup);
    }

    for (std::vector<std::shared_ptr<CGroupInfo>>::iterator it = newGroupInfoList.begin();
            it != newGroupInfoList.end(); ++it) {
        LOG(Logger::INFO, "rewrite group %s", (*it)->groupName());
        rewriteOneGroup(pRootNode, *it);
    }

    return true;
}

void RedisProxyCfg::rewriteOneGroup(TiXmlElement *pRootNode, const shared_ptr<CGroupInfo> &pInfo) const {
// insert new group
    TiXmlElement groupNode(groupName.c_str());
//    groupNode.SetAttribute("name", pInfo->groupName());
    char name[128];
    snprintf(name, 128, "group_%d_%d", pInfo->hashMin(), pInfo->hashMax());
    groupNode.SetAttribute("name", name);
    groupNode.SetAttribute("hash_min", pInfo->hashMin());
    groupNode.SetAttribute("hash_max", pInfo->hashMax());
    groupNode.SetAttribute("policy", pInfo->groupPolicy());
    HostInfoList infoList = pInfo->hosts();
    for (std::vector<CHostInfo>::iterator it = infoList.begin();
            it != infoList.end(); ++it) {
        TiXmlElement hostNode(hostName.c_str());
        hostNode.SetAttribute("host_name", it->get_hostName().c_str());
        hostNode.SetAttribute("ip", it->get_ip().c_str());
        hostNode.SetAttribute("port", it->get_port());
        hostNode.SetAttribute("master", it->get_master());
        hostNode.SetAttribute("connection_num", it->get_connectionNum());
        groupNode.InsertEndChild(hostNode);
    }
    pRootNode->InsertEndChild(groupNode);
}

bool RedisProxyCfg::foundAGroup(std::shared_ptr<CGroupInfo> groupInfo, int hashMin, int hashMax,
                                int currentIndex, const RedisServantGroup *sg) const {
    if (hashMax >= hashMin) {
        LOG(Logger::INFO, "found a group min %d max %d i %d g %s",
            hashMin, hashMax, currentIndex, sg->groupName());
        // found a new group, append group info to list
        groupInfo->setName(sg->groupName());
        groupInfo->setPolicy(sg->policy()->getName().c_str());
        groupInfo->setHashMin(hashMin);
        groupInfo->setHashMax(hashMax);

        CHostInfo hostInfo;
        RedisServant *s = sg->master(0);
        RedisServant::Option opt = s->option();

        std::string name = std::string(opt.name);
        std::string ip = std::string(s->redisAddress().ip());

        hostInfo.set_hostName(name);
        hostInfo.set_ip(ip);
        hostInfo.set_port(s->redisAddress().port());
        hostInfo.set_master(true);
        hostInfo.set_connectionNum(opt.poolSize);

        HostInfoList infoList;
        infoList.push_back(hostInfo);

        groupInfo->setHosts(infoList);
        return true;
    }

    return false;
}
