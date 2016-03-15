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

#ifndef REDIS_PROXY_CONFIG
#define REDIS_PROXY_CONFIG
#include <iostream>
#include <map>
#include <vector>
#include <string>
#include <algorithm>
#include <memory>
#include <bits/shared_ptr.h>

#include "tinyxml/tinyxml.h"
#include "tinyxml/tinystr.h"
#include "util/logger.h"
#include "redisproxy.h"

using namespace std;

typedef class COperateXml
{
public:
    COperateXml();
    ~COperateXml();

    bool xml_open(const char* xml_path);

    virtual void xml_print();
    const TiXmlElement*getRootElement();
public:
    // xml file operate pointer
    TiXmlDocument* m_docPointer;
private:
    TiXmlElement* m_rootElement;
private:
    COperateXml(const COperateXml&);
    COperateXml& operator =(const COperateXml&);
}COperateXml;


// host info
class CHostInfo {
public:
    CHostInfo();
    ~CHostInfo();
    string get_ip()const;
    string get_hostName()const;
    int get_port()const;
    bool get_master()const;
    int get_policy()const;
    int get_priority()const;
    int get_connectionNum()const;

    void set_ip(string& s);
    void set_hostName(string& s);
    void set_port(int p)  ;
    void set_master(bool m);
    void set_policy(int p);
    void set_priority(int p);
    void set_connectionNum(int p);
private:
    string ip;
    string host_name;
    int port;
    bool master;
    int priority;
    int policy;
    int connection_num;
};
typedef std::vector<CHostInfo> HostInfoList;

struct SHashInfo {
    // int hash_type;
    int hash_value_max;
};

struct SVipInfo {
    char if_alias_name[50];
    char vip_address[30];
    bool enable;
};


class HashMapping {
public:
    HashMapping() {
        hash_value = 0;
        memset(group_name, '\0', sizeof(group_name));
    }
    int  hash_value;
    char group_name[512];
};
typedef std::vector<HashMapping> HashMappingList;

class KeyMapping {
public:
    KeyMapping() {
        memset(key, '\0', sizeof(key));
        memset(group_name, '\0', sizeof(group_name));
    }
    char key[512];
    char group_name[512];
};
typedef std::vector<KeyMapping> KeyMappingList;

class MigrationOption {
public:
    MigrationOption() :
            slotNum(-1), addr(""), port(-1)
    {
    }

    int slotNum;
    string addr;
    int port;

    bool Valid() {
        if (slotNum == -1) {
            return false;
        }
        if (addr == "") {
            return false;
        }
        if (port == -1) {
            return false;
        }

        return true;
    }
};

class CGroupInfo
{
public:
    CGroupInfo();
    CGroupInfo(const CGroupInfo &info);
    ~CGroupInfo();
    const char* groupName() const { return m_groupName; }
    const char* groupPolicy() const { return m_groupPolicy; }
    int hashMin()const { return m_hashMin; }
    int hashMax()const { return m_hashMax; }
    const HostInfoList& hosts() const { return m_hosts; }
    void setGroupPolicy(const char* p) {
        strcpy(m_groupPolicy, p);
        m_groupPolicy[strlen(p) + 1] = '\0';
    }
    void setName(const char *name) {
        snprintf(m_groupName, 128, "%s", name);
    }
    void setPolicy(const char *policy) {
        snprintf(m_groupPolicy, 128, "%s", policy);
    }
    void setHashMin(int m_hashMin) {
        CGroupInfo::m_hashMin = m_hashMin;
    }

    void setHashMax(int m_hashMax) {
        CGroupInfo::m_hashMax = m_hashMax;
    }

    void setHosts(const HostInfoList &m_hosts) {
        CGroupInfo::m_hosts = m_hosts;
    }

private:
    char          m_groupName[128];
    char          m_groupPolicy[128];
    int           m_hashMin;
    int           m_hashMax;
    HostInfoList  m_hosts;
    friend class RedisProxyCfg;
};
typedef std::vector<CGroupInfo> GroupInfoList;
//typedef std::vector<std::shared_ptr<CGroupInfo>> GroupInfoList;
//typedef std::shared_ptr<CGroupInfo> CGroupInfoPtr;
typedef CGroupInfo* CGroupInfoPtr;

struct GroupOption {
    GroupOption(){
        backend_retry_interval = 1;
        backend_retry_limit = 100;
        group_retry_time = 30;
        auto_eject_group = false;
        eject_after_restore = false;
    }
    int  backend_retry_interval;
    int  backend_retry_limit;
    int  group_retry_time;
    bool auto_eject_group;
    bool eject_after_restore;
};



// read the config
class RedisProxyCfg
{
private:
    RedisProxyCfg();
    ~RedisProxyCfg();

public:
    static RedisProxyCfg * instance();

    bool loadCfg(const char* xml_path);
    bool rewriteConfig(RedisProxy *proxy);

    int groupCnt()const {return m_groupInfo->size();}
    const CGroupInfoPtr group(int index)const {return &(*m_groupInfo)[index];}
    const SHashInfo*  hashInfo()const {return &m_hashInfo;}
    const GroupOption* groupOption()const {return &m_groupOption;}
    const SVipInfo*  vipInfo()const {return &m_vip;}
    int threadNum()const {return m_threadNum;}
    int port() const {return m_port;}
    const char* logFile(){ return m_logFile; }
    bool daemonize() { return m_daemonize;}
    bool guard() { return m_guard;}
    bool topKeyEnable() { return m_topKeyEnable;}

    int hashMapCnt(){ return m_hashMappingList->size();}
    int keyMapCnt(){ return m_keyMappingList->size();}
    const HashMapping * hashMapping(int index)const {return &(*m_hashMappingList)[index];}
    const KeyMapping * keyMapping(int index)const {return &(*m_keyMappingList)[index];}

    std::vector<MigrationOption>    migrationOptions;
    COperateXml*     m_operateXmlPointer;
private:
    GroupInfoList*   m_groupInfo;
    HashMappingList* m_hashMappingList;
    KeyMappingList*  m_keyMappingList;
    SHashInfo        m_hashInfo;
    SVipInfo         m_vip;
    int              m_threadNum;
    int              m_port;
    char             m_logFile[512];
    bool             m_daemonize;
    bool             m_guard;
    bool             m_topKeyEnable;
    GroupOption      m_groupOption;
    const char       *configFile;

    const string    groupName = "group";
    const string    hostName = "host";
    const string    hashMapName = "hash_mapping";
    const string    hashMinName = "hash_min";
    const string    hashMaxName = "hash_max";

private:
    void set_groupName(CGroupInfo& group, const char* name);
    void set_hashMin(CGroupInfo& group, int num);
    void set_hashMax(CGroupInfo& group, int num);
    void addHost(CGroupInfo& group, CHostInfo& h);

    void set_groupAttribute(TiXmlAttribute* groupAttr, CGroupInfo& group);
    void set_hostAttribute(TiXmlAttribute* addrAttr, CHostInfo& pHostInfo);
    void set_hostEle(TiXmlElement* hostContactEle, CHostInfo& hostInfo);
    void getRootAttr(const TiXmlElement* pRootNode);
    void getVipAttr(const TiXmlElement* vidNode);
    void getGroupNode(TiXmlElement* pNode);
    void setHashMappingNode(TiXmlElement* pNode);
    void setKeyMappingNode(TiXmlElement* pNode);
    void setGroupOption(const TiXmlElement* pNode);
    void LoadMigrationSlots(TiXmlElement *pNode);
private:
    RedisProxyCfg(const RedisProxyCfg &);
    RedisProxyCfg & operator =(const RedisProxyCfg &);

    bool rewriteGroups(RedisProxy *proxy, TiXmlElement *pRootNode);

    bool foundAGroup(std::shared_ptr<CGroupInfo> groupInfo, int hashMin, int hashMax,
                     int currentIndex, const RedisServantGroup *sg) const;

    void rewriteOneGroup(TiXmlElement *pRootNode, const shared_ptr<CGroupInfo> &pInfo) const;

    void rewriteSlotMap(const RedisProxy *proxy, TiXmlElement *pRootNode) const;

    void rewriteKeyMap(RedisProxy *proxy, TiXmlElement *pRootNode) const;

    void rewriteMigrationSlots(RedisProxy *proxy, TiXmlElement *pRootNode) const;
};

// check the cfg is valid or not
class RedisProxyCfgChecker
{
public:
    enum {REDIS_PROXY_HASH_MAX = 1024};
    RedisProxyCfgChecker();
    ~RedisProxyCfgChecker();
    static bool isValid(RedisProxyCfg *pCfg, char *errmsg, int len);
};

#endif
