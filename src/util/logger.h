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

#ifndef LOGGER_H
#define LOGGER_H

#include <stdio.h>

#define nc_snprintf(_s, _n, ...)        \
    snprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)

#define nc_scnprintf(_s, _n, ...)       \
    snprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)

#define nc_vsnprintf(_s, _n, _f, _a)    \
    vsnprintf((char *)(_s), (size_t)(_n), _f, _a)

#define nc_vscnprintf(_s, _n, _f, _a)   \
    snprintf((char *)(_s), (size_t)(_n), _f, _a)

#define nc_strftime(_s, _n, fmt, tm)        \
    (int)strftime((char *)(_s), (size_t)(_n), fmt, tm)

#include <string.h>
#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

static int log_loggable(int)
{
    return 1;
}

#define log_vvv(...) do {                                                  \
    if (log_loggable(Logger::VVVERBOSE) != 0) {                                      \
        Logger::log2(__FILE__, __LINE__, __VA_ARGS__);                           \
    }                                                                       \
} while (0)

class Logger
{
public:
    enum MsgType {
        CRITAL  = 0,
        FATA    = 1,
        Error   = 2,
        Warning = 3,
        INFO    = 4,
        Message = 4,
        DEBUG   = 5,
        VERBOSE = 6,
        VVERBOSE = 7,
        VVVERBOSE = 8,
    };

    Logger(void);
    virtual ~Logger(void);

    //Output handler
    virtual void output(MsgType type, const char* msg);

    //Output function
    static void log(MsgType type, const char *file, int line, const char* format, ...);
#define LOG(type, ...) do { Logger::log(type, __FILENAME__, __LINE__, __VA_ARGS__); } while(0);

    //Default logger
    static Logger* defaultLogger(void);

    //Set Default logger
    static void setDefaultLogger(Logger* logger);
};



class FileLogger : public Logger
{
public:
    FileLogger(void);
    FileLogger(const char* fileName);
    ~FileLogger(void);

    bool setFileName(const char* fileName);
    const char* fileName(void) const
    { return m_fileName; }

    virtual void output(MsgType type, const char *msg);

private:
    FILE* m_fp;
    char m_fileName[512];
};



#endif
