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
#include <stdarg.h>
#include <memory.h>
#include <sys/time.h>

#include "util/string.h"
#include "logger.h"

static Logger _stdoutput;
Logger* _defaultLogger = &_stdoutput;

const char* Logger::msg_type_text[] =
{
    "CRITAL",
    "FATA",
    "Error",
    "Warning",
    "INFO",
    "DEBUG",
    "VERBOSE",
    "VVERBOSE",
    "VVVERBOSE",
};

int Logger::logLevel = Logger::DEBUG;

Logger::Logger(void)
{
}

Logger::~Logger(void)
{
}

void Logger::output(Logger::MsgType type, const char *msg)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);

    char buf[256];
    nc_strftime(buf, 256, "%Y-%m-%d %H:%M:%S", localtime(&tv.tv_sec));
    printf("%s.%03ld [%s] %s\n",
            buf, tv.tv_usec/1000, msg_type_text[type], msg);
}

#define LOG_MAX_LEN 10240
void Logger::log(Logger::MsgType type, const char *file, int line, const char *format, ...)
{
    if (!format || !_defaultLogger) {
        return;
    }

    va_list args;
    struct timeval tv;
    char buf[LOG_MAX_LEN];

    int len = 0;            /* length of output buffer */
    int size = LOG_MAX_LEN; /* size of output buffer */

    gettimeofday(&tv, NULL);
//    buf[len++] = '[';
//    len += nc_strftime(buf + len, size - len, "%Y-%m-%d %H:%M:%S.", localtime(&tv.tv_sec));
//    len += nc_scnprintf(buf + len, size - len, "%03ld", tv.tv_usec/1000);
    len += nc_scnprintf(buf + len, size - len, "%s:%d ", file, line);

    va_start(args, format);
    len += vsnprintf(buf + len, size - len, format, args);
    va_end(args);

    _defaultLogger->output(type, buf);
}

Logger *Logger::defaultLogger(void)
{
    return _defaultLogger;
}

void Logger::setDefaultLogger(Logger *logger)
{
    _defaultLogger = logger;
}



FileLogger::FileLogger(void)
{
    m_fp = NULL;
    memset(m_fileName, 0, sizeof(m_fileName));
}

FileLogger::FileLogger(const char *fileName)
{
    setFileName(fileName);
}

FileLogger::~FileLogger(void)
{
    if (m_fp) {
        fclose(m_fp);
    }
}

bool FileLogger::setFileName(const char *fileName)
{
    FILE* fp = fopen(fileName, "a+");
    if (!fp) {
        return false;
    }

    strcpy(m_fileName, fileName);
    if (m_fp) {
        fclose(m_fp);
    }
    m_fp = fp;
    return true;
}

void FileLogger::output(Logger::MsgType type, const char *msg)
{
    time_t t = time(NULL);
    tm* lt = localtime(&t);

    fprintf(m_fp, "[%d-%02d-%02d %02d:%02d:%02d] %s: %s\n",
            lt->tm_year + 1900, lt->tm_mon + 1, lt->tm_mday,
            lt->tm_hour, lt->tm_min, lt->tm_sec, msg_type_text[type], msg);
    fflush(m_fp);
}

const char * Logger::Level() {
    return msg_type_text[logLevel];
}
