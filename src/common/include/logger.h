#pragma once
#include "lockqueue.h"
#include <string>

enum LogLevel
{
    ERROR = 0, // 错误：会导致程序崩溃或严重问题的消息
    WARN = 1,  // 警告：不影响运行但需要注意的消息
    INFO = 2,  // 信息：普通的程序运行状态说明
    DEBUG = 3  // 调试：用于排查问题的详细消息
};

enum LogFilterMode
{
    LOG_LEVEL_AND_ABOVE = 0, // 输出某个级别及以上
    LOG_LEVEL_EXACT = 1      // 只输出某个级别
};

// Mprpc 框架提供的日志系统
class Logger
{
public:
    // 获取日志的单例
    static Logger& GetInstance();
    // 设置日志级别阈值，输出该级别及以上的日志
    void SetLogLevel(LogLevel level);
    // 只输出某一个级别的日志
    void SetExactLogLevel(LogLevel level);
    // 写日志
    void Log(LogLevel level, const std::string& msg);
    void SetNodeId(int nodeId);

private:
    bool ShouldLog(LogLevel level) const;

private:
    LogLevel m_minLogLevel;
    LogFilterMode m_filterMode;
    LockQueue1<std::string> m_lckQue; // 日志缓冲队列
    int  m_nodeId=-1;
    Logger();
    Logger(const Logger&) = delete;
    Logger(Logger&&) = delete;
};

// LOG_INFO("service_name:%s", service_name.c_str());
#define LOG_INFO(logmsgformat, ...) \
do \
{ \
    Logger &logger = Logger::GetInstance(); \
    char c[1024] = {0}; \
    snprintf(c, 1024, logmsgformat, ##__VA_ARGS__); \
    logger.Log(INFO, c); \
} while (0)

#define LOG_ERROR(logmsgformat, ...) \
do \
{ \
    Logger &logger = Logger::GetInstance(); \
    char c[1024] = {0}; \
    snprintf(c, 1024, logmsgformat, ##__VA_ARGS__); \
    logger.Log(ERROR, c); \
} while (0)

#define LOG_WARN(logmsgformat, ...) \
do \
{ \
    Logger &logger = Logger::GetInstance(); \
    char c[1024] = {0}; \
    snprintf(c, 1024, logmsgformat, ##__VA_ARGS__); \
    logger.Log(WARN, c); \
} while (0)

#define LOG_DEBUG(logmsgformat, ...) \
do \
{ \
    Logger &logger = Logger::GetInstance(); \
    char c[1024] = {0}; \
    snprintf(c, 1024, logmsgformat, ##__VA_ARGS__); \
    logger.Log(DEBUG, c); \
} while (0)
