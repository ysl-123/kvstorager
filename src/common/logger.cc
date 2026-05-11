#include "logger.h"

#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <thread>

namespace
{
    const char *LogLevelToString(LogLevel level)
    {
        switch (level)
        {
        case ERROR:
            return "error";
        case WARN:
            return "warn";
        case INFO:
            return "info";
        case DEBUG:
            return "debug";
        default:
            return "unknown";
        }
    }
} // namespace

Logger &Logger::GetInstance()
{
    static Logger logger;
    return logger;
}
Logger::Logger()
    : m_minLogLevel(INFO),
      m_filterMode(LOG_LEVEL_AND_ABOVE),
      m_nodeId(-1)
{
   

    std::thread writeLogTask([&]()
{
     FILE *pf = nullptr;
    std::string current_file_name;
        for (;;)
{
    std::string msg = m_lckQue.Pop();

    time_t now = time(nullptr);
    tm* nowtm = localtime(&now);

    char file_name[128];
    sprintf(file_name, "%d-%d-%d-node-%d-log.txt",
            nowtm->tm_year + 1900,
            nowtm->tm_mon + 1,
            nowtm->tm_mday,
            m_nodeId);

    if (pf == nullptr || current_file_name != file_name)
    {
        if (pf != nullptr)
        {
            fclose(pf);
        }

        pf = fopen(file_name, "a+");
        if (pf == nullptr)
        {
            std::cout << "logger file : " << file_name << " open error!" << std::endl;
            exit(EXIT_FAILURE);
        }

        current_file_name = file_name;
    }

    fputs(msg.c_str(), pf);
    fflush(pf);
} });

    writeLogTask.detach();
}

void Logger::SetLogLevel(LogLevel level)
{
    m_minLogLevel = level;
    m_filterMode = LOG_LEVEL_AND_ABOVE;
}
void Logger::SetNodeId(int nodeId)
{
    m_nodeId = nodeId;
    time_t now = time(nullptr);
    tm *nowtm = localtime(&now);

    char file_name[128];
    sprintf(file_name, "%d-%d-%d-node-%d-log.txt",
            nowtm->tm_year + 1900,
            nowtm->tm_mon + 1,
            nowtm->tm_mday,
            m_nodeId);

    FILE *pf = fopen(file_name, "a+");
    if (pf != nullptr)
    {
        fclose(pf);
    }
}

void Logger::SetExactLogLevel(LogLevel level)
{
    m_minLogLevel = level;
    m_filterMode = LOG_LEVEL_EXACT;
}

bool Logger::ShouldLog(LogLevel level) const
{
    if (m_filterMode == LOG_LEVEL_EXACT)
    {
        return level == m_minLogLevel;
    }

    return level <= m_minLogLevel;
}

void Logger::Log(LogLevel level, const std::string &msg)
{
    if (!ShouldLog(level))
    {
        return;
    }

    time_t now = time(nullptr);
    tm *nowtm = localtime(&now);

    char time_buf[128] = {0};
    sprintf(time_buf, "%d:%d:%d =>[%s] ",
            nowtm->tm_hour,
            nowtm->tm_min,
            nowtm->tm_sec,
            LogLevelToString(level));

    std::string log_msg = time_buf;
    log_msg += msg;
    log_msg += "\n";

    m_lckQue.Push(log_msg);
}
