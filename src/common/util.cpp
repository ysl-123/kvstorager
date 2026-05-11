#include "util.h"
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <iomanip>
#include <logger.h>
//不符合这个条件报错
void myAssert(bool condition, std::string message) {
  if (!condition) {
    LOG_ERROR("Error: %s", message.c_str());
    std::exit(EXIT_FAILURE);
  }
}
//实际上就是system_clock    代表的是真实的物理世界时间  也就是你电脑右下角显示的系统时间
std::chrono::_V2::system_clock::time_point now() { return std::chrono::high_resolution_clock::now(); }
//生成一个随机的选举超时时间
//很巧妙，防止选票瓜分，假如leader宕机了，5个follwer超时时间相同，同时超时，都投给自己，导致选票一直没办法超过一半，整个集群直接死了
std::chrono::milliseconds getRandomizedElectionTimeout() {
  std::random_device rd;
  std::mt19937 rng(rd());
  std::uniform_int_distribution<int> dist(minRandomizedElectionTime, maxRandomizedElectionTime);

  return std::chrono::milliseconds(dist(rng));
}

void sleepNMilliseconds(int N) { std::this_thread::sleep_for(std::chrono::milliseconds(N)); };

bool getReleasePort(short &port) {
  short num = 0;
  while (!isReleasePort(port) && num < 30) {
    ++port;
    ++num;
  }
  if (num >= 30) {
    port = -1;
    return false;
  }
  return true;
}

bool isReleasePort(unsigned short usPort) {
  int s = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
  sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = htons(usPort);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  int ret = ::bind(s, (sockaddr *)&addr, sizeof(addr));
  if (ret != 0) {
    close(s);
    return false;
  }
  close(s);
  return true;
}

void DPrintf(const char *format, ...) {
  if (!Debug) {
    return;
  }

  va_list args;
  va_start(args, format);

  char buffer[2048] = {0};
  vsnprintf(buffer, sizeof(buffer), format, args);

  va_end(args);

  Logger::GetInstance().Log(DEBUG, buffer);
}
