#include <iostream>
#include <fstream>
#include "raft.h"
#include <kvServer.h>
#include <unistd.h>
#include <random>

std::string GetIpFromConfig(const std::string &configFileName, int nodeId);
unsigned short GetPortFromConfig(const std::string &configFileName, int nodeId);
void ShowArgsHelp();

// 服务提供方：每次只启动一个节点
int main(int argc, char **argv) {
  // 用法:
  // ./raftCoreRun -i 0 -f test.conf
  // ./raftCoreRun -i 1 -f test.conf
  // ./raftCoreRun -i 2 -f test.conf

  if (argc < 2) {
    ShowArgsHelp();
    return EXIT_FAILURE;
  }

  int c = 0;
  int nodeId = -1;
  std::string configFileName;

  while ((c = getopt(argc, argv, "i:f:")) != -1) {
    switch (c) {
      case 'i':
        nodeId = atoi(optarg);
        break;
      case 'f':
        configFileName = optarg;
        break;
      default:
        ShowArgsHelp();
        return EXIT_FAILURE;
    }
  }

  if (nodeId < 0 || configFileName.empty()) {
    ShowArgsHelp();
    return EXIT_FAILURE;
  }

  std::string ip = GetIpFromConfig(configFileName, nodeId);
  unsigned short port = GetPortFromConfig(configFileName, nodeId);

  std::cout << "start raftkv node: "
            << nodeId
            << " ip: "
            << ip
            << " port: "
            << port
            << " pid: "
            << getpid()
            << std::endl;

  pid_t pid = fork();

  if (pid == 0) {
    // 子进程启动真正的 KvServer
    auto kvServer = new KvServer(nodeId, 500, configFileName, ip, port);

    // 防止退出
    pause();

  } else if (pid > 0) {

    // 父进程等待子进程初始化
    sleep(1);

  } else {

    std::cerr << "Failed to create child process." << std::endl;
    exit(EXIT_FAILURE);
  }

  // 主线程挂起
  pause();

  return 0;
}

unsigned short GetPortFromConfig(const std::string &configFileName, int nodeId) {

  std::ifstream file(configFileName);

  if (!file.is_open()) {
    std::cerr << "无法打开配置文件: " << configFileName << std::endl;
    exit(EXIT_FAILURE);
  }

  std::string targetKey =
      "node" + std::to_string(nodeId) + "port=";

  std::string line;

  while (std::getline(file, line)) {

    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }

    if (line.rfind(targetKey, 0) == 0) {

      int port = std::stoi(line.substr(targetKey.size()));

      if (port <= 0 || port > 65535) {
        std::cerr << "配置里的端口非法: " << line << std::endl;
        exit(EXIT_FAILURE);
      }

      return static_cast<unsigned short>(port);
    }
  }

  std::cerr
      << "在配置文件中找不到 node"
      << nodeId
      << " 的端口"
      << std::endl;

  exit(EXIT_FAILURE);
}

std::string GetIpFromConfig(const std::string &configFileName, int nodeId) {

  std::ifstream file(configFileName);

  if (!file.is_open()) {
    std::cerr << "无法打开配置文件: "
              << configFileName
              << std::endl;

    exit(EXIT_FAILURE);
  }

  std::string targetKey =
      "node" + std::to_string(nodeId) + "ip=";

  std::string line;

  while (std::getline(file, line)) {

    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }

    if (line.rfind(targetKey, 0) == 0) {

      std::string ip = line.substr(targetKey.size());

      if (ip.empty()) {
        std::cerr << "配置里的 ip 为空: "
                  << line
                  << std::endl;

        exit(EXIT_FAILURE);
      }

      return ip;
    }
  }

  std::cerr
      << "在配置文件中找不到 node"
      << nodeId
      << " 的 ip"
      << std::endl;

  exit(EXIT_FAILURE);
}

void ShowArgsHelp() {

  std::cout
      << "format: command -i <nodeId> -f <configFileName>"
      << std::endl;
}