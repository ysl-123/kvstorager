#include <cstdlib>
#include <fstream>
#include <iostream>
#include <random>
#include <unistd.h>

#include "logger.h"
#include "raft.h"
#include <kvServer.h>

std::string GetIpFromConfig(const std::string& configFileName, int nodeId);
unsigned short GetPortFromConfig(const std::string& configFileName, int nodeId);
void ShowArgsHelp();

// Service provider: start one raft kv node each time.
int main(int argc, char** argv) {
  // Usage:
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

  pid_t pid = fork();

  if (pid == 0) {
    Logger::GetInstance().SetNodeId(nodeId);
    Logger::GetInstance().SetLogLevel(DEBUG);
    LOG_INFO("start raftkv node: %d ip: %s port: %hu pid: %d",
             nodeId, ip.c_str(), port, getpid());
    auto kvServer = new KvServer(nodeId, 500, configFileName, ip, port);
    pause();
  } else if (pid > 0) {    sleep(1);
  } else {
    std::cerr << "Failed to create child process." << std::endl;
    exit(EXIT_FAILURE);
  }

  pause();
  return 0;
}

unsigned short GetPortFromConfig(const std::string& configFileName, int nodeId) {
  std::ifstream file(configFileName);
  if (!file.is_open()) {
    exit(EXIT_FAILURE);
  }

  std::string targetKey = "node" + std::to_string(nodeId) + "port=";
  std::string line;

  while (std::getline(file, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }

    if (line.rfind(targetKey, 0) == 0) {
      int port = std::stoi(line.substr(targetKey.size()));
      if (port <= 0 || port > 65535) {
        exit(EXIT_FAILURE);
      }
      return static_cast<unsigned short>(port);
    }
  }
  exit(EXIT_FAILURE);
}

std::string GetIpFromConfig(const std::string& configFileName, int nodeId) {
  std::ifstream file(configFileName);
  if (!file.is_open()) {
    exit(EXIT_FAILURE);
  }

  std::string targetKey = "node" + std::to_string(nodeId) + "ip=";
  std::string line;

  while (std::getline(file, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }

    if (line.rfind(targetKey, 0) == 0) {
      std::string ip = line.substr(targetKey.size());
      if (ip.empty()) {
        exit(EXIT_FAILURE);
      }
      return ip;
    }
  }
  exit(EXIT_FAILURE);
}

void ShowArgsHelp() {
  std::cout << "format: command -i <nodeId> -f <configFileName>" << std::endl;
}
