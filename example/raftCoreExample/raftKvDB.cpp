
#include <iostream>
#include "raft.h"
// #include "kvServer.h"
#include <kvServer.h>
#include <unistd.h>
#include <iostream>
#include <random>

void ShowArgsHelp();

int main(int argc, char **argv) {
  //  ./raftCoreRun -n 3 -f test.conf
  //   读取命令参数：节点数量、写入raft节点节点信息到哪个文件
  if (argc < 2) {
    ShowArgsHelp();
    exit(EXIT_FAILURE);
  }
  int c = 0;
  int nodeNum = 0;             // 存储要启动的 Raft 节点总数 (比如 3 或 5)
  //变量本身只是一个字符串（比如它等于 "raft.conf"），这个字符串当然没有魔力让节点互相认识。
  //真正起作用的，是这个文件里面写的内容
  std::string configFileName; 

  // 2. 生成一个随机的起始端口号
  // 为什么用随机？因为在一台机器上启动多个节点，端口写死（比如8080）会冲突。
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(10000, 29999);
  unsigned short startPort = dis(gen);  // 比如随机到了 15000
  // 3. 解析命令行参数 (如: ./server -n 3 -f nodes.conf)
  // "n:f:" 表示 -n 和 -f 后面都必须接具体的数值或字符串
  while ((c = getopt(argc, argv, "n:f:")) != -1) {
    switch (c) {
      case 'n':
        nodeNum = atoi(optarg);  // 把字符串转成整数，比如 "3" -> 3
        break;
      case 'f':
        configFileName = optarg; // 拿到文件名，比如 "nodes.conf"
        break;
      default:
        ShowArgsHelp();          // 输入了不认识的参数，打印帮助并退出
        exit(EXIT_FAILURE);
    }
  }
  // 4. 初始化（清空）配置文件
  // 先用 app (追加) 模式打开一下，确保文件被创建出来
  std::ofstream file(configFileName, std::ios::out | std::ios::app);
  file.close();
  // 再用 trunc (截断) 模式重新打开，这会直接清空文件里的所有旧数据！
  // 保证每次重新启动集群，通讯录都是一张白纸，防止读到上次运行时的脏数据。
  file = std::ofstream(configFileName, std::ios::out | std::ios::trunc);
  if (file.is_open()) {
    file.close();
    std::cout << configFileName << " 已清空" << std::endl;
  } else {
    std::cout << "无法打开 " << configFileName << std::endl;
    exit(EXIT_FAILURE);
  }

  // 5. 循环创建 Raft 节点进程 (最核心的并发魔法)
  for (int i = 0; i < nodeNum; i++) {
    // 给当前节点分配一个递增的专属端口 (例: 15000, 15001, 15002...)
    short port = startPort + static_cast<short>(i);
    std::cout << "start to create raftkv node:" << i << "    port:" << port << " pid:" << getpid() << std::endl;
    
    // fork() 是系统调用：调用一次，返回两次！
    // 它会把当前进程克隆成两份。父进程拿到的 pid 是子进程的 ID (>0)，子进程拿到的 pid 是 0。
    pid_t pid = fork();  
    
    if (pid == 0) {
      // ==============================
      // 这里是【子进程】的专属执行空间
      // ==============================
      // 子进程在这里摇身一变，启动真正的 KvServer 实例（核心业务上线）
      auto kvServer = new KvServer(i, 500, configFileName, port);
      
      // pause() 会让当前进程挂起（休眠），一直等信号。
      // 也就是说，子进程执行到这里就不会再往下跑 return 0 了，它在后台默默当服务器接客。
      pause(); 
    } else if (pid > 0) {
      // ==============================
      // 这里是【父进程】的专属执行空间
      // ==============================
      // 父进程休眠 1 秒，稍微等一下刚出生的子进程初始化端口和配置
      // 然后继续进入下一轮 for 循环，去“生”下一个子进程！
      sleep(1);
    } else {
      // 如果系统资源不足，fork 失败，pid 会小于 0
      std::cerr << "Failed to create child process." << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  
  // 所有的子进程都生完了，父进程也跑到了这里。
  // 父进程把自己挂起，不退出。
  // 这样你在终端就不会看到程序立刻结束，想关掉集群时，直接 Ctrl+C 就能一次性结束父子进程。
  pause();
  return 0;
}

void ShowArgsHelp() { std::cout << "format: command -n <nodeNum> -f <configFileName>" << std::endl; }
