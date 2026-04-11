//
// Created by swx on 23-6-4.
//
#include <iostream>
#include "clerk.h"
#include "util.h"
int main() {
  Clerk client;
  client.Init("test.conf");
  auto start = now();
  int count = 500;
  int tmp = count;
  while (tmp--) {
    client.Put("x", std::to_string(tmp));

    std::string get1 = client.Get("x");
    std::printf("get return :{%s}\r\n", get1.c_str());
  }
  return 0;
}
/*
这是最关键的一点。你看一下这两者的调用顺序，它们不是并列关系，而是嵌套关系：
外部调用：客户端发一个 kvServerRPC::Put 给 Leader。
内部转化：Leader 收到后，并不是立刻存入跳表，而是把这个请求打包，变成 raftRPC::AppendEntries 发给所有小弟。
最终落地：等到 Raft 层同步成功了，Leader 才会把数据塞进跳表。
*/