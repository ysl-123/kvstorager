#ifndef APPLYMSG_H
#define APPLYMSG_H
#include <string>
class ApplyMsg {
 public:
  bool CommandValid;    //开关。如果是 true，说明这单送的是普通操作命令
  std::string Command;  //里面存着真正的业务指令（比如打包好的 "Put x 1" 字符串）
  int CommandIndex;     //日志索引。这条命令在 Raft 日志序列里排第几号
  //潜台词告诉上层 KV 数据库：“注意啦！这次快递箱里装的不是普通的单条执行命令（CommandValid=false），
  //而是一份完整的快照！你收到后不要去逐条执行，而是直接覆盖你现有的全部数据！”
  bool SnapshotValid;   
  std::string Snapshot;  //具体的快照压缩包数据
  int SnapshotTerm;       //打这个快照时的 Raft 任期号
  int SnapshotIndex;     //打这个快照时截止的日志索引号

 public:
  //两个valid最开始要赋予false！！
  ApplyMsg()
      : CommandValid(false),
        Command(),
        CommandIndex(-1),
        SnapshotValid(false),
        SnapshotTerm(-1),
        SnapshotIndex(-1){

        };
};

#endif  // APPLYMSG_H