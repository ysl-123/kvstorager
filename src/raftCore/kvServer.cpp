#include "kvServer.h"

#include <rpcprovider.h>

#include "mprpcconfig.h"
#include "logger.h"

#define DPrintf LOG_DEBUG
void KvServer::DprintfKVDB()
{
  if (!Debug)
  {
    return;
  }
  std::lock_guard<std::mutex> lg(m_mtx);
  DEFER
  {
    // for (const auto &item: m_kvDB) {
    //     DPrintf("[DBInfo ----]Key : %s, Value : %s", &item.first, &item.second);
    // }
    m_skipList.display_list();
  };
}

void KvServer::ExecuteAppendOpOnKVDB(Op op)
{
  // if op.IfDuplicate {   //get请求是可重复执行的，因此可以不用判复
  //	return
  // }
  m_mtx.lock();

  m_skipList.insert_set_element(op.Key, op.Value);

  // if (m_kvDB.find(op.Key) != m_kvDB.end()) {
  //     m_kvDB[op.Key] = m_kvDB[op.Key] + op.Value;
  // } else {
  //     m_kvDB.insert(std::make_pair(op.Key, op.Value));
  // }
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();

  LOG_INFO("kv node:%d apply append key:%s client:%s request:%d",
           m_me, op.Key.c_str(), op.ClientId.c_str(), op.RequestId);

  //    DPrintf("[KVServerExeAPPEND-----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.ClientId, op.RequestId,
  //    op.Key, op.Value)
  DprintfKVDB();
}

void KvServer::ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist)
{
  m_mtx.lock();
  *value = "";
  *exist = false;
  if (m_skipList.search_element(op.Key, *value))
  {
    *exist = true;
  }
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();
  LOG_DEBUG("kv node:%d apply get key:%s client:%s request:%d exist:%d",
            m_me, op.Key.c_str(), op.ClientId.c_str(), op.RequestId, *exist);
  if (*exist)
  {
    // DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, value :%v", op.ClientId,
    // op.RequestId, op.Key, value)
  }
  else
  {
    // DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, But No KEY!!!!", op.ClientId,
    // op.RequestId, op.Key)
  }
  DprintfKVDB();
}

void KvServer::ExecutePutOpOnKVDB(Op op)
{
  m_mtx.lock();
  m_skipList.insert_set_element(op.Key, op.Value);
  // m_kvDB[op.Key] = op.Value;
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();

  LOG_INFO("kv node:%d apply put key:%s client:%s request:%d",
           m_me, op.Key.c_str(), op.ClientId.c_str(), op.RequestId);

  //    DPrintf("[KVServerExePUT----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.ClientId, op.RequestId,
  //    op.Key, op.Value)
  DprintfKVDB();
}

// clerk发起的get请求服务器收到了处理get请求
// 客户端的get请求只能发给leader
void KvServer::Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply)
{
  Op op;
  op.Operation = "Get";
  op.Key = args->key();
  op.Value = "";
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();

  // raftIndex实际上lastLogIndex + 1新插入的日志所在的index
  int raftIndex = -1;
  int _ = -1;
  bool isLeader = false;
  // 这里的isLeader并不是作为参数，只是放进去用于放参数返回值
  // 为什么m_raftNode一定是leader节点呢，因为clerk是对所有的raft节点遍历，
  // clerk调用get（key）访问到非leader节点的时候被上面判断直接排除，所以到这里的一定是leader
  // 执行完这个之后只是告诉你当前的op放进了日志里，leader节点会通过心跳同步消息，超过半数之后将op 塞进一个叫 applyCh 的通道里
  m_raftNode->Start(op, &raftIndex, &_, &isLeader);

  if (!isLeader)
  {
    reply->set_err(ErrWrongLeader);
    return;
  }

  // create waitForCh
  m_mtx.lock();

  if (waitApplyCh.find(raftIndex) == waitApplyCh.end())
  {
    waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
  }
  auto chForRaftIndex = waitApplyCh[raftIndex];

  m_mtx.unlock(); // 直接解锁，等待任务执行完成，不能一直拿锁等待

  // timeout
  Op raftCommitOp;
  // timeOutPop这个就是LockQueue提供的一个队列里存的东西超时之后自动弹出  raftCommitOp接收队列弹出来的东西
  // 队列为空且超时了走下面的if
  // 实际上在这一句会一直卡住，
  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp))
  {
    int _ = -1;
    bool isLeader = false;
    m_raftNode->GetState(&_, &isLeader);
    // 假如clerk的get请求执行成功了，返回结果的时候失败了，客户端再次发起所以应该是要继续执行并且返回结果的
    // 肯定是上面这种情况因为只有执行了ExecuteGetOpOnKVDB的，才能设置m_lastRequestId[op.ClientId] = op.RequestId
    // 并且为什么我们必须要在kv数据库再次查询一次返回结果，不能直接返回呢，因为不知道get请求执行是否成功，所以必须再试一次
    if (ifRequestDuplicate(op.ClientId, op.RequestId) && isLeader)
    {
      std::string value;
      bool exist = false;
      ExecuteGetOpOnKVDB(op, &value, &exist);
      if (exist)
      {
        reply->set_err(OK);
        reply->set_value(value);
      }
      else
      {
        reply->set_err(ErrNoKey);
        reply->set_value("");
      }
    }
    else
    {
      reply->set_err(ErrWrongLeader); // 返回这个，其实就是让clerk换一个节点重试
    }
  }
  else
  {
    if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId)
    {
      std::string value;
      bool exist = false;
      ExecuteGetOpOnKVDB(op, &value, &exist);
      if (exist)
      {
        reply->set_err(OK);
        reply->set_value(value);
      }
      else
      {
        reply->set_err(ErrNoKey);
        reply->set_value("");
      }
    }
    else
    {
      reply->set_err(ErrWrongLeader);
      // DPrintf("[GET ] 不满足：raftCommitOp.ClientId{%v} == op.ClientId{%v} && raftCommitOp.RequestId{%v}
      //== op.RequestId{%v}", raftCommitOp.ClientId, op.ClientId, raftCommitOp.RequestId, op.RequestId)
    }
  }
  m_mtx.lock(); // todo 這個可以先弄一個defer，因爲刪除優先級並不高，先把rpc發回去更加重要
  auto tmp = waitApplyCh[raftIndex];
  waitApplyCh.erase(raftIndex);
  delete tmp;
  m_mtx.unlock();
}
// 将message中指令是否执行和放到waitApplyCh
void KvServer::GetCommandFromRaft(ApplyMsg message)
{
  Op op;
  op.parseFromString(message.Command);

  DPrintf(
      "[KvServer::GetCommandFromRaft-kvserver{%d}] , Got Command --> Index:{%d} , ClientId {%s}, RequestId {%d}, "
      "Opreation {%s}, Key :{%s}, Value :{%s}",
       m_me, message.CommandIndex, op.ClientId.c_str(), op.RequestId,
    op.Operation.c_str(), op.Key.c_str(), op.Value.c_str());
  if (message.CommandIndex <= m_lastSnapShotRaftLogIndex)
  {
    return;
  }
  // 意思就是同一个client的同一个请求id不行
  // 因为可能客户充了100元，客户端put放100，结果服务端加了100后，返回过程中网断了，结果客户端再次发起，服务端必须要排除已经添加的，不然就要账户多100
  // 要注意tcp只是保证连接在时，重传可靠有序，断了不保证我目前说的就是这种情况
  if (!ifRequestDuplicate(op.ClientId, op.RequestId))
  {
    // 这里只是处理"Put"  "Append"操作，
    // 不清楚为什么get请求就不需要考虑在这个线程里直接处理好，反而是将waitApplyCh[raftIndex]->Push(op)然后让get请求得到弹出队列去自己处理
    if (op.Operation == "Put")
    {
      ExecutePutOpOnKVDB(op);
    }

    if (op.Operation == "Append")
    {
      ExecuteAppendOpOnKVDB(op);
    }
    //  kv.lastRequestId[op.ClientId] = op.RequestId  因为在Executexxx函数里已经更新
  }
  // 到这里kvDB已经制作了快照
  if (m_maxRaftState != -1)
  {
    IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
    // 如果raft的log太大（大于指定的比例）就把制作快照
  }

  // Send message to the chan of op.ClientId
  SendMessageToWaitChan(op, message.CommandIndex);
}
// 判断请求是否重复  只能要请求不重复的  就是要么clientid不存在，要么这一次requestid比以往要新
bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId)
{
  std::lock_guard<std::mutex> lg(m_mtx);
  // 因为每一次执行具体的指令的时候会进行一次m_lastRequestId[op.ClientId] = op.RequestId;
  if (m_lastRequestId.find(ClientId) == m_lastRequestId.end())
  {
    return false;
    // todo :不存在这个client就创建
  }
  return RequestId <= m_lastRequestId[ClientId];
}

// 1. Put / Append（写操作）：
//
//
// 2. Get（读操作）：
/*放在外面执行我的理解是充分利用muduo库的并发能力，因为客户端get调用之后发给服务端，
服务端一直在监听，处理的时候通过subloop ，这样能提升并发能力，通常读多写少，放在线程里std::thread t2(&KvServer::ReadRaftApplyCommandLoop, this);
导致写操作一直难以进行
*/
void KvServer::PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply)
{
  Op op;
  op.Operation = args->op();
  op.Key = args->key();
  op.Value = args->value();
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();
  int newLogIndex = -1;
  int _ = -1;
  bool isleader = false;

  m_raftNode->Start(op, &newLogIndex, &_, &isleader);

  if (!isleader)
  {
    LOG_WARN("kv node:%d reject put/append because not leader key:%s client:%s request:%d",
             m_me, op.Key.c_str(), op.ClientId.c_str(), op.RequestId);
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , but "
        "not leader",
         m_me, args->clientid().c_str(), args->requestid(), m_me, op.Key.c_str(), newLogIndex);
    reply->set_err(ErrWrongLeader);
    return;
  }
  DPrintf(
      "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , is "
      "leader ",
       m_me, args->clientid().c_str(), args->requestid(), m_me, op.Key.c_str(), newLogIndex);
  m_mtx.lock();
  if (waitApplyCh.find(newLogIndex) == waitApplyCh.end())
  {
    waitApplyCh.insert(std::make_pair(newLogIndex, new LockQueue<Op>()));
  }
  auto chForRaftIndex = waitApplyCh[newLogIndex];

  m_mtx.unlock(); // 直接解锁，等待任务执行完成，不能一直拿锁等待

  // timeout
  Op raftCommitOp;

  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp))
  {
    LOG_WARN("kv node:%d put/append timeout key:%s op:%s client:%s request:%d raftIndex:%d",
             m_me, op.Key.c_str(), op.Operation.c_str(), op.ClientId.c_str(), op.RequestId, newLogIndex);
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]TIMEOUT PUTAPPEND !!!! Server %d , get Command <-- Index:%d , "
        "ClientId %s, RequestId %d, Opreation %s Key :%s, Value :%s",
          m_me, m_me, newLogIndex, op.ClientId.c_str(), op.RequestId,
    op.Operation.c_str(), op.Key.c_str(), op.Value.c_str());
    // 确实会发重复的，那个client不是循环吗，玩意又循环回去呢，可能第一次到了正确的raft发过去了，回复的时候太慢了又发了一个过去
    // 千万不要觉得超时重传什么ack，我们第一次发的指令就是按照序号和ack，tcp栈确定成功了，我们是在应用层返回的时候失败
    if (ifRequestDuplicate(op.ClientId, op.RequestId))
    {
      reply->set_err(OK);
    }
    else
    {
      /*
      真正超时情况一下三种
      1.网络分区 网络分裂成了两部分：[A, B] 和 [C, D, E] A 依然认为自己是 Leader。此时，客户端发来一个 Put 请求 A 满口答应，把日志写进本地，然后发给 B，并发给 C, D, E。
      但是 C, D, E 在另一个网络分区，A 永远收不到它们的回复 无法 Commit 最终超时
      2.Leader 突然宕机 Leader 收到请求，分配了 Index = 100，开始 timeOutPop 阻塞 在把日志发给多数派之前，Leader 突然进程卡死，这个日志一直没有commit
      集群里的其他节点等不到心跳，立刻选出了新的 Leader。新 Leader 接收了别的客户端请求，也占用了 Index = 100 这个位置，并成功 Commit，旧leader醒来之后，
      立刻发现超时，原先的rpc的put还是要返回，
      3.就是客户端并发打过来了 10000 个请求  Raft 很快就把这 10000 个请求都复制并 Commit 了，全部塞进了一个长长的队列里，等待 Apply 线程去执行
      执行到第 9999 条日志时，可能已经过去了 2 秒钟 只等 500ms 啊 timeOutPop（这种情况不知道怎么解决）
      */
      // 所以我感觉waitapplychan的主要作用是让当时client给leader发的请求最终能得到一个结果
      reply->set_err(ErrWrongLeader); /// 这里返回这个的目的让clerk重新尝试
    }
  }
  // 下面就是执行成功了，执行成功才会弹出来呢
  else
  {
    LOG_INFO("kv node:%d put/append committed key:%s op:%s client:%s request:%d raftIndex:%d",
             m_me, op.Key.c_str(), op.Operation.c_str(), op.ClientId.c_str(), op.RequestId, newLogIndex);
    DPrintf(
    "[func -KvServer::PutAppend -kvserver{%d}] WaitChanGetRaftApplyMessage<-- Server %d, get Command <-- Index:%d, "
    "ClientId %s, RequestId %d, Operation %s, Key:%s, Value:%s",
    m_me, m_me, newLogIndex,
    op.ClientId.c_str(),
    op.RequestId,
    op.Operation.c_str(),
    op.Key.c_str(),
    op.Value.c_str());

    // 刚执行完，肯定要和刚刚执行的指令一致呀
    if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId)
    {
      reply->set_err(OK);
    }
    else
    {
      reply->set_err(ErrWrongLeader);
    }
  }

  m_mtx.lock();

  auto tmp = waitApplyCh[newLogIndex];
  waitApplyCh.erase(newLogIndex);
  delete tmp;
  m_mtx.unlock();
}

void KvServer::ReadRaftApplyCommandLoop()
{
  while (true)
  {
    // 如果只操作applyChan不用拿锁，因为applyChan自己带锁
    auto message = applyChan->Pop(); // 阻塞弹出
    DPrintf(
        "---------------tmp-------------[func-KvServer::ReadRaftApplyCommandLoop()-kvserver{%d}] 收到了下raft的消息",
        m_me);
    // listen to every command applied by its raft ,delivery to relative RPC Handler
    // m_logs的里面的存取操作，一次执行一次m_logs[i]一条
    if (message.CommandValid)
    {
      GetCommandFromRaft(message);
    }
    // 如果是快照，另一种操作的方法   这个主要是follwer节点收到的，leader不可能日志里有快照
    if (message.SnapshotValid)
    {
      GetSnapShotFromRaft(message);
    }
  }
}

// raft会与persist层交互，kvserver层也会，因为kvserver层开始的时候需要恢复kvdb的状态
//  关于快照raft层与persist的交互：保存kvserver传来的snapshot；生成leaderInstallSnapshot RPC的时候也需要读取snapshot；
//  因此snapshot的具体格式是由kvserver层来定的，raft只负责传递这个东西
//  snapShot里面包含kvserver需要维护的persist_lastRequestId 以及kvDB真正保存的数据persist_kvdb
void KvServer::ReadSnapShotToInstall(std::string snapshot)
{
  if (snapshot.empty())
  {
    // bootstrap without any state?
    return;
  }
  parseFromString(snapshot);

  //    r := bytes.NewBuffer(snapshot)
  //    d := labgob.NewDecoder(r)
  //
  //    var persist_kvdb map[string]string  //理应快照
  //    var persist_lastRequestId map[int64]int //快照这个为了维护线性一致性
  //
  //    if d.Decode(&persist_kvdb) != nil || d.Decode(&persist_lastRequestId) != nil {
  //                DPrintf("KVSERVER %d read persister got a problem!!!!!!!!!!",kv.me)
  //        } else {
  //        kv.kvDB = persist_kvdb
  //        kv.lastRequestId = persist_lastRequestId
  //    }
}

bool KvServer::SendMessageToWaitChan(const Op &op, int raftIndex)
{
  std::lock_guard<std::mutex> lg(m_mtx);
  
  if (waitApplyCh.find(raftIndex) == waitApplyCh.end())
  {
    // 因为走到这里意味着前面必定put或get将指令放到日志里（这时候已经waitapply插入了index，队列），raft取出放队列里，你才能从队列取出来执行，你此时找不到不合理
    return false;
  }
  waitApplyCh[raftIndex]->Push(op);
  DPrintf(
    "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] Send Command --> Index:{%d}, ClientId {%s}, RequestId {%d}, "
    "Operation {%s}, Key:{%s}, Value:{%s}",
    m_me, raftIndex,
    op.ClientId.c_str(),
    op.RequestId,
    op.Operation.c_str(),
    op.Key.c_str(),
    op.Value.c_str());

  return true;
}

void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion)
{
  if (m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0)
  {
    auto snapshot = MakeSnapShot(); // 这里是制作快照，
    // raftIndex即是message.CommandIndex
    // 这里就是将快照诞生前的所有指令给他删除了，包括message.CommandIndex因为他是先执行put apeend指令，后面才开始进行删除

    m_raftNode->Snapshot(raftIndex, snapshot);
  }
}
/*这个函数的执行实际上是当 Leader 发现某个 Follower（也就是当前这个节点）落后太多，旧日志已经被清理了，
Leader 就会通过网络发送一个 InstallSnapshot RPC 给这个 Follower 的底层 Raft 节点。
当前节点的底层 Raft 收到这个巨大的网络包（快照）KV Server 后台一直有一个 ApplyLoop 线程在监听 applyCh。当它从通道里摸出这个
带快照的 ApplyMsg 时，就会调用你写的这个函数：GetSnapShotFromRaft(ApplyMsg message)。 */
void KvServer::GetSnapShotFromRaft(ApplyMsg message)
{
  std::lock_guard<std::mutex> lg(m_mtx);

  if (m_raftNode->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot))
  {
    ReadSnapShotToInstall(message.Snapshot);
    m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
  }
}

std::string KvServer::MakeSnapShot()
{
  std::lock_guard<std::mutex> lg(m_mtx);
  std::string snapshotData = getSnapshotData();
  return snapshotData;
}

void KvServer::PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                         ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done)
{
  KvServer::PutAppend(request, response);
  done->Run();
}

void KvServer::Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
                   ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done)
{
  KvServer::Get(request, response);
  done->Run();
}
//  int me 我的 ID |  int maxraftstate 最大的 Raft 状态（底层日志）大小限制
//| nodeInforFileName  节点信息文件名（就是configFileName即 nodes.conf）
KvServer::KvServer(int me, int maxraftstate, std::string nodeInforFileName, std::string ip, short port)
    : m_skipList(6)
{ // 1. 初始化底层的 KV 存储结构（这里用的是跳表 SkipList）

  // 创建持久化对象，用于把 Raft 的日志和状态写到磁盘上，防止宕机丢失
  std::shared_ptr<Persister> persister = std::make_shared<Persister>(me);
  m_ip = ip;
  m_me = me;                     // 当前节点的 ID（比如 0, 1, 2）
  m_maxRaftState = maxraftstate; // Raft 日志大小阈值，超过这个大小就要打快照(Snapshot)压缩了

  // 极其关键的通信管道！
  // Raft 底层模块达成共识后，会把要执行的命令（比如 Put x=1）塞进这个队列。
  // KvServer 上层会不停地从这个队列里取命令，并真正写进跳表里。
  applyChan = std::make_shared<LockQueue<ApplyMsg>>();

  // 创建底层的 Raft 核心共识算法模块
  // 创建本地的raft
  m_raftNode = std::make_shared<Raft>();

  // =======================================================================
  // 第一步：【开门迎客】 启动本地的 RPC 服务器（作为 Server，接受别人的请求）
  // =======================================================================

  // 开一个后台线程去启动 RPC 服务，因为 provider.Run() 是个死循环，会阻塞当前主线程。
  std::thread t([this, port]() -> void
                {
    RpcProvider provider;
    provider.NotifyService(this);
    provider.NotifyService(this->m_raftNode.get()); 
    provider.Run(m_ip, port); });
    t.detach(); // 把这个线程剥离出去，让它自己在后台默默运行
  LOG_INFO("raftServer node:%d start to sleep to wait all other raftnode start!!!!", m_me);
  sleep(20);
  LOG_INFO("raftServer node:%d wake up!!!! start to connect other raftnode", m_me);

  // =======================================================================
  // 第三步：【主动出击】 作为客户端，去连接其他所有兄弟节点的 RPC 接口
  // =======================================================================

  MprpcConfig config;
  config.LoadConfigFile(nodeInforFileName.c_str()); // 读取那个记录了所有人 IP 和端口的“通讯录”
  std::vector<std::pair<std::string, short>> ipPortVt;

  // 解析通讯录文件，把所有节点的 IP 和 Port 存到 ipPortVt 数组里
  for (int i = 0; i < INT_MAX - 1; ++i)
  {
    std::string node = "node" + std::to_string(i);
    std::string nodeIp = config.Load(node + "ip");
    std::string nodePortStr = config.Load(node + "port");
    if (nodeIp.empty())
    {
      break; // 如果读不到东西了，说明节点解析完毕
    }
    ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str()));
  }

  std::vector<std::shared_ptr<RaftRpcUtil>> servers; // 用来存所有兄弟节点的 RPC 客户端代理(Stub)

  // 遍历所有节点的信息，建立连接
  for (int i = 0; i < ipPortVt.size(); ++i)
  {
    if (i == m_me)
    {
      // 如果是自己，就塞个空指针占位，自己不需要通过网络调用自己的 RPC
      servers.push_back(nullptr);
      continue;
    }
    std::string otherNodeIp = ipPortVt[i].first;
    short otherNodePort = ipPortVt[i].second;

    // 生成一个连接对方的 RPC 代理对象 (Stub)
    // 可能就是有一个文件里面写了所有节点的ip+port，所以就可以直接用
    auto *rpc = new RaftRpcUtil(otherNodeIp, otherNodePort);
    servers.push_back(std::shared_ptr<RaftRpcUtil>(rpc));

    LOG_INFO("kv node:%d create rpc stub for node:%d (%s:%d)", m_me, i, otherNodeIp.c_str(), otherNodePort);
  }

  sleep(ipPortVt.size() - me);
  m_raftNode->init(servers, m_me, persister, applyChan);

  m_lastSnapShotRaftLogIndex = 0;

  // 前面persister都清空了你读个蛋
  auto snapshot = persister->ReadSnapshot();
  if (!snapshot.empty())
  {
    ReadSnapShotToInstall(snapshot);
  }
  std::thread t2(&KvServer::ReadRaftApplyCommandLoop, this);
  t2.join();
}
