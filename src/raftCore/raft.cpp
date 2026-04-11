#include "raft.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <memory>
#include "config.h"
#include "util.h"
void Raft::AppendEntries(google::protobuf::RpcController* controller,
                         const ::raftRpcProctoc::AppendEntriesArgs* request,
                         ::raftRpcProctoc::AppendEntriesReply* response, ::google::protobuf::Closure* done) {
  AppendEntries1(request, response);
  done->Run();
}
//  args (leader给follwer)   reply (follwer给 leader)
// message AppendEntriesArgs生成的class AppendEntriesArgs    message AppendEntriesReply生成的class AppendEntriesReply
void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs* args, raftRpcProctoc::AppendEntriesReply* reply) {
  std::lock_guard<std::mutex> locker(m_mtx);
  reply->set_appstate(AppNormal);  // 能接收到代表网络是正常的
  //	不同的人收到AppendEntries的反应是不同的，要注意无论什么时候收到rpc请求和响应都要检查term
  //  比如leader宕机了，选了一个新leader，旧leader又恢复了，发消息去了
  // 1.
  if (args->term() < m_currentTerm) {
    reply->set_success(false);
    // 这里设置reply里term为自己，是为了让leader知道自己是比较小的，就会主动放弃自己leader身份，成为follwer
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(-100);  // 论文中：让领导人可以及时更新自己
    DPrintf("[func-AppendEntries-rf{%d}] 拒绝了 因为Leader{%d}的term{%v}< rf{%d}.term{%d}\n", m_me, args->leaderid(),
            args->term(), m_me, m_currentTerm);
    return;  // 注意从过期的领导人收到消息不要重设定时器
  }
  /*
  “无论这个函数接下来怎么运行，无论里面有多少个 return，
  也无论是否发生异常，在离开这个函数AppendEntries作用域的最后一刻，必须给我执行 persist()！
  */
  DEFER { persist(); };
  // 2.
  if (args->term() > m_currentTerm) {
    // 三变 ,防止遗漏，无论什么时候都是三变  State   CurrentTerm  VotedFor

    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;  //
  }
  // 前面的大于  小于都判断了，走到这里肯定是等于呀
  // 3
  // 本来你就是leader我也承认你，跟你term匹配，你就直接成功了
  myAssert(args->term() == m_currentTerm, format("assert {args.Term == rf.currentTerm} fail"));
  // 为什么要重复写这句呢，
  // 因为两个人同时超时变为candidate，竞选的时候一个网速快，
  // 大家投他之后，另一个还在拉票的过程收到了新的leader的心跳就会直接退出，因为在这一轮中他失败了
  m_status = Follower;
  // term相等
  m_lastResetElectionTime = now();
  //  DPrintf("[	AppendEntries-func-rf(%v)		] 重置了选举超时定时器\n", rf.me);

  // 不能无脑的从prevlogIndex开始阶段日志，因为rpc可能会延迟，导致发过来的log是很久之前的

  //	那么就比较日志，日志有3种情况
  //  就是确认3的时候，得到的日志的最后一位或者已经存储的快照的最后一位比3小
  if (args->prevlogindex() > getLastLogIndex()) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(getLastLogIndex() + 1);
    return;
  }
  // 如果prevlogIndex还没有更上快照
  else if (args->prevlogindex() < m_lastSnapshotIncludeIndex) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    // 从这开始确认的原因就是prevlogindex()=3  快照50，等leader+1对齐，很久，日志部分少，慢慢对其到没事
    reply->set_updatenextindex(m_lastSnapshotIncludeIndex + 1);
  }
  // 这个就是args的断言确认的，比快照最后一位>=，比日志记录的最后一位<=
  // 返回值是本地args->prevlogindex()日志的term是否和args->prevlogterm()相等呢
  if (matchLog(args->prevlogindex(), args->prevlogterm())) {
    for (int i = 0; i < args->entries_size(); i++) {
      auto log = args->entries(i);
      // 我理解的情况是只能log.logindex() = getLastLogIndex()+1，不懂，后期可以试着改为+1看会不会从出错呢
      if (log.logindex() > getLastLogIndex()) {
        // 超过就直接添加日志
        m_logs.push_back(log);
      } else {
        // 这里就是你当前发来的日志我还是有，目前还是想办法确认两边一样
        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() == log.logterm() &&
            m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() != log.command()) {
          // 相同位置的log ，其logTerm相等，但是命令却不相同，不符合raft的前向匹配，异常了！
          myAssert(false, format("[func-AppendEntries-rf{%d}] 两节点logIndex{%d}和term{%d}相同，但是其command{%d:%d}   "
                                 " {%d:%d}却不同！！\n",
                                 m_me, log.logindex(), log.logterm(), m_me,
                                 m_logs[getSlicesIndexFromLogIndex(log.logindex())].command(), args->leaderid(),
                                 log.command()));
        }
        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() != log.logterm()) {
          // 不匹配就更新
          m_logs[getSlicesIndexFromLogIndex(log.logindex())] = log;
        }
      }
    }

    // 在处理完 Leader 发来的日志后，我本地的最后一条日志的索引，必须【大于或等于】这批日志的最后一条的索引。
    myAssert(
        // 我理解的情况是getLastLogIndex() == args->prevlogindex() + args->entries_size()不懂
        //因为可能会网络不好，重写发送，比如先发5 6 7 8 网络卡住 过了一会发送了 5 6 7 8 9 先到了，然后后面的 5 6 7 8才到
        getLastLogIndex() >= args->prevlogindex() + args->entries_size(),
        format("[func-AppendEntries1-rf{%d}]rf.getLastLogIndex(){%d} != args.PrevLogIndex{%d}+len(args.Entries){%d}",
               m_me, getLastLogIndex(), args->prevlogindex(), args->entries_size()));
    if (args->leadercommit() > m_commitIndex) {
      m_commitIndex = std::min(args->leadercommit(), getLastLogIndex());
      // 这个地方不能无脑跟上getLastLogIndex()，因为可能存在args->leadercommit()落后于 getLastLogIndex()的情况
    }

    // 领导会一次发送完所有的日志
    myAssert(getLastLogIndex() >= m_commitIndex,
             format("[func-AppendEntries1-rf{%d}]  rf.getLastLogIndex{%d} < rf.commitIndex{%d}", m_me,
                    getLastLogIndex(), m_commitIndex));
    reply->set_success(true);
    reply->set_term(m_currentTerm);
    return;
  }
  //实际上这个else还是符合 args->prevlogindex()比快照最后一位>=，比日志记录的最后一位<=
  //只是两者的任期不匹配
   else {
    reply->set_updatenextindex(args->prevlogindex());
       
    for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index) {
      //直到找到两者任期相同，返回给leader告诉直接从这里进行匹配就行
      if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex())) {
        reply->set_updatenextindex(index + 1);
        break;
      }
    }
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    return;
  }

  // fmt.Printf("[func-AppendEntries,rf{%v}]:len(rf.logs):%v, rf.commitIndex:%v\n", rf.me, len(rf.logs), rf.commitIndex)
}

void Raft::InstallSnapshot(google::protobuf::RpcController* controller,
                           const ::raftRpcProctoc::InstallSnapshotRequest* request,
                           ::raftRpcProctoc::InstallSnapshotResponse* response, ::google::protobuf::Closure* done) {
  InstallSnapshot(request, response);

  done->Run();
}

void Raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest* args,
                           raftRpcProctoc::InstallSnapshotResponse* reply) {
  m_mtx.lock();
  /*
  “无论这个函数接下来怎么运行，无论里面有多少个 return，
  也无论是否发生异常，在离开这个函数AppendEntries作用域的最后一刻，必须给我执行 persist()！
  */
  DEFER { m_mtx.unlock(); };
  if (args->term() < m_currentTerm) {  // 可能是上一届leader刚刚睡醒
    reply->set_term(m_currentTerm);
    return;
  }
  if (args->term() > m_currentTerm) {
    // 后面两种情况都要接收日志
    m_currentTerm = args->term();
    // 假如本来是4届  m_votedFor = -1 的意思是：“我把上一届的旧票作废了，我现在的身份是‘第 5
    // 届，我还没在这一届投过票呢！ 因为一届选民在一届只能投一次票
    m_votedFor = -1;  // 把这一届的投票记录清空（因为是新的届数了）
    m_status = Follower;
    persist();
  }
  m_status = Follower;
  m_lastResetElectionTime = now();
  // outdated snapshot
  if (args->lastsnapshotincludeindex() <= m_lastSnapshotIncludeIndex) {
    return;
  }

  auto lastLogIndex = getLastLogIndex();
  // 如果我本地的最新日志，比快照包含的进度还要新（长）
  // 比如：快照只包含了前 100 条（最后是 100），但我本地账本已经记到了 120 条。
  if (lastLogIndex > args->lastsnapshotincludeindex()) {
    // 动作：把前 100 条日志从数组里“撕掉”（erase），只保留 101~120 条！
    // getSlicesIndexFromLogIndex() 是算出第 100 条日志在数组里的真实下标。
    m_logs.erase(m_logs.begin(), m_logs.begin() + getSlicesIndexFromLogIndex(args->lastsnapshotincludeindex()) + 1);
  } else {
    // 如果我本地的最新日志，比快照包含的进度还要短
    m_logs.clear();
  }
  m_commitIndex = std::max(m_commitIndex, args->lastsnapshotincludeindex());
  m_lastApplied = std::max(m_lastApplied, args->lastsnapshotincludeindex());
  m_lastSnapshotIncludeIndex = args->lastsnapshotincludeindex();
  m_lastSnapshotIncludeTerm = args->lastsnapshotincludeterm();

  reply->set_term(m_currentTerm);
  ApplyMsg msg;
  msg.SnapshotValid = true;
  msg.Snapshot = args->data();
  msg.SnapshotTerm = args->lastsnapshotincludeterm();
  msg.SnapshotIndex = args->lastsnapshotincludeindex();
  // 可能是这个队列，线程访问多，害怕锁住，卡的时间长
  std::thread t(&Raft::pushMsgToKvServer, this, msg);  // 创建新线程并执行b函数，并传递参数
  t.detach();
  m_persister->Save(persistData(), args->data());
}

void Raft::RequestVote(google::protobuf::RpcController* controller, const ::raftRpcProctoc::RequestVoteArgs* request,
                       ::raftRpcProctoc::RequestVoteReply* response, ::google::protobuf::Closure* done) {
  RequestVote(request, response);
  done->Run();
}

// 选民根据candidate发来的请求投票里的信息，返回给candidate告诉他我是否投给你了票
void Raft::RequestVote(const raftRpcProctoc::RequestVoteArgs* args, raftRpcProctoc::RequestVoteReply* reply) {
  std::lock_guard<std::mutex> lg(m_mtx);
  DEFER {
    // 应该先持久化，再撤销lock
    persist();
  };
  // 对args的term的三种情况分别进行处理，大于小于等于自己的term都是不同的处理
  if (args->term() < m_currentTerm) {
    reply->set_term(m_currentTerm);
    reply->set_votestate(Expire);
    reply->set_votegranted(false);
    return;
  }
  // fig2:右下角，如果任何时候rpc请求或者响应的term大于自己的term，更新term，并变成follower
  if (args->term() > m_currentTerm) {
    m_status = Follower;
    m_currentTerm = args->term();
    // 先变为-1否则他一直记录着上一次投给了谁，但是为什么不投给当前的呢，因为他要后面综合考虑log的一些东西
    m_votedFor = -1;
  }
  myAssert(args->term() == m_currentTerm,
           format("[func--rf{%d}] 前面校验过args.Term==rf.currentTerm，这里却不等", m_me));
  //	现在节点任期都是相同的(任期小的也已经更新到新的args的term了)，还需要检查log的term和index是不是匹配的了

  int lastLogTerm = getLastLogTerm();
  // 只有没投票，且candidate的日志的新的程度 ≥ 接受者的日志新的程度 才会授票
  if (!UpToDate(args->lastlogindex(), args->lastlogterm())) {
    // args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
    // 日志太旧了
    if (args->lastlogterm() < lastLogTerm) {
    } else {
    }
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);
    return;
  }
  if (m_votedFor != -1 && m_votedFor != args->candidateid()) {
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);
    return;
  } else {
    m_votedFor = args->candidateid();
    m_lastResetElectionTime = now();  // 认为必须要在投出票的时候才重置定时器，
    reply->set_term(m_currentTerm);
    reply->set_votestate(Normal);
    reply->set_votegranted(true);

    return;
  }
}
// 每隔一段时间，去看（Raft）有没有新敲定的决议（Committed Logs），如果有，就打包拿出来，扔到（applyChan）

void Raft::applierTicker() {
  while (true) {
    m_mtx.lock();
    if (m_status == Leader) {
      DPrintf("[Raft::applierTicker() - raft{%d}]  m_lastApplied{%d}   m_commitIndex{%d}", m_me, m_lastApplied,
              m_commitIndex);
    }
    auto applyMsgs = getApplyLogs();
    m_mtx.unlock();
    // 使用匿名函数是因为传递管道的时候不用拿锁
    //  todo:好像必须拿锁，因为不拿锁的话如果调用多次applyLog函数，可能会导致应用的顺序不一样
    if (!applyMsgs.empty()) {
      DPrintf("[func- Raft::applierTicker()-raft{%d}] 向kvserver報告的applyMsgs長度爲：{%d}", m_me, applyMsgs.size());
    }
    for (auto& message : applyMsgs) {
      applyChan->Push(message);
    }
    // usleep(1000 * ApplyInterval);
    sleepNMilliseconds(ApplyInterval);
  }
}

bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot) {
  return true;
  //// Your code here (2D).
  // rf.mu.Lock()
  // defer rf.mu.Unlock()
  // DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex {%v} to check
  // whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)
  //// outdated snapshot
  // if lastIncludedIndex <= rf.commitIndex {
  //	return false
  // }
  //
  // lastLogIndex, _ := rf.getLastLogIndexAndTerm()
  // if lastIncludedIndex > lastLogIndex {
  //	rf.logs = make([]LogEntry, 0)
  // } else {
  //	rf.logs = rf.logs[rf.getSlicesIndexFromLogIndex(lastIncludedIndex)+1:]
  // }
  //// update dummy entry with lastIncludedTerm and lastIncludedIndex
  // rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
  //
  // rf.persister.Save(rf.persistData(), snapshot)
  // return true
}

void Raft::doElection() {
  std::lock_guard<std::mutex> g(m_mtx);
  // 这个if应该只是防御性编程，因为electionTimeOutTicker的时候，如果是leader就会一直睡下去呀
  if (m_status == Leader) {
    // fmt.Printf("[       ticker-func-rf(%v)              ] is a Leader,wait the  lock\n", rf.me)
  }
  // fmt.Printf("[       ticker-func-rf(%v)              ] get the  lock\n", rf.me)

  if (m_status != Leader) {
    DPrintf("[ticker-func-rf(%d)              ]  选举定时器到期且不是leader，开始选举 \n", m_me);
    // 当选举的时候定时器超时就必须重新选举，不然没有选票就会一直卡主
    // 重竞选超时，term也会增加的
    m_status = Candidate;
    /// 开始新一轮的选举
    m_currentTerm += 1;
    m_votedFor = m_me;  // 即是自己给自己投，也避免candidate给同辈的candidate投
    persist();
    std::shared_ptr<int> votedNum = std::make_shared<int>(1);  // 使用 make_shared 函数初始化 !! 亮点
    //	重新设置定时器
    /*
    必须要更新，因为doelection函数是另一线程执行，当前线程依然执行electtimeouttricker函数，如果不更新，
    睡眠时间就要小于0了，而且这样跟新也是为了多个志愿者没投出最终leader的情况下，
    当前超时后可以继续开启一轮新的doelection*/
    m_lastResetElectionTime = now();
    //	发布RequestVote RPC
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      int lastLogIndex = -1, lastLogTerm = -1;
      getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);  // 获取最后一个log的term和下标

      std::shared_ptr<raftRpcProctoc::RequestVoteArgs> requestVoteArgs =
          std::make_shared<raftRpcProctoc::RequestVoteArgs>();
      requestVoteArgs->set_term(m_currentTerm);
      requestVoteArgs->set_candidateid(m_me);
      requestVoteArgs->set_lastlogindex(lastLogIndex);
      requestVoteArgs->set_lastlogterm(lastLogTerm);
      auto requestVoteReply = std::make_shared<raftRpcProctoc::RequestVoteReply>();

      // 注意到上面有一个for循环实际上是开辟了raft节点减去1的个数的线程向这些家伙发送投票请求
      std::thread t(&Raft::sendRequestVote, this, i, requestVoteArgs, requestVoteReply,
                    votedNum);  // 创建新线程并执行b函数，并传递参数
      t.detach();
    }
  }
}

void Raft::doHeartBeat() {
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status == Leader) {
    DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了且拿到mutex，开始发送AE\n", m_me);
    auto appendNums = std::make_shared<int>(1);  // 当前已经成功追加这条日志的节点总数

    // 对Follower（除了自己外的所有节点发送AE）
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了 index:{%d}\n", m_me, i);
      myAssert(m_nextIndex[i] >= 1, format("rf.nextIndex[%d] = {%d}", i, m_nextIndex[i]));
      // 日志压缩加入后要判断是发送快照还是发送AE
      if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) {
        std::thread t(&Raft::leaderSendSnapShot, this, i);  // 创建新线程并执行b函数，并传递参数
        t.detach();
        continue;
      }
      // 构造发送值
      int preLogIndex = -1;
      int PrevLogTerm = -1;
      // 给确认的日志赋值呗
      getPrevLogInfo(i, &preLogIndex, &PrevLogTerm);
      std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs =
          std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
      appendEntriesArgs->set_term(m_currentTerm);
      appendEntriesArgs->set_leaderid(m_me);
      appendEntriesArgs->set_prevlogindex(preLogIndex);
      appendEntriesArgs->set_prevlogterm(PrevLogTerm);
      appendEntriesArgs->clear_entries();
      appendEntriesArgs->set_leadercommit(m_commitIndex);
      // 我感觉这个if else完全是多次一举，反正preLogIndex >= m_lastSnapshotIncludeIndex，日志直接放进去就完事了
      if (preLogIndex != m_lastSnapshotIncludeIndex) {
        // 如果是心跳的话就可以直接日志内容就不放了 sendEntryPtr这个里就不放了，我们直接将身份证明和 对账单发过去就行
        // 不等于的话只需要去发送我们preLogIndex这个之后的就可以了
        for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < m_logs.size(); ++j) {
          raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
          *sendEntryPtr = m_logs[j];  //=是可以点进去的，可以点进去看下protobuf如何重写这个的
        }
      } else {
        // 等于的话日志后面的就要全部发过去
        for (const auto& item : m_logs) {
          raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
          *sendEntryPtr = item;  //=是可以点进去的，可以点进去看下protobuf如何重写这个的
        }
      }
      int lastLogIndex = getLastLogIndex();
      // leader对每个节点发送的日志长短不一，但是都保证从prevIndex发送直到最后
      myAssert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex,
               format("appendEntriesArgs.PrevLogIndex{%d}+len(appendEntriesArgs.Entries){%d} != lastLogIndex{%d}",
                      appendEntriesArgs->prevlogindex(), appendEntriesArgs->entries_size(), lastLogIndex));
      // 构造返回值
      const std::shared_ptr<raftRpcProctoc::AppendEntriesReply> appendEntriesReply =
          std::make_shared<raftRpcProctoc::AppendEntriesReply>();
      appendEntriesReply->set_appstate(Disconnected);
      //  appendNums依然是在当前服务器对appendNums进行操作
      std::thread t(&Raft::sendAppendEntries, this, i, appendEntriesArgs, appendEntriesReply, appendNums);
      t.detach();
    }
    m_lastResetHearBeatTime = now();  // leader发送心跳，就不是随机时间了
  }
}

void Raft::electionTimeOutTicker() {
  // Check if a Leader election should be started.
  while (true) {
    /**
     * 如果不睡眠，那么对于leader，这个函数会一直空转，浪费cpu。且加入协程之后，空转会导致其他协程无法运行，对于时间敏感的AE，会导致心跳无法正常发送导致异常
     */
    while (m_status == Leader) {
      usleep(
          HeartBeatTimeout);  // 定时时间没有严谨设置，因为HeartBeatTimeout比选举超时一般小一个数量级，因此就设置为HeartBeatTimeout了
    }
    // std::ratio<1, 1000000000>：这是时间的单位！1 除以 10亿，也就是 十亿分之一秒（1纳秒）。
    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
    std::chrono::system_clock::time_point wakeTime{};
    {
      m_mtx.lock();
      wakeTime = now();
      suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
      m_mtx.unlock();
    }
    // 毫秒了  .count()就相当于取出里面的数了
    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
      // steady_clock只是当前服务器开始   服务器按电源开机的那一刻，它从 0 开始滴答滴答走
      auto start = std::chrono::steady_clock::now();

      usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
      // std::this_thread::sleep_for(suitableSleepTime);

      // 获取函数运行结束后的时间点
      auto end = std::chrono::steady_clock::now();

      // 计算时间差并输出结果（单位为毫秒）
      std::chrono::duration<double, std::milli> duration = end - start;

      // 使用ANSI控制序列将输出颜色修改为紫色
      std::cout << "\033[1;35m electionTimeOutTicker();函数设置睡眠时间为: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                << std::endl;
      std::cout << "\033[1;35m electionTimeOutTicker();函数实际睡眠时间为: " << duration.count() << " 毫秒\033[0m"
                << std::endl;
    }
    // 每次收到心跳（也会包含信息）的时候，就会重置一次自己的m_lastResetElectionTime
    if (std::chrono::duration<double, std::milli>(m_lastResetElectionTime - wakeTime).count() > 0) {
      // 说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠
      continue;
    }
    // 走到这一步就说明超时了，我要变为候选者了
    doElection();
  }
}

std::vector<ApplyMsg> Raft::getApplyLogs() {
  std::vector<ApplyMsg> applyMsgs;
  myAssert(m_commitIndex <= getLastLogIndex(), format("[func-getApplyLogs-rf{%d}] commitIndex{%d} >getLastLogIndex{%d}",
                                                      m_me, m_commitIndex, getLastLogIndex()));
  
  while (m_lastApplied < m_commitIndex) {
    m_lastApplied++;
    //这个断言感觉有点废话，但也许是多线程会进行干扰
    myAssert(m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() == m_lastApplied,
             format("rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogIndex{%d} != rf.lastApplied{%d} ",
                    m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(), m_lastApplied));
    ApplyMsg applyMsg;
    applyMsg.CommandValid = true;
    applyMsg.SnapshotValid = false;
    applyMsg.Command = m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].command();
    applyMsg.CommandIndex = m_lastApplied;
    applyMsgs.emplace_back(applyMsg);
    //        DPrintf("[	applyLog func-rf{%v}	] apply Log,logIndex:%v  ，logTerm：{%v},command：{%v}\n",
    //        rf.me, rf.lastApplied, rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogTerm,
    //        rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].Command)
  }
  return applyMsgs;
}

// 获取新命令应该分配的Index
int Raft::getNewCommandIndex() {
  //	如果len(logs)==0,就为快照的index+1，否则为log最后一个日志+1
  auto lastLogIndex = getLastLogIndex();
  return lastLogIndex + 1;
}

// leader调用，传入：服务器index，传出：发送的AE的preLogIndex和PrevLogTerm
void Raft::getPrevLogInfo(int server, int* preIndex, int* preTerm) {
  // logs长度为0返回0,0，不是0就根据nextIndex数组的数值返回
  if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1) {
    // 要发送的日志是第一个日志，因此直接返回m_lastSnapshotIncludeIndex和m_lastSnapshotIncludeTerm
    *preIndex = m_lastSnapshotIncludeIndex;
    *preTerm = m_lastSnapshotIncludeTerm;
    return;
  }
  auto nextIndex = m_nextIndex[server];
  *preIndex = nextIndex - 1;
  *preTerm = m_logs[getSlicesIndexFromLogIndex(*preIndex)].logterm();
}

// GetState return currentTerm and whether this server
// believes it is the Leader.
void Raft::GetState(int* term, bool* isLeader) {
  m_mtx.lock();
  DEFER {
    // todo 暂时不清楚会不会导致死锁
    m_mtx.unlock();
  };

  // Your code here (2A).
  *term = m_currentTerm;
  *isLeader = (m_status == Leader);
}

void Raft::pushMsgToKvServer(ApplyMsg msg) { applyChan->Push(msg); }
// 心跳函数（每隔一段时间进行一次心跳）
void Raft::leaderHearBeatTicker() {
  while (true) {
    while (m_status != Leader) {
      usleep(1000 * HeartBeatTimeout);  // 和sleep区别就是微秒而已
    }
    static std::atomic<int32_t> atomicCount = 0;
    // 别管后面的括号就理解为是创建了这两个变量吧
    // 还能睡的时间
    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
    // 现在醒来的时间
    std::chrono::system_clock::time_point wakeTime{};
    // 这都是对类的操作，可能内部写了重载+号
    {
      std::lock_guard<std::mutex> lock(m_mtx);
      wakeTime = now();
      suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout) + m_lastResetHearBeatTime - wakeTime;
    }
    // 如果算出来的‘剩余可睡眠时间’，大于 1 毫秒，那我就准备去睡了。
    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
      //\033[1;35m，“加粗紫色      \033[0m，回到了普通的白字
      std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数设置睡眠时间为: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                << std::endl;
      // 获取当前时间点
      auto start = std::chrono::steady_clock::now();

      usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());

      // 获取函数运行结束后的时间点
      auto end = std::chrono::steady_clock::now();

      // 计算时间差并输出结果（单位为毫秒）
      std::chrono::duration<double, std::milli> duration = end - start;

      // 使用ANSI控制序列将输出颜色修改为紫色
      std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数实际睡眠时间为: " << duration.count()
                << " 毫秒\033[0m" << std::endl;
      ++atomicCount;  // 就是第几次leaderHearBeatTicker()的作用而已
    }
    // 客户端此时发来其他存的请求，其他线程给follwer发了消息，就代表可以替代一次心跳
    // 比如 心跳间隔是50  上一次的心跳是  10.00  当前时间是 10.20  所以睡觉时间是30
    // 睡的 过程中在10.40发了消息，所以当10.50醒来的时候就是看见10.40大于10.20 继续上面进行睡觉
    // 每次doheartbeat最后都会让m_lastResetHearBeatTime变成=now（）
    if (std::chrono::duration<double, std::milli>(m_lastResetHearBeatTime - wakeTime).count() > 0) {
      continue;
    }
    // 做心跳（顺便同步消息）
    // 目前我感觉doHeartBeat()是不占用太多时间的，因为花时间和阻塞的都是开辟了一个新的线程
    // 但是会不会每一次我们做心跳都是网络不好，都没接收我们每隔一段时间触发一次心跳呢，
    // 感觉应该是electiontimertricker负责处理这些事情，follwer如果长时间没收到，就会自己成为候选人，看能否成为leader
    // 如果leader发送成功了，follwer也收到了，但是follwer发给leader的时候失败了。leader下一次心跳发送的时候nextindex就不用更新可以继续发送
    doHeartBeat();
  }
}

void Raft::leaderSendSnapShot(int server) {
  m_mtx.lock();
  raftRpcProctoc::InstallSnapshotRequest args;
  args.set_leaderid(m_me);
  args.set_term(m_currentTerm);
  args.set_lastsnapshotincludeindex(m_lastSnapshotIncludeIndex);
  args.set_lastsnapshotincludeterm(m_lastSnapshotIncludeTerm);
  args.set_data(m_persister->ReadSnapshot());

  raftRpcProctoc::InstallSnapshotResponse reply;
  m_mtx.unlock();
  // 实际上会调用重写的callmethod方法，然后实际执行InstallSnapshot是对方的raft节点执行
  bool ok = m_peers[server]->InstallSnapshot(&args, &reply);
  m_mtx.lock();
  DEFER { m_mtx.unlock(); };
  if (!ok) {
    return;
  }
  // m_currentTerm !=
  // args.term()这个就是你发送完InstallSnapshot之后，突然选出了新的，等你收到reply的时候，发现term与你的args是不一样的
  if (m_status != Leader || m_currentTerm != args.term()) {
    return;  // 中间释放过锁，可能状态已经改变了
  }
  //	无论什么时候都要判断term
  if (reply.term() > m_currentTerm) {
    // 三变
    m_currentTerm = reply.term();
    m_votedFor = -1;  // 任期比我大立刻放弃leader位置  但是当前票没投所以还是-1
    m_status = Follower;
    persist();
    // 认清现实、退位成了 Follower，你的本职工作就是耐心等待新老板发心跳过来。通过把时间重置为
    // now()，如果后面超时了，我在成为候选者
    m_lastResetElectionTime = now();
    return;
  }
  m_matchIndex[server] = args.lastsnapshotincludeindex();
  m_nextIndex[server] = m_matchIndex[server] + 1;
}

void Raft::leaderUpdateCommitIndex() {
  m_commitIndex = m_lastSnapshotIncludeIndex;
  // for index := rf.commitIndex+1;index < len(rf.log);index++ {
  // for index := rf.getLastIndex();index>=rf.commitIndex+1;index--{
  for (int index = getLastLogIndex(); index >= m_lastSnapshotIncludeIndex + 1; index--) {
    int sum = 0;
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        sum += 1;
        continue;
      }
      if (m_matchIndex[i] >= index) {
        sum += 1;
      }
    }

    //        !!!只有当前term有新提交的，才会更新commitIndex！！！！
    // log.Printf("lastSSP:%d, index: %d, commitIndex: %d, lastIndex: %d",rf.lastSSPointIndex, index, rf.commitIndex,
    // rf.getLastIndex())
    if (sum >= m_peers.size() / 2 + 1 && getLogTermFromLogIndex(index) == m_currentTerm) {
      m_commitIndex = index;
      break;
    }
  }
  //    DPrintf("[func-leaderUpdateCommitIndex()-rf{%v}] Leader %d(term%d) commitIndex
  //    %d",rf.me,rf.me,rf.currentTerm,rf.commitIndex)
}

// 进来前要保证logIndex是存在的，即≥rf.lastSnapshotIncludeIndex	，而且小于等于rf.getLastLogIndex()
bool Raft::matchLog(int logIndex, int logTerm) {
  myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(),
           format("不满足：logIndex{%d}>=rf.lastSnapshotIncludeIndex{%d}&&logIndex{%d}<=rf.getLastLogIndex{%d}",
                  logIndex, m_lastSnapshotIncludeIndex, logIndex, getLastLogIndex()));
  return logTerm == getLogTermFromLogIndex(logIndex);
}

void Raft::persist() {
  auto data = persistData();
  m_persister->SaveRaftState(data);
}

bool Raft::UpToDate(int index, int term) {
  // lastEntry := rf.log[len(rf.log)-1]

  int lastIndex = -1;
  int lastTerm = -1;
  getLastLogIndexAndTerm(&lastIndex, &lastTerm);
  return term > lastTerm || (term == lastTerm && index >= lastIndex);
}

// 函数目的：获取当前节点拥有的“最后一条日志”的索引 (Index) 和任期 (Term)。
// 参数使用指针传出结果。
void Raft::getLastLogIndexAndTerm(int* lastLogIndex, int* lastLogTerm) {
  // ---------------------------------------------------------
  // 分支一：内存中的日志数组为空 (触发了快照/日志压缩机制)
  // ---------------------------------------------------------
  if (m_logs.empty()) {
    // 为什么日志数组会为空？
    // 在 Raft 运行久了之后，日志会无限增长。为了节省内存，系统会定期生成一个“快照 (Snapshot)”。
    // 生成快照后，那些已经被包含进快照的旧日志，就会从 m_logs 数组中被物理删除 (Discarded)。
    // 如果所有的日志都被打进了快照，m_logs 就会变成 empty()。

    // 此时，“最后一条日志”其实就是“快照所包含的最后一条日志”。
    // 这两个元数据 (Metadata) 是我们在保存快照时专门记录下来的。
    *lastLogIndex = m_lastSnapshotIncludeIndex;
    *lastLogTerm = m_lastSnapshotIncludeTerm;
    return;

    // ---------------------------------------------------------
    // 分支二：内存中的日志数组不为空 (常规状态)
    // ---------------------------------------------------------
  } else {
    // 数组里有日志，那么最后一条日志自然就是数组的最后一个元素。
    // m_logs.size() - 1 是最后一个元素的下标 (C++ 数组从 0 开始)。

    // 直接调用 Protobuf 生成的 getter 方法获取这条日志的 index 和 term。
    *lastLogIndex = m_logs[m_logs.size() - 1].logindex();
    *lastLogTerm = m_logs[m_logs.size() - 1].logterm();
    return;
  }
}
/**
 *
 * @return 最新的log的logindex，即log的逻辑index。区别于log在m_logs中的物理index
 * 可见：getLastLogIndexAndTerm()
 */
int Raft::getLastLogIndex() {
  int lastLogIndex = -1;
  int _ = -1;
  getLastLogIndexAndTerm(&lastLogIndex, &_);
  return lastLogIndex;
}

int Raft::getLastLogTerm() {
  int _ = -1;
  int lastLogTerm = -1;
  getLastLogIndexAndTerm(&_, &lastLogTerm);
  return lastLogTerm;
}

/**
 *
 * @param logIndex log的逻辑index。注意区别于m_logs的物理index
 * @return
 */
int Raft::getLogTermFromLogIndex(int logIndex) {
  myAssert(logIndex >= m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} < rf.lastSnapshotIncludeIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));

  int lastLogIndex = getLastLogIndex();

  myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                            m_me, logIndex, lastLogIndex));

  if (logIndex == m_lastSnapshotIncludeIndex) {
    return m_lastSnapshotIncludeTerm;
  } else {
    return m_logs[getSlicesIndexFromLogIndex(logIndex)].logterm();
  }
}

int Raft::GetRaftStateSize() { return m_persister->RaftStateSize(); }

// 找到index对应的真实下标位置！！！
// 限制，输入的logIndex必须保存在当前的logs里面（不包含snapshot）
int Raft::getSlicesIndexFromLogIndex(int logIndex) {
  myAssert(logIndex > m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} <= rf.lastSnapshotIncludeIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));
  int lastLogIndex = getLastLogIndex();
  myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                            m_me, logIndex, lastLogIndex));
  int SliceIndex = logIndex - m_lastSnapshotIncludeIndex - 1;
  return SliceIndex;
}
// 请求选民投票
bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                           std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum) {
  // 这个ok是网络是否正常通信的ok，而不是requestVote rpc是否投票的rpc
  //  ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  //  todo
  auto start = now();
  DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 發送 RequestVote 開始", m_me, m_currentTerm, getLastLogIndex());
  bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
  DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 發送 RequestVote 完畢，耗時:{%d} ms", m_me, m_currentTerm,
          getLastLogIndex(), now() - start);

  if (!ok) {
    return ok;  // 不知道为什么不加这个的话如果服务器宕机会出现问题的，通不过2B
  }
  std::lock_guard<std::mutex> lg(m_mtx);
  // 比你大，说明你不配当作leader
  if (reply->term() > m_currentTerm) {
    m_status = Follower;  // 三变：身份，term，和投票
    m_currentTerm = reply->term();
    m_votedFor = -1;
    persist();
    return true;
    // 比你小，说明是可能是你上次发来的，网络延迟了
  } else if (reply->term() < m_currentTerm) {
    return true;
  }
  // 才是同意你的志愿当leader请求，所以会改为和你一样的任期
  myAssert(reply->term() == m_currentTerm, format("assert {reply.Term==rf.currentTerm} fail"));

  // 不同意直接return  别被return true误导了就是不接受
  if (!reply->votegranted()) {
    return true;
  }
  // 当前投票数加1呗
  *votedNum = *votedNum + 1;
  if (*votedNum >= m_peers.size() / 2 + 1) {
    // 变成leader
    *votedNum = 0;
    if (m_status == Leader) {
      // 如果已经是leader了，那么是就是了，不会进行下一步处理了k
      myAssert(false,
               format("[func-sendRequestVote-rf{%d}]  term:{%d} 同一个term当两次领导，error", m_me, m_currentTerm));
    }
    //	第一次变成leader，初始化状态和nextIndex、matchIndex
    m_status = Leader;

    DPrintf("[func-sendRequestVote rf{%d}] elect success  ,current term:{%d} ,lastLogIndex:{%d}\n", m_me, m_currentTerm,
            getLastLogIndex());

    int lastLogIndex = getLastLogIndex();
    for (int i = 0; i < m_nextIndex.size(); i++) {
      m_nextIndex[i] = lastLogIndex + 1;  // 有效下标从1开始，因此要+1
      m_matchIndex[i] = 0;                // 每换一个领导都是从0开始，见fig2
    }
    // 只发送了这一次，主要可能是为了快速同步，后续的heartbeat还是主要靠那个while循环的心跳函数
    std::thread t(&Raft::doHeartBeat, this);  // 马上向其他节点宣告自己就是leader
    t.detach();

    persist();
  }
  return true;
}

bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply,
                             std::shared_ptr<int> appendNums) {
  // 这个ok是网络是否正常通信的ok，而不是requestVote rpc是否投票的rpc
  //  如果网络不通的话肯定是没有返回的，不用一直重试
  //  todo： paper中5.3节第一段末尾提到，如果append失败应该不断的retries ,直到这个log成功的被store
  DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc開始 ， args->entries_size():{%d}", m_me,
          server, args->entries_size());
  // 发送心跳的时候夹杂着一些同步的信息，没有同步的信息就是只发送心跳是吗
  // 编译器傻逼了，可能学校服务器没安环境，实际上要理解到虽然是leader但此时仍然是客户端，客户端调用所谓的AppendEntries方法，客户端实际上继承的是raftRpc_Stub
  // RaftRpcUtil::RaftRpcUtil(std::string ip, short port) {RaftRpcUtil::RaftRpcUtil(std::string ip, short port) {
  // stub_ = new raftRpcProctoc::raftRpc_Stub(new MprpcChannel(ip, port, true));
  // 所以最终我们用的是MprpcChannel重写的callmethod方法，将这个发过去了，对面收到之后执行这个
  //
  bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());

  // 这两种应该看来都算是失败情况，但是他弄一个ok真的非常误导人呀
  if (!ok) {
    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc失敗", m_me, server);
    return ok;
  }
  DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc成功", m_me, server);
  if (reply->appstate() == Disconnected) {
    return ok;
  }
  std::lock_guard<std::mutex> lg1(m_mtx);

  // 对reply进行处理
  // 对于rpc通信，无论什么时候都要检查term
  if (reply->term() > m_currentTerm) {
    m_status = Follower;
    m_currentTerm = reply->term();
    m_votedFor = -1;
    return ok;
  } else if (reply->term() < m_currentTerm) {
    DPrintf("[func -sendAppendEntries  rf{%d}]  节点：{%d}的term{%d}<rf{%d}的term{%d}\n", m_me, server, reply->term(),
            m_me, m_currentTerm);
    return ok;
  }

  if (m_status != Leader) {
    // 如果不是leader，那么就不要对返回的情况进行处理了
    return ok;
  }
  // term相等

  myAssert(reply->term() == m_currentTerm,
           format("reply.Term{%d} != rf.currentTerm{%d}   ", reply->term(), m_currentTerm));
  if (!reply->success()) {
    // 日志不匹配，正常来说就是index要往前-1，既然能到这里，第一个日志（idnex =
    //  1）发送后肯定是匹配的，因此不用考虑变成负数 因为真正的环境不会知道是服务器宕机还是发生网络分区了
    if (reply->updatenextindex() != -100) {
      DPrintf("[func -sendAppendEntries  rf{%d}]  返回的日志term相等，但是不匹配，回缩nextIndex[%d]：{%d}\n", m_me,
              server, reply->updatenextindex());
      m_nextIndex[server] = reply->updatenextindex();  // 失败是不更新mathIndex的
    }
  } else {
    //   *appendNums当前已经成功追加这条日志的节点总数，因为当前这个收到了所以就需要加1
    *appendNums = *appendNums + 1;
    DPrintf("---------------------------tmp------------------------- 節點{%d}返回true,當前*appendNums{%d}", server,
            *appendNums);
    m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());
    m_nextIndex[server] = m_matchIndex[server] + 1;
    int lastLogIndex = getLastLogIndex();
    // lastLogIndex + 1是因为加入一个follwer已经和leader的日志持平，加入leader日志的最后一条是3
    // follwer就会发来下一条要4，所以必须要+1
    myAssert(m_nextIndex[server] <= lastLogIndex + 1,
             format("error msg:rf.nextIndex[%d] > lastLogIndex+1, len(rf.logs) = %d   lastLogIndex{%d} = %d", server,
                    m_logs.size(), server, lastLogIndex));
    // 收到一半节点以上的回复就代表这个消息可以持久化了
    if (*appendNums >= 1 + m_peers.size() / 2) {
      // 可以commit了
      // 两种方法保证幂等性，1.赋值为0 	2.上面≥改为==

      *appendNums = 0;
      if (args->entries_size() > 0) {
        DPrintf("args->entries(args->entries_size()-1).logterm(){%d}   m_currentTerm{%d}",
                args->entries(args->entries_size() - 1).logterm(), m_currentTerm);
      }
      if (args->entries_size() > 0 && args->entries(args->entries_size() - 1).logterm() == m_currentTerm) {
        DPrintf(
            "---------------------------tmp------------------------- 當前term有log成功提交，更新leader的m_commitIndex "
            "from{%d} to{%d}",
            m_commitIndex, args->prevlogindex() + args->entries_size());

        m_commitIndex = std::max(m_commitIndex, args->prevlogindex() + args->entries_size());
      }
      myAssert(m_commitIndex <= lastLogIndex,
               format("[func-sendAppendEntries,rf{%d}] lastLogIndex:%d  rf.commitIndex:%d\n", m_me, lastLogIndex,
                      m_commitIndex));
    }
  }
  return ok;
}

void Raft::Start(Op command, int* newLogIndex, int* newLogTerm, bool* isLeader) {
  std::lock_guard<std::mutex> lg1(m_mtx);
  //    m_mtx.lock();
  //    Defer ec1([this]()->void {
  //       m_mtx.unlock();
  //    });
  if (m_status != Leader) {
    DPrintf("[func-Start-rf{%d}]  is not leader");
    *newLogIndex = -1;
    *newLogTerm = -1;
    *isLeader = false;
    return;
  }

  raftRpcProctoc::LogEntry newLogEntry;
  newLogEntry.set_command(command.asString());
  newLogEntry.set_logterm(m_currentTerm);
  newLogEntry.set_logindex(getNewCommandIndex());
  m_logs.emplace_back(newLogEntry);

  int lastLogIndex = getLastLogIndex();

  // leader应该不停的向各个Follower发送AE来维护心跳和保持日志同步，目前的做法是新的命令来了不会直接执行，而是等待leader的心跳触发
  DPrintf("[func-Start-rf{%d}]  lastLogIndex:%d,command:%s\n", m_me, lastLogIndex, &command);
  // rf.timer.Reset(10) //接收到命令后马上给follower发送,改成这样不知为何会出现问题，待修正 todo
  persist();
  *newLogIndex = newLogEntry.logindex();
  *newLogTerm = newLogEntry.logterm();
  *isLeader = true;
}

/*
peers (通讯录)：std::vector<std::shared_ptr<RaftRpcUtil>>
含义：保存了集群中所有节点的 RPC 通信句柄。如果集群有 3 台机器，这个数组长度就是 3。Raft
节点通过这个数组来给别人发心跳、拉选票。 me (工号)：int 含义：告诉这个节点：“你在 peers 通讯录里的索引是多少”。比如 me =
1，那就代表 peers[1] 是自己，不用给自己发网络请求。 persister (日记本/硬盘存储)：std::shared_ptr<Persister>
含义：底层存储的接口。Raft 节点用它来把关键数据（任期 Term、选票 VotedFor、日志
Logs）写进硬盘，或者在断电重启时从硬盘读出来。

applyCh (向上级汇报的管道)：std::shared_ptr<LockQueue<ApplyMsg>>

含义：一条通往上层（KV 数据库）的线程安全队列。当 Raft 确定某条日志已经安全（被多数派确认），就会打包成 ApplyMsg
扔进这个管道，上层 KV 数据库就会从管道另一头取出来执行。
*/
void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {
  m_peers = peers;
  m_persister = persister;
  m_me = me;
  // Your initialization code here (2A, 2B, 2C).
  m_mtx.lock();

  // applier
  this->applyChan = applyCh;
  //    rf.ApplyMsgQueue = make(chan ApplyMsg)
  m_currentTerm = 0;
  m_status = Follower;
  m_commitIndex = 0;
  m_lastApplied = 0;
  m_logs.clear();
  for (int i = 0; i < m_peers.size(); i++) {
    m_matchIndex.push_back(0);
    m_nextIndex.push_back(0);
  }
  m_votedFor = -1;

  m_lastSnapshotIncludeIndex = 0;
  m_lastSnapshotIncludeTerm = 0;
  m_lastResetElectionTime = now();
  m_lastResetHearBeatTime = now();

  // initialize from state persisted before a crash
  readPersist(m_persister->ReadRaftState());
  if (m_lastSnapshotIncludeIndex > 0) {
    m_lastApplied = m_lastSnapshotIncludeIndex;
    // rf.commitIndex = rf.lastSnapshotIncludeIndex   todo ：崩溃恢复为何不能读取commitIndex
  }

  DPrintf("[Init&ReInit] Sever %d, term %d, lastSnapshotIncludeIndex {%d} , lastSnapshotIncludeTerm {%d}", m_me,
          m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);

  m_mtx.unlock();

  m_ioManager = std::make_unique<monsoon::IOManager>(FIBER_THREAD_NUM, FIBER_USE_CALLER_THREAD);

  // start ticker fiber to start elections
  // 启动三个循环定时器
  // todo:原来是启动了三个线程，现在是直接使用了协程，三个函数中leaderHearBeatTicker
  // electionTimeOutTicker执行时间是恒定的，applierTicker时间受到数据库响应延迟和两次apply之间请求数量的影响，这个随着数据量增多可能不太合理，最好其还是启用一个线程。
  m_ioManager->scheduler([this]() -> void { this->leaderHearBeatTicker(); });
  m_ioManager->scheduler([this]() -> void { this->electionTimeOutTicker(); });

  std::thread t3(&Raft::applierTicker, this);
  t3.detach();

  // std::thread t(&Raft::leaderHearBeatTicker, this);
  // t.detach();
  //
  // std::thread t2(&Raft::electionTimeOutTicker, this);
  // t2.detach();
  //
  // std::thread t3(&Raft::applierTicker, this);
  // t3.detach();
}

/*函数的作用是把散落在 Raft 对象里的变量 统一塞进一个名叫 BoostPersistRaftNode 的“纸箱子”里，
然后用压缩机（Boost）压成一段长长的字符串
*/
std::string Raft::persistData() {
  // 1. 找一个专门用来“持久化打包”的空纸箱
  BoostPersistRaftNode boostPersistRaftNode;

  // 2. 把当前的重要变量，一个个扔进纸箱里
  boostPersistRaftNode.m_currentTerm = m_currentTerm;  // 存入：当前的朝代 (Term)
  boostPersistRaftNode.m_votedFor = m_votedFor;        // 存入：我把票投给了谁

  // 这两个是和“快照”相关的进度记录，也一起存进去（防止忘了已经备份到哪了）
  boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
  boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;

  // 3. 重点来了：存日志！
  // 因为 m_logs 里面装的是 Protobuf 对象，不能直接扔进纸箱
  for (auto& item : m_logs) {
    // 把每一条日志先“序列化”（变成一串乱码字节流），然后再塞进纸箱的日志数组里
    boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());
  }

  // 4. 准备一台“压缩机”（这里用的是 Boost 库的文本序列化工具）
  std::stringstream ss;                  // 准备一个内存池用来接压缩后的数据
  boost::archive::text_oarchive oa(ss);  // 启动压缩机，出口对准 ss

  // 5. 把整个装满数据的纸箱，扔进压缩机！
  // 此时，整个对象被变成了一串长长的、机器能读懂的字符串
  oa << boostPersistRaftNode;

  // 6. 把这串压缩好的字符串交出去
  return ss.str();
}

void Raft::readPersist(std::string data) {
  if (data.empty()) {
    return;
  }
  std::stringstream iss(data);
  boost::archive::text_iarchive ia(iss);
  // read class state from archive
  BoostPersistRaftNode boostPersistRaftNode;
  ia >> boostPersistRaftNode;

  m_currentTerm = boostPersistRaftNode.m_currentTerm;
  m_votedFor = boostPersistRaftNode.m_votedFor;
  m_lastSnapshotIncludeIndex = boostPersistRaftNode.m_lastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = boostPersistRaftNode.m_lastSnapshotIncludeTerm;
  m_logs.clear();
  for (auto& item : boostPersistRaftNode.m_logs) {
    raftRpcProctoc::LogEntry logEntry;
    logEntry.ParseFromString(item);
    m_logs.emplace_back(logEntry);
  }
}

void Raft::Snapshot(int index, std::string snapshot) {
  std::lock_guard<std::mutex> lg(m_mtx);

  if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex) {
    DPrintf(
        "[func-Snapshot-rf{%d}] rejects replacing log with snapshotIndex %d as current snapshotIndex %d is larger or "
        "smaller ",
        m_me, index, m_lastSnapshotIncludeIndex);
    return;
  }
  auto lastLogIndex = getLastLogIndex();  // 为了检查snapshot前后日志是否一样，防止多截取或者少截取日志

  // 制造完此快照后剩余的所有日志
  int newLastSnapshotIncludeIndex = index;
  int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
  std::vector<raftRpcProctoc::LogEntry> trunckedLogs;
  // todo :这种写法有点笨，待改进，而且有内存泄漏的风险
  for (int i = index + 1; i <= getLastLogIndex(); i++) {
    // 注意有=，因为要拿到最后一个日志
    trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
  }
  m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;
  m_logs = trunckedLogs;
  m_commitIndex = std::max(m_commitIndex, index);
  m_lastApplied = std::max(m_lastApplied, index);

  // rf.lastApplied = index //lastApplied 和 commit应不应该改变呢？？？ 为什么  不应该改变吧
  m_persister->Save(persistData(), snapshot);

  DPrintf("[SnapShot]Server %d snapshot snapshot index {%d}, term {%d}, loglen {%d}", m_me, index,
          m_lastSnapshotIncludeTerm, m_logs.size());
  myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
           format("len(rf.logs){%d} + rf.lastSnapshotIncludeIndex{%d} != lastLogjInde{%d}", m_logs.size(),
                  m_lastSnapshotIncludeIndex, lastLogIndex));
}