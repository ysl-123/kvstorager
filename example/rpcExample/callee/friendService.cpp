//
// Created by swx on 23-12-21.
//
#include <mprpcchannel.h>
#include <iostream>
#include <string>
#include "rpcExample/friend.pb.h"

#include <vector>
#include "rpcprovider.h"

class FriendService : public fixbug::FiendServiceRpc {
 public:
  std::vector<std::string> GetFriendsList(uint32_t userid) {
    std::cout << "local do GetFriendsList service! userid:" << userid << std::endl;
    std::vector<std::string> vec;
    vec.push_back("gao yang");
    vec.push_back("liu hong");
    vec.push_back("wang shuo");
    return vec;
  }

  // 重写基类方法
  void GetFriendsList(::google::protobuf::RpcController *controller, const ::fixbug::GetFriendsListRequest *request,
                      ::fixbug::GetFriendsListResponse *response, ::google::protobuf::Closure *done) {
    uint32_t userid = request->userid();
    //这里不是就调用所谓的上面的方法吗
    std::vector<std::string> friendsList = GetFriendsList(userid);
    response->mutable_result()->set_errcode(0);
    response->mutable_result()->set_errmsg("");
    for (std::string &name : friendsList) {
      std::string *p = response->add_friends();
      *p = name;
    }
    //就会主动执行将运行结果response发回给客户端
    done->Run();
  }
};

int main(int argc, char **argv) {
  std::string ip = "127.0.0.1";
  short port = 7788;
  // （我也不知道他写这行是干什么的，明明这是属于客户端的代码吧） auto stub = new fixbug::FiendServiceRpc_Stub(new MprpcChannel(ip, port, false));
  // provider是一个rpc网络服务对象。把UserService对象发布到rpc节点上
  RpcProvider provider;
  //这个就是将当前的FriendService里提供的方法发布出去，整体provider就知道了呢
  provider.NotifyService(new FriendService());

  // 启动一个rpc服务发布节点   Run以后，进程进入阻塞状态，等待远程的rpc调用请求
  provider.Run(1, 7788);

  return 0;
}
