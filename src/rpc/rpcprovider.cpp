#include "rpcprovider.h"
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include <fstream>
#include <string>
#include "rpcheader.pb.h"
#include "util.h"
/*
service_name =>  service描述
                        =》 service* 记录服务对象
                        method_name  =>  method方法对象
json   protobuf
*/
// 这里是框架提供给外部使用的，可以发布rpc方法的函数接口
// 只是简单的把服务描述符和方法描述符全部保存在本地而已
// todo 待修改 要把本机开启的ip和端口写在文件里面
void RpcProvider::NotifyService(google::protobuf::Service *service) {
  ServiceInfo service_info;

  // 获取了服务对象的描述信息
  const google::protobuf::ServiceDescriptor *pserviceDesc = service->GetDescriptor();
  // 获取服务的名字
  std::string service_name = pserviceDesc->name();
  // 获取服务对象service的方法的数量
  int methodCnt = pserviceDesc->method_count();

  std::cout << "service_name:" << service_name << std::endl;

  for (int i = 0; i < methodCnt; ++i) {
    // 获取了服务对象指定下标的服务方法的描述（抽象描述） UserService   Login
    const google::protobuf::MethodDescriptor *pmethodDesc = pserviceDesc->method(i);
    std::string method_name = pmethodDesc->name();
    service_info.m_methodMap.insert({method_name, pmethodDesc});
  }
  service_info.m_service = service;
  m_serviceMap.insert({service_name, service_info});
}

// 启动rpc服务节点，开始提供rpc远程网络调用服务
void RpcProvider::Run(int nodeIndex, short port) {
  // --- 第一部分：获取本机可用的 IP 地址 ---
  char *ipC;           // 用于暂存转换后的字符串格式 IP
  char hname[128];     // 存储主机名
  struct hostent *hent;// 存储主机信息的结构体指针

  // 1. 获取当前机器的主机名（比如 "DESKTOP-LFLKPMP"）
  gethostname(hname, sizeof(hname));

  // 2. 根据主机名获取主机的详细信息（包括 IP 地址列表）
  hent = gethostbyname(hname);

  // 3. 遍历该主机所有的 IP 地址列表
  // h_addr_list 是一个数组，最后一位是 NULL
  for (int i = 0; hent->h_addr_list[i]; i++) {
    // inet_ntoa: 将网络字节序的二进制 IP 转换为人类可读的字符串（如 "192.168.1.10"）
    //实际上这个循环最终  ipC 只会保留最后一个找到的地址。
    ipC = inet_ntoa(*(struct in_addr *)(hent->h_addr_list[i])); 
  }
  // 将最后一个找到的有效 IP 转换为 C++ 的 string 对象
  std::string ip = std::string(ipC);
 
  // --- 第二部分：将配置信息写入文件 ---
  
  // 拼接节点名称，例如：nodeIndex 为 1，则 node 为 "node1"
  std::string node = "node" + std::to_string(nodeIndex);
  
  std::ofstream outfile;
  // 以“追加模式”(std::ios::app) 打开 test.conf，如果文件不存在则创建
  outfile.open("test.conf", std::ios::app); 

  // 检查文件是否成功打开（防止权限问题或磁盘满）
  if (!outfile.is_open()) {
    std::cout << "打开文件失败！" << std::endl;
    exit(EXIT_FAILURE); // 严重错误，直接退出程序
  }

  // 写入格式：node1ip=192.168.1.10
  outfile << node + "ip=" + ip << std::endl;
  // 写入格式：node1port=8080
  outfile << node + "port=" + std::to_string(port) << std::endl;

  // 关闭文件句柄，确保数据真正写入磁盘
  outfile.close();
  //创建服务器
  muduo::net::InetAddress address(ip, port);

  m_muduo_server = std::make_shared<muduo::net::TcpServer>(&m_eventLoop, address, "RpcProvider");
  m_muduo_server->setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
  m_muduo_server->setMessageCallback(
      std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));

  // 设置muduo库的线程数量
  m_muduo_server->setThreadNum(4);

  // rpc服务端准备启动，打印信息
  std::cout << "RpcProvider start service at ip:" << ip << " port:" << port << std::endl;

  // 启动网络服务
  m_muduo_server->start();
  m_eventLoop.loop();
}

// 新的socket连接回调
void RpcProvider::OnConnection(const muduo::net::TcpConnectionPtr &conn) {
  // 如果是新连接就什么都不干，即正常的接收连接即可
  if (!conn->connected()) {
    // 和rpc client的连接断开了
    conn->shutdown();
  }
}

/*
在框架内部，RpcProvider和RpcConsumer协商好之间通信用的protobuf数据类型
service_name method_name args    定义proto的message类型，进行数据头的序列化和反序列化
                                 service_name method_name args_size
16UserServiceLoginzhang san123456

header_size(4个字节) + header_str + args_str
10 "10"
10000 "1000000"
std::string   insert和copy方法
*/
// 已建立连接用户的读写事件回调 如果远程有一个rpc服务的调用请求，那么OnMessage方法就会响应
// 这里来的肯定是一个远程调用请求
// 因此本函数需要：解析请求，根据服务名，方法名，参数，来调用service的来callmethod来调用本地的业务
void RpcProvider::OnMessage(const muduo::net::TcpConnectionPtr &conn, muduo::net::Buffer *buffer, muduo::Timestamp) {
  // 网络上接收的远程rpc调用请求的字符流    Login args
  std::string recv_buf = buffer->retrieveAllAsString();

  // 使用protobuf的CodedInputStream来解析数据流
  google::protobuf::io::ArrayInputStream array_input(recv_buf.data(), recv_buf.size());
  google::protobuf::io::CodedInputStream coded_input(&array_input);
  uint32_t header_size{};

  coded_input.ReadVarint32(&header_size);  // 解析header_size

  // 根据header_size读取数据头的原始字符流，反序列化数据，得到rpc请求的详细信息
  std::string rpc_header_str;
  RPC::RpcHeader rpcHeader;
  std::string service_name;
  std::string method_name;

  // 设置读取限制，不必担心数据读多
  google::protobuf::io::CodedInputStream::Limit msg_limit = coded_input.PushLimit(header_size);
  coded_input.ReadString(&rpc_header_str, header_size);
  // 恢复之前的限制，以便安全地继续读取其他数据
  coded_input.PopLimit(msg_limit);
  uint32_t args_size{};
  if (rpcHeader.ParseFromString(rpc_header_str)) {
    // 数据头反序列化成功
    service_name = rpcHeader.service_name();
    method_name = rpcHeader.method_name();
    args_size = rpcHeader.args_size();
  } else {
    // 数据头反序列化失败
    std::cout << "rpc_header_str:" << rpc_header_str << " parse error!" << std::endl;
    return;
  }

  // 获取rpc方法参数的字符流数据
  std::string args_str;
  // 直接读取args_size长度的字符串数据
  bool read_args_success = coded_input.ReadString(&args_str, args_size);

  if (!read_args_success) {
    // 处理错误：参数数据读取失败
    return;
  }

  // 获取service对象和method对象
  auto it = m_serviceMap.find(service_name);
  if (it == m_serviceMap.end()) {
    std::cout << "服务：" << service_name << " is not exist!" << std::endl;
    std::cout << "当前已经有的服务列表为:";
    for (auto item : m_serviceMap) {
      std::cout << item.first << " ";
    }
    std::cout << std::endl;
    return;
  }

  auto mit = it->second.m_methodMap.find(method_name);
  if (mit == it->second.m_methodMap.end()) {
    std::cout << service_name << ":" << method_name << " is not exist!" << std::endl;
    return;
  }

  google::protobuf::Service *service = it->second.m_service;       // 获取service对象  new UserService
  const google::protobuf::MethodDescriptor *method = mit->second;  // 获取method对象  Login

  // 生成rpc方法调用的请求request和响应response参数,由于是rpc的请求，因此请求需要通过request来序列化
  google::protobuf::Message *request = service->GetRequestPrototype(method).New();
  if (!request->ParseFromString(args_str)) {
    std::cout << "request parse error, content:" << args_str << std::endl;
    return;
  }
  google::protobuf::Message *response = service->GetResponsePrototype(method).New();

  // 给下面的method方法的调用，绑定一个Closure的回调函数
  // closure是执行完本地方法之后会发生的回调，因此需要完成序列化和反向发送请求的操作
  google::protobuf::Closure *done =
      google::protobuf::NewCallback<RpcProvider, const muduo::net::TcpConnectionPtr &, google::protobuf::Message *>(
          this, &RpcProvider::SendRpcResponse, conn, response);

  //服务端调用的一般是自动生成的那个callmethod，客户端才是调用的是mprpcchanenl重写的callmethod方法
  /*
}void raftRpc::CallMethod(const ::PROTOBUF_NAMESPACE_ID::MethodDescriptor* method,
                             ::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                             const ::PROTOBUF_NAMESPACE_ID::Message* request,
                             ::PROTOBUF_NAMESPACE_ID::Message* response,
                             ::google::protobuf::Closure* done) {
  GOOGLE_DCHECK_EQ(method->service(), file_level_service_descriptors_raftRPC_2eproto[0]);
  switch(method->index()) {
    case 0:
      AppendEntries(controller,
             ::PROTOBUF_NAMESPACE_ID::internal::DownCast<const ::raftRpcProctoc::AppendEntriesArgs*>(
                 request),
             ::PROTOBUF_NAMESPACE_ID::internal::DownCast<::raftRpcProctoc::AppendEntriesReply*>(
                 response),
             done);
      break;
}
       AppendEntries不是我们重写的方法吗
}*/ 
//这里就是执行这个method和将response进行返回
  service->CallMethod(method, nullptr, request, response, done);
}

// Closure的回调操作，用于序列化rpc的响应和网络发送,发送响应回去
void RpcProvider::SendRpcResponse(const muduo::net::TcpConnectionPtr &conn, google::protobuf::Message *response) {
  std::string response_str;
  if (response->SerializeToString(&response_str))  // response进行序列化
  {
    // 序列化成功后，通过网络把rpc方法执行的结果发送会rpc的调用方
    conn->send(response_str);
  } else {
    std::cout << "serialize response_str error!" << std::endl;
  }
  //    conn->shutdown(); // 模拟http的短链接服务，由rpcprovider主动断开连接  //改为长连接，不主动断开
}

RpcProvider::~RpcProvider() {
  std::cout << "[func - RpcProvider::~RpcProvider()]: ip和port信息：" << m_muduo_server->ipPort() << std::endl;
  m_eventLoop.quit();
  //    m_muduo_server.   怎么没有stop函数，奇奇怪怪，看csdn上面的教程也没有要停止，甚至上面那个都没有
}
