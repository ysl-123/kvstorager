#ifndef __MONSOON_UTIL_H__
#define __MONSOON_UTIL_H__

#include <assert.h>
#include <cxxabi.h>
#include <execinfo.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <vector>

namespace monsoon {
pid_t GetThreadId();
u_int32_t GetFiberId();
// 获取当前启动的毫秒数
// 系统从启动到当前时刻的毫秒数
static uint64_t GetElapsedMS() {
  struct timespec ts = {0};
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

// 将原始函数名解析为可读函数名
static std::string demangle(const char *str) {
  size_t size = 0;
  int status = 0;
  std::string rt;
  rt.resize(256);
  if (1 == sscanf(str, "%*[^(]%*[^_]%255[^)+]", &rt[0])) {
    // 解析函数
    char *v = abi::__cxa_demangle(&rt[0], nullptr, &size, &status);
    if (v) {
      std::string result(v);
      free(v);
      return result;
    }
  }
  // 解析失败，返回原始函数名
  if (1 == sscanf(str, "%255s", &rt[0])) {
    return rt;
  }
  return str;
}
// 获取当前线程的调用栈信息
static void Backtrace(std::vector<std::string> &bt, int size, int skip) {
  // 分配用于存储调用栈信息的数组
  void **array = (void **)malloc((sizeof(void *) * size));
  size_t s = ::backtrace(array, size);

  // 获取调用栈信息
  char **strings = backtrace_symbols(array, s);
  if (strings == NULL) {
    std::cout << "backtrace_synbols error" << std::endl;
    return;
  }
  // 解析每一个调用栈的信息，并将解析后的函数名添加到bt中
  for (size_t i = skip; i < s; ++i) {
    bt.push_back(demangle(strings[i]));
  }

  free(strings);
  free(array);
}

static std::string BacktraceToString(int size, int skip, const std::string &prefix) {
  std::vector<std::string> bt;
  Backtrace(bt, size, skip);
  std::stringstream ss;
  for (size_t i = 0; i < bt.size(); ++i) {
    ss << prefix << bt[i] << std::endl;
  }
  return ss.str();
}

// 断言处理
// 目的是期望condition是true
static void CondPanic(bool condition, std::string err) {
  if (!condition) {
    /*__FILE__ 和 __LINE__：这是 C++ 编译器的内置宏。它们会自动被替换成当前这行代码所在的文件名和行号（比如 scheduler.cc:42）
      BacktraceToString(6, 3, "")：这是一个非常实用的调试函数（通常底层封装了 Linux 的 backtrace 和 backtrace_symbols 函数）。
      它的作用是顺藤摸瓜，把程序是怎么一步步运行到报错这一行的历史轨迹全打印出来。
    */
    std::cout << "[assert by] (" << __FILE__ << ":" << __LINE__ << "),err: " << err << std::endl;
    std::cout << "[backtrace]\n" << BacktraceToString(6, 3, "") << std::endl;
    assert(condition);
  }
}
}  // namespace monsoon

#endif