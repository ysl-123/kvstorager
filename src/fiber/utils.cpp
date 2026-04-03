#include "utils.hpp"

namespace monsoon {
  //获取运行这一句的线程id
pid_t GetThreadId() { return syscall(SYS_gettid); }

u_int32_t GetFiberId() {
  // TODO
  return 0;
}
}  // namespace monsoon