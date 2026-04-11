//
// Created by swx on 24-1-5.
//

#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>

#define STORE_FILE "store/dumpFile"

static std::string delimiter = ":";

// Class template to implement node
template <typename K, typename V>
class Node {
 public:
  Node() {}

  Node(K k, V v, int);

  ~Node();
  //加了const就是这个函数里面不能修改类定义的一些局部变量
  K get_key() const;

  V get_value() const;

  void set_value(V);

// 这是一个“指针数组”，每一项都指向不同层级的“下一个节点”
// forward[0] 指向第 1 层（最底层）的下一个节点
// forward[i] 指向第 i+1 层的下一个节点
// 它是跳表能够实现“快速跳跃”查找的关键
//Node<K, V> **forward = new Node<K, V>*[level]; 
//Node<int, std::string>* nodeA = new Node<int, std::string>(10, "hello", 3);
//forward[0] = nodeA; 
//这里这个2维数组实现的效果其实比较像1维的
//然后一个节点要存储他这一层到到下面所有层的他这个节点之后的节点是谁
  Node<K, V> **forward; 

// 记录当前这个节点“长到了多少层”
// 每一条数据的层数通常是根据概率随机生成的
// 比如 node_level = 3，代表这个节点在第 1, 2, 3 层都出现了，forward 数组的大小也就是 3
 int node_level;
 private:
  K key;
  V value;
};

template <typename K, typename V>
Node<K, V>::Node(const K k, const V v, int level) {
  this->key = k;
  this->value = v;
  this->node_level = level;

  //这里不就是相当于创建二维数组的行
  this->forward = new Node<K, V> *[level + 1];

  memset(this->forward, 0, sizeof(Node<K, V> *) * (level + 1));
};

template <typename K, typename V>
Node<K, V>::~Node() {
  delete[] forward;
};

template <typename K, typename V>
K Node<K, V>::get_key() const {
  return key;
};

template <typename K, typename V>
V Node<K, V>::get_value() const {
  return value;
};
template <typename K, typename V>
void Node<K, V>::set_value(V value) {
  this->value = value;
};
// SkipList快照的作用
template <typename K, typename V>
class SkipListDump {
 public:
 //序列化的作用 
 //是boost::serialization::access可以直接去访问当前的类的私有成员变量和函数
  friend class boost::serialization::access;

  template <class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar &keyDumpVt_;//相当于 ar << keyDumpVt_。它会把 vector 里的所有数据依次倒进文件里
    ar &valDumpVt_;//相当于 ar >> keyDumpVt_。它会从文件里抓取数据，重新填满这个 vector。
  }
  std::vector<K> keyDumpVt_;
  std::vector<V> valDumpVt_;

 public:
  void insert(const Node<K, V> &node);
};
// Class template for Skip list
template <typename K, typename V>
class SkipList {
 public:
  SkipList(int);
  ~SkipList();
// 获取随机层数：，决定新插入的节点能“长到几层高”
int get_random_level();
// 创建节点：在内存中申请空间，根据层数 new 出一个带有 forward 指针数组的新节点
Node<K, V> *create_node(K, V, int);
// 插入元素：跳表最核心的函数！找到合适位置，把新节点织入各层链表中
int insert_element(K, V);
// 打印列表：把跳表每一层的样子画在控制台上，方便调试查看结构
void display_list();
// 查找元素：从顶层开始“跳跃式”查找，若找到，将结果存入 value 并返回 true
bool search_element(K, V &value);
// 删除元素：找到对应 Key 的节点，把每一层指向它的指针都断开并重新连接，最后释放内存
void delete_element(K);
// 插入/设置元素：通常用于加载文件时，如果 Key 已存在则更新 Value，不存在则插入
void insert_set_element(K &, V &);
// 持久化存档：将内存中的跳表数据转换成字符串格式，准备写入硬盘文件
std::string dump_file();
// 数据加载：从硬盘读取字符串，重新解析并“重建”出一个一模一样的跳表
void load_file(const std::string &dumpStr);
// 递归删除节点：清空整个跳表，从头节点开始一个接一个地释放所有内存，防止泄露
void clear(Node<K, V> *);
// 返回大小：告诉用户当前跳表里一共存了多少条数据
int size();

private:
 // 字符串拆解：从读取到的每一行字符串（如 "age:25"）中，把 Key("age") 和 Value("25") 剥离出来
 void get_key_value_from_string(const std::string &str, std::string *key, std::string *value);
 
 // 格式检查：判断从文件读进来的这行字符串是否合法（比如是否有冒号分隔，是否为空）
 bool is_valid_string(const std::string &str);
 private:
  // Maximum level of the skip list  无论怎样都不可以超过这个高度
  int _max_level;

  // current level of skip list  树实际存在的最大的高度
  int _skip_list_level;

  // pointer to header node
  Node<K, V> *_header;

  // file operator
  std::ofstream _file_writer;
  std::ifstream _file_reader;

  // skiplist current element count
  int _element_count;

  std::mutex _mtx;  // mutex for critical section
};
// create new node
template <typename K, typename V>
Node<K, V> *SkipList<K, V>::create_node(const K k, const V v, int level) {
  Node<K, V> *newNode = new Node<K, V>(k, v, level);
  return newNode;
}

// Insert given key and value in skip list
// return 1 means element exists
// return 0 means insert successfully
/*
                           +------------+
                           |  insert 50 |
                           +------------+
level 4     +-->1+                                                      100
                 |
                 |                      insert +----+
level 3         1+-------->10+---------------> | 50 |          70       100
                                               |    |
                                               |    |
level 2         1          10         30       | 50 |          70       100
                                               |    |
                                               |    |
level 1         1    4     10         30       | 50 |          70       100
                                               |    |
                                               |    |
level 0         1    4   9 10         30   40  | 50 |  60      70       100
                                               +----+

*/
template <typename K, typename V>
int SkipList<K, V>::insert_element(const K key, const V value) {
  _mtx.lock();
  Node<K, V> *current = this->_header;

  // create update array and initialize it
  // update is array which put node that the node->forward[i] should be operated later
  Node<K, V> *update[_max_level + 1];
  memset(update, 0, sizeof(Node<K, V> *) * (_max_level + 1));

  //其实最终update[i]记录的都是要插入的节点最终位置的前一个位置  
  //如果是  40  50  60 要插入50最终就是update[i] = current=40
  for (int i = _skip_list_level; i >= 0; i--) {
    while (current->forward[i] != NULL && current->forward[i]->get_key() < key) {
      current = current->forward[i];
    }
    update[i] = current;
  }

  current = current->forward[0];

  // key已经存在就直接不操作
  if (current != NULL && current->get_key() == key) {
    std::cout << "key: " << key << ", exists" << std::endl;
    _mtx.unlock();
    return 1;
  }

  // if current is NULL that means we have reached to end of the level
  // if current's key is not equal to key that means we have to insert node between update[0] and current node
  if (current == NULL || current->get_key() != key) {
    // Generate a random level for node
    int random_level = get_random_level();

    // If random level is greater thar skip list's current level, initialize update value with pointer to header
    if (random_level > _skip_list_level) {
      for (int i = _skip_list_level + 1; i < =random_level; i++) {
        update[i] = _header;
      }
      _skip_list_level = random_level;
    }

    // create new node with random level generated
    Node<K, V> *inserted_node = create_node(key, value, random_level);

    // insert node
    for (int i = 0; i <= random_level; i++) {
      inserted_node->forward[i] = update[i]->forward[i];
      update[i]->forward[i] = inserted_node;
    }
    std::cout << "Successfully inserted key:" << key << ", value:" << value << std::endl;
    _element_count++;
  }
  _mtx.unlock();
  return 0;
}

// 展示跳表的结构
template <typename K, typename V>
void SkipList<K, V>::display_list() {
  std::cout << "\n*****Skip List*****"
            << "\n";
  for (int i = 0; i <= _skip_list_level; i++) {
    Node<K, V> *node = this->_header->forward[i];
    std::cout << "Level " << i << ": ";
    while (node != NULL) {
      std::cout << node->get_key() << ":" << node->get_value() << ";";
      node = node->forward[i];
    }
    std::cout << std::endl;
  }
}


template <typename K, typename V>
std::string SkipList<K, V>::dump_file() {
  // 【核心技巧】：只遍历第 0 层！
  // 因为第 0 层（最底层）包含了跳表里 100% 的节点。
  // 我们只需要保存数据本身，不需要保存上面那些用于加速的“空中连廊（索引）”。
  // 读取的时候，把数据拿出来重新插入（重新摇骰子建楼）就行了。
  //就只有一个header呀，header节点的第0层不就是最下面吗，没什么号疑惑的
  Node<K, V> *node = this->_header->forward[0];
  // 创建一个专门用来“打包”的辅助容器，它知道如何跟 Boost 序列化库打交道
  SkipListDump<K, V> dumper;
  while (node != nullptr) {
    dumper.insert(*node);
    node = node->forward[0];
  }
  // 创建一个字符串流，你可以把它理解为内存里的一个“空白文本文档”
  std::stringstream ss;
  // 创建一个文本输出的“脱水机”（archive），并且把它绑到刚才的空白文档 ss 上
  boost::archive::text_oarchive oa(ss); 
  // 启动脱水机！把我们装满节点的打包箱 dumper，压缩并按照一定格式转成纯文本，流进 ss 里
  oa << dumper; 
  // 把流对象 ss 转换成真正的 std::string 字符串并返回
  // 这样外层调用这个函数的代码，就能拿到这个字符串，直接一把写进 .txt 或者 .dump 文件了
  return ss.str();
}

// 将第0层存到文件的数据从文件中读取出来一个个插进去
template <typename K, typename V>
void SkipList<K, V>::load_file(const std::string &dumpStr) {
  if (dumpStr.empty()) {
    return;
  }
  SkipListDump<K, V> dumper;
  std::stringstream iss(dumpStr);
  boost::archive::text_iarchive ia(iss);
  ia >> dumper;
  for (int i = 0; i < dumper.keyDumpVt_.size(); ++i) {
    insert_element(dumper.keyDumpVt_[i], dumper.valDumpVt_[i]);
  }
}

// Get current SkipList size
template <typename K, typename V>
int SkipList<K, V>::size() {
  return _element_count;
}

template <typename K, typename V>
void SkipList<K, V>::get_key_value_from_string(const std::string &str, std::string *key, std::string *value) {
  if (!is_valid_string(str)) {
    return;
  }
  //static std::string delimiter = ":";
  *key = str.substr(0, str.find(delimiter));
  *value = str.substr(str.find(delimiter) + 1, str.length() - (str.find(delimiter) + 1));
}

template <typename K, typename V>
bool SkipList<K, V>::is_valid_string(const std::string &str) {
  if (str.empty()) {
    return false;
  }
  //类似于str.end()的效果
  if (str.find(delimiter) == std::string::npos) {
    return false;
  }
  return true;
}

// Delete element from skip list
template <typename K, typename V>
void SkipList<K, V>::delete_element(K key) {
  _mtx.lock();
  Node<K, V> *current = this->_header;
  Node<K, V> *update[_max_level + 1];
  memset(update, 0, sizeof(Node<K, V> *) * (_max_level + 1));

  // 主要就是最高层可能是  1    100，如果你直接50想去找，最高层，不行
  //甚至可能最下面才有50
  for (int i = _skip_list_level; i >= 0; i--) {
    while (current->forward[i] != NULL && current->forward[i]->get_key() < key) {
      current = current->forward[i];
    }
    update[i] = current;
  }

  current = current->forward[0];
  if (current != NULL && current->get_key() == key) {
    // start for lowest level and delete the current node of each level
    for (int i = 0; i <= _skip_list_level; i++) {
      // 因为当前节点的上一层可能不存在呀 比如说你要删除60  除了0层在，其他的都是不在的呀
      /*
                           +------------+
                           |  insert 50 |
                           +------------+
level 4     +-->1+                                                      100
                 |
                 |                      insert +----+
level 3         1+-------->10+---------------> | 50 |          70       100
                                               |    |
                                               |    |
level 2         1          10         30       | 50 |          70       100
                                               |    |
                                               |    |
level 1         1    4     10         30       | 50 |          70       100
                                               |    |
                                               |    |
level 0         1    4   9 10         30   40  | 50 |  60      70       100
                                               +----+

*/
      if (update[i]->forward[i] != current) break;
      //不用先delete，就比如50 我们还要上面好几个都要进行释放，最后一次性一起delete
      update[i]->forward[i] = current->forward[i];
    }

    // 删除掉这个元素之后最高层就没有了
    while (_skip_list_level > 0 && _header->forward[_skip_list_level] == 0) {
      _skip_list_level--;
    }

    std::cout << "Successfully deleted key " << key << std::endl;
    delete current;
    _element_count--;
  }
  _mtx.unlock();
  return;
}

/**
 * \brief 作用与insert_element相同类似，
 * insert_element是插入新元素，
 * insert_set_element是插入元素，如果元素存在则改变其值
 */
template <typename K, typename V>
void SkipList<K, V>::insert_set_element(K &key, V &value) {
  V oldValue;
  if (search_element(key, oldValue)) {
    delete_element(key);
  }
  insert_element(key, value);
}

// Search for element in skip list
/*
                           +------------+
                           |  select 60 |
                           +------------+
level 4     +-->1+                                                      100
                 |
                 |
level 3         1+-------->10+------------------>50+           70       100
                                                   |
                                                   |
level 2         1          10         30         50|           70       100
                                                   |
                                                   |
level 1         1    4     10         30         50|           70       100
                                                   |
                                                   |
level 0         1    4   9 10         30   40    50+-->60      70       100
*/
template <typename K, typename V>
bool SkipList<K, V>::search_element(K key, V &value) {
  std::cout << "search_element-----------------" << std::endl;
  Node<K, V> *current = _header;

  // start from highest level of skip list
  for (int i = _skip_list_level; i >= 0; i--) {
    while (current->forward[i] && current->forward[i]->get_key() < key) {
      current = current->forward[i];
    }
  }

  // reached level 0 and advance pointer to right node, which we search
  current = current->forward[0];

  // if current node have key equal to searched key, we get it
  if (current and current->get_key() == key) {
    value = current->get_value();
    std::cout << "Found key: " << key << ", value: " << current->get_value() << std::endl;
    return true;
  }

  std::cout << "Not Found Key:" << key << std::endl;
  return false;
}

template <typename K, typename V>
void SkipListDump<K, V>::insert(const Node<K, V> &node) {
  keyDumpVt_.emplace_back(node.get_key());
  valDumpVt_.emplace_back(node.get_value());
}

// construct skip list
template <typename K, typename V>
SkipList<K, V>::SkipList(int max_level) {
  this->_max_level = max_level;
  this->_skip_list_level = 0;
  this->_element_count = 0;

  // create header node and initialize key and value to null
  K k;
  V v;
  this->_header = new Node<K, V>(k, v, _max_level);
};

template <typename K, typename V>
SkipList<K, V>::~SkipList() {
  if (_file_writer.is_open()) {
    _file_writer.close();
  }
  if (_file_reader.is_open()) {
    _file_reader.close();
  }

  //递归删除跳表链条
  if (_header->forward[0] != nullptr) {
    clear(_header->forward[0]);
  }
  delete (_header);
}
template <typename K, typename V>
void SkipList<K, V>::clear(Node<K, V> *cur) {
  if (cur->forward[0] != nullptr) {
    clear(cur->forward[0]);
  }
  delete (cur);
}

template <typename K, typename V>
int SkipList<K, V>::get_random_level() {
  int k = 1;
  while (rand() % 2) {
    k++;
  }
  k = (k < _max_level) ? k : _max_level;
  return k;
};
// vim: et tw=100 ts=4 sw=4 cc=120
#endif  // SKIPLIST_H
