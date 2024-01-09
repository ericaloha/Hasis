#include "bplus_tree.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <unordered_map>
#include <map>
#include <deque>

#include <liburing.h>
//#include <linux/io_uring.h>

// for linux aio only
#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <fcntl.h>
#include <aio.h>
#include <unistd.h>
#include <signal.h>
#include <set>
// for backgound thread
#include <mutex>
#include <pthread.h>
#include <condition_variable>
#include <thread>

// file lock
#include <sys/file.h>

// Schema  
    //  create table orders (
    //  o_id int not null,
    //  o_d_id tinyint not null,
    //  o_w_id smallint not null,
    //  o_c_id int, 16
    //  o_entry_d datetime, 20
    //  o_carrier_id tinyint, 16
    //  o_ol_cnt tinyint, 16
    //  o_all_local tinyint, 16
    //  PRIMARY KEY(o_w_id, o_d_id, o_id) ) Engine=InnoDB

    //INSERT INTO orders values(
    //  2999,
    //  10,
    //  2,
    //  2203,
    //  '2023-02-26 21:56:14',
    //  NULL,
    //  12,
    //   1
    //)


//hkc-modify-1
// global variable

// switch
const int async_mode_orders_ = 0; // 0 for sync IO; 1 for async IO
const int kAIOBatchSize_item = 20;
const int kOrder_orders_ = 700; //how many record a page can contain
const int kOrder_orders_Plog = kOrder_orders_ / 10;
// const int kOrder_orders_ = 50; //for key
const int kOrder_orders_Index = 2000; // for index

static_assert(kOrder_orders_ >= 3,
              "The order of B+Tree should be greater than or equal to 3.");


const int kMaxKeySize_orders_ = 48;
const int kMaxLSNSize_orders_ = 8;
// const int kMaxValueSize_orders_ = 256;
const int kMaxValueSize_orders_col1_ = 16;
const int kMaxValueSize_orders_col2_ = 20;
const int kMaxValueSize_orders_col3_ = 16;
const int kMaxValueSize_orders_col4_ = 16;
const int kMaxValueSize_orders_col5_ = 16; 
const int kMaxValueSize_orders_col_all = 16+20+16+16+16;
const off_t kMetaOffset_orders_ = 0;

//hkc-plog-1
const int KPosCarrier = 16+20;

int kMaxPages_orders_item = 1000;     // 100M
const int kMaxFreePages_orders_ = 1100; 
// const int kMaxPages_orders_item = 80000; //1G

typedef char Key[kMaxKeySize_orders_];
typedef char Value1[kMaxValueSize_orders_col1_];
typedef char Value2[kMaxValueSize_orders_col2_];
typedef char Value3[kMaxValueSize_orders_col3_];
typedef char Value4[kMaxValueSize_orders_col4_];
typedef char Value5[kMaxValueSize_orders_col5_];
typedef char Value_all[kMaxValueSize_orders_col_all]; //for log record

// newly defined
const int kUnitNodeSize = 144 * 1024;
const int kLeafPageSize = 128 * 1024;
const int kPlogPageSize = 16 * 1024;
const int kIndexPageSize = 128 * 1024;
const int maxPendGlogSegs =10;

const int innerKeyoffset_orders_ = 0;
const int innercoloffset_orders_ = 0;

const int kMaxUniqueKInGLogSeg=3000;
  


struct BPlusOrdersTree::Monitor{
  Monitor(){
    bg_id =0;
    plog_insert = 0;
    leaf_insert = 0;
    leaf_split = 0;
    hits_glog =0;
    hits_plog =0;
    hits_bp =0;
    disk_read =0;
    global_lsn = 1;
    fore_wait_all = 0;
    warmup_flag = 0;
    recover_flag = 0;

    applied =0;
    pends=-1;

    error_cnt =0;
    phy_plog_write = 0;
    phy_plog_read = 0;
    phy_page_read = 0;
    phy_page_write = 0;
    logi_page_read = 0;
    logi_page_write = 0;

    FLU_max_lsn = 0; // max oldest lsn in flu
    FLU_min_lsn = 0;

    io_consume =0;
    enable_uring_workers = true;
    curCacheSize = 0;
    curFLUSize = 0;
    cached_eles = 0;

      
    //hkc-ap-00
    //for AP scan 
    ap_wait = 0;
        exe_time=0;
    ap_left =0;
    ap_right =0;
    ap_lsn =0;

    lock_contention_file = 0;
    lock_contention_state = 0;
    flu_batch_flushes = 0;
    flu_sparse_flushes = 0;
    lru_batch_flushes = 0;
    lru_sparse_flushes = 0;
    normal_chpt_now = 0;
    add_new = 0;
    
    lazy_chpt_lsn_flag = 1;
    load_flag =1;

    cur_Glog_seg = -1;
    Seg_size_page = 50000; // max number of pages in each segment
    Cur_KVs_in_Seg[10]={0};
    Seg_size_KV = 100000; // max number of KVs in each segment
    apply_each_time = 1;
    should_build_new_seg = 0;
    MAX_Segs = 1;
    // WAL
    global_log_offset = 0;
    plog_flag = -1;             // 1: plog enabled; else plog disabled
    back_plog_thread_init = -1; // 1: init a bakground thread; else disable
    FLU_disabled = 1;

    // const int bitset_size = 1e8 + 1;
    new_write_flag = 0;
  
    bypass_plog = 0;

    pending_KVs_in_Glog = 0;

    normal_recovery_flag = 0;
    normal_skipped = 0;
    normal_replayed = 0;
    cur_lsn = 0;
    update_flu = 0;
    init_lru_ring=-1;

    global_fd = -1;
    global_fd_log = -1;
    global_fd_chpt = -1;

    //hkc-ap-01
    global_shadow_plog = -1;
    global_shadow_plog_off = 0;

    trverse_cnt = 0;
    pending_plog_cnts = 0;
    empty_leaf=0;
    dirtys=0;
    cleans=0;

  }

  //hkc-ap-02 
  bool ap_wait;
    off_t exe_time;
  off_t ap_lsn;
  off_t ap_left;
  off_t ap_right;

  off_t dirtys;
  off_t cleans;
  off_t global_lsn;
  off_t normal_chpt_now;
  uint64_t lock_contention_file;
  uint64_t lock_contention_state;
  off_t global_log_offset;
  off_t cur_lsn ;
  off_t FLU_max_lsn; // max oldest lsn in flu
  off_t FLU_min_lsn; // min oldest lsn in flu
  uint64_t fore_wait_all;
  off64_t io_consume;


  bool enable_uring_workers ;
  
  int bg_id;
  int empty_leaf;
  int lazy_chpt_lsn_flag;
  int load_flag;
  int curCacheSize;
  int curFLUSize;
  int cached_eles;
  int flu_batch_flushes;
  int flu_sparse_flushes;
  int lru_batch_flushes;
  int lru_sparse_flushes;
  int trverse_cnt;
  int cur_Glog_seg ;
  int Seg_size_page; // max number of pages in each segment
  int Cur_KVs_in_Seg[10];
  int Seg_size_KV ; // max number of KVs in each segment
  int apply_each_time ;
  int MAX_Segs;
  int should_build_new_seg;
  int add_new;
  int plog_insert;
  int leaf_insert;
  int leaf_split;
  int hits_glog;
  int hits_plog;
  int hits_bp;
  int disk_read;
  int warmup_flag;
  int recover_flag;
  int check_lsn_flag;
  int applied;
  int pends;
  int error_cnt;
  int phy_plog_write;
  int phy_plog_read;
  int phy_page_read;
  int phy_page_write;
  int logi_page_read;
  int logi_page_write;
  // WAL
  int plog_flag ;             // 1: plog enabled; else plog disabled
  int back_plog_thread_init ; // 1: init a bakground thread; else disable
  int FLU_disabled ;
  int new_write_flag ;
  int bypass_plog;
  int pending_KVs_in_Glog ;
  int normal_recovery_flag;
  int normal_skipped ;
  int normal_replayed ;
  int update_flu ;
  int global_fd  ;
  int global_fd_log ;
  int global_fd_chpt ;
  int bg_fd;
  int pending_plog_cnts;
  int init_lru_ring;

  //hkc-ap-03
  int global_shadow_plog;
  off_t global_shadow_plog_off;

  //hkc-scan-02
  std::unordered_map<off_t, std::pair<off_t,off_t>> leaf_links;

  
  std::mutex mtx_mian2back;
  std::condition_variable produce_main2back, consume_main2back;
  // critical section
  std::deque<std::pair<int, std::map<off_t, std::pair<char*,char*>>*>> Main2Back; //in que
  std::deque<std::pair<int, std::map<off_t, std::pair<char*,char*>>*>> Back2Main; //out que

  //for glog IO
  //std::map<off_t, std::pair<std::string,std::string>> Q_GLog_seg;
  std::deque<std::map<off_t, std::pair<std::string,std::string>>*> Q_GLog; //glog seg 
  std::deque<std::unordered_map<std::string, off_t>*> GLog_Index; // key: lsn// for get operation
  std::deque<std::map<off_t, std::pair<char*,char*>>*> Plog_Write_Pool; //leaf_offset: plog ptr and leaf node ptr (if necessary)
  
  std::unordered_map<off_t,int> pendingWrites_In_Glog;
  std::deque<int> WaitForPersist; //in que
  
  std::deque<std::pair<char*, std::deque<std::pair<off_t, int>>*>> PeristGroups; //
  // this is the basic structure for virtual page!!!
  // leaf offset: <glog kv map>, plog ptr, leafnode ptr
  std::unordered_map<off_t, std::pair<std::map<std::string, std::pair<off_t, std::string>> *, std::pair<char *, char *>>> Virtual_Page_In_Cache;
  // const int bitset_size = 1e8 + 1;
  std::set<off_t> IndexOrNot;
  std::deque<std::unordered_map<off_t, std::map<std::string, std::pair<off_t, std::string>> *> *> GLog;
  std::unordered_map<off_t, int> Persisting; // offset: persisted(1)
  std::unordered_map<off_t, int> InFLUOrNot;
  // for background thread
  
  
  
  std::unordered_map<off_t, char *> WaitForDelete;
  std::unordered_map<off_t, char *> CleanPages;
  
  std::unordered_map<off_t, char *> PendingPlogs;

};

struct BPlusOrdersTree::Meta
{
  off_t offset;  // ofset of self
  off_t root;    // offset of root
  off_t block;   // offset of next new node
  size_t height; // height of B+Tree
  size_t size;   // key size
};

struct BPlusOrdersTree::Index
{
  Index() : offset(0) { std::memset(key, 0, sizeof(key)); }

  off_t offset;
  Key key;
  off_t cur_lsn;

  void UpdateIndex(off_t of, off_t lsn, const char *k)
  {
    offset = of;
    cur_lsn = lsn;
    strncpy(key, k, kMaxKeySize_orders_);
  }
  void UpdateKey(const char *k, off_t lsn)
  {
    cur_lsn = lsn;
    strncpy(key, k, kMaxKeySize_orders_);
  }
};

//hkc-modify-2
struct BPlusOrdersTree::Record
{
  Key key;
  Value1 value_1;
  Value2 value_2;
  Value3 value_3;
  Value4 value_4;
  Value5 value_5;
  off_t cur_lsn;
  //hkc-plog-*
  bool partial; 

  void UpdateKV(const char *k, const char *v1, const char *v2, const char *v3, const char *v4,const char *v5, off_t lsn)
  {
    cur_lsn = lsn;
    strncpy(key, k, kMaxKeySize_orders_);
    strncpy(value_1, v1, kMaxValueSize_orders_col1_);
    strncpy(value_2, v2, kMaxValueSize_orders_col2_);
    strncpy(value_3, v3, kMaxValueSize_orders_col3_);
    strncpy(value_4, v4, kMaxValueSize_orders_col4_);
    strncpy(value_5, v5, kMaxValueSize_orders_col5_);
  }

  //hkc-plog-2
  void UpdateDeltaKV(const char *k,const char *v_carrier, off_t lsn)
  {
    cur_lsn = lsn;
    partial = true;
    strncpy(key, k, kMaxKeySize_orders_);
    strncpy(value_3, v_carrier, kMaxValueSize_orders_col3_);
  }
  void UpdateKey(const char *k, off_t lsn)
  {
    cur_lsn = lsn;
    strncpy(key, k, kMaxKeySize_orders_);
  }
  void UpdateValue(const char *v1, const char *v2, const char *v3, const char *v4,const char *v5, off_t lsn)
  {
    cur_lsn = lsn;
    strncpy(value_1, v1, kMaxValueSize_orders_col1_);
    strncpy(value_2, v2, kMaxValueSize_orders_col2_);
    strncpy(value_3, v3, kMaxValueSize_orders_col3_);
    strncpy(value_4, v4, kMaxValueSize_orders_col4_);
    strncpy(value_5, v5, kMaxValueSize_orders_col5_);
  }

  //hkc-plog-3
  void UpdateValueDelta(const char *v_carrier,off_t lsn)
  {
    cur_lsn = lsn;
    strncpy(value_3, v_carrier, kMaxValueSize_orders_col3_);
  }
};


//hkc-modify-3
struct BPlusOrdersTree::PAXKeyRecord
{
  Key key;
  off_t cur_lsn;

  void UpdateKV(const char *k, off_t lsn)
  {
    cur_lsn = lsn;
    strncpy(key, k, kMaxKeySize_orders_);
  }
  void UpdateKey(const char *k, off_t lsn)
  {
    cur_lsn = lsn;
    strncpy(key, k, kMaxKeySize_orders_);
  }
};

//hkc-modify-4
struct BPlusOrdersTree::PAXColumn1Record
{
  Value1 value_1;

  void UpdateKV(const char *k, const char *v1, off_t lsn)
  {
    strncpy(value_1, v1, kMaxValueSize_orders_col1_);
  }

  void UpdateValue(const char *v1)
  {
    strncpy(value_1, v1, kMaxValueSize_orders_col1_);
  }
};

//hkc-modify-5
struct BPlusOrdersTree::PAXColumn2Record
{
  Value2 value_2;

  void UpdateKV(const char *k, const char *v2,  off_t lsn)
  {
    strncpy(value_2, v2, kMaxValueSize_orders_col2_);
  }
  void UpdateValue(const char *v2, off_t lsn)
  {
    strncpy(value_2, v2, kMaxValueSize_orders_col2_);
  }
};

//hkc-modify-6
struct BPlusOrdersTree::PAXColumn3Record
{
  Value3 value_3;

  void UpdateKV(const char *k, const char *v3, off_t lsn)
  {
    strncpy(value_3, v3, kMaxValueSize_orders_col3_);
  }

  void UpdateValue(const char *v3, off_t lsn)
  {
    strncpy(value_3, v3, kMaxValueSize_orders_col3_);
  }
};

//hkc-modify-7
struct BPlusOrdersTree::PAXColumn4Record
{
  Value4 value_4;

  void UpdateKV( const char *v4, off_t lsn)
  {
    strncpy(value_4, v4, kMaxValueSize_orders_col4_);
  }

  void UpdateValue(const char *v4, off_t lsn)
  {
    strncpy(value_4, v4, kMaxValueSize_orders_col4_);
  }
};

//hkc-modify-8
struct BPlusOrdersTree::PAXColumn5Record
{
  Value5 value_5;

  void UpdateKV( const char *v5, off_t lsn)
  {
    strncpy(value_5, v5, kMaxValueSize_orders_col5_);
  }

  void UpdateValue(const char *v5, off_t lsn)
  {
    strncpy(value_5, v5, kMaxValueSize_orders_col5_);
  }
};




struct BPlusOrdersTree::Per_page_Log
{
  Key key;
  Value_all value_all;
  off_t cur_lsn;

  void UpdateKV(const char *k, const char *v, off_t lsn)
  {
    cur_lsn = lsn;
    strncpy(key, k, kMaxKeySize_orders_);
    strncpy(value_all, v, kMaxValueSize_orders_col_all);
  }
  void UpdateKey(const char *k, off_t lsn)
  {
    cur_lsn = lsn;
    strncpy(key, k, kMaxKeySize_orders_);
  }
  void UpdateValue(const char *v, off_t lsn)
  {
    cur_lsn = lsn;
    strncpy(value_all, v, kMaxValueSize_orders_col_all);
  }
};

//===========================Log Record=======================================================

struct BPlusOrdersTree::LogRecord
{
  LogRecord() : page_offset(0), log_lsn(0), log_offset(0), next_log_offset(0), page_index(0)
  {
    memset(key, 0, kMaxKeySize_orders_);
    memset(value, 0, kMaxValueSize_orders_col_all);
  }
  off_t page_offset;     // the detanited page id
  off_t log_lsn;         // the current log lsn
  off_t log_offset;      // the offset of the log record within the log file
  off_t next_log_offset; // a pointer to the next log record
  off_t page_index;      // the index, of the KV, in the leaf node
  char key[kMaxKeySize_orders_];
  char value[kMaxValueSize_orders_col_all];

  void UpdateKV(const char *k, const char *v)
  {
    strncpy(key, k, kMaxKeySize_orders_);
    strncpy(value, v, kMaxValueSize_orders_col_all);
  }
  void UpdateKey(const char *k) { strncpy(key, k, kMaxKeySize_orders_); }
  void UpdateValue(const char *v) { strncpy(value, v, kMaxValueSize_orders_col_all); }
};

struct BPlusOrdersTree::Checkpoint
{
  Checkpoint() : safe_lsn(0), safe_offset_in_log_file(0)
  {
  }
  off_t safe_lsn;                // all lsn < safe_lsn can be skipped
  off_t safe_offset_in_log_file; // all offset < safe_offset_in_log_file can be skipped
};

//===========================================

struct BPlusOrdersTree::Node
{
 //Node() : parent(0), plog_offset(0), left(0), origin_lsn(0), oldest_lsn(0), newest_lsn(0), right(0), count(0), dirty(0) {}
 // Node(off_t parent_, off_t leaf_, off_t right_, size_t count_, off_t lsn)
 //     : parent(parent_), left(leaf_), right(right_), count(count_), origin_lsn(0), plog_offset(0), oldest_lsn(lsn), newest_lsn(lsn), dirty(0) {}
 // ~Node() = default;

   //hkc-ap-04
  Node() : parent(0), sha_plog_offset(0), left(0), origin_lsn(0), oldest_lsn(0), newest_lsn(0), right(0), count(0), dirty(0) {}
  Node(off_t parent_, off_t leaf_, off_t right_, size_t count_, off_t lsn)
      : parent(parent_), left(leaf_), right(right_), count(count_), origin_lsn(0), sha_plog_offset(0), oldest_lsn(lsn), newest_lsn(lsn), dirty(0) {}
  ~Node() = default;

  off_t offset; // offset of self
  off_t parent; // offset of parent
  off_t left;   // offset of left node(may be sibling)
  off_t right;  // offset of right node(may be sibling)
  size_t count; // count of keys
  off_t origin_lsn;
  off_t oldest_lsn;
  off_t newest_lsn;
  // for plog

  //hkc-ap-05
  off_t sha_plog_offset;


  int dirty;
};

struct BPlusOrdersTree::IndexNode : BPlusOrdersTree::Node
{
  IndexNode() = default;
  ~IndexNode() = default;

  const char *FirstKey() const
  {
    //assert(count > 0);
    return indexes[0].key;
  }

  const char *LastKey() const
  {
    //assert(count > 0);
    return indexes[count - 1].key;
  }

  const char *Key(int index) const
  {
    //assert(count > 0);
    //assert(index >= 0);
    //assert(index <= kOrder_orders_Index);
    return indexes[index].key;
  }

  void UpdateKey(int index, const char *k, off_t lsn)
  {
    //assert(index >= 0);
    //assert(index <= kOrder_orders_Index);
    indexes[index].UpdateKey(k, lsn);
  }

  void UpdateOffset(int index, off_t offset)
  {
    //assert(index >= 0);
    //assert(index <= kOrder_orders_Index);
    indexes[index].offset = offset;
  }

  off_t GetSmallestLSN()
  {
    off_t smallest = -1;
    for (int i = 0; i < count; i++)
    {
      if (indexes[i].cur_lsn < smallest || smallest == -1)
      {
        smallest = indexes[i].cur_lsn;
      }
    }
    return smallest;
  }

  off_t GetBiggestLSN()
  {
    off_t biggest = -1;
    for (int i = 0; i < count; i++)
    {
      if (indexes[i].cur_lsn > biggest || biggest == -1)
      {
        biggest = indexes[i].cur_lsn;
      }
    }
    return biggest;
  }

  void UpdateIndex(int index, const char *k, off_t offset, off_t lsn)
  {
    //assert(index >= 0);
    //assert(index <= kOrder_orders_Index);
    UpdateKey(index, k, lsn);
    UpdateOffset(index, offset);
  }

  void DeleteKeyAtIndex(int index)
  {
    //assert(index >= 0);
    //assert(index <= kOrder_orders_Index);
    std::memmove(&indexes[index], &indexes[index + 1],
                 sizeof(indexes[0]) * (count-- - index));
  }

  void InsertKeyAtIndex(int index, const char *k, off_t lsn)
  {
    //assert(index >= 0);
    //assert(index <= kOrder_orders_Index);
    std::memmove(&indexes[index + 1], &indexes[index],
                 sizeof(indexes[0]) * (++count - index));
    UpdateKey(index, k, lsn);
  }

  void InsertIndexAtIndex(int index, const char *k, off_t offset, off_t lsn)
  {
    //assert(index >= 0);
    //assert(index <= kOrder_orders_Index);
    std::memmove(&indexes[index + 1], &indexes[index],
                 sizeof(indexes[0]) * (++count - index));
    UpdateIndex(index, k, offset, lsn);
  }

  void MergeLeftSibling(IndexNode *sibling)
  {
    std::memmove(&indexes[sibling->count + 1], &indexes[0],
                 sizeof(indexes[0]) * (count + 1));
    std::memcpy(&indexes[0], &sibling->indexes[0],
                sizeof(indexes[0]) * (sibling->count + 1));
    count += (sibling->count + 1);
  }

  void MergeRightSibling(IndexNode *sibling)
  {
    std::memcpy(&indexes[count], &sibling->indexes[0],
                sizeof(indexes[0]) * (sibling->count + 1));
    count += sibling->count;
  }

  Index indexes[kOrder_orders_Index + 1];
};

//hkc-modify-9
struct BPlusOrdersTree::LeafNode : BPlusOrdersTree::Node
{
  LeafNode() = default;
  ~LeafNode() = default;

  const char *FirstKey() const
  {
    //assert(count > 0);
    return records[0].key;
  }

  const char *LastKey() const
  {
    //assert(count > 0);
    return records[count - 1].key;
  }

  const char *Key(int index) const
  {
    //assert(count > 0);
    //assert(index >= 0);
    return records[index].key;
  }

  const char *FirstValue() const
  {
    //assert(count > 0);
    return records[0].value_1;
  }

  const char *LastValue() const
  {
    //assert(count > 0);
    return records[count - 1].value_1;
  }

  const char *Value(int index) const
  {
    //assert(count > 0);
    return records[index].value_1;
  }

  
  char *ValueALL(int index) const
  {
    //assert(count > 0);
    char* all = (char*)malloc(sizeof(char)*kMaxValueSize_orders_col_all);
    int off =0;
    memcpy(&all[off],records[index].value_1,kMaxValueSize_orders_col1_);
    off+=kMaxValueSize_orders_col1_;
    memcpy(&all[off],records[index].value_2,kMaxValueSize_orders_col2_);
    off+=kMaxValueSize_orders_col2_;
    memcpy(&all[off],records[index].value_3,kMaxValueSize_orders_col3_);
    off+=kMaxValueSize_orders_col3_;
    memcpy(&all[off],records[index].value_4,kMaxValueSize_orders_col4_);
    off+=kMaxValueSize_orders_col4_;
    memcpy(&all[off],records[index].value_5,kMaxValueSize_orders_col5_);
    return all;
  }



  off_t GetSmallestLSN()
  {
    off_t smallest = -1;
    for (int i = 0; i < count; i++)
    {
      if (records[i].cur_lsn < smallest || smallest == -1)
      {
        smallest = records[i].cur_lsn;
      }
    }
    return smallest;
  }

  off_t GetBiggestLSN()
  {
    off_t biggest = -1;
    for (int i = 0; i < count; i++)
    {
      if (records[i].cur_lsn > biggest || biggest == -1)
      {
        biggest = records[i].cur_lsn;
      }
    }
    return biggest;
  }

  void UpdateValue(int index, const char *v1, const char *v2, const char *v3, const char *v4, const char *v5, off_t lsn)
  {
    //assert(index >= 0);
    records[index].UpdateValue(v1, v2, v3,v4,v5, lsn);
  }

  //hkc-plog-4
  void UpdateValueDelta(int index, const char *v_carrier, off_t lsn)
  {
    //assert(index >= 0);
    records[index].UpdateValueDelta(v_carrier, lsn);
  }
  void UpdateLSNsForAllRecords(off_t lsn)
  {
    for (int i = 0; i < count; i++)
    {
      records[i].cur_lsn = lsn;
    }
  }
  void UpdateKey(int index, const char *k, off_t lsn)
  {
    //assert(index >= 0);
    records[index].UpdateKey(k, lsn);
  }

  void UpdateKV(int index, const char *k, const char *v1,const char *v2,const char *v3,const char *v4, const char *v5,off_t lsn)
  {
    //assert(index >= 0);
    records[index].UpdateKV(k, v1,v2,v3,v4,v5, lsn);
  }

  //hkc-plog-5
  void UpdateDeltaKV(int index, const char *k, const char *v_carrier, off_t lsn)
  {
    //assert(index >= 0);
    records[index].UpdateDeltaKV(k, v_carrier, lsn);
  }

  void AppendKV(int index, const char *k, const char *v1,const char *v2,const char *v3,const char *v4,const char *v5, off_t lsn)
  {
    //assert(index >= 0);
    records[index].UpdateKV(k, v1,v2,v3,v4,v5, lsn);
  }

  void InsertKVAtIndex(int index, const char *k, const char *v1,const char *v2,const char *v3,const char *v4,const char *v5, off_t lsn)
  {
    //assert(index >= 0);
    //assert(index < kOrder_orders_);
    std::memmove(&records[index + 1], &records[index],
                 sizeof(records[0]) * (count++ - index));
    UpdateKV(index, k, v1,v2,v3,v4,v5, lsn);
  }

  //hkc-plog-6
  void InsertDeltaKVAtIndex(int index, const char *k, const char *v_carrier, off_t lsn)
  {
    //assert(index >= 0);
    //assert(index < kOrder_customer_);
    std::memmove(&records[index + 1], &records[index],
                 sizeof(records[0]) * (count++ - index));
    UpdateDeltaKV(index, k, v_carrier, lsn);
  }

  void DeleteKVAtIndex(int index)
  {
    //assert(index >= 0);
    //assert(index < kOrder_orders_);
    std::memmove(&records[index], &records[index + 1],
                 sizeof(records[0]) * (--count - index));
  }


  off_t offset_leaf;
  off_t offset_k;
  off_t offset_lsn;
  off_t offset_v1;
  off_t offset_v2;
  off_t offset_v3;
  off_t offset_v4;
  off_t offset_v5;
  BPlusOrdersTree::Record records[kOrder_orders_];
  // BPlusOrdersTree::Record Per_page_logs[kOrder_orders_/2];
  // off_t cur_pplog_index;
};



//hkc-modify-10
struct BPlusOrdersTree::Plog : BPlusOrdersTree::Node
{
  Plog() = default;
  ~Plog() = default;

  const char *FirstKey() const
  {
    //assert(count > 0);
    return records[0].key;
  }

  const char *LastKey() const
  {
    //assert(count > 0);
    return records[count - 1].key;
  }

  const char *Key(int index) const
  {
    //assert(count > 0);
    //assert(index >= 0);
    return records[index].key;
  }

  const char *FirstValue() const
  {
    //assert(count > 0);
    return records[0].value_1;
  }

  const char *LastValue() const
  {
    //assert(count > 0);
    return records[count - 1].value_1;
  }

  const char *Value(int index) const
  {
    //assert(count > 0);
    return records[index].value_1;
  }
  off_t GetSmallestLSN()
  {
    off_t smallest = -1;
    for (int i = 0; i < count; i++)
    {
      if (records[i].cur_lsn < smallest || smallest == -1)
      {
        smallest = records[i].cur_lsn;
      }
    }
    return smallest;
  }

  off_t GetBiggestLSN()
  {
    off_t biggest = -1;
    for (int i = 0; i < count; i++)
    {
      if (records[i].cur_lsn > biggest || biggest == -1)
      {
        biggest = records[i].cur_lsn;
      }
    }
    return biggest;
  }

  void UpdateValue(int index, const char *v1,const char *v2,const char *v3,const char *v4,const char *v5, off_t lsn)
  {
    //assert(index >= 0);
    records[index].UpdateValue(v1,v2,v3,v4,v5,lsn);
  }

  //hkc-plog-7
  void UpdateValueDelta(int index, const char *v_carrier, off_t lsn)
  {
    //assert(index >= 0);
    records[index].UpdateValueDelta(v_carrier,lsn);
  }

  void UpdateLSNsForAllRecords(off_t lsn)
  {
    for (int i = 0; i < count; i++)
    {
      records[i].cur_lsn = lsn;
    }
  }
  void UpdateKey(int index, const char *k, off_t lsn)
  {
    //assert(index >= 0);
    records[index].UpdateKey(k, lsn);
  }

  void UpdateKV(int index, const char *k, const char *v1,const char *v2,const char *v3,const char *v4,const char *v5, off_t lsn)
  {
    //assert(index >= 0);
    records[index].UpdateKV(k, v1,v2,v3,v4,v5, lsn);
  }

  //hkc-plog-7-1
  void UpdateKVDelta(int index, const char *k, const char *v_carrier, off_t lsn)
  {
    //assert(index >= 0);
    records[index].UpdateDeltaKV(k, v_carrier, lsn);
  }

  void AppendKV(int index, const char *k, const char *v1,const char *v2,const char *v3,const char *v4,const char *v5, off_t lsn)
  {
    //assert(index >= 0);
    records[index].UpdateKV(k, v1,v2,v3,v4, v5,lsn);
  }

  void InsertKVAtIndex(int index, const char *k, const char *v1,const char *v2,const char *v3,const char *v4,const char *v5, off_t lsn)
  {
    //assert(index >= 0);
    //assert(index < kOrder_orders_Plog);
    std::memmove(&records[index + 1], &records[index],
                 sizeof(records[0]) * (count++ - index));
    UpdateKV(index, k,v1,v2,v3,v4, v5, lsn);
  }

  //hkc-plog-6-2
  void InsertDeltaKVAtIndex(int index, const char *k, const char *v_carrier, off_t lsn)
  {
    //assert(index >= 0);
    //assert(index < kOrder_customer_Plog);
    std::memmove(&records[index + 1], &records[index],
                 sizeof(records[0]) * (count++ - index));
    UpdateKVDelta(index, k,v_carrier,lsn);
  }

  void DeleteKVAtIndex(int index)
  {
    //assert(index >= 0);
    //assert(index < kOrder_orders_Plog);
    std::memmove(&records[index], &records[index + 1],
                 sizeof(records[0]) * (--count - index));
  }



  // BPlusOrdersTree::Record records[kOrder_orders_/2];
  BPlusOrdersTree::Record records[kOrder_orders_Plog];
};

class BPlusOrdersTree::BlockCache
{
  struct Node;
  

public:
  BlockCache(bool enable_backthd)
  {
    // 0. init size
    size_flu = 0;
    size_free = 0;
    size_lru = 0;
    //async_uring = true;
    async_uring = enable_backthd;
    Queue_depth_cache =200;

    // 1. init 3 list
    head_free = new Node();
    head_lru = new Node();
    head_flu = new Node();
    head_lru->next_lru = head_lru;
    head_lru->prev_lru = head_lru;
    head_flu->next_flu = head_flu;
    head_flu->prev_flu = head_flu;
    head_free->next_free = head_free;
    head_free->prev_free = head_free;

    // pre_allocate
    // 2. init free list
    char *init_ptr = (char *)malloc(kMaxFreePages_orders_ * kLeafPageSize); // i.e., for each page
    memset(init_ptr, 0, kMaxFreePages_orders_ * kLeafPageSize);

    char *flag_addr = init_ptr;
    for (int i = 0; i < kMaxFreePages_orders_; i++)
    {
      Node *free_node = new Node(flag_addr + i * kLeafPageSize, 0, 0);
      InsertHead_FREE(free_node);
    }

    // 3. init io_uring
    //io_uring_queue_init(Queue_depth_cache, &ring_cache, 0);


    if(async_uring){
      async_uring = false;
      uringWorker = std::thread(&BlockCache::GlogWorker,this);
      uringWorker.detach();
      //enable_uring_workers = false;
      //async_mode_orders_

    }
  }

  ~BlockCache()
  {
    // 0. print size
    //--+printf("current LRU size: %d\n", size_lru);
    //--+printf("current FLU size: %d\n", size_flu);
    //--+printf("current FREE size: %d\n", size_free);

    // 1. de-init io_uring
    //io_uring_queue_exit(&ring_cache);

    // 1. persist all leaf node to file
    for (auto it = offset2node_leaf.begin(); it != offset2node_leaf.end(); it++)
    {
      Node *node = it->second;
      // off_t page_offset = node->offset & ~(sysconf(_SC_PAGE_SIZE) - 1);
      char *start = reinterpret_cast<char *>(node->block);
      // void* addr = static_cast<void*>(&start[page_offset - node->offset]);
      // lseek(global_fd, node->offset, 0);
      // write(global_fd,reinterpret_cast<char*>(node->block),kLeafPageSize);
      // if (munmap(addr, node->size + node->offset - page_offset) != 0) {
      //   Exit("munmap");
      // }
      delete node;
    }
    
    close(mtr_cache->global_fd);
    close(mtr_cache->global_fd_log);
    close(mtr_cache->global_fd_chpt);
    Node *tmp_free = head_free->next_free;
    while (tmp_free)
    {
      char *start = reinterpret_cast<char *>(tmp_free->block);
      delete start;
      Node *delt = tmp_free;
      tmp_free = tmp_free->next_free;
      if (tmp_free == head_free)
      {
        break;
      }
      DeleteNode_Free(delt);
      delete delt;
    }

    // 2. delete all head nodes
    delete head_flu;
    delete head_free;
    delete head_lru;
  }

//hkc-modify-11
void RowToColumn_Cache(char* initial_page, char* converted_page){
  
  LeafNode* row_leaf = reinterpret_cast<LeafNode*>(initial_page);
  if(row_leaf->count == 0 || row_leaf->records[0].cur_lsn == 0){
    printf("empty row page!!");
    memcpy(converted_page,initial_page,kLeafPageSize);
    return;
  }
  /*
const int kMaxKeySize_orders_ = 48; 36K
const int kMaxLSNSize_orders_ = 8;  8K
const int kMaxValueSize_orders_col1_ = 16;  16K  
const int kMaxValueSize_orders_col2_ = 20; 16K
const int kMaxValueSize_orders_col3_ = 16;  16K
const int kMaxValueSize_orders_col4_ = 16;  16K
const int kMaxValueSize_orders_col5_ = 16;  16K
const int kMaxValueSize_orders_col_all = 16+20+16+16+16;
  */
  row_leaf->offset_k = 4*1024;
  row_leaf->offset_lsn =(4+48)*1024;
  row_leaf->offset_v1 = (4+48+8)*1024;
  row_leaf->offset_v2 = (4+48+8+12)*1024;
  row_leaf->offset_v3 = (4+48+8+12+16)*1024;
  row_leaf->offset_v4 = (4+48+8+12+16+12)*1024;
  row_leaf->offset_v5 = (4+48+8+12+16+12+12)*1024;
  memset(converted_page,0,kLeafPageSize);
  char* buf = converted_page;
  off_t fence = 0;
  off_t unit_size = 4*1024;

  //copy meta data
  memcpy(&buf[fence],&(row_leaf->offset), sizeof(row_leaf->offset));
  //check 
  //off_t test_of = 0;
  //memcpy((char*)&test_of,&buf[0],8);


  fence+=sizeof(row_leaf->offset);
  memcpy(&buf[fence],&(row_leaf->offset_leaf), sizeof(row_leaf->offset_leaf));
  fence+=sizeof(row_leaf->offset_leaf);
  memcpy(&buf[fence],&(row_leaf->offset_k), sizeof(row_leaf->offset_k));
  fence+=sizeof(row_leaf->offset_k);
  memcpy(&buf[fence],&(row_leaf->offset_lsn), sizeof(row_leaf->offset_lsn));
  fence+=sizeof(row_leaf->offset_lsn);
  memcpy(&buf[fence],&(row_leaf->offset_v1), sizeof(row_leaf->offset_v1));
  fence+=sizeof(row_leaf->offset_v1);
  memcpy(&buf[fence],&(row_leaf->offset_v2), sizeof(row_leaf->offset_v2));
  fence+=sizeof(row_leaf->offset_v2);
  memcpy(&buf[fence],&(row_leaf->offset_v3), sizeof(row_leaf->offset_v3));
  fence+=sizeof(row_leaf->offset_v3);
  memcpy(&buf[fence],&(row_leaf->offset_v4), sizeof(row_leaf->offset_v4));
  fence+=sizeof(row_leaf->offset_v4);
  memcpy(&buf[fence],&(row_leaf->offset_v5), sizeof(row_leaf->offset_v5));
  fence+=sizeof(row_leaf->offset_v5);

  //hkc-ap-06
  memcpy(&buf[fence],&(row_leaf->sha_plog_offset), sizeof(row_leaf->sha_plog_offset));
  fence+=sizeof(row_leaf->sha_plog_offset);
  

  memcpy(&buf[fence],&(row_leaf->count), sizeof(row_leaf->count));
  fence+=sizeof(row_leaf->count);

  memcpy(&buf[fence],&(row_leaf->dirty), sizeof(row_leaf->dirty));
  fence+=sizeof(row_leaf->dirty);

  memcpy(&buf[fence],&(row_leaf->left), sizeof(row_leaf->left));
  fence+=sizeof(row_leaf->left);

  memcpy(&buf[fence],&(row_leaf->right), sizeof(row_leaf->right));
  fence+=sizeof(row_leaf->right);

  memcpy(&buf[fence],&(row_leaf->parent), sizeof(row_leaf->parent));
  fence+=sizeof(row_leaf->parent);

  memcpy(&buf[fence],&(row_leaf->newest_lsn), sizeof(row_leaf->newest_lsn));
  fence+=sizeof(row_leaf->newest_lsn);

  memcpy(&buf[fence],&(row_leaf->oldest_lsn), sizeof(row_leaf->oldest_lsn));
  fence+=sizeof(row_leaf->oldest_lsn);
  
  memcpy(&buf[fence],&(row_leaf->origin_lsn), sizeof(row_leaf->origin_lsn));
  fence+=sizeof(row_leaf->origin_lsn);
  
  
  if(fence > 4*1024){
    printf("overflow!\n");
  } 
  //convert row as column and fit in sub pages

    /*
      const int kMaxKeySize_orders_ = 16; 16K
      const int kMaxValueSize_orders_col1_ = 16; 16K
      const int kMaxValueSize_orders_col2_ = 24; 24K
      const int kMaxValueSize_orders_col3_ = 8; 8K
      const int kMaxValueSize_orders_col4_ = 50; 50K
    */

  off_t k_of  = row_leaf->offset_k;
  off_t lsn_of = row_leaf->offset_lsn;
  off_t v1_of = row_leaf->offset_v1;
  off_t v2_of = row_leaf->offset_v2;
  off_t v3_of = row_leaf->offset_v3;
  off_t v4_of = row_leaf->offset_v4;
  off_t v5_of = row_leaf->offset_v5;
  for(int i=0; i<row_leaf->count;i++){
    memcpy(&buf[k_of],&(row_leaf->records[i].key),kMaxKeySize_orders_);
    k_of+=kMaxKeySize_orders_;
    memcpy(&buf[lsn_of],&(row_leaf->records[i].cur_lsn),sizeof(row_leaf->records[i].cur_lsn));
    lsn_of+=sizeof(row_leaf->records[i].cur_lsn);
    memcpy(&buf[v1_of],&(row_leaf->records[i].value_1),kMaxValueSize_orders_col1_);
    v1_of+=kMaxValueSize_orders_col1_;
    memcpy(&buf[v2_of],&(row_leaf->records[i].value_2),kMaxValueSize_orders_col2_);
    v2_of+=kMaxValueSize_orders_col2_;
    memcpy(&buf[v3_of],&(row_leaf->records[i].value_3),kMaxValueSize_orders_col3_);
    v3_of+=kMaxValueSize_orders_col3_;
    memcpy(&buf[v4_of],&(row_leaf->records[i].value_4),kMaxValueSize_orders_col4_);
    v4_of+=kMaxValueSize_orders_col4_;
    memcpy(&buf[v5_of],&(row_leaf->records[i].value_5),kMaxValueSize_orders_col5_);
    v5_of+=kMaxValueSize_orders_col5_;
  }


  return; 
}


//hkc-modify-12
void ColumnToRow_Cache(char* column_page, char* row_page){
  memset(row_page,0,kLeafPageSize);
  LeafNode* row_leaf = reinterpret_cast<LeafNode*>(row_page);
  row_leaf->offset_k = 4*1024;
  row_leaf->offset_lsn =(4+48)*1024;
  row_leaf->offset_v1 = (4+48+8)*1024;
  row_leaf->offset_v2 = (4+48+8+12)*1024;
  row_leaf->offset_v3 = (4+48+8+12+16)*1024;
  row_leaf->offset_v4 = (4+48+8+12+16+12)*1024;
  row_leaf->offset_v5 = (4+48+8+12+16+12+12)*1024;
  
  char* buf = column_page;
  off_t fence = 0;

  //restore meta data
  //memcpy(&buf[fence],&(row_leaf->offset), sizeof(row_leaf->offset));
  //row_leaf->offset = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->offset,&buf[fence],sizeof(row_leaf->offset));
  fence+=sizeof(row_leaf->offset);
  //memcpy(&buf[fence],&(row_leaf->offset_leaf), sizeof(row_leaf->offset_leaf));
  //row_leaf->offset_leaf = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->offset_leaf,&buf[fence],sizeof(row_leaf->offset_leaf));
  fence+=sizeof(row_leaf->offset_leaf);
  //memcpy(&buf[fence],&(row_leaf->offset_k), sizeof(row_leaf->offset_k));
  //row_leaf->offset_k = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->offset_k,&buf[fence],sizeof(row_leaf->offset_k));
  fence+=sizeof(row_leaf->offset_k);
  //memcpy(&buf[fence],&(row_leaf->offset_lsn), sizeof(row_leaf->offset_lsn));
  //row_leaf->offset_lsn = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->offset_lsn,&buf[fence],sizeof(row_leaf->offset_lsn));
  fence+=sizeof(row_leaf->offset_lsn);
  //memcpy(&buf[fence],&(row_leaf->offset_v1), sizeof(row_leaf->offset_v1));
  //row_leaf->offset_v1 = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->offset_v1,&buf[fence],sizeof(row_leaf->offset_v1));
  fence+=sizeof(row_leaf->offset_v1);
  //memcpy(&buf[fence],&(row_leaf->offset_v2), sizeof(row_leaf->offset_v2));
  //row_leaf->offset_v2 = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->offset_v2,&buf[fence],sizeof(row_leaf->offset_v2));
  fence+=sizeof(row_leaf->offset_v2);
  //memcpy(&buf[fence],&(row_leaf->offset_v3), sizeof(row_leaf->offset_v3));
  //row_leaf->offset_v3 = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->offset_v3,&buf[fence],sizeof(row_leaf->offset_v3));
  fence+=sizeof(row_leaf->offset_v3);
  //memcpy(&buf[fence],&(row_leaf->offset_v4), sizeof(row_leaf->offset_v4));
  //row_leaf->offset_v4 = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->offset_v4,&buf[fence],sizeof(row_leaf->offset_v4));
  fence+=sizeof(row_leaf->offset_v4);
  
  memcpy((char*)&row_leaf->offset_v5,&buf[fence],sizeof(row_leaf->offset_v5));
  fence+=sizeof(row_leaf->offset_v5);
  //memcpy(&buf[fence],&(row_leaf->plog_offset), sizeof(row_leaf->plog_offset));
  //row_leaf->plog_offset = reinterpret_cast<off_t>(&buf[fence]);

  //hkc-ap-07
  memcpy((char*)&row_leaf->sha_plog_offset,&buf[fence],sizeof(row_leaf->sha_plog_offset));
  fence+=sizeof(row_leaf->sha_plog_offset);
  

  //memcpy(&buf[fence],&(row_leaf->count), sizeof(row_leaf->count));
  //row_leaf->count = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->count,&buf[fence],sizeof(row_leaf->count));
  fence+=sizeof(row_leaf->count);

  //memcpy(&buf[fence],&(row_leaf->dirty), sizeof(row_leaf->dirty));
  //row_leaf->dirty = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->dirty,&buf[fence],sizeof(row_leaf->dirty));
  fence+=sizeof(row_leaf->dirty);

  //memcpy(&buf[fence],&(row_leaf->left), sizeof(row_leaf->left));
  //row_leaf->left = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->left,&buf[fence],sizeof(row_leaf->left));
  fence+=sizeof(row_leaf->left);

  //memcpy(&buf[fence],&(row_leaf->right), sizeof(row_leaf->right));
  row_leaf->right = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->right,&buf[fence],sizeof(row_leaf->right));
  fence+=sizeof(row_leaf->right);

  //memcpy(&buf[fence],&(row_leaf->parent), sizeof(row_leaf->parent));
  //row_leaf->parent = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->parent,&buf[fence],sizeof(row_leaf->parent));
  fence+=sizeof(row_leaf->parent);

  //memcpy(&buf[fence],&(row_leaf->newest_lsn), sizeof(row_leaf->newest_lsn));
  //row_leaf->newest_lsn = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->newest_lsn,&buf[fence],sizeof(row_leaf->newest_lsn));
  fence+=sizeof(row_leaf->newest_lsn);

  //memcpy(&buf[fence],&(row_leaf->oldest_lsn), sizeof(row_leaf->oldest_lsn));
  //row_leaf->oldest_lsn = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->oldest_lsn,&buf[fence],sizeof(row_leaf->oldest_lsn));
  fence+=sizeof(row_leaf->oldest_lsn);
  
  //memcpy(&buf[fence],&(row_leaf->origin_lsn), sizeof(row_leaf->origin_lsn));
  //row_leaf->origin_lsn = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->origin_lsn,&buf[fence],sizeof(row_leaf->origin_lsn));
  fence+=sizeof(row_leaf->origin_lsn);
  
  
  if(fence > 4*1024){
    printf("overflow!\n");
  } 
  //convert row as column and fit in sub pages

    /*
      const int kMaxKeySize_orders_ = 16; 16K
      const int kMaxValueSize_orders_col1_ = 16; 16K
      const int kMaxValueSize_orders_col2_ = 24; 24K
      const int kMaxValueSize_orders_col3_ = 8; 8K
      const int kMaxValueSize_orders_col4_ = 50; 50K
    */

  off_t k_of  = row_leaf->offset_k;
  off_t lsn_of = row_leaf->offset_lsn;
  off_t v1_of = row_leaf->offset_v1;
  off_t v2_of = row_leaf->offset_v2;
  off_t v3_of = row_leaf->offset_v3;
  off_t v4_of = row_leaf->offset_v4;
  off_t v5_of = row_leaf->offset_v5;
  for(int i=0; i<row_leaf->count;i++){
    memcpy(&(row_leaf->records[i].key),&buf[k_of],kMaxKeySize_orders_);
    k_of+=kMaxKeySize_orders_;
    memcpy(&(row_leaf->records[i].cur_lsn),&buf[lsn_of],sizeof(row_leaf->records[i].cur_lsn));
    lsn_of+=sizeof(row_leaf->records[i].cur_lsn);
    memcpy(&(row_leaf->records[i].value_1),&buf[v1_of],kMaxValueSize_orders_col1_);
    v1_of+=kMaxValueSize_orders_col1_;
    memcpy(&(row_leaf->records[i].value_2),&buf[v2_of],kMaxValueSize_orders_col2_);
    v2_of+=kMaxValueSize_orders_col2_;
    memcpy(&(row_leaf->records[i].value_3),&buf[v3_of],kMaxValueSize_orders_col3_);
    v3_of+=kMaxValueSize_orders_col3_;
    memcpy(&(row_leaf->records[i].value_4),&buf[v4_of],kMaxValueSize_orders_col4_);
    v4_of+=kMaxValueSize_orders_col4_;
    memcpy(&(row_leaf->records[i].value_5),&buf[v5_of],kMaxValueSize_orders_col5_);
    v5_of+=kMaxValueSize_orders_col5_;
  }


  return; 
}





  // leaf node
  template <typename T>
  LeafNode *PushToCache(off_t offset)
  {

    Node *cache_node = DeleteTail_Free();
    cache_node->offset = offset;
    //here to define the page format for each table schema!!!!!

    LeafNode *leaf = reinterpret_cast<LeafNode *>(cache_node->block);
    leaf->offset = offset;
    // Node* cache_node = new Node(block, offset, kPageSize);
    offset2node_leaf.emplace(offset, cache_node);

    mtr_cache->cached_eles = offset2node_leaf.size();
    cache_node->retrieving = 1;
    InsertHead_LRU(cache_node);
    cache_node->retrieving = 0;
    return leaf;
  }

  void DeletePersistedNode(Node *node)
  {
    if (node->next_lru == node->prev_lru && nullptr == node->next_lru)
      return;
    node->prev_lru->next_lru = node->next_lru;
    node->next_lru->prev_lru = node->prev_lru;
    node->next_lru = node->prev_lru = nullptr;
    size_lru -= 1;
    mtr_cache->curCacheSize -= 1;
  }

  void DeletePersistedFLUNode(Node *node)
  {
    // if(node->persisted!=0){
    //   //--+printf("error44!");
    // }
    if (node->next_flu == node->prev_flu && nullptr == node->next_flu)
      return;
    node->prev_flu->next_flu = node->next_flu;
    node->next_flu->prev_flu = node->prev_flu;
    node->next_flu = node->prev_flu = nullptr;
    size_flu -= 1;
    mtr_cache->curFLUSize -= 1;
  }

  void DeleteNode_LRU(Node *node)
  {
    if (node->persisted == 1 || node->persisted == 2)
    {
      //--+printf("error in delete lru node!\n");
    }
    if (node->next_lru == node->prev_lru && nullptr == node->next_lru)
      return;
    node->prev_lru->next_lru = node->next_lru;
    node->next_lru->prev_lru = node->prev_lru;
    node->next_lru = node->prev_lru = nullptr;
    size_lru -= 1;
    mtr_cache->curCacheSize -= 1;
  }

  void DeleteNode_FLU(Node *node)
  {
    if (node->persisted == 1 || node->persisted == 2)
    {
      //--+printf("error in delete flu node!\n");
    }
    if(node->offset == 20480){
      int x=0;
    }
    if (node->next_flu == node->prev_flu && nullptr == node->next_flu)
      return;
    node->prev_flu->next_flu = node->next_flu;
    node->next_flu->prev_flu = node->prev_flu;
    node->next_flu = node->prev_flu = nullptr;
    size_flu -= 1;
    mtr_cache->curFLUSize -= 1;
  }

  void DeleteNode_Free(Node *node)
  {
    if (node->persisted == 1 || node->persisted == 2)
    {
      //--+printf("error in delete free node!\n");
    }
    if (node->next_free == node->prev_free && nullptr == node->next_free)
      return;
    node->prev_free->next_free = node->next_free;
    node->next_free->prev_free = node->prev_free;
    node->next_free = node->prev_free = nullptr;
    size_free -= 1;
  }

  void InsertHead_LRU(Node *node)
  {
    if (node->persisted == 1 || node->persisted == 2)
    {
      //--+printf("error insert LRU list!");
    }
    node->next_lru = head_lru->next_lru;
    node->prev_lru = head_lru;
    head_lru->next_lru->prev_lru = node;
    head_lru->next_lru = node;
    size_lru += 1;
    mtr_cache->curCacheSize += 1;
  }

  void InsertHead_FLU(Node *node)
  {
    if (node->persisted == 1 || node->persisted == 2)
    {
      //--+printf("error insert FLU list!");
    }
    if(node->offset == 20480){
      int x=0;
    }
    node->next_flu = head_flu->next_flu;
    node->prev_flu = head_flu;
    head_flu->next_flu->prev_flu = node;
    head_flu->next_flu = node;
    size_flu += 1;
    mtr_cache->curFLUSize += 1;
  }
  void InPlaceInsertToFLU(off_t offset_leaf, off_t offset_split)
  {
    mtr_cache->InFLUOrNot.emplace(offset_split, 1);
    if(offset_leaf == 20480 || offset_split ==20480){
      int x=0;
    }
    if (mtr_cache->InFLUOrNot.find(offset_leaf) == mtr_cache->InFLUOrNot.end())
    {
      //--+printf("error_1 ! FLU re- direct insert");
    }

    Node *node_leaf = nullptr;
    if (offset2node_leaf.find(offset_leaf) == offset2node_leaf.end())
    {
      //--+printf("error_2-1 ! FLU reinsert");
    }
    else
    {
      node_leaf = offset2node_leaf[offset_leaf];
    }
    Node *node_split = nullptr;
    if (offset2node_leaf.find(offset_split) == offset2node_leaf.end())
    {
      //--+printf("error_2-2 ! FLU re direct insert");
    }
    else
    {
      node_split = offset2node_leaf[offset_split];
    }
    // insert the split node directly after the laef node
    node_split->next_flu = node_leaf->next_flu;
    node_split->prev_flu = node_leaf;
    node_leaf->next_flu->prev_flu = node_split;
    node_leaf->next_flu = node_split;
    size_flu += 1;
    mtr_cache->curFLUSize += 1;
    return;
  }

  void ReinsertToFLU(off_t offset, off_t ori_oldset_lsn)
  {
    if (mtr_cache->InFLUOrNot.find(offset) == mtr_cache->InFLUOrNot.end())
    {
      //--+printf("error_1 ! FLU reinsert");
    }

    Node *node = nullptr;
    if (offset2node_leaf.find(offset) == offset2node_leaf.end())
    {
      //--+printf("error_2 ! FLU reinsert");
    }
    else
    {
      node = offset2node_leaf[offset];
    }
    LeafNode *leaf = reinterpret_cast<LeafNode *>(node->block);
    int cur_lsn = leaf->oldest_lsn;
    if (ori_oldset_lsn < cur_lsn)
    {
      // from cur to left, i.e., move with prev ptr
      // 1. prepare the iterator on the left of the cur node
      Node *it = node->prev_flu;
      // 2. delete the cur node from FLU List
      DeleteNode_FLU(node);
      // 3. traverse the prev flu start from the it
      LeafNode *it_leaf = reinterpret_cast<LeafNode *>(it->block);
      while (it_leaf->oldest_lsn < leaf->oldest_lsn)
      {
        it = it->prev_flu;
        it_leaf = reinterpret_cast<LeafNode *>(it->block);
        if (it == head_flu)
        {
          InsertHead_FLU(node);
          return;
        }
      }
      if (it == head_flu)
      {
        InsertHead_FLU(node);
        return;
      }
      else
      {
        // insert the current node to the right of the it
        node->next_flu = it->next_flu;
        node->prev_flu = it;
        it->next_flu->prev_flu = node;
        it->next_flu = node;
        size_flu += 1;
        mtr_cache->curFLUSize += 1;
        return;
      }
    }
    else
    {
      // from cur to right, i.e., move with next ptr
      // 1. prepare the iterator on the left of the cur node
      Node *it = node->next_flu;
      // 2. delete the cur node from FLU List
      DeleteNode_FLU(node);
      // 3. traverse the next flu start from the it
      LeafNode *it_leaf = reinterpret_cast<LeafNode *>(it->block);
      while (it_leaf->oldest_lsn > leaf->oldest_lsn)
      {
        it = it->next_flu;
        if (it == head_flu)
        {
          // i.e., cur_lsn is the global smallest, we need to insert it at the tail of the FLU
          it = it->prev_flu;
          node->next_flu = it->next_flu;
          node->prev_flu = it;
          it->next_flu->prev_flu = node;
          it->next_flu = node;
          size_flu += 1;
          mtr_cache->curFLUSize += 1;
          return;
        }
        it_leaf = reinterpret_cast<LeafNode *>(it->block);
      }
      it = it->prev_flu;
      // insert the current node to the left of the it
      node->next_flu = it->next_flu;
      node->prev_flu = it;
      it->next_flu->prev_flu = node;
      it->next_flu = node;
      size_flu += 1;
      mtr_cache->curFLUSize += 1;
      return;
    }
  }

 

  void InsertIntoSortedFLU(off_t offset)
  {
    if(offset == 20480){
      int x=0;
    }
    mtr_cache->InFLUOrNot.emplace(offset, 1);
    Node *node = nullptr;
    if (offset2node_leaf.find(offset) == offset2node_leaf.end())
    {
      //--+printf("error!");
    }
    else
    {
      node = offset2node_leaf[offset];
    }

    LeafNode *leaf = reinterpret_cast<LeafNode *>(node->block);
    int cur_lsn = leaf->oldest_lsn;
    if (head_flu->next_flu == head_flu)
    {
      InsertHead_FLU(node);
      return;
    }
    else
    {
      Node *it = head_flu->next_flu;

      // Node* it=head_flu->prev_flu;
      LeafNode *it_leaf = reinterpret_cast<LeafNode *>(it->block);
      while (cur_lsn < it_leaf->oldest_lsn)
      {
        // while(it_leaf->oldest_lsn < cur_lsn){
        it = it->next_flu;
        mtr_cache->trverse_cnt++;
        if (it == head_flu)
        {
          // i.e., cur_lsn is the global smallest, we need to insert it at the tail of the FLU
          it = it->prev_flu;
          node->next_flu = it->next_flu;
          node->prev_flu = it;
          it->next_flu->prev_flu = node;
          it->next_flu = node;
          size_flu += 1;
          mtr_cache->curFLUSize += 1;
          return;
        }
        it_leaf = reinterpret_cast<LeafNode *>(it->block);
      }

      it = it->prev_flu;

      // insert the current node to the left of the it
      node->next_flu = it->next_flu;
      node->prev_flu = it;
      it->next_flu->prev_flu = node;
      it->next_flu = node;
      size_flu += 1;
      mtr_cache->curFLUSize += 1;
      return;
    }
  }

  void InsertHead_FREE(Node *node)
  {
    if (node->persisted == 1 || node->persisted == 2)
    {
      //--+printf("error insert Free list!");
    }
    node->next_free = head_free->next_free;
    node->prev_free = head_free;
    head_free->next_free->prev_free = node;
    head_free->next_free = node;
    size_free += 1;
  }

  Node *DeleteTail_LRU()
  {
    if (size_lru == 0)
    {
      //--+printf("error in delete lru tail!\n");
      //assert(head_lru->next_lru == head_lru);
      //assert(head_lru->prev_lru == head_lru);
      return nullptr;
    }
    Node *tail = head_lru->prev_lru;
    if (tail->offset == 0)
    {
      tail = tail->prev_lru;
    }
    DeleteNode_LRU(tail);
    return tail;
  }

  Node *DeleteTail_FLU()
  {
    if (size_flu == 0)
    {
      //--+printf("error in delete flu tail!\n");
      //assert(head_flu->next_flu == head_flu);
      //assert(head_flu->prev_flu == head_flu);
      return nullptr;
    }
    Node *tail = head_flu->prev_flu;
    if (tail->offset == 0)
    {
      tail = tail->prev_flu;
    }
    DeleteNode_FLU(tail);
    return tail;
  }

  Node *DeleteTail_Free()
  {
    if (size_free == 0)
    {
      //--+printf("error in delete free tail!\n");
      //assert(head_free->next_free == head_free);
      //assert(head_free->prev_free == head_free);
      return nullptr;
    }
    Node *tail = head_free->prev_free;
    if (tail->offset == 0)
    {
      tail = tail->prev_free;
    }
    DeleteNode_Free(tail);
    return tail;
  }

  template <typename T>
  void Put(T *block, int is_plog)
  {
    if(is_plog == 101){
      
      //PrepareGlogIO();
      //mu.lock();
      std::unique_lock<std::mutex> lck(mtr_cache->mtx_mian2back);

      while(mtr_cache->Main2Back.size() == maxPendGlogSegs){
        mtr_cache->produce_main2back.wait(lck);
      }
      std::map<off_t, std::pair<char*,char*>>* piece_Plog_Write_Pool= mtr_cache->Plog_Write_Pool.front();
      mtr_cache->Plog_Write_Pool.pop_front();
      int size = piece_Plog_Write_Pool->size();
      mtr_cache->pends = size;
      //check
      //int have_cont=0;
      //for(std::map<off_t, std::pair<char*,char*>>::iterator it1=piece_Plog_Write_Pool->begin();it1!=piece_Plog_Write_Pool->end();it1++){
      //  
      //  Plog* p_buf = reinterpret_cast<Plog*>(it1->second.first);
      //  if(p_buf->count>0 || p_buf->offset!=0){
      //    have_cont++;
      //  }
      //}
      //WaitForPersist.push_back(size);
      mtr_cache->Main2Back.push_back(std::make_pair(size,piece_Plog_Write_Pool));


      std::map<off_t, std::pair<char*,char*>>* piece_Plog_Write_Pool_applied = nullptr;
      if(!mtr_cache->Back2Main.empty()){
        int ready_pop = mtr_cache->Back2Main.size();
        piece_Plog_Write_Pool_applied = mtr_cache->Back2Main.front().second;
        mtr_cache->Back2Main.pop_front();
      }
      mtr_cache->consume_main2back.notify_all();
      lck.unlock();

      if(piece_Plog_Write_Pool_applied!=nullptr){  
        ApplyGlogIO(piece_Plog_Write_Pool_applied);  
        mtr_cache->pends =-1;
        mtr_cache->applied =0;
        std::map<off_t, std::pair<std::string,std::string>>* log_seg = mtr_cache->Q_GLog.front();
        mtr_cache->Q_GLog.pop_front();
        log_seg->clear();
        delete log_seg;
        std::unordered_map<std::string, off_t> * index_seg = mtr_cache->GLog_Index.front();
        mtr_cache->GLog_Index.pop_front();
        index_seg->clear();
        delete index_seg;
      }
      
      //mu.unlock();
    }

    
   
    mtr_cache->logi_page_write++;

    if (async_mode_orders_ == 0)
    {
      if (size_lru > kMaxPages_orders_item * 0.9)
      {
        KickLRUbyUring();
      }
      if (mtr_cache->plog_flag != 1 && mtr_cache->FLU_disabled == 0 && size_flu > kMaxPages_orders_item * 0.5)
      {
        KickLFUbyUring();
      }
    }
    return;
  }
  void SetMtr(BPlusOrdersTree::Monitor *mtr_1){
    mtr_cache = mtr_1;
  }
  int Check_Leaf(off_t offset)
  {
    if (offset2node_leaf.find(offset) != offset2node_leaf.end())
    {
      return 1;
    }
    else
    {
      return 0;
    }
  }
  
  
  char *GetMemCopy_Leaf(off_t offset)
  {
    Node *node = offset2node_leaf[offset];
    char *tmp = reinterpret_cast<char *>(node->block);
    return tmp;
  }

  void WarmUpVPCache(int x)
  {
    for (std::unordered_map<off_t, Node *>::iterator it = offset2node_leaf.begin(); it != offset2node_leaf.end(); it++)
    {
      //and do at least one time persistence for data page, avoiding the LRU directly drop un-persisted data pages
      char *leaf_ptr = reinterpret_cast<char *>(it->second->block);
      off_t of = it->first;
      lseek(mtr_cache->global_fd,of,0);
      char* tmp = (char*)malloc(kLeafPageSize*sizeof(char));
      RowToColumn_Cache(leaf_ptr,tmp);
      write(mtr_cache->global_fd,tmp,kLeafPageSize);
      free(tmp);
    }
    return;
  }


  template <typename T>
  T *Get(int fd, off_t offset, int is_plog, char *plog_copy)
  {
    mtr_cache->logi_page_read++;
    //hkc-ap-08
    if(is_plog == 5){
      int scan_ratio = 35; //should be divided by 100
      int buf_size = kUnitNodeSize*scan_ratio/100;
      char *page = (char *)malloc(sizeof(char) * buf_size);
      memset(page, 0, buf_size);  
      lseek(mtr_cache->global_fd, offset, 0);
      read(mtr_cache->global_fd, page, buf_size);
      //char* tmp_read = (char*)malloc(sizeof(char)*kLeafPageSize);
      //char* tmp_row = (char*)malloc(sizeof(char)*kLeafPageSize);
      //memcpy(tmp_row, &page[0], kLeafPageSize);
      //ColumnToRow_Cache(tmp_row,tmp_read);
        // mu.unlock();
      //memcpy(page, &tmp_read[0], kLeafPageSize);
      //free(tmp_read);
      //free(tmp_row);
      //char *row = (char *)malloc(sizeof(char) * kLeafPageSize);
      //memcpy(row, &page[0], kLeafPageSize);
      //memcpy(plog_copy, &page[kLeafPageSize], kPlogPageSize);
      free(page);
      return nullptr;

    }
    
    if(is_plog == 4){
      //for range query, only read once
      char *page = (char *)malloc(sizeof(char) * kUnitNodeSize);
      memset(page, 0, kUnitNodeSize);  
      lseek(mtr_cache->global_fd, offset, 0);
      read(mtr_cache->global_fd, page, kUnitNodeSize);
      char* tmp_read = (char*)malloc(sizeof(char)*kLeafPageSize);
      char* tmp_row = (char*)malloc(sizeof(char)*kLeafPageSize);
      memcpy(tmp_row, &page[0], kLeafPageSize);
      ColumnToRow_Cache(tmp_row,tmp_read);
        // mu.unlock();
      memcpy(page, &tmp_read[0], kLeafPageSize);
      free(tmp_read);
      free(tmp_row);
      char *row = (char *)malloc(sizeof(char) * kLeafPageSize);
      memcpy(row, &page[0], kLeafPageSize);
      memcpy(plog_copy, &page[kLeafPageSize], kPlogPageSize);
      free(page);
      return reinterpret_cast<T *>(&row[0]);





    }
    if (is_plog == 1)
    {

      off_t plog_offset = offset + kLeafPageSize;
      if (mtr_cache->Virtual_Page_In_Cache.find(offset) != mtr_cache->Virtual_Page_In_Cache.end())
      {
        char *buf = mtr_cache->Virtual_Page_In_Cache[offset].second.first;
        Plog *plog = reinterpret_cast<Plog *>(buf);
        if (!(plog->offset == 0 || plog->offset == offset + kLeafPageSize))
        {
          memset(buf,0,kPlogPageSize);
          plog->offset = offset + kLeafPageSize; // int x=1;
          mtr_cache->error_cnt++; ////--+printf("error offset!\n");
        }
        return reinterpret_cast<T *>(&buf[0]);
      }
      else
      {
        char *buf = (char *)malloc(sizeof(char) * kPlogPageSize);
        memset(buf, 0, kPlogPageSize);
        ////mu.lock();
        lseek(mtr_cache->global_fd, plog_offset, 0);
        read(mtr_cache->global_fd, buf, kPlogPageSize);
        ////mu.unlock();
        mtr_cache->phy_plog_read++;
        Plog *plog = reinterpret_cast<Plog *>(buf);
        if (!(plog->offset == 0 || plog->offset == offset + kLeafPageSize))
        {
          int x=1;
          //--+printf("error offset!\n");
        }
        //Plog_Write_Pool.emplace(offset,std::make_pair(buf,nullptr));
        return reinterpret_cast<T *>(&buf[0]);
      }
    }

    // only for pages that have already been written to data file
    if (offset2node_leaf.find(offset) == offset2node_leaf.end())
    {

      mtr_cache->cached_eles = offset2node_leaf.size();
      // i.e., For Get a node, if the node is not in the cache, pull it from disk
      // auto t1 = std::chrono::steady_clock::now();
      ////mu.lock();
      // auto t2 = std::chrono::steady_clock::now();
      // lock_contention_file +=std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();

      Node *tmp = DeleteTail_Free();
      Plog *plog_c = nullptr;
      // LeafNode* leaf= reinterpret_cast<LeafNode*>(tmp->block);
      char *buf = reinterpret_cast<char *>(tmp->block);
      memset(buf, 0, kLeafPageSize);
      
      // memset(tmp,0,kPageSize);
      if (is_plog == 3) //read both data page and plog at one time IO
      {
        char *tmp_unit = (char *)malloc(sizeof(char) * kUnitNodeSize);
        memset(tmp_unit, 0, kUnitNodeSize);
        // mu.lock();
        lseek(mtr_cache->global_fd, offset, 0);
        read(mtr_cache->global_fd, tmp_unit, kUnitNodeSize);
        char* tmp_read = (char*)malloc(sizeof(char)*kLeafPageSize);
        char* tmp_row = (char*)malloc(sizeof(char)*kLeafPageSize);
        memcpy(tmp_row, &tmp_unit[0], kLeafPageSize);
        ColumnToRow_Cache(tmp_row,tmp_read);
        // mu.unlock();
        memcpy(buf, &tmp_read[0], kLeafPageSize);
        free(tmp_read);
        free(tmp_row);
        //memcpy(buf, &tmp_unit[0], kLeafPageSize);
        LeafNode *check_l = reinterpret_cast<LeafNode *>(&buf[0]);
        if(check_l->offset == 0){
          int x=0;
        }
        
        memcpy(plog_copy, &tmp_unit[kLeafPageSize], kPlogPageSize);
        // for test Only
        plog_c = reinterpret_cast<Plog *>(plog_copy);
        if (!(plog_c->offset == offset + kLeafPageSize || plog_c->offset == 0))
        {
          //--+printf("++++++++++++++wrong plog get, leaf offset is: %ld, but now is: %ld++++++++++++++\n\n\n++++++++++++++\n", offset + kLeafPageSize, plog_c->offset);
          // actually plog do not need to maintain the offset, so that we can now update plog offset by its leaf node offset
          // plog_c->offset = offset + kLeafPageSize
        }
        free(tmp_unit);
      }
      else
      {
         //mu.lock();
        lseek(mtr_cache->global_fd, offset, 0);
        read(mtr_cache->global_fd, buf, kLeafPageSize);
        
        char* tmp_read = (char*)malloc(sizeof(char)*kLeafPageSize);
        char* ori = (char*)malloc(sizeof(char)*kLeafPageSize);
        memcpy(&ori[0],buf, kLeafPageSize);
        ColumnToRow_Cache(&ori[0],&tmp_read[0]);
        // mu.unlock();
        memcpy(buf, &tmp_read[0], kLeafPageSize);
        free(tmp_read);
        free(ori);

        // mu.unlock();
      }
      mtr_cache->phy_page_read++;
      LeafNode *block = reinterpret_cast<LeafNode *>(&buf[0]);
      // block->offset = offset
      block->newest_lsn = mtr_cache->global_lsn;
      block->UpdateLSNsForAllRecords(mtr_cache->global_lsn);
      block->oldest_lsn = mtr_cache->global_lsn;
      block->origin_lsn = mtr_cache->global_lsn;
      if (block->offset != offset)
      {
        mtr_cache->error_cnt++; //--+printf("error in retrieve leaf node!\n");
        block->offset = offset;
      }
      Node *node = tmp;
      node->offset = block->offset;
      block->dirty = 0;
      offset2node_leaf.emplace(offset, node);

      mtr_cache->cached_eles = offset2node_leaf.size();
      InsertHead_LRU(node);
      return reinterpret_cast<T *>(&buf[0]);

    }
   
   
    // i.e., return the node to btree by retrieving from cache
    Node *node = offset2node_leaf[offset];
    mtr_cache->cached_eles = offset2node_leaf.size();
    ++node->ref;
    DeleteNode_LRU(node);
    InsertHead_LRU(node);
    
    LeafNode *block = reinterpret_cast<LeafNode *>(node->block);
    if (block->offset != offset)
    {
      //--+printf("error in retrieve leaf node!\n");
    }
    return static_cast<T *>(node->block);
  }

private:
  // std::unordered_map<off_t,char*> WaitForDeleteFLU;
  void KickLFUbyUring()
  {
    struct io_uring ring;
    int Queue_depth = 200;
    io_uring_queue_init(Queue_depth, &ring, 0);
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    struct iovec iov[Queue_depth];
    int i, fd, ret;
    fd = mtr_cache->global_fd;

    Node *tail = head_flu->prev_flu;
    Node *tmp = tail;
    Checkpoint *chpt_flu = new Checkpoint();
    std::vector<char*> tmps;
    for (i = 0; i < Queue_depth; i++)
    {
      Node *befree = tmp;
      tmp = tmp->prev_flu;
      if (tmp == head_flu)
      {
        break;
      }
      LeafNode *leaf = reinterpret_cast<LeafNode *>(befree->block);
      // if(leaf->oldest_lsn!=1){
      //   CheckLSNOrder();
      // }
      mtr_cache->cur_lsn = leaf->oldest_lsn;
      chpt_flu->safe_lsn = leaf->oldest_lsn;
      if (mtr_cache->FLU_min_lsn < leaf->oldest_lsn)
      {
        // //--+printf("error in flush dirty pages!");
        mtr_cache->FLU_min_lsn = leaf->oldest_lsn;
      }
      chpt_flu->safe_offset_in_log_file = leaf->offset;
      mtr_cache->normal_chpt_now = leaf->oldest_lsn;
      leaf->origin_lsn = leaf->oldest_lsn;
      leaf->oldest_lsn = leaf->newest_lsn;
      leaf->UpdateLSNsForAllRecords(leaf->newest_lsn);

      // processing
      sqe = io_uring_get_sqe(&ring);
      // offset=leaf->offset;
      Node *node = reinterpret_cast<Node *>(leaf);
      char* tmp = (char*)malloc(sizeof(char)*kLeafPageSize);
      RowToColumn_Cache(reinterpret_cast<char*>(leaf),tmp);
      int debug_flag=0;
      if(debug_flag==1){
        char* row = (char*)malloc(sizeof(char)*kLeafPageSize);
        ColumnToRow_Cache(tmp,row);
        LeafNode * row_leaf_test = reinterpret_cast<LeafNode*>(row);
        //comparison
        if( (leaf->count!=row_leaf_test->count) || 
            (leaf->dirty!=row_leaf_test->dirty) || 
            (leaf->left!=row_leaf_test->left) || 
            (leaf->right!=row_leaf_test->right) || 
            (leaf->parent!=row_leaf_test->parent) || 
            (leaf->newest_lsn!=row_leaf_test->newest_lsn) || 
            (leaf->oldest_lsn!=row_leaf_test->oldest_lsn) || 
            (leaf->sha_plog_offset!=row_leaf_test->sha_plog_offset) ||  //hkc-ap-09
            (leaf->offset!=row_leaf_test->offset) || 
            (leaf->offset_k!=row_leaf_test->offset_k) || 
            (leaf->offset_v1!=row_leaf_test->offset_v1) || 
            (leaf->offset_v2!=row_leaf_test->offset_v2) || 
            (leaf->offset_v3!=row_leaf_test->offset_v3) || 
            (leaf->offset_v4!=row_leaf_test->offset_v4) || 
            (leaf->offset_leaf!=row_leaf_test->offset_leaf) ||
            (leaf->offset_lsn!=row_leaf_test->offset_lsn) 
        ){
          printf("Convert #1 error occur!!!\n");
        }
        for(int i =0; i< leaf->count;i++){
          if(
            (leaf->records[i].cur_lsn != row_leaf_test->records[i].cur_lsn) ||
            strcmp(leaf->records[i].key,row_leaf_test->records[i].key) ||
            strcmp(leaf->records[i].value_1,row_leaf_test->records[i].value_1) ||
            strcmp(leaf->records[i].value_2,row_leaf_test->records[i].value_2) ||
            strcmp(leaf->records[i].value_3,row_leaf_test->records[i].value_3) ||
            strcmp(leaf->records[i].value_4,row_leaf_test->records[i].value_4)
          ){
            printf("Convert #2 error occur!!!\n");
          }
        }
        free(row);
      }
      iov[i].iov_base = tmp;
      tmps.push_back(tmp);
      iov[i].iov_len = kLeafPageSize;
      io_uring_prep_writev(sqe, fd, &iov[i], 1, leaf->offset);
      mtr_cache->phy_page_write++;
      // WaitForDeleteFLU.emplace(leaf->offset,reinterpret_cast<char*>(tmp));
      // lseek(global_fd,befree->offset,0);
      // write(global_fd,reinterpret_cast<char*>(befree->block),kLeafPageSize);
      mtr_cache->InFLUOrNot.erase(befree->offset);
      DeletePersistedFLUNode(befree);
    }
    ret = io_uring_submit(&ring);
    for (i = 0; i < Queue_depth; i++)
    {
      ret = io_uring_wait_cqe(&ring, &cqe);
      io_uring_cqe_seen(&ring, cqe);
    }
    io_uring_queue_exit(&ring);

    for(int i=0;i<tmps.size();i++){
      free(tmps[i]);
    }

    mtr_cache->update_flu++;
    off_t tmp_of = lseek(mtr_cache->global_fd_chpt,0,SEEK_CUR);
    int z4=write(mtr_cache->global_fd_chpt,reinterpret_cast<char *>(chpt_flu), sizeof(*chpt_flu));
    mtr_cache->cur_lsn = chpt_flu->safe_lsn;
    //close(fp1);
    return;
  }


  void GlogWorker(){
    sleep(100);
    int batch_size =200;
    int bg = mtr_cache->bg_id;
    std::map<off_t, std::pair<char*,char*>>* last_applied = nullptr;
    int last_applied_size=0;
    
    while(true){
      
      if(mtr_cache->load_flag == 1){
        continue;
      }
      bg = mtr_cache->bg_id;
      std::unique_lock<std::mutex> lck(mtr_cache->mtx_mian2back);
      if(mtr_cache->Main2Back.size() == 0){
        mtr_cache->consume_main2back.wait(lck);
      }
      if(last_applied!=nullptr){
        mtr_cache->Main2Back.pop_front();
        mtr_cache->Back2Main.push_back(std::make_pair(last_applied_size,last_applied));
        last_applied = nullptr;
        last_applied_size =0;
      }
      if(mtr_cache->Main2Back.size() == 0)
      {
        mtr_cache->produce_main2back.notify_all();
        lck.unlock();
      }else{
        int applied_1 = mtr_cache->Main2Back.front().first;
        std::map<off_t, std::pair<char*,char*>>* piece_Plog_Write_Pool = mtr_cache->Main2Back.front().second;
        mtr_cache->produce_main2back.notify_all();
        lck.unlock();
        //check
        //int have_cont=0;
        //for(std::map<off_t, std::pair<char*,char*>>::iterator it1=piece_Plog_Write_Pool->begin();it1!=piece_Plog_Write_Pool->end();it1++){
        //
        //  Plog* p_buf = reinterpret_cast<Plog*>(it1->second.first);
        //  if(p_buf->count>0 || p_buf->offset!=0){
        //    have_cont++;
        //  }
        //}
        int applied_0 = DoGlogIO(0, piece_Plog_Write_Pool);
        if(applied_0 != applied_1){
          //--+printf("mistake!s\n");
        }
        last_applied = piece_Plog_Write_Pool;
        last_applied_size = applied_0;      
      }
      
      
      
      /*
      if(WaitForPersist.size()>0 && WaitForPersist.at(0)>0){
        mu.lock();
        if(WaitForPersist.at(0)<=0){
          mu.unlock();
          continue;
        }
        mu.unlock();
        int applied_ = DoGlogIO(&ring,0);
        mu.lock();
        WaitForPersist[0]-=applied_; //has been persisted
        if(WaitForPersist[0]!=0){
          //--+printf("error flush!\n");
        }
        mu.unlock();
      }
      */
      
      //persist the pending pages
    }
  }


  void DoGlogIO_At(struct io_uring *ring,char* pending_buf, std::deque<std::pair<off_t, int>>* pending_meta, int group_cnt){
    //0.init
    int batch_size=group_cnt;
    char* plog_submit =  pending_buf;
    std::deque<std::pair<off_t, int>>* plog_meta = pending_meta;
    // 1. prepare aio contents
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    
    struct iovec iov[batch_size];
    int fd_now =mtr_cache->global_fd;
    int ret;
    int plog_submit_offset = 0;
    // 2. prepare the contents
    int i = 0;
    std::vector<char*> tmps;
    // for(i=0; i<cnt;i++){
    for(std::deque<std::pair<off_t, int>> ::iterator it = plog_meta->begin(); it!=plog_meta->end();it++){
      sqe = io_uring_get_sqe(ring);
      int offset =it->first;
      int buf_size = it->second;
      if(buf_size == kPlogPageSize){
        iov[i].iov_base = &plog_submit[plog_submit_offset+kLeafPageSize];
        iov[i].iov_len = buf_size;
        io_uring_prep_writev(sqe, fd_now, &iov[i], 1, offset+kLeafPageSize);
      }else{
        char* tmp_column = (char*)malloc(sizeof(char)*kLeafPageSize);
        char* tmp_row = (char*)malloc(sizeof(char)*kLeafPageSize);
        memcpy(tmp_row,&plog_submit[plog_submit_offset],kLeafPageSize);
        RowToColumn_Cache(tmp_row,tmp_column);
        iov[i].iov_base = tmp_column;
        iov[i].iov_len = buf_size;
        io_uring_prep_writev(sqe, fd_now, &iov[i], 1, offset);
        free(tmp_row);
      }
      plog_submit_offset+=kUnitNodeSize;

    }
    ret = io_uring_submit(ring);
    for (int j = 0; j < i; j++)
    {
      ret = io_uring_wait_cqe(ring, &cqe);
      io_uring_cqe_seen(ring, cqe);
      //reclaim plog and leafnode copy
    }
    for(int i=0;i<tmps.size();i++){
      free(tmps[i]);
    }
    return;
}






  void ApplyGlogIO(std::map<off_t, std::pair<char*,char*>>* piece_Plog_Write_Pool){

    //std::map<off_t, std::pair<char*,char*>>* piece_Plog_Write_Pool= Plog_Write_Pool.front();
    for(std::map<off_t, std::pair<char*,char*>>::iterator it =piece_Plog_Write_Pool->begin();it!=piece_Plog_Write_Pool->end();it++){
      mtr_cache->applied++;
      //pendingWrites_In_Glog.erase(it->first);
      std::pair<char*, char*> addr = it->second;
      char* plog_copy = addr.first;
      if(plog_copy == nullptr){
          int x=0;
      }else{
        free(plog_copy);
          //memset(plog_copy,0,kPlogPageSize);
      }
      char* leaf_copy = addr.second;
      if(leaf_copy == nullptr){
          int x=0;
      }else{
          LeafNode* leaf = reinterpret_cast<LeafNode*>(leaf_copy);
          if(leaf->offset== it->first)  {
            free(leaf_copy);
          }
      }
    }
    piece_Plog_Write_Pool->clear();
    delete piece_Plog_Write_Pool;
    return;
}

  int DoGlogIO(int index, std::map<off_t, std::pair<char*,char*>>* piece_Plog_Write_Pool){
    //check
      //int have_cont=0;
      //for(std::map<off_t, std::pair<char*,char*>>::iterator it1=piece_Plog_Write_Pool->begin();it1!=piece_Plog_Write_Pool->end();it1++){
      //  
      ///  Plog* p_buf = reinterpret_cast<Plog*>(it1->second.first);
      //  if(p_buf->count>0 || p_buf->offset!=0){
      //    have_cont++;
      //  }
      //}
    
    //0.init
    struct io_uring ring;
    int batch_size=200;
    io_uring_queue_init(batch_size, &ring, 0);
    char* plog_submit = nullptr;
    if(plog_submit == nullptr){
      plog_submit = (char*)malloc(sizeof(char)*kUnitNodeSize*batch_size);  
    }
    int plog_flush=0;
    int zero_plog = 0;
    int leaf_flush=0;
    // 1. prepare aio contents
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    
    struct iovec iov[batch_size];
    int fd_now = mtr_cache->global_fd;
    int ret;
    int app =0;
    //mu.lock();
    //std::map<off_t, std::pair<char *, char *>> *piece_Plog_Write_Pool = Plog_Write_Pool.front();
    //mu.unlock();
    std::map<off_t, std::pair<char*,char*>>::iterator tmp = piece_Plog_Write_Pool->begin();
    std::vector<char*> tmps_buf;
    while (app < piece_Plog_Write_Pool->size()){

      memset(plog_submit,0,sizeof(char)*kUnitNodeSize*batch_size);
      int plog_submit_offset = 0;
      // 2. prepare the contents
      int i = 0;

      
      for(std::map<off_t, std::pair<char*,char*>>::iterator it = tmp; 
        it!=piece_Plog_Write_Pool->end();it++){
        
        app++;
        sqe = io_uring_get_sqe(&ring);
      
        int buf_size=kPlogPageSize;
        off_t file_off = it->first;

        char* leaf_buf = it->second.second;
        if(leaf_buf!=nullptr){
          LeafNode* l = reinterpret_cast<LeafNode*>(leaf_buf);
          if(l->offset == 0){
            printf("check offset: %ld",it->first);  
          }
          buf_size=kUnitNodeSize;

        }
        char* plog_buf = it->second.first;
        if(plog_buf == nullptr){
          //--+printf("error!\n");
        }
        Plog* p = reinterpret_cast<Plog*>(plog_buf);
        if(!(p->offset == 0 || p->offset == it->first + kLeafPageSize)){
          memset(plog_buf,0,kPlogPageSize);
          p->offset = it->first + kLeafPageSize;
          mtr_cache->error_cnt++; ////--+printf("error!\n");
        }
        if(p->offset == 0){
          zero_plog++;
          ////--+printf("check offset: %ld\n",it->first);
        }
        if(buf_size == kPlogPageSize){
          memcpy(&plog_submit[plog_submit_offset+kLeafPageSize],plog_buf,kPlogPageSize);
          iov[i].iov_base = &plog_submit[plog_submit_offset+kLeafPageSize];
          iov[i].iov_len = buf_size;
          io_uring_prep_writev(sqe, fd_now, &iov[i], 1, file_off+kLeafPageSize);
          mtr_cache->phy_plog_write;
        }else{
          memcpy(&plog_submit[plog_submit_offset],leaf_buf,kLeafPageSize);
          memcpy(&plog_submit[plog_submit_offset+kLeafPageSize],plog_buf,kPlogPageSize);          
          char tmp_cache[kLeafPageSize] = {'\0'};
          RowToColumn_Cache(leaf_buf,&tmp_cache[0]);
          memcpy(&plog_submit[plog_submit_offset],&tmp_cache[0],kLeafPageSize);
          //iov[i].iov_base = tmp_cache;
          //tmps_buf.push_back(tmp_cache);
          iov[i].iov_base = &plog_submit[plog_submit_offset];
          iov[i].iov_len = buf_size;
          io_uring_prep_writev(sqe, fd_now, &iov[i], 1, file_off);
          mtr_cache->phy_page_write++;
        }
        plog_submit_offset+=kUnitNodeSize;
        mtr_cache->phy_plog_write += 1;
        i += 1;
        if(i==batch_size){
          tmp=it;
          tmp++;
          break;
        } 
      }
      ret = io_uring_submit(&ring);
      for (int j = 0; j < i; j++)
      {
        ret = io_uring_wait_cqe(&ring, &cqe);
        io_uring_cqe_seen(&ring, cqe);
        //reclaim plog and leafnode copy
      }

    }
    io_uring_queue_exit(&ring);

     int debug_mark = 0;
    // debug
    if (debug_mark == 1){
      for(std::map<off_t, std::pair<char*,char*>>::iterator it = piece_Plog_Write_Pool->begin(); 
        it!=piece_Plog_Write_Pool->end();it++)
      {
        if(it->first == 39206912){
          int x=0;
        }
        off_t of = it->first;
        // 1. in-mem version
        LeafNode *leaf = reinterpret_cast<LeafNode *>(it->second.first);
        // 2. just persisted version
        lseek(mtr_cache->global_fd, of, 0);
        char *test_buf = (char *)malloc(sizeof(char) * kLeafPageSize);
        read(mtr_cache->global_fd, test_buf, kLeafPageSize);
        LeafNode *leaf_p = reinterpret_cast<LeafNode *>(test_buf);
        if (leaf_p->offset != leaf->offset)
        {
          //--+printf("error987556!\n");
        }
        free(test_buf);
      }
    }
    free(plog_submit);
    return app;
}




  void KickLRUbyUring()
  {
    
    Node *ptr = head_lru->prev_lru;
    if(mtr_cache->plog_flag == 1){
      //directly discard the evict page
      for (int i = 0; i < 200; i++){
        Node *tmp = ptr;
        if(tmp == head_lru){
          break;
        }
        ptr = ptr->prev_lru;
        if(mtr_cache->pendingWrites_In_Glog.find(tmp->offset)!=mtr_cache->pendingWrites_In_Glog.end()){
          continue;
        }
        DeletePersistedNode(tmp);
        offset2node_leaf.erase(tmp->offset);
       // if(tmp->offset == 1694560256){
        //  int x=0;
       // }
        if (mtr_cache->Virtual_Page_In_Cache.find(tmp->offset) != mtr_cache->Virtual_Page_In_Cache.end())
        {
          char* p_buf= mtr_cache->Virtual_Page_In_Cache[tmp->offset].second.first;
          free(p_buf);
          mtr_cache->Virtual_Page_In_Cache.erase(tmp->offset);
        }
        char *buf = reinterpret_cast<char *>(tmp->block);
        memset(buf, 0, kLeafPageSize);
        tmp->persisted = 0;
        tmp->offset = 0;
        tmp->ref = 0;
        tmp->size = 0;
        InsertHead_FREE(tmp);
      }
      return;
    }
    
    
    int Queue_depth = 200;
    int i=0;
    for (i = 0; i < Queue_depth; i++)
    {
      
      Node *tmp = ptr;
      if(tmp->offset == 39206912){
      int x=0;
      }
      ptr = ptr->prev_lru;
      DeletePersistedNode(tmp);
      if (mtr_cache->plog_flag!=1 && mtr_cache->InFLUOrNot.find(tmp->offset) != mtr_cache->InFLUOrNot.end())
      {
        DeletePersistedFLUNode(tmp);
        mtr_cache->InFLUOrNot.erase(tmp->offset);
      }
      offset2node_leaf.erase(tmp->offset);
      if (mtr_cache->Virtual_Page_In_Cache.find(tmp->offset) != mtr_cache->Virtual_Page_In_Cache.end())
      {
        char* p_buf= mtr_cache->Virtual_Page_In_Cache[tmp->offset].second.first;
        free(p_buf);
        mtr_cache->Virtual_Page_In_Cache.erase(tmp->offset);
      }
      LeafNode *leaf = reinterpret_cast<LeafNode *>(tmp->block);
      // LeafNode* leaf= reinterpret_cast<LeafNode*>(ptr->block);
      if(leaf->dirty == 0){
        //mtr_cache->WaitForDelete.emplace(tmp->offset, reinterpret_cast<char *>(tmp));
        mtr_cache->CleanPages.emplace(tmp->offset, reinterpret_cast<char *>(tmp));
        continue;
      }
      leaf->dirty =0;
      leaf->origin_lsn = leaf->oldest_lsn;
      leaf->oldest_lsn = leaf->newest_lsn;
      
      // ptr->persisted=11;
      leaf->UpdateLSNsForAllRecords(leaf->newest_lsn);
      mtr_cache->phy_page_write++;
      mtr_cache->WaitForDelete.emplace(tmp->offset, reinterpret_cast<char *>(tmp));
    }
    i = 0;
    std::deque<char*> pendset;
    mtr_cache->dirtys+=mtr_cache->WaitForDelete.size();
    mtr_cache->cleans+=mtr_cache->CleanPages.size();
    if(mtr_cache->WaitForDelete.size()>0)
    {
      int batch_size = 200;
      struct io_uring_sqe *sqe_lru;
      struct io_uring_cqe *cqe_lru;
      struct iovec iov_lru[batch_size];
      struct io_uring ring_lru_;
      io_uring_queue_init(batch_size, &ring_lru_, 0);
      sqe_lru = io_uring_get_sqe(&ring_lru_);
      i=0;
      std::vector<char*> tmps_buf;
      for (std::unordered_map<off_t, char *>::iterator it = mtr_cache->WaitForDelete.begin(); it != mtr_cache->WaitForDelete.end(); it++)
      {
        // processing
        sqe_lru = io_uring_get_sqe(&ring_lru_);
        // offset=leaf->offset;
        Node *node = reinterpret_cast<Node *>(it->second);
        if(mtr_cache->load_flag==1){
          char* pend = (char*)malloc(sizeof(char)*kUnitNodeSize);
          memset(pend,0,kUnitNodeSize);
          pendset.push_back(pend);
          //memcpy(pend,reinterpret_cast<char*>(node->block),kLeafPageSize);
          char tmp_row[kLeafPageSize] = {'\0'};
          memcpy(tmp_row,reinterpret_cast<char*>(node->block),kLeafPageSize);
          char* tmp_cache = (char*)malloc(sizeof(char)*kLeafPageSize);
          RowToColumn_Cache(&tmp_row[0],tmp_cache);
          memcpy(pend,tmp_cache,kLeafPageSize);
          iov_lru[i].iov_base = pend;
          free(tmp_cache);
          //iov_lru[i].iov_base = pend;
          iov_lru[i].iov_len = kUnitNodeSize;
        }else{
          char* tmp_cache = (char*)malloc(sizeof(char)*kLeafPageSize);
          RowToColumn_Cache(reinterpret_cast<char*>(node->block),tmp_cache);
          tmps_buf.push_back(tmp_cache);
          iov_lru[i].iov_base = tmp_cache;
          iov_lru[i].iov_len = kLeafPageSize;
        }
        io_uring_prep_writev(sqe_lru, mtr_cache->global_fd, &iov_lru[i], 1, it->first);
        mtr_cache->phy_page_write++;
          // //--+printf("Offset %lx\n", it->first);
        // pwritev(fd, &iov[i], 1, it->first);
        i += 1;
      }
      int ret = io_uring_submit(&ring_lru_);
      for (int j = 0; j < i; j++)
      {
        ret = io_uring_wait_cqe(&ring_lru_, &cqe_lru);
        io_uring_cqe_seen(&ring_lru_, cqe_lru);
      }

      for(int i=0;i<tmps_buf.size();i++){
        free(tmps_buf[i]);
      }
      io_uring_queue_exit(&ring_lru_);
    }
    //check

    //io_uring_queue_exit(&ring);
    int debug_mark = 0;
    // debug
    if (debug_mark == 1){
      for (std::unordered_map<off_t, char *>::iterator it = mtr_cache->WaitForDelete.begin(); it != mtr_cache->WaitForDelete.end(); it++)
      {
        if(it->first == 39206912){
          int x=0;
        }
        off_t of = it->first;
        // 1. in-mem version
        Node *node = reinterpret_cast<Node *>(it->second);
        LeafNode *leaf = reinterpret_cast<LeafNode *>(node->block);
        // 2. just persisted version
        lseek(mtr_cache->global_fd, of, 0);
        char *test_buf = (char *)malloc(sizeof(char) * kLeafPageSize);
        read(mtr_cache->global_fd, test_buf, kLeafPageSize);
        LeafNode *leaf_p = reinterpret_cast<LeafNode *>(test_buf);
        if (leaf_p->offset != leaf->offset)
        {
          //--+printf("error987556!\n");
        }
        free(test_buf);
      }
    }
    i = 0;
    if(mtr_cache->CleanPages.size()>0){
      for (std::unordered_map<off_t, char *>::iterator it = mtr_cache->CleanPages.begin(); it != mtr_cache->CleanPages.end(); it++)
      {
        i += 1;
        // Node* tmp = offset2node_leaf[it->first];
        // DeletePersistedNode(tmp);
        Node *tmp = reinterpret_cast<Node *>(it->second);
        LeafNode *leaf = reinterpret_cast<LeafNode *>(tmp->block);
        // offset2node_leaf.erase(tmp->offset);
        char *buf = reinterpret_cast<char *>(tmp->block);
        memset(buf, 0, kLeafPageSize);
        tmp->persisted = 0;
        tmp->offset = 0;
        tmp->ref = 0;
        tmp->size = 0;
        InsertHead_FREE(tmp);
     }
     mtr_cache->CleanPages.clear();  
    }
    i=0;
    for (std::unordered_map<off_t, char *>::iterator it = mtr_cache->WaitForDelete.begin(); it != mtr_cache->WaitForDelete.end(); it++)
    {
      i += 1;
      // Node* tmp = offset2node_leaf[it->first];
      // DeletePersistedNode(tmp);
      Node *tmp = reinterpret_cast<Node *>(it->second);
      LeafNode *leaf = reinterpret_cast<LeafNode *>(tmp->block);
      // offset2node_leaf.erase(tmp->offset);
      char *buf = reinterpret_cast<char *>(tmp->block);
      memset(buf, 0, kLeafPageSize);
      tmp->persisted = 0;
      tmp->offset = 0;
      tmp->ref = 0;
      tmp->size = 0;
      InsertHead_FREE(tmp);
    }
    mtr_cache->WaitForDelete.clear();
    if(mtr_cache->load_flag ==1){
      for(int m=0;m<pendset.size();m++){
        char* tmp = pendset[m];
        free(tmp);
      }
      pendset.clear();
    }
  }

  
  void CheckLSNOrder()
  {
    Node *tmp = head_flu->prev_flu;
    LeafNode *leaf = reinterpret_cast<LeafNode *>(tmp->block);
    off_t smallest_lsn = leaf->oldest_lsn;
    // //--+printf("lsn order ==> %d ", smallest_lsn);
    off_t last_leaf_of = leaf->offset;
    tmp = tmp->prev_flu;
    int cnt = 1;
    while (tmp != head_flu)
    {
      cnt++;
      LeafNode *leaf = reinterpret_cast<LeafNode *>(tmp->block);
      if (leaf->offset == last_leaf_of)
      {
        //--+printf("error!\n");
      }
      else
      {
        last_leaf_of = leaf->offset;
      }
      off_t cur_smallest_lsn = leaf->oldest_lsn;
      if (cur_smallest_lsn < smallest_lsn)
      {
        //--+printf("++++++++ error!+++++++++");
      }
      else
      {
        smallest_lsn = cur_smallest_lsn;
        // //--+printf("%d ", smallest_lsn);
      }
      tmp = tmp->prev_flu;
    }
    // //--+printf("\n cur flu list size: %d , cur lru size: %d\n",cnt,offset2node_leaf.size());
  }
  struct Node
  {
    Node()
        : block(nullptr),
          offset(0),
          size(0),
          ref(0),
          persisted(0),
          retrieving(0),
          prev_lru(nullptr),
          next_lru(nullptr),
          prev_flu(nullptr),
          next_flu(nullptr),
          prev_free(nullptr),
          next_free(nullptr) {}

    Node(void *block_, off_t offset_, size_t size_)
        : block(block_),
          offset(offset_),
          size(0),
          retrieving(0),
          ref(1),
          persisted(0),
          prev_lru(nullptr),
          next_lru(nullptr),
          prev_flu(nullptr),
          next_flu(nullptr),
          prev_free(nullptr),
          next_free(nullptr) {}

    void *block;
    off_t offset;
    size_t size;
    size_t ref;
    Node *prev_lru;
    Node *next_lru;
    Node *prev_flu;
    Node *next_flu;
    Node *prev_free;
    Node *next_free;
    int persisted; // 0: in buffer pool; 1: waiting for persisitence; 2: persisted
    int retrieving;
  };

  Node *head_lru;
  Node *head_flu;
  Node *head_free;
  size_t size_flu;
  size_t size_lru;
  size_t size_free;
  //std::thread BackGround_flu;
  //std::thread BackGround_lru;
  //std::thread BackGround_uaio;

  Monitor* mtr_cache;
  std::thread uringWorker;
  bool async_uring;

  std::unordered_map<off_t, Node *> offset2node_leaf;
  //struct io_uring ring_cache;
  int Queue_depth_cache ;
};

class BPlusOrdersTree::InternalCache
{
  struct Node;

public:
  InternalCache()
  {
    size_index = 0;
    head_index = new Node();
    head_index->next = head_index;
    head_index->prev = head_index;
  }

  ~InternalCache()
  {
    // persist all index node to file
    /*int index_fd = open("/media/hkc/csd_3/Test_Plog_dir/index_file.db", O_DIRECT, O_CREAT | O_RDWR, 0600);
    for (auto it = offset2node_Index.begin(); it != offset2node_Index.end(); it++)
    {
      Node *node = it->second;
      char *start = reinterpret_cast<char *>(node->block);
      lseek(index_fd, node->offset, 0);
      write(index_fd, reinterpret_cast<char *>(node->block), kIndexPageSize);
      delete node;
    }
    close(index_fd);
    delete head_index;
    */
  }
  template <typename T>
  void PushToCache(off_t offset, T *block)
  {
    Node *cache_node = new Node(block, offset, kIndexPageSize);
    offset2node_Index.emplace(offset, cache_node);
    mtr_internal->IndexOrNot.insert(offset);
    InsertHead(cache_node);
    return;
  }
  void DeleteNode(Node *node)
  {
    if (node->next == node->prev && nullptr == node->next)
      return;
    node->prev->next = node->next;
    node->next->prev = node->prev;
    node->next = node->prev = nullptr;
    size_index -= node->size;
  }
  void SetMtr(BPlusOrdersTree::Monitor *mtr_2){
    mtr_internal = mtr_2;
  }
  void InsertHead(Node *node)
  {
    node->next = head_index->next;
    node->prev = head_index;
    head_index->next->prev = node;
    head_index->next = node;
    size_index += node->size;
  }

  Node *DeleteTail()
  {
    if (size_index == 0)
    {
      //--+printf("error in delete tail of index node.\n");
      //assert(head_index->next == head_index);
      //assert(head_index->prev == head_index);
      return nullptr;
    }
    Node *tail = head_index->prev;
    if (tail->offset == 0)
    {
      tail = tail->prev;
    }
    DeleteNode(tail);
    return tail;
  }

  template <typename T>
  void Put(T *block)
  {
    // internal node never kick out
    if (offset2node_Index.find(block->offset) == offset2node_Index.end())
    {
      //--+printf("internal cache error!");
      return;
    }
    else
    {
      return;
    }
  }

  template <typename T>
  T *Get(int fd, off_t offset)
  {
    // only for pages that have already been written to data file
    if (offset2node_Index.find(offset) == offset2node_Index.end())
    {
       // never happend
      printf("error happens for internal cache get!\n");
    }
    // i.e., return the node to btree by retrieving from cache
    Node *node = offset2node_Index[offset];
    ++node->ref;
    IndexNode *block = reinterpret_cast<IndexNode *>(node->block);
    if (block->offset != offset)
    {
      //--+printf("error in retrieve leaf node!\n");
    }
    return static_cast<T *>(node->block);
  }

private:
  struct Node
  {
    Node()
        : block(nullptr),
          offset(0),
          size(0),
          ref(0),
          prev(nullptr),
          next(nullptr) {}

    Node(void *block_, off_t offset_, size_t size_)
        : block(block_),
          offset(offset_),
          size(size_),
          ref(1),
          prev(nullptr),
          next(nullptr) {}

    void *block;
    off_t offset;
    size_t size;
    size_t ref;
    // only lru
    Node *prev;
    Node *next;
  };

  Node *head_index;
  size_t size_index;
  Monitor *mtr_internal;
  std::unordered_map<off_t, Node *> offset2node_Index;
};


//hkc-ap-10*
void BPlusOrdersTree::ScanAP_impl(off_t &left, off_t &right, std::string &value){

  if(left != right) {
    int uring_flag=1;
    if(uring_flag==1){
      int scan_batch_size =200;  
      std::vector<off_t> *scan_offs = new std::vector<off_t>;
      scan_offs->push_back(left);
      int total_pages = PrepareReadSetAP(left,right,scan_offs);
      
      //hkc-ub-scan-10
      int buf_ratio =35;
      int buf_size = 144 * 1024*buf_ratio/100;
      // alloc memory buffer for scan result
      char* scan_buffer = (char*)malloc(sizeof(char)*scan_batch_size*buf_size);
      

      // init uring
      struct io_uring_sqe *sqe_scan;
      struct io_uring_cqe *cqe_scan;
      
      struct iovec iov_scan[scan_batch_size];
      struct io_uring ring_scan;
      io_uring_queue_init(scan_batch_size, &ring_scan, 0);
      sqe_scan = io_uring_get_sqe(&ring_scan);
      //prepare uring
      int read_pages=0;
      while(read_pages<total_pages){
        off_t cur_ptr=0;
        int i=0;
        for(i; i< scan_batch_size;i++){
          sqe_scan = io_uring_get_sqe(&ring_scan);
          iov_scan[i].iov_base = &scan_buffer[cur_ptr];
          cur_ptr+=buf_size;
          iov_scan[i].iov_len = buf_size;
          io_uring_prep_readv(sqe_scan,mtr_->global_fd,&iov_scan[i],1,(*scan_offs)[read_pages]);
          read_pages++;
          if(read_pages>=total_pages){
            break;
          }
        }
        int ret = io_uring_submit(&ring_scan);
        //wait uring return
        for (int j = 0; j < i; j++)
        {
          ret = io_uring_wait_cqe(&ring_scan, &cqe_scan);
          io_uring_cqe_seen(&ring_scan, cqe_scan);
        }
      }

      io_uring_queue_exit(&ring_scan);

      free(scan_buffer);
      delete scan_offs;
      value = std::to_string(read_pages);
      return;
    }
  }
}

//hkc-ap-10
void BPlusOrdersTree::APWorker(){
  sleep(20);
  while(true){
    
    if(mtr_->ap_wait){
      std::string value;
      auto t1 = std::chrono::steady_clock::now();
      ScanAP_impl(mtr_->ap_left, mtr_->ap_right,value);
      auto t2 = std::chrono::steady_clock::now();
      auto tmp = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
      mtr_->ap_wait = false;
      mtr_->exe_time = tmp;

    }
    sleep(10);
  }

}


//hkc-ap-11
// /fd_(open(path, O_DIRECT|O_CREAT|O_RDWR, 0600))
BPlusOrdersTree::BPlusOrdersTree(const char *path, const char *log_path, const char *chpt_path, const int bg_id_, const bool enable_backthd, const char *shadow_plog_path)
    : fd_(open(path, O_CREAT|O_DIRECT|O_RDWR)),fd_shadow_plog_(open(shadow_plog_path, O_CREAT|O_DIRECT|O_RDWR)),log_fd_(open(log_path, O_CREAT|O_APPEND|O_RDWR)),chpt_fd_(open(chpt_path, O_CREAT|O_APPEND|O_RDWR)), block_cache_(new BlockCache(enable_backthd)), internal_cache_(new InternalCache())
{
  // we need extra element
  //1. number of columns
  //2. an list indicates the value size of each column
  mtr_ = new Monitor();
  mtr_->bg_fd=bg_id_;

  //hkc-ap-12
  scan_ap = true;

  block_cache_->SetMtr(mtr_);
  internal_cache_->SetMtr(mtr_);
  //block_cache_->mtr_cache = mtr_;
  if (fd_ == -1)
    //Exit("open db");
    printf("open orders db failed.\n");
  if (log_fd_ == -1)
    //Exit("open log");
    printf("open orders log failed.\n");
  if (chpt_fd_ == -1)
    //Exit("open chpt");
    printf("open orders chpt failed.\n");
  
  //hkc-ap-13
  if (fd_shadow_plog_ == -1) printf("open shadow plog file failed.\n");
  mtr_->global_shadow_plog = fd_shadow_plog_;
  shadow_plog_off = 0;
   
  mtr_->global_fd = fd_;
  //global_fd = fd_;
  mtr_->global_fd_log = log_fd_;
  //global_fd_log = log_fd_;
  mtr_->global_fd_chpt = chpt_fd_;
  // meta_ = Map<Meta>(kMetaOffset_orders_);
  meta_ = new Meta();
  log_obj = new LogRecord();
  chpt = new Checkpoint();
  // UnMap<Meta>(meta_);
  // meta_ = Alloc<Meta>();

  if (meta_->height == 0)
  {
    // Initialize B+tree;
    constexpr off_t of_root = kMetaOffset_orders_ + kUnitNodeSize;
    meta_->block = of_root;
    // LeafNode* root = new (Map<LeafNode>(of_root)) LeafNode();
    LeafNode *root = AllocLeaf<LeafNode>();
    root->offset = of_root;
    meta_->height = 1;
    meta_->root = of_root;
    // meta_->block = of_root + sizeof(LeafNode);
    meta_->block = of_root + kUnitNodeSize;
    UnMap<LeafNode>(root, 1, 0);
  }
  //hkc-ap-14
  if(scan_ap){
    std::thread thd_ap = std::thread(&BPlusOrdersTree::APWorker,this);
    thd_ap.detach();
  }
}

BPlusOrdersTree::~BPlusOrdersTree()
{
  UnMap(meta_, 0, 0);
  delete block_cache_;
  close(fd_);
}

//hkc-ap-15
int BPlusOrdersTree::PrepareReadSetAP(off_t left, off_t right, void *scan_ptr){
  std::vector<off_t> *scan_offs = reinterpret_cast<std::vector<off_t>*>(scan_ptr);
  std::unordered_map<off_t, std::pair<off_t, off_t>> links = mtr_->leaf_links;
  if(links.find(left)!=links.end()){
    off_t next = links[left].second;
    while(next<right && next!=0){
      scan_offs->push_back(next);
      next = links[next].second;
    }
  }
  int total_pages = scan_offs->size();

  return total_pages;

}



void BPlusOrdersTree::SetPlog(){
  mtr_->plog_flag =1;
  mtr_->FLU_disabled =1;
  mtr_->warmup_flag =1;
  return;
}

void BPlusOrdersTree::GetStatic(int &hits_bp, int &hits_glog, int &hits_plog, int &disk_read, int &io_consume, int &error_cnt, int &dirtys, int &cleans){
  hits_bp = mtr_->hits_bp;
  hits_glog = mtr_->hits_glog;
  hits_plog = mtr_->hits_plog;
  disk_read = mtr_->disk_read;
  io_consume = mtr_->io_consume;
  error_cnt = mtr_->error_cnt;
  dirtys = mtr_->dirtys;
  cleans = mtr_->cleans;
  
  return;
}
off_t BPlusOrdersTree::GetLSN(){
  return mtr_->global_lsn;
}
void BPlusOrdersTree::IncreLSN(){
  mtr_->global_lsn++;
  return;
}

void BPlusOrdersTree::ResetStatic(){
    mtr_->hits_bp = 0; 
    mtr_->hits_glog = 0; 
    mtr_->hits_plog =0;
    mtr_->disk_read=0;
    mtr_->io_consume =0;
    mtr_->error_cnt=0;
    mtr_->dirtys = 0;
    mtr_->cleans = 0;
}
void BPlusOrdersTree::SetBaseline(){
  mtr_->FLU_disabled = 0;
  mtr_->plog_flag =-1;
  mtr_->warmup_flag =0;
}

void BPlusOrdersTree::SetIOStaic(){
    mtr_->plog_insert=0;
    mtr_->leaf_insert=0;
    mtr_->leaf_split=0;
    mtr_->phy_plog_write=0;
    mtr_->phy_plog_read=0;
    mtr_->phy_page_read=0;
    mtr_->phy_page_write=0;
    mtr_->logi_page_read=0;
    mtr_->logi_page_write=0;

    return;
}
off_t BPlusOrdersTree::GetChpt(){
  return mtr_->normal_chpt_now;
}
void BPlusOrdersTree::SetLoadFlag(int flag){
  mtr_->load_flag = flag;
}
void BPlusOrdersTree::DisableFLUOrNot(int flag){
  mtr_->FLU_disabled = flag;
}
void BPlusOrdersTree::LoadIOStatic(int &plog_insert,int &leaf_insert,int &leaf_split,int &phy_plog_write,int &phy_plog_read,int &phy_page_read,int &phy_page_write,int &logi_page_read,int &logi_page_write){
    plog_insert = mtr_->plog_insert;
    leaf_insert = mtr_->leaf_insert;
    leaf_split = mtr_->leaf_split;
    phy_plog_write = mtr_->phy_plog_write;
    phy_plog_read = mtr_->phy_plog_read;
    phy_page_read = mtr_->phy_page_read;
    phy_page_write = mtr_->phy_page_write;
    logi_page_read = mtr_->logi_page_read;
    logi_page_write = mtr_->logi_page_write;
    return;
}

int BPlusOrdersTree::ApplyPendPages(std::map<off_t, std::pair<char*,char*>>* source, std::map<off_t, std::pair<char*,char*>>* target, std::map<off_t,std::pair<int,int>> *EmptyPages){

  int repeat =0;
  int empty =0;
  for(std::map<off_t, std::pair<char*,char*>>::iterator tar_it = target->begin(); tar_it!=target->end();tar_it++){
    off_t of = tar_it->first;
    if(source->find(of)!=source->end()){
      char* buf = (*source)[of].first;
      Plog* p_source = reinterpret_cast<Plog*>(buf);
      repeat++;
      char* tar_buf = tar_it->second.first;
      memcpy(tar_buf,buf,kPlogPageSize);
      Plog* p_tar = reinterpret_cast<Plog*>(tar_buf);
      if(p_source->offset == 0 || p_tar->offset == 0 || p_source->count == 0 || p_tar->count == 0){
        empty++;
      }
      (*EmptyPages)[of].first = 1;
    }
  }
  return repeat;
}

/*
void BPlusOrdersTree::BackGround_GLog_Func()
{
  int total_KV=0;
  int final_KV=0;
  int total_Plog =0;
  int final_Plog =0;
  int total_leaf =0;
  int final_leaf=0;
  // check whether to conduct the minor compaction and major compaction
  std::map<off_t, std::pair<std::string,std::string>>* pending_Seg =nullptr;
  std::map<off_t, std::pair<char*,char*>>* piece_Plog_Write_Pool= nullptr;
  int index =mtr_->Q_GLog.size()-2;
  if(index<0){
    //--+printf("???\n");
  }

  piece_Plog_Write_Pool =new std::map<off_t, std::pair<char*,char*>>;
  mtr_->Plog_Write_Pool.push_back(piece_Plog_Write_Pool);
  pending_Seg = mtr_->Q_GLog.at(index);
  //1. merge KVs and only reserve unique and newest KVs
  // it can be replaced by GlogIndex since GLog index has already held the newest KVs
  std::unordered_map<std::string, off_t>* NewestK = mtr_->GLog_Index.at(index); // k:lsn
 


  std::map<off_t,std::pair<int,int>>* EmptyPages = new std::map<off_t,std::pair<int,int>>; //offset: <cnt,reused>
  //plog_buffer_id=0;
  //0.1 prepare pages with offset
  for(std::unordered_map<std::string, off_t>::iterator it = NewestK->begin();it!=NewestK->end();it++){
    off_t of_leaf = GetLeafOffset(it->first.c_str()); //get offset by key
    if(EmptyPages->find(of_leaf) == EmptyPages->end()){
      EmptyPages->emplace(of_leaf,std::make_pair(0,1));
      //char *buf = &plog_buffer[plog_buffer_id*kPlogPageSize];
      char* buf = (char*)malloc(sizeof(char)*kPlogPageSize);
      memset(buf,0,kPlogPageSize);
      piece_Plog_Write_Pool->emplace(of_leaf,std::make_pair(buf,nullptr));
      //plog_buffer_id++;
    }else{
      int cnt = (*EmptyPages)[of_leaf].second;
      cnt++;
      (*EmptyPages)[of_leaf]=std::make_pair(0,cnt);
    }
  }
  int repeat =0;
  if(mtr_->Main2Back.size()>0){
    std::map<off_t, std::pair<char*,char*>>* pend_piece_Plog_Write_Pool = mtr_->Main2Back.at(0).second;
    repeat += ApplyPendPages(pend_piece_Plog_Write_Pool, piece_Plog_Write_Pool, EmptyPages);
  }
  if(mtr_->Back2Main.size()>0){
    for(int i=0;i<mtr_->Back2Main.size();i++){
      std::map<off_t, std::pair<char*,char*>>* pend_piece_Plog_Write_Pool = mtr_->Back2Main.at(i).second;
      repeat+=ApplyPendPages(pend_piece_Plog_Write_Pool, piece_Plog_Write_Pool, EmptyPages);
    }

  }

  //0.2 read all plog pages by liburing
  int app =0;
  //if (EmptyPages->size()>kMaxUniqueKInGLogSeg){
  //  //--+printf("error alloc empty pages\n");
  //}
  if(repeat < piece_Plog_Write_Pool->size()){
    int read_batch_size =200;
    struct io_uring_sqe *sqe_read;
    struct io_uring_cqe *cqe_read;
    
    struct iovec iov_read[read_batch_size];
    struct io_uring ring_;
    io_uring_queue_init(read_batch_size, &ring_, 0);
    sqe_read = io_uring_get_sqe(&ring_);
    //read pages in batch
    
    std::map<off_t,std::pair<int,int>>::iterator tmp = EmptyPages->begin();
    
    while (app < EmptyPages->size()){
      int i=0;
      for(std::map<off_t,std::pair<int,int>>::iterator it = tmp;it!=EmptyPages->end();
        it++){
        app++;
        if(it->second.first == 1){
          continue;
        }
        sqe_read = io_uring_get_sqe(&ring_);
        off_t offset = it->first;
        iov_read[i].iov_base = (*piece_Plog_Write_Pool)[offset].first;
        iov_read[i].iov_len = kPlogPageSize;
        io_uring_prep_readv(sqe_read,mtr_->global_fd,&iov_read[i],1,offset+kLeafPageSize);
        i++;
        if(i == read_batch_size){
          tmp=it;
          tmp++;
          break;
        }
      }
      if(i>0){
        int ret = io_uring_submit(&ring_);
        for (int j = 0; j < i; j++)
        {
          ret = io_uring_wait_cqe(&ring_, &cqe_read);
          io_uring_cqe_seen(&ring_, cqe_read);
          //reclaim plog and leafnode copy
        }
      }
    }
    io_uring_queue_exit(&ring_);

  }
  if(app!=EmptyPages->size()){
      //--+printf("error read batch!\n");
  }
  EmptyPages->clear();
  delete EmptyPages;
  
  int check_cnt=0;
  for(std::map<off_t, std::pair<char*,char*>>::iterator it = piece_Plog_Write_Pool->begin();it!=piece_Plog_Write_Pool->end();it++){
    Plog* p = reinterpret_cast<Plog*>(it->second.first);
    if(p->offset!=0 || p->count !=0){
      check_cnt+=1;
    }
    if(p->count>5){
      int x=0;
    }
  }


  //2. do Plog retrieve and in-place update
  for(std::map<off_t, std::pair<std::string,std::string>>::iterator it = pending_Seg->begin(); it!=pending_Seg->end();it++){    
      total_KV++;
      off_t cur_lsn = it->first;
      std::string key = it->second.first;
      off_t now_lsn = (*NewestK)[key];
      if(now_lsn > cur_lsn){
        //skip the older KVs
        continue;
      }else{
        final_KV++;
        total_Plog++;
        //do the apply (i.e., minor compaction)
        off_t of_leaf = GetLeafOffset(it->second.first.c_str()); //get offset by key
        //pendingWrites_In_Glog.emplace(of_leaf,1);
        //if(of_leaf == 1694560256){
        //  char* cc = nullptr;
        //  LeafNode* te= Map<LeafNode>(of_leaf,1,0,cc);
        //  //--+printf("%ld\n",te->offset);
        //  //--+printf("break\n");
        //}
        char *p = nullptr;
        
        Plog *plog = nullptr;
        //3.1 load plog page 
        if(piece_Plog_Write_Pool->find(of_leaf)!=piece_Plog_Write_Pool->end()){
          plog = reinterpret_cast<Plog*>((*piece_Plog_Write_Pool)[of_leaf].first);
          //Plog *plog = Map<Plog>(of_leaf, 1, 1, p);
          if (plog->offset == 0){
            plog->offset = of_leaf + kLeafPageSize;
          }else{
            if (plog->offset != of_leaf + kLeafPageSize){
              mtr_->error_cnt++;
              memset(reinterpret_cast<char*>(plog),0,kPlogPageSize);
              plog->offset = of_leaf + kLeafPageSize;
              ////--+printf("plog offset error!\n");
            }
          }
        }else{
          final_Plog++;
          char *buf = (char *)malloc(sizeof(char) * kPlogPageSize);
          memset(buf, 0, kPlogPageSize);
          lseek(mtr_->global_fd, of_leaf+kLeafPageSize, 0);
          read(mtr_->global_fd, buf, kPlogPageSize);
          plog = reinterpret_cast<Plog *>(buf);
          plog->offset = of_leaf + kLeafPageSize;
          //plog = Map<Plog>(of_leaf, 1, 1, p);
          piece_Plog_Write_Pool->emplace(of_leaf,std::make_pair(buf,nullptr));
          //Plog *plog = Map<Plog>(of_leaf, 1, 1, p);
          if (plog->offset == 0){
            plog->offset = of_leaf + kLeafPageSize;
          }else{
            if (plog->offset != of_leaf + kLeafPageSize){
              memset(reinterpret_cast<char*>(plog),0,kPlogPageSize);
              plog->offset = of_leaf + kLeafPageSize;
              mtr_->error_cnt++;
              ////--+printf("plog offset error!\n");
            }
          }
        } 
        //check the correctness of plog
        if(plog->offset != of_leaf + kLeafPageSize){
          //error
          int x=0;
        }
        //3.2 do the minor compaction
        std::string value = it->second.second;
          //2023-02-26T13:56:04.057992Z	    3 Query	create table item (
          //i_id int not null, 
          //i_im_id int, 
          //i_name varchar(24), 
          //i_price decimal(5,2), 
          //i_data varchar(50),
          //PRIMARY KEY(i_id) ) Engine=InnoDB
          //hkc-modify-14
          std::string value1 = value.substr(0,16);
          std::string value2 = value.substr(16,20);
          std::string value3 = value.substr(36,16);
          std::string value4 = value.substr(52,16);
          std::string value5 = value.substr(68,16);
        if (InsertKVIntoPlog(plog, key.data(), value1.data(),value2.data(),value3.data(),value4.data(),value5.data(), cur_lsn) <=
          GetMaxKeys(0)){
          mtr_->plog_insert += 1;

          //Plog_Write_Pool.push_back(std::make_pair(reinterpret_cast<char*>(plog),nullptr));
        }else{
          total_leaf++;
          // do the major compaction for the plog, i.e., merge to leaf page

          // plog is full, we need to merge plog with its leaf node
          //LeafNode *leaf_node = Map<LeafNode>(of_leaf, 1, 0, p);
          LeafNode *leaf_node = nullptr;
          
          char* buf = (*piece_Plog_Write_Pool)[of_leaf].second;
          //3.3 load data page
          if(buf!=nullptr){
            leaf_node = reinterpret_cast<LeafNode*>(buf);
            if(leaf_node->offset!=of_leaf){
              leaf_node->offset = of_leaf;
            }
          }else{
            final_leaf++;
            //noted that, we do not want to pollute the buffer pool 
            //when we want to do the major compaction, so we directly load the page
            buf = (char *)malloc(sizeof(char) * kLeafPageSize);
            memset(buf, 0, kLeafPageSize);
            lseek(mtr_->global_fd, of_leaf, 0);
            read(mtr_->global_fd, buf, kLeafPageSize);
            char* ori = (char*)malloc(sizeof(char)*kLeafPageSize);
            memcpy(ori,buf,kLeafPageSize);
            char* tmp_read = (char*)malloc(sizeof(char)*kLeafPageSize);
            ColumnToRow(&ori[0],&tmp_read[0]);
            // mu.unlock();
            memcpy(buf, &tmp_read[0], kLeafPageSize);
            free(tmp_read);
            free(ori);
            (*piece_Plog_Write_Pool)[of_leaf].second= buf;
            leaf_node = reinterpret_cast<LeafNode*>(buf);
            leaf_node->offset = of_leaf;
            
          }

          //3.4 do the major compaction, i.e., merge KVs into data page
          
          //note: now we assume the data page will not split, 
          //      but actually it do happen, at aother API (serial IO), 
          //      we implement the page split
          //hkc-modify-15
          //3.4.1 merge plog KVs into data page
          for (int i = 0; i < kOrder_orders_Plog; i++){
            if (InsertKVIntoLeafNode( leaf_node,
                                      plog->records[i].key, 
                                      plog->records[i].value_1, 
                                      plog->records[i].value_2, 
                                      plog->records[i].value_3, 
                                      plog->records[i].value_4,
                                      plog->records[i].value_5, 
                                      plog->records[i].cur_lsn
                                    ) <= GetMaxKeys(1)){
            
              // UnMap<LeafNode>(leaf_node,1,0);
              mtr_->leaf_insert += 1;
            }else{
              //split happens, pending...
              //--+printf("split happens, need to handle!\n");
            }
          }

          //3.4.2
          //reset plog
          memset(plog, 0, kPlogPageSize);   
          //(*piece_Plog_Write_Pool)[of_leaf].first= nullptr;
          //free(plog);       
        }

        //4. persist the data page and leaf page based on iouring
        // pending: 
        //do IO every 200 pages
        //1027



        
      }
    }
        ////--+printf("[%d,%d,%d,%d,%d,%d]\n",total_KV,final_KV,total_Plog,final_Plog,total_leaf,final_leaf);
        UnMap<Plog>(0x0,1,101);
        //5. reclaim

    //pending for implement

  return;
}
*/

void BPlusOrdersTree::BackGround_GLog_Func()
{
  int total_KV=0;
  int final_KV=0;
  int total_Plog =0;
  int final_Plog =0;
  int total_leaf =0;
  int final_leaf=0;
  // check whether to conduct the minor compaction and major compaction
  std::map<off_t, std::pair<std::string,std::string>>* pending_Seg =nullptr;
  std::map<off_t, std::pair<char*,char*>>* piece_Plog_Write_Pool= nullptr;
  int index =mtr_->Q_GLog.size()-2;
  if(index<0){
    //--+printf("???\n");
  }

  piece_Plog_Write_Pool =new std::map<off_t, std::pair<char*,char*>>;
  mtr_->Plog_Write_Pool.push_back(piece_Plog_Write_Pool);
  pending_Seg = mtr_->Q_GLog.at(index);
  //1. merge KVs and only reserve unique and newest KVs
  // it can be replaced by GlogIndex since GLog index has already held the newest KVs
  std::unordered_map<std::string, off_t>* NewestK = mtr_->GLog_Index.at(index); // k:lsn
 


  std::map<off_t,std::pair<int,int>>* EmptyPages = new std::map<off_t,std::pair<int,int>>; //offset: <cnt,reused>
  //plog_buffer_id=0;
  /*0.1 prepare pages with offset*/
  for(std::unordered_map<std::string, off_t>::iterator it = NewestK->begin();it!=NewestK->end();it++){
    off_t of_leaf = GetLeafOffset(it->first.c_str()); //get offset by key
    if(EmptyPages->find(of_leaf) == EmptyPages->end()){
      EmptyPages->emplace(of_leaf,std::make_pair(0,1));
      //char *buf = &plog_buffer[plog_buffer_id*kPlogPageSize];
      char* buf = (char*)malloc(sizeof(char)*kPlogPageSize);
      memset(buf,0,kPlogPageSize);
      piece_Plog_Write_Pool->emplace(of_leaf,std::make_pair(buf,nullptr));
      //plog_buffer_id++;
    }else{
      int cnt = (*EmptyPages)[of_leaf].second;
      cnt++;
      (*EmptyPages)[of_leaf]=std::make_pair(0,cnt);
    }
  }
  int repeat =0;
  if(mtr_->Main2Back.size()>0){
    std::map<off_t, std::pair<char*,char*>>* pend_piece_Plog_Write_Pool = mtr_->Main2Back.at(0).second;
    repeat += ApplyPendPages(pend_piece_Plog_Write_Pool, piece_Plog_Write_Pool, EmptyPages);
  }
  if(mtr_->Back2Main.size()>0){
    for(int i=0;i<mtr_->Back2Main.size();i++){
      std::map<off_t, std::pair<char*,char*>>* pend_piece_Plog_Write_Pool = mtr_->Back2Main.at(i).second;
      repeat+=ApplyPendPages(pend_piece_Plog_Write_Pool, piece_Plog_Write_Pool, EmptyPages);
    }

  }

  /*0.2 read all plog pages by liburing*/
  int app =0;
  //if (EmptyPages->size()>kMaxUniqueKInGLogSeg){
  //  //--+printf("error alloc empty pages\n");
  //}
  if(repeat < piece_Plog_Write_Pool->size()){
    int read_batch_size =200;
    struct io_uring_sqe *sqe_read;
    struct io_uring_cqe *cqe_read;
    
    struct iovec iov_read[read_batch_size];
    struct io_uring ring_;
    io_uring_queue_init(read_batch_size, &ring_, 0);
    sqe_read = io_uring_get_sqe(&ring_);
    //read pages in batch
    
    std::map<off_t,std::pair<int,int>>::iterator tmp = EmptyPages->begin();
    
    while (app < EmptyPages->size()){
      int i=0;
      for(std::map<off_t,std::pair<int,int>>::iterator it = tmp;it!=EmptyPages->end();
        it++){
        app++;
        if(it->second.first == 1){
          continue;
        }
        sqe_read = io_uring_get_sqe(&ring_);
        off_t offset = it->first;
        iov_read[i].iov_base = (*piece_Plog_Write_Pool)[offset].first;
        iov_read[i].iov_len = kPlogPageSize;
        io_uring_prep_readv(sqe_read,mtr_->global_fd,&iov_read[i],1,offset+kLeafPageSize);
        i++;
        if(i == read_batch_size){
          tmp=it;
          tmp++;
          break;
        }
      }
      if(i>0){
        int ret = io_uring_submit(&ring_);
        for (int j = 0; j < i; j++)
        {
          ret = io_uring_wait_cqe(&ring_, &cqe_read);
          io_uring_cqe_seen(&ring_, cqe_read);
          //reclaim plog and leafnode copy
        }
      }
    }
    io_uring_queue_exit(&ring_);

  }
  if(app!=EmptyPages->size()){
      //--+printf("error read batch!\n");
  }
  EmptyPages->clear();
  delete EmptyPages;
  
  int check_cnt=0;
  for(std::map<off_t, std::pair<char*,char*>>::iterator it = piece_Plog_Write_Pool->begin();it!=piece_Plog_Write_Pool->end();it++){
    Plog* p = reinterpret_cast<Plog*>(it->second.first);
    if(p->offset!=0 || p->count !=0){
      check_cnt+=1;
    }
    if(p->count>5){
      int x=0;
    }
  }


  //2. do Plog retrieve and in-place update
  for(std::map<off_t, std::pair<std::string,std::string>>::iterator it = pending_Seg->begin(); it!=pending_Seg->end();it++){    
      total_KV++;
      off_t cur_lsn = it->first;
      std::string key = it->second.first;
      off_t now_lsn = (*NewestK)[key];
      if(now_lsn > cur_lsn){
        //skip the older KVs
        continue;
      }else{
        final_KV++;
        total_Plog++;
        //do the apply (i.e., minor compaction)
        off_t of_leaf = GetLeafOffset(it->second.first.c_str()); //get offset by key
        //pendingWrites_In_Glog.emplace(of_leaf,1);
        //if(of_leaf == 1694560256){
        //  char* cc = nullptr;
        //  LeafNode* te= Map<LeafNode>(of_leaf,1,0,cc);
        //  //--+printf("%ld\n",te->offset);
        //  //--+printf("break\n");
        //}
        char *p = nullptr;
        
        Plog *plog = nullptr;
        //3.1 load plog page 
        if(piece_Plog_Write_Pool->find(of_leaf)!=piece_Plog_Write_Pool->end()){
          plog = reinterpret_cast<Plog*>((*piece_Plog_Write_Pool)[of_leaf].first);
          //Plog *plog = Map<Plog>(of_leaf, 1, 1, p);
          if (plog->offset == 0){
            plog->offset = of_leaf + kLeafPageSize;
          }else{
            if (plog->offset != of_leaf + kLeafPageSize){
              mtr_->error_cnt++;
              memset(reinterpret_cast<char*>(plog),0,kPlogPageSize);
              plog->offset = of_leaf + kLeafPageSize;
              ////--+printf("plog offset error!\n");
            }
          }
        }else{
          final_Plog++;
          char *buf = (char *)malloc(sizeof(char) * kPlogPageSize);
          memset(buf, 0, kPlogPageSize);
          lseek(mtr_->global_fd, of_leaf+kLeafPageSize, 0);
          read(mtr_->global_fd, buf, kPlogPageSize);
          plog = reinterpret_cast<Plog *>(buf);
          plog->offset = of_leaf + kLeafPageSize;
          //plog = Map<Plog>(of_leaf, 1, 1, p);
          piece_Plog_Write_Pool->emplace(of_leaf,std::make_pair(buf,nullptr));
          //Plog *plog = Map<Plog>(of_leaf, 1, 1, p);
          if (plog->offset == 0){
            plog->offset = of_leaf + kLeafPageSize;
          }else{
            if (plog->offset != of_leaf + kLeafPageSize){
              memset(reinterpret_cast<char*>(plog),0,kPlogPageSize);
              plog->offset = of_leaf + kLeafPageSize;
              mtr_->error_cnt++;
              ////--+printf("plog offset error!\n");
            }
          }
        } 
        //3.2 do the minor compaction
        std::string value = it->second.second;
        bool overflow =0;
          //hkc-plog-8
          if(value[0]=='U'){
            //update operation, only need to record the deltas
            std::string value_carrier = value.substr(2,16);
            if(InsertDeltaIntoPlog(plog,key.data(),value_carrier.data(),cur_lsn)>GetMaxKeys(0)){
              overflow = true;
              mtr_->plog_insert += 1;
            }else{
              mtr_->plog_insert += 1;
            }          
          }else{
            std::string value1 = value.substr(0,16);
            std::string value2 = value.substr(16,20);
            std::string value3 = value.substr(36,16);
            std::string value4 = value.substr(52,16);
            std::string value5 = value.substr(68,16);
                
            if (InsertKVIntoPlog(plog, key.data(), value1.data(),value2.data(),value3.data(),value4.data(),value5.data(),cur_lsn) > GetMaxKeys(0)){
                mtr_->plog_insert += 1;
                overflow == true;
            }else{
                mtr_->plog_insert += 1;
            }
          }
              
          if(overflow == true){

            //hkc-ap-16
            //before compaction, handle the situation when ap freeze the major compaction
            if( plog->offset > mtr_->ap_left && plog->offset < mtr_->ap_right){
            //  plog->GetSmallestLSN() < mtr_->ap_lsn){
            //pending
            //we need also consider the key range!!!
              //the major compaction should be disabled
              char *sha_buf = (char *)malloc(sizeof(char) * kPlogPageSize);
              memset(sha_buf, 0, kPlogPageSize);
              memcpy(sha_buf,reinterpret_cast<char*>(plog),kPlogPageSize);
              memset(plog, 0, kPlogPageSize);
              plog->sha_plog_offset = mtr_->global_shadow_plog_off;
              lseek(mtr_->global_shadow_plog, mtr_->global_shadow_plog_off, 0);
              write(mtr_->global_shadow_plog, reinterpret_cast<char *>(sha_buf), kPlogPageSize);
              
              mtr_->global_shadow_plog_off+=kPlogPageSize;
              UnMap<Plog>(0x0,1,101);

              return;



            }

            total_leaf++;
            LeafNode *leaf_node = nullptr;
            char* buf = (*piece_Plog_Write_Pool)[of_leaf].second;
            //3.3 load data page
            if(buf!=nullptr){
              leaf_node = reinterpret_cast<LeafNode*>(buf);
              if(leaf_node->offset!=of_leaf){
                  leaf_node->offset = of_leaf;
              }
            }else{
              final_leaf++;
              //noted that, we do not want to pollute the buffer pool 
              //when we want to do the major compaction, so we directly load the page
              buf = (char *)malloc(sizeof(char) * kLeafPageSize);
              memset(buf, 0, kLeafPageSize);
              lseek(mtr_->global_fd, of_leaf, 0);
              read(mtr_->global_fd, buf, kLeafPageSize);
              char* ori = (char*)malloc(sizeof(char)*kLeafPageSize);
              memcpy(ori,buf,kLeafPageSize);
              char* tmp_read = (char*)malloc(sizeof(char)*kLeafPageSize);
              ColumnToRow(&ori[0],&tmp_read[0]);
              // mu.unlock();
              memcpy(buf, &tmp_read[0], kLeafPageSize);
              free(tmp_read);
              free(ori);
              (*piece_Plog_Write_Pool)[of_leaf].second= buf;
              leaf_node = reinterpret_cast<LeafNode*>(buf);
              leaf_node->offset = of_leaf;    
            }

            //3.4 do the major compaction, i.e., merge KVs into data page
              
            //note: now we assume the data page will not split, 
            //      but actually it do happen, at aother API (serial IO), 
            //      we implement the page split
            //hkc-modify-15
            //3.4.1 merge plog KVs into data page
            bool leaf_overflow =false;
            for (int i = 0; i < kOrder_orders_Plog; i++){
              if(plog->records[i].partial == true){
                //only update columns
                if(InsertDeltaKVIntoLeafNode( leaf_node,
                                          plog->records[i].key, 
                                          plog->records[i].value_3, 
                                          plog->records[i].cur_lsn)<= GetMaxKeys(1)){
                  mtr_->leaf_insert += 1;
                }else{
                  leaf_overflow = true;
                }
              }else{
                if(InsertKVIntoLeafNode( leaf_node,
                                          plog->records[i].key, 
                                          plog->records[i].value_1, 
                                          plog->records[i].value_2, 
                                          plog->records[i].value_3, 
                                          plog->records[i].value_4,
                                          plog->records[i].value_5,
                                          plog->records[i].cur_lsn
                                        )<= GetMaxKeys(1)){
                  mtr_->leaf_insert += 1;
                }else{
                  leaf_overflow = true;
                }
              }
              if(leaf_overflow == true){
                printf("Leaf overflow during blind update!!!\n");
                int x=1;
              }
            }

            //3.4.2
            //reset plog
            memset(plog, 0, kPlogPageSize);   
            //(*piece_Plog_Write_Pool)[of_leaf].first= nullptr;
            //free(plog);       
          }else{
            //it is ok and no major compaction
          }
      }
  }
  ////--+printf("[%d,%d,%d,%d,%d,%d]\n",total_KV,final_KV,total_Plog,final_Plog,total_leaf,final_leaf);
  UnMap<Plog>(0x0,1,101);

  return;
}



//hkc-modify-21
void BPlusOrdersTree::RowToColumn(char* initial_page, char* converted_page){
  LeafNode* row_leaf = reinterpret_cast<LeafNode*>(initial_page);
  if(row_leaf->count == 0 || row_leaf->records[0].cur_lsn == 0){
    printf("empty row page!!");
    memcpy(converted_page,initial_page,kLeafPageSize);
    return;
  }
  /*
const int kMaxKeySize_orders_ = 48; 36K
const int kMaxLSNSize_orders_ = 8;  8K
const int kMaxValueSize_orders_col1_ = 16;  16K  
const int kMaxValueSize_orders_col2_ = 20; 16K
const int kMaxValueSize_orders_col3_ = 16;  16K
const int kMaxValueSize_orders_col4_ = 16;  16K
const int kMaxValueSize_orders_col5_ = 16;  16K
const int kMaxValueSize_orders_col_all = 16+20+16+16+16;
  */
  row_leaf->offset_k = 4*1024;
  row_leaf->offset_lsn =(4+48)*1024;
  row_leaf->offset_v1 = (4+48+8)*1024;
  row_leaf->offset_v2 = (4+48+8+12)*1024;
  row_leaf->offset_v3 = (4+48+8+12+16)*1024;
  row_leaf->offset_v4 = (4+48+8+12+16+12)*1024;
  row_leaf->offset_v5 = (4+48+8+12+16+12+12)*1024;
  memset(converted_page,0,kLeafPageSize);
  char* buf = converted_page;
  off_t fence = 0;
  off_t unit_size = 4*1024;

  //copy meta data
  memcpy(&buf[fence],&(row_leaf->offset), sizeof(row_leaf->offset));
  //check 
  //off_t test_of = 0;
  //memcpy((char*)&test_of,&buf[0],8);


  fence+=sizeof(row_leaf->offset);
  memcpy(&buf[fence],&(row_leaf->offset_leaf), sizeof(row_leaf->offset_leaf));
  fence+=sizeof(row_leaf->offset_leaf);
  memcpy(&buf[fence],&(row_leaf->offset_k), sizeof(row_leaf->offset_k));
  fence+=sizeof(row_leaf->offset_k);
  memcpy(&buf[fence],&(row_leaf->offset_lsn), sizeof(row_leaf->offset_lsn));
  fence+=sizeof(row_leaf->offset_lsn);
  memcpy(&buf[fence],&(row_leaf->offset_v1), sizeof(row_leaf->offset_v1));
  fence+=sizeof(row_leaf->offset_v1);
  memcpy(&buf[fence],&(row_leaf->offset_v2), sizeof(row_leaf->offset_v2));
  fence+=sizeof(row_leaf->offset_v2);
  memcpy(&buf[fence],&(row_leaf->offset_v3), sizeof(row_leaf->offset_v3));
  fence+=sizeof(row_leaf->offset_v3);
  memcpy(&buf[fence],&(row_leaf->offset_v4), sizeof(row_leaf->offset_v4));
  fence+=sizeof(row_leaf->offset_v4);
  memcpy(&buf[fence],&(row_leaf->offset_v5), sizeof(row_leaf->offset_v5));
  fence+=sizeof(row_leaf->offset_v5);

  //hkc-ap-08
  memcpy(&buf[fence],&(row_leaf->sha_plog_offset), sizeof(row_leaf->sha_plog_offset));
  fence+=sizeof(row_leaf->sha_plog_offset);
  

  memcpy(&buf[fence],&(row_leaf->count), sizeof(row_leaf->count));
  fence+=sizeof(row_leaf->count);

  memcpy(&buf[fence],&(row_leaf->dirty), sizeof(row_leaf->dirty));
  fence+=sizeof(row_leaf->dirty);

  memcpy(&buf[fence],&(row_leaf->left), sizeof(row_leaf->left));
  fence+=sizeof(row_leaf->left);

  memcpy(&buf[fence],&(row_leaf->right), sizeof(row_leaf->right));
  fence+=sizeof(row_leaf->right);

  memcpy(&buf[fence],&(row_leaf->parent), sizeof(row_leaf->parent));
  fence+=sizeof(row_leaf->parent);

  memcpy(&buf[fence],&(row_leaf->newest_lsn), sizeof(row_leaf->newest_lsn));
  fence+=sizeof(row_leaf->newest_lsn);

  memcpy(&buf[fence],&(row_leaf->oldest_lsn), sizeof(row_leaf->oldest_lsn));
  fence+=sizeof(row_leaf->oldest_lsn);
  
  memcpy(&buf[fence],&(row_leaf->origin_lsn), sizeof(row_leaf->origin_lsn));
  fence+=sizeof(row_leaf->origin_lsn);
  
  
  if(fence > 4*1024){
    printf("overflow!\n");
  } 
  //convert row as column and fit in sub pages

    /*
      const int kMaxKeySize_orders_ = 16; 16K
      const int kMaxValueSize_orders_col1_ = 16; 16K
      const int kMaxValueSize_orders_col2_ = 24; 24K
      const int kMaxValueSize_orders_col3_ = 8; 8K
      const int kMaxValueSize_orders_col4_ = 50; 50K
    */

  off_t k_of  = row_leaf->offset_k;
  off_t lsn_of = row_leaf->offset_lsn;
  off_t v1_of = row_leaf->offset_v1;
  off_t v2_of = row_leaf->offset_v2;
  off_t v3_of = row_leaf->offset_v3;
  off_t v4_of = row_leaf->offset_v4;
  off_t v5_of = row_leaf->offset_v5;
  for(int i=0; i<row_leaf->count;i++){
    memcpy(&buf[k_of],&(row_leaf->records[i].key),kMaxKeySize_orders_);
    k_of+=kMaxKeySize_orders_;
    memcpy(&buf[lsn_of],&(row_leaf->records[i].cur_lsn),sizeof(row_leaf->records[i].cur_lsn));
    lsn_of+=sizeof(row_leaf->records[i].cur_lsn);
    memcpy(&buf[v1_of],&(row_leaf->records[i].value_1),kMaxValueSize_orders_col1_);
    v1_of+=kMaxValueSize_orders_col1_;
    memcpy(&buf[v2_of],&(row_leaf->records[i].value_2),kMaxValueSize_orders_col2_);
    v2_of+=kMaxValueSize_orders_col2_;
    memcpy(&buf[v3_of],&(row_leaf->records[i].value_3),kMaxValueSize_orders_col3_);
    v3_of+=kMaxValueSize_orders_col3_;
    memcpy(&buf[v4_of],&(row_leaf->records[i].value_4),kMaxValueSize_orders_col4_);
    v4_of+=kMaxValueSize_orders_col4_;
    memcpy(&buf[v5_of],&(row_leaf->records[i].value_5),kMaxValueSize_orders_col5_);
    v5_of+=kMaxValueSize_orders_col5_;
  }


  return; 
}


//hkc-modify-22
void BPlusOrdersTree::ColumnToRow(char* column_page, char* row_page){
  memset(row_page,0,kLeafPageSize);
  LeafNode* row_leaf = reinterpret_cast<LeafNode*>(row_page);
  row_leaf->offset_k = 4*1024;
  row_leaf->offset_lsn =(4+48)*1024;
  row_leaf->offset_v1 = (4+48+8)*1024;
  row_leaf->offset_v2 = (4+48+8+12)*1024;
  row_leaf->offset_v3 = (4+48+8+12+16)*1024;
  row_leaf->offset_v4 = (4+48+8+12+16+12)*1024;
  row_leaf->offset_v5 = (4+48+8+12+16+12+12)*1024;
  
  char* buf = column_page;
  off_t fence = 0;

  //restore meta data
  //memcpy(&buf[fence],&(row_leaf->offset), sizeof(row_leaf->offset));
  //row_leaf->offset = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->offset,&buf[fence],sizeof(row_leaf->offset));
  fence+=sizeof(row_leaf->offset);
  //memcpy(&buf[fence],&(row_leaf->offset_leaf), sizeof(row_leaf->offset_leaf));
  //row_leaf->offset_leaf = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->offset_leaf,&buf[fence],sizeof(row_leaf->offset_leaf));
  fence+=sizeof(row_leaf->offset_leaf);
  //memcpy(&buf[fence],&(row_leaf->offset_k), sizeof(row_leaf->offset_k));
  //row_leaf->offset_k = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->offset_k,&buf[fence],sizeof(row_leaf->offset_k));
  fence+=sizeof(row_leaf->offset_k);
  //memcpy(&buf[fence],&(row_leaf->offset_lsn), sizeof(row_leaf->offset_lsn));
  //row_leaf->offset_lsn = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->offset_lsn,&buf[fence],sizeof(row_leaf->offset_lsn));
  fence+=sizeof(row_leaf->offset_lsn);
  //memcpy(&buf[fence],&(row_leaf->offset_v1), sizeof(row_leaf->offset_v1));
  //row_leaf->offset_v1 = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->offset_v1,&buf[fence],sizeof(row_leaf->offset_v1));
  fence+=sizeof(row_leaf->offset_v1);
  //memcpy(&buf[fence],&(row_leaf->offset_v2), sizeof(row_leaf->offset_v2));
  //row_leaf->offset_v2 = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->offset_v2,&buf[fence],sizeof(row_leaf->offset_v2));
  fence+=sizeof(row_leaf->offset_v2);
  //memcpy(&buf[fence],&(row_leaf->offset_v3), sizeof(row_leaf->offset_v3));
  //row_leaf->offset_v3 = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->offset_v3,&buf[fence],sizeof(row_leaf->offset_v3));
  fence+=sizeof(row_leaf->offset_v3);
  //memcpy(&buf[fence],&(row_leaf->offset_v4), sizeof(row_leaf->offset_v4));
  //row_leaf->offset_v4 = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->offset_v4,&buf[fence],sizeof(row_leaf->offset_v4));
  fence+=sizeof(row_leaf->offset_v4);
  
  memcpy((char*)&row_leaf->offset_v5,&buf[fence],sizeof(row_leaf->offset_v5));
  fence+=sizeof(row_leaf->offset_v5);
  //memcpy(&buf[fence],&(row_leaf->plog_offset), sizeof(row_leaf->plog_offset));
  //row_leaf->plog_offset = reinterpret_cast<off_t>(&buf[fence]);

  //hkc-ap-09
  memcpy((char*)&row_leaf->sha_plog_offset,&buf[fence],sizeof(row_leaf->sha_plog_offset));
  fence+=sizeof(row_leaf->sha_plog_offset);
  

  //memcpy(&buf[fence],&(row_leaf->count), sizeof(row_leaf->count));
  //row_leaf->count = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->count,&buf[fence],sizeof(row_leaf->count));
  fence+=sizeof(row_leaf->count);

  //memcpy(&buf[fence],&(row_leaf->dirty), sizeof(row_leaf->dirty));
  //row_leaf->dirty = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->dirty,&buf[fence],sizeof(row_leaf->dirty));
  fence+=sizeof(row_leaf->dirty);

  //memcpy(&buf[fence],&(row_leaf->left), sizeof(row_leaf->left));
  //row_leaf->left = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->left,&buf[fence],sizeof(row_leaf->left));
  fence+=sizeof(row_leaf->left);

  //memcpy(&buf[fence],&(row_leaf->right), sizeof(row_leaf->right));
  row_leaf->right = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->right,&buf[fence],sizeof(row_leaf->right));
  fence+=sizeof(row_leaf->right);

  //memcpy(&buf[fence],&(row_leaf->parent), sizeof(row_leaf->parent));
  //row_leaf->parent = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->parent,&buf[fence],sizeof(row_leaf->parent));
  fence+=sizeof(row_leaf->parent);

  //memcpy(&buf[fence],&(row_leaf->newest_lsn), sizeof(row_leaf->newest_lsn));
  //row_leaf->newest_lsn = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->newest_lsn,&buf[fence],sizeof(row_leaf->newest_lsn));
  fence+=sizeof(row_leaf->newest_lsn);

  //memcpy(&buf[fence],&(row_leaf->oldest_lsn), sizeof(row_leaf->oldest_lsn));
  //row_leaf->oldest_lsn = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->oldest_lsn,&buf[fence],sizeof(row_leaf->oldest_lsn));
  fence+=sizeof(row_leaf->oldest_lsn);
  
  //memcpy(&buf[fence],&(row_leaf->origin_lsn), sizeof(row_leaf->origin_lsn));
  //row_leaf->origin_lsn = reinterpret_cast<off_t>(&buf[fence]);
  memcpy((char*)&row_leaf->origin_lsn,&buf[fence],sizeof(row_leaf->origin_lsn));
  fence+=sizeof(row_leaf->origin_lsn);
  
  
  if(fence > 4*1024){
    printf("overflow!\n");
  } 
  //convert row as column and fit in sub pages

    /*
      const int kMaxKeySize_orders_ = 16; 16K
      const int kMaxValueSize_orders_col1_ = 16; 16K
      const int kMaxValueSize_orders_col2_ = 24; 24K
      const int kMaxValueSize_orders_col3_ = 8; 8K
      const int kMaxValueSize_orders_col4_ = 50; 50K
    */

  off_t k_of  = row_leaf->offset_k;
  off_t lsn_of = row_leaf->offset_lsn;
  off_t v1_of = row_leaf->offset_v1;
  off_t v2_of = row_leaf->offset_v2;
  off_t v3_of = row_leaf->offset_v3;
  off_t v4_of = row_leaf->offset_v4;
  off_t v5_of = row_leaf->offset_v5;
  for(int i=0; i<row_leaf->count;i++){
    memcpy(&(row_leaf->records[i].key),&buf[k_of],kMaxKeySize_orders_);
    k_of+=kMaxKeySize_orders_;
    memcpy(&(row_leaf->records[i].cur_lsn),&buf[lsn_of],sizeof(row_leaf->records[i].cur_lsn));
    lsn_of+=sizeof(row_leaf->records[i].cur_lsn);
    memcpy(&(row_leaf->records[i].value_1),&buf[v1_of],kMaxValueSize_orders_col1_);
    v1_of+=kMaxValueSize_orders_col1_;
    memcpy(&(row_leaf->records[i].value_2),&buf[v2_of],kMaxValueSize_orders_col2_);
    v2_of+=kMaxValueSize_orders_col2_;
    memcpy(&(row_leaf->records[i].value_3),&buf[v3_of],kMaxValueSize_orders_col3_);
    v3_of+=kMaxValueSize_orders_col3_;
    memcpy(&(row_leaf->records[i].value_4),&buf[v4_of],kMaxValueSize_orders_col4_);
    v4_of+=kMaxValueSize_orders_col4_;
    memcpy(&(row_leaf->records[i].value_5),&buf[v5_of],kMaxValueSize_orders_col5_);
    v5_of+=kMaxValueSize_orders_col5_;
  }


  return; 
}


//hkc-gb-04
off_t BPlusOrdersTree::GetOnlyLeafOffset(const std::string &key){
  off_t of_leaf = GetLeafOffset(key.data());
  return of_leaf;
}
//hkc-gb-05
char* BPlusOrdersTree::GetLeafCopy(const std::string &key){
  off_t of_leaf = GetLeafOffset(key.data());
  char* p = (char*)malloc(sizeof(char)*kPlogPageSize);
  LeafNode *leaf_node = Map<LeafNode>(of_leaf, 1, 4, p);
  free(p);
  return reinterpret_cast<char*>(leaf_node);
  
}

//hkc-gb-06
void BPlusOrdersTree::UpdateLeafCopy(const std::string &key, const std::string &value, off_t lsn, char* leaf_copy){
  LeafNode* leaf = reinterpret_cast<LeafNode*>(&leaf_copy[0]);
  std::string value1 = value.substr(0,16);
  std::string value2 = value.substr(16,20);
  std::string value3 = value.substr(36,16);
  std::string value4 = value.substr(52,16);
  std::string value5 = value.substr(68,16);
  if (InsertKVIntoLeafNode(leaf, key.data(),  value1.data(),value2.data(),value3.data(),value4.data(),value5.data(), lsn) <= GetMaxKeys(1)){
      //it is OK and now we do not consider of the split operations
      int x=1;
  }else{
    int x=2;
  }
  return;
}

//hkc-gb-07
void BPlusOrdersTree::GetRowRecord(const std::string &key, std::string &value, char* leaf_copy) {
  LeafNode* leaf  = reinterpret_cast<LeafNode*>(&leaf_copy[0]);
  int index = GetIndexFromLeafNode(leaf, key.data());
  char* ptr = leaf->ValueALL(index);
  std::string tmp_value(ptr,ptr+kMaxValueSize_orders_col_all);
  value = tmp_value;
  free(ptr);
  return ;

}
//hkc-gb-08
void BPlusOrdersTree::TriggerFlush(const off_t* offsets, const int size, char* evict_buf){
  int batch_size = 200;
  struct io_uring_sqe *sqe_flush;
  struct io_uring_cqe *cqe_flush;
  struct iovec iov_lru[batch_size];
  struct io_uring ring_flush_;
  io_uring_queue_init(batch_size, &ring_flush_, 0);
  //sqe_flush = io_uring_get_sqe(&ring_flush_);
  std::vector<char*> tmp_cache;
  for (int i=0;i<size;i++){
    tmp_cache.push_back((char*)malloc(sizeof(char)*kLeafPageSize));
  }
  //char tmp_cache[size][kLeafPageSize] ={'\0'};
  for(int i=0;i<size;i++){
    sqe_flush = io_uring_get_sqe(&ring_flush_);
    RowToColumn(reinterpret_cast<char*>(&evict_buf[i*1024*128]),tmp_cache[i]);
    iov_lru[i].iov_base = tmp_cache[i];
    iov_lru[i].iov_len = kLeafPageSize;
    io_uring_prep_writev(sqe_flush, mtr_->global_fd, &iov_lru[i], 1, offsets[i]);
  }
  int ret = io_uring_submit(&ring_flush_);
  for (int j = 0; j < size; j++)
  {
    ret = io_uring_wait_cqe(&ring_flush_, &cqe_flush);
    io_uring_cqe_seen(&ring_flush_, cqe_flush);
  }
  io_uring_queue_exit(&ring_flush_);
  for (int i=0;i<size;i++){
    char* buf = tmp_cache[i];
    free(buf);
    //tmp_cache.push_back((char*)malloc(sizeof(char)*kLeafPageSize));
  }
  return;
}



void BPlusOrdersTree::Put(const std::string &key, const std::string &value, off_t lsn)
{
    // for plog, just load the plog from disk, there is no disk for it
  if (mtr_->warmup_flag == 1 && mtr_->plog_flag == 1)
  {
    int x = 1;
    auto tmp1 = std::chrono::steady_clock::now();
    WarmUpVirtualPageCache(x);
    auto tmp2 = std::chrono::steady_clock::now();
    mtr_->warmup_flag = 0;
    //--+printf("+++++++++++++++++++++\n warmup done, time used: %d ms +++++++++++++++++++++\n", std::chrono::duration_cast<std::chrono::microseconds>(tmp2 - tmp1).count() / 1000);
  }

  if(mtr_->Q_GLog.size()>1){

    if(mtr_->add_new == 1){
      //mu.lock();
      auto t1 = std::chrono::steady_clock::now();
      mtr_->add_new =0;
      BackGround_GLog_Func();
      auto t2 = std::chrono::steady_clock::now();
      mtr_->io_consume +=std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
      //mu.unlock();
    }

    //if(WaitForPersist[0]<=0){
    //  mu.lock();
    //  UnMap<Plog>(0x0,1,102);
    //  mu.unlock();
    //}
  }
  if(mtr_->plog_flag == 1){ //temprorary disable
    //1. write wal
    log_obj->log_lsn = lsn;
    log_obj->log_offset = mtr_->global_log_offset;
    log_obj->page_offset = 0;
    log_obj->UpdateKV(key.data(), value.data());
    mtr_->global_log_offset += sizeof(*log_obj);
    log_obj->next_log_offset = mtr_->global_log_offset;
    char *log_ptr = reinterpret_cast<char *>(log_obj);
   
    // 2. register in-memory version
    off_t tmp_of = lseek(mtr_->global_fd_log,0,SEEK_CUR);
    //we need to convert row page as column page before flush
    int z1 = write(mtr_->global_fd_log,log_ptr, sizeof(*log_obj));

    //2. directly insert into glog buffer
    std::map<off_t, std::pair<std::string,std::string>> *Q_GLog_seg = nullptr;
    std::unordered_map<std::string, off_t> * index_seg = nullptr;
    //mu.lock();
    if(mtr_->Q_GLog.size()==0){
      Q_GLog_seg =new std::map<off_t, std::pair<std::string,std::string>>;
      index_seg = new std::unordered_map<std::string, off_t>;
      mtr_->GLog_Index.push_back(index_seg);
      mtr_->Q_GLog.push_back(Q_GLog_seg);
    }else{
      Q_GLog_seg = mtr_->Q_GLog.back();
      index_seg = mtr_->GLog_Index.back();
    }
    //mu.unlock();
    Q_GLog_seg->emplace(lsn, std::make_pair(key,value));
    if(index_seg->find(key)!=index_seg->end()){
      (*index_seg)[key]=lsn;
    }else{
      (*index_seg)[key]=lsn;
    }
    if(index_seg->size()>kMaxUniqueKInGLogSeg){
      //mu.lock();
      mtr_->Q_GLog.push_back(new std::map<off_t, std::pair<std::string,std::string>>);
      mtr_->GLog_Index.push_back(new std::unordered_map<std::string, off_t>);
      mtr_->add_new=1;
      //mu.lock();
    }
    return;
  }



  // 1. Find Leaf node.

  off_t of_leaf = GetLeafOffset(key.data());

  if (mtr_->plog_flag == -1 && mtr_->normal_recovery_flag != 1)
  {
    //=======================A SIMPLE WAL=============================

    // 1*. update log object
    log_obj->log_lsn = lsn;
    log_obj->log_offset = mtr_->global_log_offset;
    log_obj->page_offset = of_leaf;
    log_obj->UpdateKV(key.data(), value.data());
    mtr_->global_log_offset += sizeof(*log_obj);
    log_obj->next_log_offset = mtr_->global_log_offset;
    
    /*
    // 3. check the log record
    if (mtr_->global_lsn == -1)
    {
      int offset = 0;
      for (int i = 1; i < 0; i++)
      {
        close(mtr_->global_fd_log);
        int fp = open("/media/hkc/csd_3/Test_Plog_dir/logFILE", O_DIRECT, O_CREAT | O_RDWR, 0600);
        mtr_->global_fd_log = fp;
        char *log_ptr_check = (char *)malloc(sizeof(*log_obj));
        memset(log_ptr_check, 0, sizeof(*log_obj));
        //int cur_offset = ftell(fp);
        //--+printf("check log!\n");
        lseek(fp, offset, 0);
        read(fp, log_ptr_check, sizeof(*log_obj));
        close(fp);
        LogRecord *test = reinterpret_cast<LogRecord *>(log_ptr_check);
        offset = test->next_log_offset;
        free(log_ptr_check);
      }
    }
    */
    //=======================END OF A SIMPLE WAL=======================
  }
 
  char *p = nullptr;
  LeafNode *leaf_node = Map<LeafNode>(of_leaf, 1, 0, p);
  if(leaf_node == nullptr){
    return;
  }
  //UnMap<LeafNode>(leaf_node,1,0);
  // int s = sizeof(leaf_node);
  // int z = sizeof(*leaf_node);
  if(leaf_node->offset == 20480){
    int x=0;
  }
  if (mtr_->plog_flag == -1 && mtr_->normal_recovery_flag == 10)
  {
    int index = GetIndexFromLeafNode(leaf_node, key.data());
    if (index != -1)
    {
      off_t now_lsn = leaf_node->records[index].cur_lsn;
      if (now_lsn >= lsn)
      {
        mtr_->normal_skipped++;
        UnMap<LeafNode>(leaf_node, 1, 0);
        return;
      }
    }
  }
  //hkc-modify-16
    std::string value1 = value.substr(0,16);
          std::string value2 = value.substr(16,20);
          std::string value3 = value.substr(36,16);
          std::string value4 = value.substr(52,16);
          std::string value5 = value.substr(68,16);
  if (InsertKVIntoLeafNode(leaf_node, key.data(), value1.data(),value2.data(),value3.data(),value4.data(),value5.data(), lsn) <=
      GetMaxKeys(1))
  {
    // 2.If records of leaf node less than or equals kOrder_orders_ - 1 then finish.
    // UnMap<LeafNode>(leaf_node);
    if (mtr_->FLU_disabled == 0 && mtr_->InFLUOrNot.find(leaf_node->offset) == mtr_->InFLUOrNot.end())
    {
      if (mtr_->FLU_max_lsn == 0 || leaf_node->oldest_lsn > mtr_->FLU_max_lsn)
      {
       mtr_->FLU_max_lsn = leaf_node->oldest_lsn;
      }
      if (mtr_->FLU_min_lsn == 0)
      {
        mtr_->FLU_min_lsn = leaf_node->oldest_lsn;
      }
      if (leaf_node->oldest_lsn > mtr_->FLU_min_lsn && leaf_node->oldest_lsn < mtr_->FLU_max_lsn)
      {
        // //--+printf("error in FLU write!\n");
      }

      InsertDirty<LeafNode>(leaf_node->offset);
    }
    UnMap<LeafNode>(leaf_node, 1, 0);
    if (mtr_->plog_flag == -1 && mtr_->normal_recovery_flag != 1)
    {
      // when baseline persist log record
      // persist to log file
      char *log_ptr = reinterpret_cast<char *>(log_obj);
      off_t tmp_of = lseek(mtr_->global_fd_log,0,SEEK_CUR);
      //int x=write(global_fd_log, log_ptr, sizeof(*log_obj));
      int z2=write(mtr_->global_fd_log, log_ptr, sizeof(*log_obj));
      //close(fp);
    }
    return;
  }
  if (mtr_->new_write_flag == 1)
  {
    // //--+printf("should be update only!!!\n");
  }
  if (mtr_->FLU_disabled == 0 && mtr_->InFLUOrNot.find(leaf_node->offset) == mtr_->InFLUOrNot.end())
  {
    if (mtr_->FLU_max_lsn == 0 || leaf_node->oldest_lsn > mtr_->FLU_max_lsn)
    {
      mtr_->FLU_max_lsn = leaf_node->oldest_lsn;
    }
    if (mtr_->FLU_min_lsn == 0)
    {
      mtr_->FLU_min_lsn = leaf_node->oldest_lsn;
    }
    if (leaf_node->oldest_lsn > mtr_->FLU_min_lsn && leaf_node->oldest_lsn < mtr_->FLU_max_lsn)
    {
      // //--+printf("error in FLU write!\n");
    }
    InsertDirty<LeafNode>(leaf_node->offset);
  }
  UnMap<LeafNode>(leaf_node, 1, 0);
  // 3. Split leaf node to two leaf nodes.

  LeafNode *split_node = SplitLeafNode(leaf_node);
  if (mtr_->plog_flag == -1 && mtr_->normal_recovery_flag != 1)
  {
    // when baseline persist log record
    // persist to log file
    int index = GetIndexFromLeafNode(split_node, key.data());
    if (index != -1)
    {
      log_obj->page_offset = split_node->offset;
    }
    char *log_ptr = reinterpret_cast<char *>(log_obj);
    off_t tmp_of = lseek(mtr_->global_fd_log,0,SEEK_CUR);
    int z3 = write(mtr_->global_fd_log, log_ptr,sizeof(*log_obj));
    //close(fp);
  }

  const char *mid_key = split_node->FirstKey();
  IndexNode *parent_node = GetOrCreateParent(leaf_node);
  int x = sizeof(parent_node);
  int y = sizeof(*parent_node);
  // int size=sizeof(*parent_node);
  UnMap<IndexNode>(parent_node, 0, 0);
  off_t of_parent = leaf_node->parent;
  split_node->parent = of_parent;

  // 4.Insert key to parent of splited leaf nodes and
  // link two splited left nodes to parent.
  if (InsertKeyIntoIndexNode(parent_node, mid_key, leaf_node, split_node, lsn) <=
      GetMaxIndexKeys())
  {
    // UnMap<LeafNode>(leaf_node);
    // UnMap<LeafNode>(split_node);
    // UnMap<IndexNode>(parent_node);
    return;
  }

  // 5.Split index node from bottom to up repeatedly
  // until count <= kOrder_orders_ - 1.
  size_t count;
  do
  {
    IndexNode *child_node = parent_node;
    char *p = nullptr;
    Map<IndexNode>(child_node->offset, 0, 0, p);
    UnMap<IndexNode>(child_node, 0, 0);
    IndexNode *split_node = SplitIndexNode(child_node);
    UnMap<IndexNode>(split_node, 0, 0);
    const char *mid_key = child_node->Key(child_node->count);
    parent_node = GetOrCreateParent(child_node);
    UnMap<IndexNode>(parent_node, 0, 0);
    of_parent = child_node->parent;
    split_node->parent = of_parent;
    count =
        InsertKeyIntoIndexNode(parent_node, mid_key, child_node, split_node, lsn);

  } while (count > GetMaxIndexKeys());
  // UnMap<IndexNode>(parent_node);
}


/*
bool BPlusOrdersTree::Get(const std::string &key, std::string &value) const
{

  off_t of_leaf = GetLeafOffset(key.data());
  
 // if(of_leaf == 1694560256){
  //  int x =0;
  //}
  // need to warm up (build) the virtual page (in-memory) firstly
  if (mtr_->plog_flag != 1)
  {
    // baseline
    char *p = nullptr;
    LeafNode *leaf_node = Map<LeafNode>(of_leaf, 1, 0, p);
    if(block_cache_->Check_Leaf(of_leaf)==1){
      mtr_->hits_bp++;
    }else{
      mtr_->disk_read++;
    }
    int index = GetIndexFromLeafNode(leaf_node, key.data());
    if (index == -1)
    {
      UnMap<LeafNode>(leaf_node, 1, 0);
      return false;
    }
    value = leaf_node->Value(index);
    UnMap<LeafNode>(leaf_node, 1, 0);
    return true;
  }
  // pending issue: we need to first read out the plog before touch the data page
  //!!!!
  // fixed
  if(mtr_->GLog_Index.size()!=0){
    for (int i=0;i<mtr_->GLog_Index.size();i++){
     if (mtr_->GLog_Index[i]->find(key)!=mtr_->GLog_Index[i]->end() ){
      off_t lsn = (*mtr_->GLog_Index[i])[key];
      std::map<off_t, std::pair<std::string, std::string>> * seg_lsns = mtr_->Q_GLog[i];
      value = (*seg_lsns)[lsn].second;
      mtr_->hits_glog++;
      return true;
     }
    }
  }

  //if(block_cache_->Check_Leaf(of_leaf)==1 && mtr_->Virtual_Page_In_Cache.find(of_leaf) == mtr_->Virtual_Page_In_Cache.end()){
  if(block_cache_->Check_Leaf(of_leaf)==1){
    mtr_->hits_bp++;
    //such page is write-only before, have not been in registered in the virtual page cache
    //i.e., the updates in plog is persisted while the data page is not flushed
    char* cc =nullptr;
    //Plog* p = Map<Plog>(of_leaf,1,1,cc);
    //p->offset = of_leaf + kLeafPageSize;
    LeafNode* leaf_node = Map<LeafNode>(of_leaf,1,0,cc);
    //mtr_->Virtual_Page_In_Cache.emplace(of_leaf, std::make_pair(nullptr, std::make_pair(reinterpret_cast<char*>(p), reinterpret_cast<char *>(leaf_node))));
    int index = GetIndexFromLeafNode(leaf_node, key.data());
    if(index == -1){
        return false;
    }else{
        value = leaf_node->Value(index);
        return true;
    }
  }
  else if(mtr_->Virtual_Page_In_Cache.find(of_leaf) != mtr_->Virtual_Page_In_Cache.end()){
    mtr_->hits_bp++;
    std::pair<char *, char *> unit_ = mtr_->Virtual_Page_In_Cache[of_leaf].second;
    Plog *plog = reinterpret_cast<Plog *>(unit_.first);
    int index = GetIndexFromPlog(plog, key.data());
    if(index!=-1){
      value = plog->Value(index);
      return true;
    }else{
      //LeafNode *leaf = reinterpret_cast<LeafNode *>(unit_.second);
      char* x= nullptr;
      LeafNode *leaf = Map<LeafNode>(of_leaf,1,0,x);
      index = GetIndexFromLeafNode(leaf, key.data());
      if(index == -1){
        return false;
      }else{
        value = leaf->Value(index);
        return true;
      }
    } 
  }else{
      char *p = (char *)malloc(sizeof(char) * kPlogPageSize);
      memset(p, 0, kPlogPageSize);
      UnMap<LeafNode>(0x0,1,0);
      LeafNode *leaf_node = Map<LeafNode>(of_leaf, 1, 3, p);
      if(leaf_node== nullptr){
        mtr_->empty_leaf++;
        return false;
      }
      if(leaf_node->offset == 0){
        //--+printf("%ld\n",of_leaf);
      }
      //for detecting the LRU eviction
      Plog *plog_ = reinterpret_cast<Plog *>(p);
      if(!(plog_->offset == 0 || plog_->offset == of_leaf + kLeafPageSize)){
        plog_->offset = of_leaf+kLeafPageSize;
      }
      mtr_->Virtual_Page_In_Cache.emplace(of_leaf, std::make_pair(nullptr, std::make_pair(p, reinterpret_cast<char *>(leaf_node))));
      int index = GetIndexFromPlog(plog_, key.data());
      if (index != -1){
        mtr_->hits_plog++;
        value = plog_->Value(index);
        return true;  
      }else{
        mtr_->disk_read++;
        index = GetIndexFromLeafNode(leaf_node, key.data());
        if(index == -1) {
          return false;
        }else{
          value = leaf_node->Value(index);
          return true;
        }
      }
  }
}
*/


bool BPlusOrdersTree::Get(const std::string &key, std::string &value) const
{

  off_t of_leaf = GetLeafOffset(key.data());
  
 // if(of_leaf == 1694560256){
  //  int x =0;
  //}
  // need to warm up (build) the virtual page (in-memory) firstly
  if (mtr_->plog_flag != 1)
  {
    // baseline
    char *p = nullptr;
    LeafNode *leaf_node = Map<LeafNode>(of_leaf, 1, 0, p);
    if(block_cache_->Check_Leaf(of_leaf)==1){
      mtr_->hits_bp++;
    }else{
      mtr_->disk_read++;
    }
    int index = GetIndexFromLeafNode(leaf_node, key.data());
    if (index == -1)
    {
      UnMap<LeafNode>(leaf_node, 1, 0);
      return false;
    }
    char* ptr = leaf_node->ValueALL(index);
    std::string tmp_value(ptr,ptr+kMaxValueSize_orders_col_all);
    value = tmp_value;
    free(ptr);
    UnMap<LeafNode>(leaf_node, 1, 0);
    return true;
  }
  // pending issue: we need to first read out the plog before touch the data page
  //!!!!
  // fixed
  if(mtr_->GLog_Index.size()!=0){
    for (int i=0;i<mtr_->GLog_Index.size();i++){
     if (mtr_->GLog_Index[i]->find(key)!=mtr_->GLog_Index[i]->end() ){
      off_t lsn = (*mtr_->GLog_Index[i])[key];
      std::map<off_t, std::pair<std::string, std::string>> * seg_lsns = mtr_->Q_GLog[i];
      value = (*seg_lsns)[lsn].second;
      mtr_->hits_glog++;
      return true;
     }
    }
  }
  /*
  if(GLog_Index.find(key)!=GLog_Index.end()){
    off_t lsn = GLog_Index[key];
    std::map<off_t, std::pair<std::string, std::string>> * seg_lsns = Q_GLog.front();
    value = (*seg_lsns)[lsn].second;
    return true;
  }*/
  //if(block_cache_->Check_Leaf(of_leaf)==1 && mtr_->Virtual_Page_In_Cache.find(of_leaf) == mtr_->Virtual_Page_In_Cache.end()){
  if(block_cache_->Check_Leaf(of_leaf)==1){
    mtr_->hits_bp++;
    //such page is write-only before, have not been in registered in the virtual page cache
    //i.e., the updates in plog is persisted while the data page is not flushed
    char* cc =nullptr;
    //Plog* p = Map<Plog>(of_leaf,1,1,cc);
    //p->offset = of_leaf + kLeafPageSize;
    LeafNode* leaf_node = Map<LeafNode>(of_leaf,1,0,cc);
    //mtr_->Virtual_Page_In_Cache.emplace(of_leaf, std::make_pair(nullptr, std::make_pair(reinterpret_cast<char*>(p), reinterpret_cast<char *>(leaf_node))));
    int index = GetIndexFromLeafNode(leaf_node, key.data());
    if(index == -1){
        return false;
    }else{
        char* ptr = leaf_node->ValueALL(index);
        std::string tmp_value(ptr,ptr+kMaxValueSize_orders_col_all);
        value = tmp_value;
        free(ptr);
        return true;
    }
  }
  else if(mtr_->Virtual_Page_In_Cache.find(of_leaf) != mtr_->Virtual_Page_In_Cache.end()){
    mtr_->hits_bp++;
    std::pair<char *, char *> unit_ = mtr_->Virtual_Page_In_Cache[of_leaf].second;
    Plog *plog = reinterpret_cast<Plog *>(unit_.first);
    int index = GetIndexFromPlog(plog, key.data());
    if(index!=-1){
      value = plog->Value(index);
      return true;
    }else{
      //LeafNode *leaf = reinterpret_cast<LeafNode *>(unit_.second);
      char* x= nullptr;
      LeafNode *leaf = Map<LeafNode>(of_leaf,1,0,x);
      index = GetIndexFromLeafNode(leaf, key.data());
      if(index == -1){
        return false;
      }else{
    char* ptr = leaf->ValueALL(index);
    std::string tmp_value(ptr,ptr+kMaxValueSize_orders_col_all);
    value = tmp_value;
    free(ptr);
        return true;
      }
    } 
  }else{
      char *p = (char *)malloc(sizeof(char) * kPlogPageSize);
      memset(p, 0, kPlogPageSize);
      UnMap<LeafNode>(0x0,1,0);
      LeafNode *leaf_node = Map<LeafNode>(of_leaf, 1, 3, p);
      if(leaf_node== nullptr){
        mtr_->empty_leaf++;
        return false;
      }
      if(leaf_node->offset == 0){
        //--+printf("%ld\n",of_leaf);
      }
      //for detecting the LRU eviction
      Plog *plog_ = reinterpret_cast<Plog *>(p);
      if(!(plog_->offset == 0 || plog_->offset == of_leaf + kLeafPageSize)){
        plog_->offset = of_leaf+kLeafPageSize;
      }
      mtr_->Virtual_Page_In_Cache.emplace(of_leaf, std::make_pair(nullptr, std::make_pair(p, reinterpret_cast<char *>(leaf_node))));
      int index = GetIndexFromPlog(plog_, key.data());
      if (index != -1){
        mtr_->hits_plog++;
        value = plog_->Value(index);
        return true;  
      }else{
        mtr_->disk_read++;
        index = GetIndexFromLeafNode(leaf_node, key.data());
        if(index == -1) {
          return false;
        }else{
          char* ptr = leaf_node->ValueALL(index);
          std::string tmp_value(ptr,ptr+kMaxValueSize_orders_col_all);
          value = tmp_value;
          free(ptr);
          return true;
        }
      }
  }
}



bool BPlusOrdersTree::Scan(const std::string &start, const std::string &end, std::string &value) const
{
  if(start>end){
    int x=1;
  }
  //off_t of_leaf = GetLeafOffset(key.data());
  off_t left = GetLeafOffset(start.data());
  off_t right = GetLeafOffset(end.data());
  if(left == right){
    // in the same node
    //assume there is no hybird cache
    char* p = (char*)malloc(sizeof(char)*kPlogPageSize);
    int id_cnt=0;
    LeafNode *leaf_node = Map<LeafNode>(left, 1, 5, p); //read out both plog and page
    return true;
    /*
    int index_start = GetIndexFromLeafNode(leaf_node, start.data());
    int index_end = GetIndexFromLeafNode(leaf_node, end.data());
    if(index_start == -1 || index_end == -1){
        return false;
    }else{
        //value = leaf_node->Value(index_start);
        id_cnt++;
        int gap = index_end - index_start;
        int cnt = 1;
        while (cnt<gap){
          id_cnt++;
          cnt++;
        }
        id_cnt++;
        
        value = std::to_string(id_cnt);
        char* buf = reinterpret_cast<char*>(leaf_node);
        free(buf);
        free(p);
        //UnMap<LeafNode>(leaf_node, 1, 0);
        return true;
    }*/

  }else{

    //hkc-scan-01
    int uring_flag=1;
    if(uring_flag==1){
      int scan_batch_size =200;  
      std::vector<off_t> scan_offs;
      scan_offs.push_back(left);
      if(mtr_->leaf_links.find(left)!=mtr_->leaf_links.end()){
        off_t next = mtr_->leaf_links[left].second;
        while(next<right && next!=0){
          scan_offs.push_back(next);
          next = mtr_->leaf_links[next].second;
        }
      }
      int total_pages = scan_offs.size();
      
      //hkc-ub-scan-10
      int buf_ratio =35;
      int buf_size = kUnitNodeSize*buf_ratio/100;
      // alloc memory buffer for scan result
      char* scan_buffer = (char*)malloc(sizeof(char)*scan_batch_size*buf_size);
      

      // init uring
      struct io_uring_sqe *sqe_scan;
      struct io_uring_cqe *cqe_scan;
      
      struct iovec iov_scan[scan_batch_size];
      struct io_uring ring_scan;
      io_uring_queue_init(scan_batch_size, &ring_scan, 0);
      sqe_scan = io_uring_get_sqe(&ring_scan);
      //prepare uring
      int read_pages=0;
      while(read_pages<total_pages){
        off_t cur_ptr=0;
        int i=0;
        for(i; i< scan_batch_size;i++){
          sqe_scan = io_uring_get_sqe(&ring_scan);
          iov_scan[i].iov_base = &scan_buffer[cur_ptr];
          cur_ptr+=buf_size;
          iov_scan[i].iov_len = buf_size;
          io_uring_prep_readv(sqe_scan,mtr_->global_fd,&iov_scan[i],1,scan_offs[read_pages]);
          read_pages++;
          if(read_pages>=total_pages){
            break;
          }
        }
        int ret = io_uring_submit(&ring_scan);
        //wait uring return
        for (int j = 0; j < i; j++)
        {
          ret = io_uring_wait_cqe(&ring_scan, &cqe_scan);
          io_uring_cqe_seen(&ring_scan, cqe_scan);
        }
      }

      io_uring_queue_exit(&ring_scan);

      free(scan_buffer);
      value = std::to_string(read_pages);
      return true;
    }
      
    // in the same node
    //assume there is no hybird cache
    char* p = (char*)malloc(sizeof(char)*kPlogPageSize);
    off_t cur_off = left;
    int id_cnt=0;
    int first_cycle = 0;
    while (cur_off<=right){
      LeafNode *leaf_cur = Map<LeafNode>(cur_off, 1, 4, p); //read out both plog and page
      int index_start = GetIndexFromLeafNode(leaf_cur, start.data());
      int index_end = GetIndexFromLeafNode(leaf_cur, end.data());
      if (index_end == -1){
        index_end = leaf_cur->count-1;
      }
      
      if(first_cycle == 0 && index_start == -1){
        free(p);
        char* buf = reinterpret_cast<char*>(leaf_cur);
        free(buf);
        value = std::to_string(id_cnt);
        return false;
      }else{
        //value = leaf_node->Value(index_start);
        id_cnt+=1;
        int gap = index_end - index_start;
        int cnt = 1;
        while (cnt<gap){
          id_cnt++;
          cnt++;
        }
        id_cnt++;
      }
      cur_off = leaf_cur->right;
      if(cur_off == 0) break;
      //cur_off +=144*1024;
      first_cycle =1;
      char* buf = reinterpret_cast<char*>(leaf_cur);
      free(buf);
      //UnMap<LeafNode>(leaf_cur,1,0);
    }
    value = std::to_string(id_cnt);
    free(p);
    return true;
  }


}

//hkc-ap-19
bool BPlusOrdersTree::ScanAP(const std::string &start, const std::string &end, std::string &value) const
{

  if(start>end){
    int x=1;
  }
  //off_t of_leaf = GetLeafOffset(key.data());
  off_t left = GetLeafOffset(start.data());
  off_t right = GetLeafOffset(end.data());
  mtr_->ap_wait = true;
  mtr_->ap_left = left;
  mtr_->ap_right = right;
  mtr_->ap_lsn = mtr_->global_lsn;
  return true;
  if(left == right){
    // in the same node
    //assume there is no hybird cache
    char* p = (char*)malloc(sizeof(char)*kPlogPageSize);
    int id_cnt=0;
    LeafNode *leaf_node = Map<LeafNode>(left, 1, 5, p); //read out both plog and page
    return true;
    /*
    int index_start = GetIndexFromLeafNode(leaf_node, start.data());
    int index_end = GetIndexFromLeafNode(leaf_node, end.data());
    if(index_start == -1 || index_end == -1){
        return false;
    }else{
        //value = leaf_node->Value(index_start);
        id_cnt++;
        int gap = index_end - index_start;
        int cnt = 1;
        while (cnt<gap){
          id_cnt++;
          cnt++;
        }
        id_cnt++;
        
        value = std::to_string(id_cnt);
        char* buf = reinterpret_cast<char*>(leaf_node);
        free(buf);
        free(p);
        //UnMap<LeafNode>(leaf_node, 1, 0);
        return true;
    }*/

  }else{

    //hkc-scan-01
    int uring_flag=1;
    if(uring_flag==1){
      int scan_batch_size =200;  
      std::vector<off_t> scan_offs;
      scan_offs.push_back(left);
      if(mtr_->leaf_links.find(left)!=mtr_->leaf_links.end()){
        off_t next = mtr_->leaf_links[left].second;
        while(next<right && next!=0){
          scan_offs.push_back(next);
          next = mtr_->leaf_links[next].second;
        }
      }
      int total_pages = scan_offs.size();
      
      //hkc-ub-scan-10
      int buf_ratio =35;
      int buf_size = kUnitNodeSize*buf_ratio/100;
      // alloc memory buffer for scan result
      char* scan_buffer = (char*)malloc(sizeof(char)*scan_batch_size*buf_size);
      

      // init uring
      struct io_uring_sqe *sqe_scan;
      struct io_uring_cqe *cqe_scan;
      
      struct iovec iov_scan[scan_batch_size];
      struct io_uring ring_scan;
      io_uring_queue_init(scan_batch_size, &ring_scan, 0);
      sqe_scan = io_uring_get_sqe(&ring_scan);
      //prepare uring
      int read_pages=0;
      while(read_pages<total_pages){
        off_t cur_ptr=0;
        int i=0;
        for(i; i< scan_batch_size;i++){
          sqe_scan = io_uring_get_sqe(&ring_scan);
          iov_scan[i].iov_base = &scan_buffer[cur_ptr];
          cur_ptr+=buf_size;
          iov_scan[i].iov_len = buf_size;
          io_uring_prep_readv(sqe_scan,mtr_->global_fd,&iov_scan[i],1,scan_offs[read_pages]);
          read_pages++;
          if(read_pages>=total_pages){
            break;
          }
        }
        int ret = io_uring_submit(&ring_scan);
        //wait uring return
        for (int j = 0; j < i; j++)
        {
          ret = io_uring_wait_cqe(&ring_scan, &cqe_scan);
          io_uring_cqe_seen(&ring_scan, cqe_scan);
        }
      }

      io_uring_queue_exit(&ring_scan);

      free(scan_buffer);
      value = std::to_string(read_pages);
      return true;
    }
      
    // in the same node
    //assume there is no hybird cache
    char* p = (char*)malloc(sizeof(char)*kPlogPageSize);
    off_t cur_off = left;
    int id_cnt=0;
    int first_cycle = 0;
    while (cur_off<=right){
      LeafNode *leaf_cur = Map<LeafNode>(cur_off, 1, 4, p); //read out both plog and page
      int index_start = GetIndexFromLeafNode(leaf_cur, start.data());
      int index_end = GetIndexFromLeafNode(leaf_cur, end.data());
      if (index_end == -1){
        index_end = leaf_cur->count-1;
      }
      
      if(first_cycle == 0 && index_start == -1){
        free(p);
        char* buf = reinterpret_cast<char*>(leaf_cur);
        free(buf);
        value = std::to_string(id_cnt);
        return false;
      }else{
        //value = leaf_node->Value(index_start);
        id_cnt+=1;
        int gap = index_end - index_start;
        int cnt = 1;
        while (cnt<gap){
          id_cnt++;
          cnt++;
        }
        id_cnt++;
      }
      cur_off = leaf_cur->right;
      if(cur_off == 0) break;
      //cur_off +=144*1024;
      first_cycle =1;
      char* buf = reinterpret_cast<char*>(leaf_cur);
      free(buf);
      //UnMap<LeafNode>(leaf_cur,1,0);
    }
    value = std::to_string(id_cnt);
    free(p);
    return true;
  }


}




int BPlusOrdersTree::Check(off_t offset)
{
  return block_cache_->Check_Leaf(offset);
}

char *BPlusOrdersTree::GetMemCopy(off_t offset)
{
  return block_cache_->GetMemCopy_Leaf(offset);
}

template <typename T>
T *BPlusOrdersTree::Map(off_t offset, int isleaf, int is_plog, char *&plog_copy) const
{
  if (isleaf == 1)
  {
    return block_cache_->Get<T>(fd_, offset, is_plog, plog_copy);
    // while(t==nullptr){//disable
    //   sleep(0.1);
    //   t =block_cache_->Get<T>(fd_, offset, is_plog, plog_copy);
    //   if(t!=nullptr){
    //     break;
    //   }
    // }
    // return t;
  }
  else
  {
    return internal_cache_->Get<T>(fd_, offset);
  }
}

template <typename T>
void BPlusOrdersTree::InsertDirty(off_t offset) const
{
  
  block_cache_->InsertIntoSortedFLU(offset);
  return;
}

template <typename T>
void BPlusOrdersTree::RelocateDirty(off_t offset, off_t ori_oldset_lsn) const
{
  block_cache_->ReinsertToFLU(offset, ori_oldset_lsn); 
  return;
}

template <typename T>
void BPlusOrdersTree::DirectInsertDirty(off_t offset_leaf, off_t offset_split) const
{
  block_cache_->InPlaceInsertToFLU(offset_leaf, offset_split);
  return;
}

template <typename T>
void BPlusOrdersTree::UnMap(T *map_obj, int isleaf, int is_plog) const
{
  if (isleaf == 1)
  {
    block_cache_->Put<T>(map_obj, is_plog);
  }
  else
  {
    internal_cache_->Put<T>(map_obj);
  }
}

size_t BPlusOrdersTree::GetMinKeys(int flag)
{
  if (flag == 0)
  {
    // plog page, i.e., plog
    return (kOrder_orders_Plog + 1) / 2 - 1;
  }
  else
  {
    // data page, i.e., leaf node
    return (kOrder_orders_ + 1) / 2 - 1;
  }
}
// 1: leaf node ; 0: plog
size_t BPlusOrdersTree::GetMaxKeys(int flag)
{
  if (flag == 0)
  {
    // plog page, i.e., plog
    return kOrder_orders_Plog - 1;
  }
  else
  {
    // data page, i.e., leaf node
    return kOrder_orders_ - 1;
  }
}
size_t BPlusOrdersTree::GetMinIndexKeys() { return (kOrder_orders_Index + 1) / 2 - 1; }
size_t BPlusOrdersTree::GetMaxIndexKeys() { return kOrder_orders_Index - 1; }

BPlusOrdersTree::IndexNode *BPlusOrdersTree::GetOrCreateParent(Node *node)
{
  if (node->parent == 0)
  {
    // Split root node.
    IndexNode *parent_node = AllocIndex<IndexNode>();
    // UnMap<IndexNode>(parent_node);
    node->parent = parent_node->offset;
    meta_->root = parent_node->offset;
    ++meta_->height;
    return parent_node;
  }
  char *p = nullptr;
  return Map<IndexNode>(node->parent, 0, 0, p);
}
// flag: 1: data page; 0: plog
template <typename T>
int BPlusOrdersTree::UpperBound(T arr[], int n, const char *key, int flag) const
{
  if (flag == 0)
  {
    ////assert(n <= GetMaxKeys(0));
  }
  else
  {
    ////assert(n <= GetMaxKeys(1));
  }

  int l = 0, r = n - 1;
  while (l <= r)
  {
    int mid = (l + r) >> 1;
    if (std::strncmp(arr[mid].key, key, kMaxKeySize_orders_) <= 0)
    {
      l = mid + 1;
    }
    else
    {
      r = mid - 1;
    }
  }
  return l;
}

template <typename T>
int BPlusOrdersTree::UpperBoundIndex(T arr[], int n, const char *key) const
{
  ////assert(n <= GetMaxIndexKeys());
  int l = 0, r = n - 1;
  while (l <= r)
  {
    int mid = (l + r) >> 1;
    if (std::strncmp(arr[mid].key, key, kMaxKeySize_orders_) <= 0)
    {
      l = mid + 1;
    }
    else
    {
      r = mid - 1;
    }
  }
  return l;
}

template <typename T>
int BPlusOrdersTree::LowerBoundIndex(T arr[], int n, const char *key) const
{
  ////assert(n <= GetMaxIndexKeys());
  int l = 0, r = n - 1;
  while (l <= r)
  {
    int mid = (l + r) >> 1;
    if (std::strncmp(arr[mid].key, key, kMaxKeySize_orders_) < 0)
    {
      l = mid + 1;
    }
    else
    {
      r = mid - 1;
    }
  }
  return l;
}
// flag: 1: data page; 0: plog
template <typename T>
int BPlusOrdersTree::LowerBound(T arr[], int n, const char *key, int flag) const
{
  if (flag == 0)
  {
    //assert(n <= GetMaxKeys(0));
  }
  else
  {
    //assert(n <= GetMaxKeys(1));
  }

  int l = 0, r = n - 1;
  while (l <= r)
  {
    int mid = (l + r) >> 1;
    if (std::strncmp(arr[mid].key, key, kMaxKeySize_orders_) < 0)
    {
      l = mid + 1;
    }
    else
    {
      r = mid - 1;
    }
  }
  return l;
}

template <typename T>
T *BPlusOrdersTree::AllocLeaf()
{ // 1: leaf node, 0: internal node
  LeafNode *node = block_cache_->PushToCache<LeafNode *>(meta_->block);
  node->oldest_lsn = mtr_->global_lsn;
  // node->cur_pplog_index=0;
  node->origin_lsn = node->oldest_lsn;
  node->offset = meta_->block;
  meta_->block += kUnitNodeSize;
  return node;
}

template <typename T>
T *BPlusOrdersTree::AllocIndex()
{ // 1: leaf node, 0: internal node

  char *tmp = (char *)malloc(sizeof(char) * kIndexPageSize);
  memset(tmp, 0, kIndexPageSize);
  IndexNode *node = reinterpret_cast<IndexNode *>(tmp);
  node->oldest_lsn = mtr_->global_lsn;
  node->origin_lsn = node->oldest_lsn;
  node->offset = meta_->block;
  meta_->block += kIndexPageSize;
  internal_cache_->PushToCache<IndexNode>(node->offset, node);
  return node;

  /*
  //T* node = new (Map<T>(meta_->block)) T();
  block_cache_->
  char* tmp=(char*)malloc(sizeof(char)*kPageSize);
  memset(tmp,0,kPageSize);
  T* leaf_node = reinterpret_cast<T*>(tmp);
  //T* node = new T();
  leaf_node->offset = meta_->block;
  meta_->block += kPageSize;
  if(leaf_node->offset == 868352){
    ////--+printf("check!");
  }
  Node* cache_node = new Node(leaf_node, leaf_node->offset, kPageSize);
  offset2node_.emplace(leaf_node->offset, cache_node);
  InsertHead(cache_node);
  return node;
  */
}

template <typename T>
void BPlusOrdersTree::Dealloc(T *node)
{
  UnMap<T>(node);
}

void BPlusOrdersTree::WarmUpVirtualPageCache(int x) const
{
  return block_cache_->WarmUpVPCache(x);
}
off_t BPlusOrdersTree::GetLeafOffset(const char *key) const
{
  size_t height = meta_->height;
  off_t offset = meta_->root;
  if (height <= 1)
  {
    //assert(height == 1);
    return offset;
  }
  // 1. Find bottom index node.
  char *p = nullptr;
  IndexNode *index_node = Map<IndexNode>(offset, 0, 0, p);
  UnMap<IndexNode>(index_node, 0, 0);
  while (--height > 1)
  {
    int index = UpperBoundIndex(index_node->indexes, index_node->count, key);
    off_t of_child = index_node->indexes[index].offset;
    if (of_child == 0)
    {
      //--+printf("error!8\n");
    }
    char *p = nullptr;
    index_node = Map<IndexNode>(of_child, 0, 0, p);
    if (of_child != index_node->offset)
    {
      int x = 0;
    }
    UnMap(index_node, 0, 0);
    offset = of_child;
  }
  // 2. Get offset of leaf node.
  int index = UpperBoundIndex(index_node->indexes, index_node->count, key);
  off_t of_child = index_node->indexes[index].offset;
  // UnMap<IndexNode>(index_node);
  return of_child;
}

inline size_t BPlusOrdersTree::InsertKeyIntoIndexNode(IndexNode *index_node,
                                                const char *key,
                                                Node *left_node,
                                                Node *right_node, off_t cur_lsn)
{
  //assert(index_node->count <= GetMaxIndexKeys());
  index_node->newest_lsn = cur_lsn;
  int index = UpperBoundIndex(index_node->indexes, index_node->count, key);
  index_node->InsertIndexAtIndex(index, key, left_node->offset, cur_lsn);
  index_node->UpdateOffset(index + 1, right_node->offset);
  return index_node->count;
}

//hkc-modify-13
size_t BPlusOrdersTree::InsertKVIntoLeafNode(LeafNode *leaf_node, const char *key,
                                       const char *value_1, const char *value_2, const char *value_3, const char *value_4, const char *value_5, off_t cur_lsn)
{
  //assert(leaf_node->count <= GetMaxKeys(1));

  leaf_node->newest_lsn = cur_lsn;
  if (leaf_node->dirty == 0)
  {
    leaf_node->dirty = 1;
  }

  int isize = sizeof(*leaf_node);
  int index = UpperBound(leaf_node->records, leaf_node->count, key, 1);
  if (index > 0 &&
      std::strncmp(leaf_node->Key(index - 1), key, kMaxKeySize_orders_) == 0)
  {
    leaf_node->UpdateValue(index - 1, value_1,value_2,value_3,value_4,value_5, cur_lsn);
    return leaf_node->count;
  }
  leaf_node->InsertKVAtIndex(index, key, value_1,value_2,value_3,value_4,value_5,cur_lsn);
  ++meta_->size;
  return leaf_node->count;
}

//hkc-plog-9
size_t BPlusOrdersTree::InsertDeltaKVIntoLeafNode(LeafNode *leaf_node, const char *key,const char* value_carrier, off_t cur_lsn)
{
  //assert(leaf_node->count <= GetMaxKeys(1));

  leaf_node->newest_lsn = cur_lsn;
  if (leaf_node->dirty == 0)
  {
    leaf_node->dirty = 1;
  }

  int index = UpperBound(leaf_node->records, leaf_node->count, key, 1);
  if (index > 0 &&
      std::strncmp(leaf_node->Key(index - 1), key, kMaxKeySize_orders_) == 0)
  {
    leaf_node->UpdateValueDelta(index - 1, value_carrier,cur_lsn);
    return leaf_node->count;
  }
  leaf_node->InsertDeltaKVAtIndex(index, key,  value_carrier, cur_lsn);
  ++meta_->size;
  return leaf_node->count;
}

//hkc-modify-14
size_t BPlusOrdersTree::InsertKVIntoPlog(Plog *plog, const char *key,
                                   const char* value1, const char* value2,const char* value3,const char* value4,const char* value5, off_t cur_lsn)
{
  //assert(plog->count <= GetMaxKeys(0));

  plog->newest_lsn = cur_lsn;

  int isize = sizeof(*plog);
  int index = UpperBound(plog->records, plog->count, key, 0);
  if (index > 0 &&
      std::strncmp(plog->Key(index - 1), key, kMaxKeySize_orders_) == 0)
  {
    plog->UpdateValue(index - 1, value1,value2,value3,value4,value5, cur_lsn);
    return plog->count;
  }
  plog->InsertKVAtIndex(index, key, value1,value2,value3,value4,value5, cur_lsn);
  ++meta_->size;
  return plog->count;
}

//hkc-plog-10
//InsertDeltaIntoPlog(Plog* plog, const char* key,const char* value_balance, const char* value_data,off_t cur_lsn)
size_t BPlusOrdersTree::InsertDeltaIntoPlog(Plog *plog, const char *key, const char* value_carrier, off_t cur_lsn)
{
  //assert(plog->count <= GetMaxKeys(0));

  plog->newest_lsn = cur_lsn;

  int isize = sizeof(*plog);
  int index = UpperBound(plog->records, plog->count, key, 0);
  if (index > 0 &&
      std::strncmp(plog->Key(index - 1), key, kMaxKeySize_orders_) == 0)
  {
    plog->UpdateValueDelta(index - 1, value_carrier, cur_lsn);
    return plog->count;
  }
  //KV NOT EXIST IN THE PLOG, SO WE PRETEND TO HAVE ALL KVS
  plog->InsertDeltaKVAtIndex(index, key, value_carrier, cur_lsn);
  ++meta_->size;
  return plog->count;
}

BPlusOrdersTree::LeafNode *BPlusOrdersTree::SplitLeafNode(LeafNode *leaf_node)
{
  //assert(leaf_node->count == kOrder_orders_);
  off_t origian_lsn = leaf_node->GetSmallestLSN();
  constexpr int mid = (kOrder_orders_ - 1) >> 1;
  constexpr int left_count = mid;
  constexpr int right_count = kOrder_orders_ - mid;

  LeafNode *split_node = AllocLeaf<LeafNode>();

  // Change count.
  leaf_node->count = left_count;
  split_node->count = right_count;

  // Copy right part of index_node.
  std::memcpy(&split_node->records[0], &leaf_node->records[mid],
              sizeof(split_node->records[0]) * right_count);

  // a simplified version, i.e., when to split a page,
  // we use the origin lsn to represent the oldest lsn for both newly generated split node and original leaf node
  if (mtr_->lazy_chpt_lsn_flag == 1)
  {

    // update lsns
    leaf_node->origin_lsn = origian_lsn;
    // leaf_node->origin_lsn=leaf_node->GetSmallestLSN();
    leaf_node->oldest_lsn = leaf_node->origin_lsn;
    leaf_node->newest_lsn = leaf_node->GetBiggestLSN();
    // if(leaf_node->newest_lsn < leaf_node->oldest_lsn){
    //   leaf_node->newest_lsn = leaf_node->oldest_lsn;
    // }

    // split_node->oldest_lsn = split_node->GetSmallestLSN();
    split_node->oldest_lsn = origian_lsn;
    // split_node->oldest_lsn = leaf_node->oldest_lsn;
    split_node->origin_lsn = split_node->oldest_lsn;
    split_node->newest_lsn = split_node->GetBiggestLSN();
    // if(split_node->newest_lsn < split_node->oldest_lsn){
    //   split_node->newest_lsn = split_node->oldest_lsn;
    // }

    // then we only need to insert the split node into the flu right behind the leaf node
    if (mtr_->FLU_disabled == 0 && mtr_->InFLUOrNot.find(split_node->offset) == mtr_->InFLUOrNot.end())
    {
      DirectInsertDirty<LeafNode>(leaf_node->offset, split_node->offset);
    }
  }
  else
  {
    // update lsns
    leaf_node->oldest_lsn = leaf_node->GetSmallestLSN();
    leaf_node->origin_lsn = leaf_node->oldest_lsn;
    leaf_node->newest_lsn = leaf_node->GetBiggestLSN();
    // due to that the original leaf node's oldsest lsn has been updated, we need to
    //  (1) firstly eliminate it from FLU
    //  (2) re-insert it into the FLU with appropriate locations
    split_node->oldest_lsn = split_node->GetSmallestLSN();
    split_node->origin_lsn = split_node->oldest_lsn;
    split_node->newest_lsn = split_node->GetBiggestLSN();

    if (mtr_->FLU_disabled == 0 && origian_lsn != leaf_node->oldest_lsn)
    {
      RelocateDirty<LeafNode>(leaf_node->offset, origian_lsn);
    }
    if (mtr_->FLU_disabled == 0 && mtr_->InFLUOrNot.find(split_node->offset) == mtr_->InFLUOrNot.end())
    {
      InsertDirty<LeafNode>(split_node->offset);
    }
  }

  // Link siblings.
  split_node->left = leaf_node->offset;
  split_node->right = leaf_node->right;
  leaf_node->right = split_node->offset;

    //hkc-scan-03
  mtr_->leaf_links[split_node->offset]=std::make_pair(leaf_node->offset,leaf_node->right);
  if(mtr_->leaf_links.find(leaf_node->offset)!=mtr_->leaf_links.end()){
    //leaf_node is reigistered
    std::pair<off_t,off_t> leaf_left_right = mtr_->leaf_links[leaf_node->offset];
    //update leaf's right
    mtr_->leaf_links[leaf_node->offset].second= split_node->offset;
  }else{
    mtr_->leaf_links[leaf_node->offset]=std::make_pair(0,split_node->offset);
  }
  if(mtr_->leaf_links.find(split_node->right)!=mtr_->leaf_links.end()){
    mtr_->leaf_links[split_node->right].first=split_node->offset;
  }else{
    mtr_->leaf_links[split_node->right] =std::make_pair(split_node->offset,0);
  }

  if (split_node->right != 0)
  {
    char *p = nullptr;
    LeafNode *new_sibling = Map<LeafNode>(split_node->right, 1, 0, p);
    if (new_sibling->offset != split_node->right)
    {
      //--+printf("error9");
    }
    new_sibling->left = split_node->offset;
    UnMap(new_sibling, 1, 0);
  }
  split_node->dirty = 1;

  UnMap<LeafNode>(split_node, 1, 0);
  leaf_node->dirty = 1;
  UnMap<LeafNode>(leaf_node, 1, 0);
  return split_node;
}

BPlusOrdersTree::IndexNode *BPlusOrdersTree::SplitIndexNode(IndexNode *index_node)
{
  //assert(index_node->count == kOrder_orders_Index);
  constexpr int mid = (kOrder_orders_Index - 1) >> 1;
  constexpr int left_count = mid;
  constexpr int right_count = kOrder_orders_Index - mid - 1;

  IndexNode *split_node = AllocIndex<IndexNode>();

  // Change count.
  index_node->count = left_count;
  split_node->count = right_count;

  // Copy right part of index_node.
  std::memcpy(&split_node->indexes[0], &index_node->indexes[mid + 1],
              sizeof(split_node->indexes[0]) * (right_count + 1));

  // update lsn
  index_node->oldest_lsn = index_node->GetSmallestLSN();
  index_node->newest_lsn = index_node->GetBiggestLSN();
  split_node->oldest_lsn = split_node->GetSmallestLSN();
  split_node->newest_lsn = split_node->GetBiggestLSN();

  // Link old childs to new splited parent.
  for (int i = mid + 1; i <= kOrder_orders_Index; ++i)
  {
    off_t of_child = index_node->indexes[i].offset;

    if (mtr_->IndexOrNot.find(of_child)!=mtr_->IndexOrNot.end())
    {
      char *p = nullptr;
      IndexNode *child_node = Map<IndexNode>(of_child, 0, 0, p);
      child_node->parent = split_node->offset;
      UnMap(child_node, 0, 0);
    }
    else
    {
      char *p = nullptr;
      LeafNode *child_node = Map<LeafNode>(of_child, 1, 0, p);
      child_node->parent = split_node->offset;
      UnMap(child_node, 1, 0);
    }
  }

  // Link siblings.
  split_node->left = index_node->offset;
  split_node->right = index_node->right;
  index_node->right = split_node->offset;
  if (split_node->right != 0)
  {
    char *p = nullptr;
    IndexNode *new_sibling = Map<IndexNode>(split_node->right, 0, 0, p);
    new_sibling->left = split_node->offset;
    UnMap<IndexNode>(new_sibling, 0, 0);
  }
  return split_node;
}

inline int BPlusOrdersTree::GetIndexFromLeafNode(LeafNode *leaf_node,
                                           const char *key) const
{
  int index = LowerBound(leaf_node->records, leaf_node->count, key, 1);
  return index < static_cast<int>(leaf_node->count) &&
                 std::strncmp(leaf_node->Key(index), key, kMaxKeySize_orders_) == 0
             ? index
             : -1;
}

inline int BPlusOrdersTree::GetIndexFromPlog(Plog *plog,
                                       const char *key) const
{
  int index = LowerBound(plog->records, plog->count, key, 0);
  return index < static_cast<int>(plog->count) &&
                 std::strncmp(plog->Key(index), key, kMaxKeySize_orders_) == 0
             ? index
             : -1;
}

std::vector<std::pair<std::string, std::string>> BPlusOrdersTree::GetRange(
    const std::string &left_key, const std::string &right_key) const
{
  std::vector<std::pair<std::string, std::string>> res;
  off_t of_leaf = GetLeafOffset(left_key.data());
  char *p = nullptr;
  LeafNode *leaf_node = Map<LeafNode>(of_leaf, 1, 0, p);
  int index = LowerBound(leaf_node->records, leaf_node->count, left_key.data(), 1);
  for (int i = index; i < leaf_node->count; ++i)
  {
    res.emplace_back(leaf_node->Key(i), leaf_node->Value(i));
  }

  of_leaf = leaf_node->right;
  bool finish = false;
  while (of_leaf != 0 && !finish)
  {
    char *p = nullptr;
    LeafNode *right_leaf_node = Map<LeafNode>(of_leaf, 1, 0, p);
    for (int i = 0; i < right_leaf_node->count; ++i)
    {
      if (strncmp(right_leaf_node->Key(i), right_key.data(), kMaxKeySize_orders_) <=
          0)
      {
        res.emplace_back(right_leaf_node->Key(i), right_leaf_node->Value(i));
      }
      else
      {
        finish = true;
        break;
      }
    }
    of_leaf = right_leaf_node->right;
    UnMap(right_leaf_node, 1, 0);
  }

  UnMap(leaf_node, 1, 0);
  return res;
}

bool BPlusOrdersTree::Empty() const { return meta_->size == 0; }

size_t BPlusOrdersTree::Size() const { return meta_->size; }