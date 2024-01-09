#include <cstdio>
#include <string>
#include <vector>
#include <list>
#include <unordered_map>

//for backgound thread
#include <mutex> 
#include <shared_mutex>
#include <thread>
#include <pthread.h>
#include <condition_variable>
#include <thread>
#include <deque>

#include <liburing.h>
#include <map>





//void BackGroundManager();

class BPlusTree {
  struct Meta;
  struct Monitor;
  struct Index;
  struct Record;
  struct Per_page_Log;
  struct Node;
  struct IndexNode;
  struct LeafNode;
  struct Plog;
  class BlockCache;
  class InternalCache;
  struct LogRecord;
  struct Checkpoint;
  
        

 public:
  BPlusTree(const char* path, const char* log_path, const char* chpt_path, const int bg_id_, const bool enable_backthd, const int key_size, const int no_of_col, std::vector<int> *value_of_col_);
  ~BPlusTree();

  void SetIOStaic();
  off_t GetChpt();
  void SetLoadFlag(int flag);
  void DisableFLUOrNot(int flag);
  void LoadIOStatic(int &plog_insert, int &leaf_insert, int &leaf_split,int &phy_plog_write,int &phy_plog_read,int &phy_page_read,int &phy_page_write,int &logi_page_read,int &logi_page_write);
  void SetPlog();
  void SetBaseline();
  void GetStatic(int &hits_bp, int &hits_glog, int &hits_plog, int &disk_read, int &io_consume, int &error_cnt,int &dirtys, int &cleans);
  void ResetStatic();
  off_t GetLSN();
  void IncreLSN();

  
  void Put(const std::string& key, const std::string& value, off_t lsn);
  //void Put_list(const std::string& key, const std::vector<std::string> value_list, off_t lsn);
  void InstantRecovery(off_t checkpoint);
  void NormalRecovery(off64_t checkpoint);
  void ApplyOneDiskToDataFile(int seg_id);
  void BackGround_GLog_Func();
  int  ApplyPendPages(std::map<off_t, std::pair<char*,char*>>* source, std::map<off_t, std::pair<char*,char*>>* target, std::map<off_t,std::pair<int,int>> *EmptyPages);
  void AIOFLushPlog_Demand(struct io_uring *ring);
  void InitBGGlog();
 
  bool Delete(const std::string& key);
  bool Get(const std::string& key, std::string& value) const;
   bool Scan(const std::string &start, const std::string &end, std::string &value) const;
  std::vector<std::pair<std::string, std::string>> GetRange(
      const std::string& left_key, const std::string& right_key) const;
  bool Empty() const;
  size_t Size() const;

#ifdef DEBUG
  void Dump();
#endif

 //private:

  template <typename T>
  T* AllocInMemPage()const;

  template <typename T>
  T* Map(off_t offset, int isleaf, int is_plog, char* &plog_copy) const; //is_plog: 1:leafnode; 1:plog; 3: Get both leaf and plog copy by one time  

 
  
  template <typename T>
  T* MapIndex(int page_id) const;

  template <typename T>
  T* MapLeaf(int page_id) const;

  
  template <typename T>
  void InsertDirty(off_t offset) const;
  template <typename T>
  void RelocateDirty(off_t offset_leaf, off_t offset_split) const;

  template <typename T>
  void DirectInsertDirty(off_t offset, off_t ori_oldset_lsn) const;

  
  template <typename T>
  void UnMap(T* map_obj, int isleaf, int is_plog) const;
  template <typename T>
  T* AllocLeaf();
  template <typename T>
  T* AllocIndex();
  template <typename T>
  void Dealloc(T* node);

  size_t GetMinKeys(int flag);
  size_t GetMaxKeys(int flag);
  size_t GetMinIndexKeys();
  size_t GetMaxIndexKeys();

  template <typename T>
  int UpperBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int UpperBoundIndex(T arr[], int n, const char* target) const;
  template <typename T>
  int LowerBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int LowerBoundIndex(T arr[], int n, const char* target) const;

  off_t GetLeafOffset(const char* key) const;
  void WarmUpVirtualPageCache(int x) const;
  LeafNode* SplitLeafNode(LeafNode* leaf_node);
  IndexNode* SplitIndexNode(IndexNode* index_node);
  size_t InsertKeyIntoIndexNode(IndexNode* index_node, const char* key,
                                Node* left_node, Node* right_node,off_t cur_lsn);
  size_t InsertKVIntoLeafNode(LeafNode* leaf_node, const char* key,
                              const char* value, off_t cur_lsn);
  size_t InsertKVIntoPlog(Plog* plog, const char* key,
                              const char* value, off_t cur_lsn);
  int GetIndexFromLeafNode(LeafNode* leaf_node, const char* key) const;
  int GetIndexFromPlog(Plog* plog, const char* key) const;
  IndexNode* GetOrCreateParent(Node* node);

  bool BorrowFromLeftLeafSibling(LeafNode* leaf_node);
  bool BorrowFromRightLeafSibling(LeafNode* leaf_node);
  bool BorrowFromLeafSibling(LeafNode* leaf_node);
  bool MergeLeftLeaf(LeafNode* leaf_node);
  bool MergeRightLeaf(LeafNode* leaf_node);
  LeafNode* MergeLeaf(LeafNode* leaf_node);

  bool BorrowFromLeftIndexSibling(IndexNode* index_node);
  bool BorrowFromRightIndexSibling(IndexNode* index_node);
  bool BorrowFromIndexSibling(IndexNode* index_node);
  bool MergeLeftIndex(IndexNode* index_node);
  bool MergeRightIndex(IndexNode* index_node);
  IndexNode* MergeIndex(IndexNode* index_node);

  int Check(off_t offset);

  char* GetMemCopy(off_t offset);
  Monitor* GetMtr();
  

  int fd_;
  int log_fd_;
  int chpt_fd_;
  int bg_id;
  //for column store
  int no_of_column_;
  std::vector<int>* value_size_of_each_column_;
  int key_size_;
  //based on such info to define 128 KB pages
  BlockCache* block_cache_;
  InternalCache* internal_cache_;
  Meta* meta_;
  Monitor* mtr_;
  LogRecord* log_obj;
  Checkpoint* chpt;
  std::thread BackGround_Glog;
};



class BPlusItemTree {
  struct Meta;
  struct Monitor;
  struct Index;
  struct Record;
  struct PAXKeyRecord;
  struct PAXColumn1Record;
  struct PAXColumn2Record;
  struct PAXColumn3Record;
  struct PAXColumn4Record;
  struct Per_page_Log;
  struct Node;
  struct IndexNode;
  struct LeafNode;
  struct PAXLeafNode;
  struct Plog;
  class BlockCache;
  class InternalCache;
  struct LogRecord;
  struct Checkpoint;
  
        

 public:
  //BPlusItemTree(const char* path, const char* log_path, const char* chpt_path, const int bg_id_, const bool enable_backthd);
  //~BPlusItemTree();
  //hkc-ap-00
  BPlusItemTree(const char* path, const char* log_path, const char* chpt_path, const int bg_id_, const bool enable_backthd, const char *shadow_plog_path);
  ~BPlusItemTree();

  void  SetIOStaic();

  off_t GetChpt();
  
  //hkc-ap-01
  void ScanAP_impl(off_t &left, off_t &right, std::string &value);
  //hkc-ap-02
  void APWorker();
  //hkc-ap-03
  void Getfd(int &fd);
  //hkc-ap-04
  int PrepareReadSetAP(off_t left, off_t right,void *scan_ptr);
  bool ScanAP(const std::string &start, const std::string &end, std::string &value) const;

  void SetLoadFlag(int flag);
  void DisableFLUOrNot(int flag);
  void LoadIOStatic(int &plog_insert, int &leaf_insert, int &leaf_split,int &phy_plog_write,int &phy_plog_read,int &phy_page_read,int &phy_page_write,int &logi_page_read,int &logi_page_write);
  void SetPlog();



  void SetBaseline();
  void GetStatic(int &hits_bp, int &hits_glog, int &hits_plog, int &disk_read, int &io_consume, int &error_cnt,int &dirtys, int &cleans);
  void ResetStatic();
  off_t GetLSN();
  void IncreLSN();

  
  void Put(const std::string& key, const std::string& value, off_t lsn);
  
  //hkc-gb-01
  off_t GetOnlyLeafOffset(const std::string& key);
  //hkc-gb-02
  char* GetLeafCopy(const std::string& key);
  void FlushAllPages();
  //hkc-gb-03
  void UpdateLeafCopy(const std::string& key, const std::string& value, off_t lsn, char* leaf_copy);
  //hkc-gb-03-1
  void GetRowRecord(const std::string &key,  std::string &value,  char* leaf_copy);
  //hkc-gb-03-2
  void TriggerFlush(const off_t* offsets, const int size, char* evict_buf);
  
  
  void RowToColumn(char* initial_page, char* converted_page);
  void ColumnToRow(char* initial_page, char* converted_page);
  //void Put_list(const std::string& key, const std::vector<std::string> value_list, off_t lsn);
  void InstantRecovery(off_t checkpoint);
  void NormalRecovery(off64_t checkpoint);
  void ApplyOneDiskToDataFile(int seg_id);
  void BackGround_GLog_Func();
  int  ApplyPendPages(std::map<off_t, std::pair<char*,char*>>* source, std::map<off_t, std::pair<char*,char*>>* target, std::map<off_t,std::pair<int,int>> *EmptyPages);
  void AIOFLushPlog_Demand(struct io_uring *ring);
  void InitBGGlog();
 
  bool Delete(const std::string& key);
  bool Get(const std::string& key, std::string& value) const;
  bool Scan(const std::string &start, const std::string &end, std::string &value) const;
  std::vector<std::pair<std::string, std::string>> GetRange(
      const std::string& left_key, const std::string& right_key) const;
  bool Empty() const;
  size_t Size() const;


 //private:

  template <typename T>
  T* AllocInMemPage()const;

  template <typename T>
  T* Map(off_t offset, int isleaf, int is_plog, char* &plog_copy) const; //is_plog: 1:leafnode; 1:plog; 3: Get both leaf and plog copy by one time  

 
  
  template <typename T>
  T* MapIndex(int page_id) const;

  template <typename T>
  T* MapLeaf(int page_id) const;

  
  template <typename T>
  void InsertDirty(off_t offset) const;
  template <typename T>
  void RelocateDirty(off_t offset_leaf, off_t offset_split) const;

  template <typename T>
  void DirectInsertDirty(off_t offset, off_t ori_oldset_lsn) const;

  
  template <typename T>
  void UnMap(T* map_obj, int isleaf, int is_plog) const;
  template <typename T>
  T* AllocLeaf();
  template <typename T>
  T* AllocIndex();
  template <typename T>
  void Dealloc(T* node);

  size_t GetMinKeys(int flag);
  size_t GetMaxKeys(int flag);
  size_t GetMinIndexKeys();
  size_t GetMaxIndexKeys();

  template <typename T>
  int UpperBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int UpperBoundIndex(T arr[], int n, const char* target) const;
  template <typename T>
  int LowerBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int LowerBoundIndex(T arr[], int n, const char* target) const;

  off_t GetLeafOffset(const char* key) const;
  void WarmUpVirtualPageCache(int x) const;
  LeafNode* SplitLeafNode(LeafNode* leaf_node);
  IndexNode* SplitIndexNode(IndexNode* index_node);
  size_t InsertKeyIntoIndexNode(IndexNode* index_node, const char* key,
                                Node* left_node, Node* right_node,off_t cur_lsn);
  size_t InsertKVIntoLeafNode(LeafNode* leaf_node, const char* key,
                              const char* value1, const char* value2,const char* value3,const char* value4, off_t cur_lsn);
  size_t InsertKVIntoPlog(Plog* plog, const char* key,
                              const char* value1, const char* value2,const char* value3,const char* value4, off_t cur_lsn);
  int GetIndexFromLeafNode(LeafNode* leaf_node, const char* key) const;
  int GetIndexFromPlog(Plog* plog, const char* key) const;
  IndexNode* GetOrCreateParent(Node* node);

  bool BorrowFromLeftLeafSibling(LeafNode* leaf_node);
  bool BorrowFromRightLeafSibling(LeafNode* leaf_node);
  bool BorrowFromLeafSibling(LeafNode* leaf_node);
  bool MergeLeftLeaf(LeafNode* leaf_node);
  bool MergeRightLeaf(LeafNode* leaf_node);
  LeafNode* MergeLeaf(LeafNode* leaf_node);

  bool BorrowFromLeftIndexSibling(IndexNode* index_node);
  bool BorrowFromRightIndexSibling(IndexNode* index_node);
  bool BorrowFromIndexSibling(IndexNode* index_node);
  bool MergeLeftIndex(IndexNode* index_node);
  bool MergeRightIndex(IndexNode* index_node);
  IndexNode* MergeIndex(IndexNode* index_node);

  int Check(off_t offset);

  char* GetMemCopy(off_t offset);
  Monitor* GetMtr();
  

  //hkc-ap-05
  int fd_shadow_plog_;
  off_t shadow_plog_off;
  //hkc-ap-06
  bool scan_ap;

  int fd_;
  int log_fd_;
  int chpt_fd_;
  int bg_id;



  BlockCache* block_cache_;
  InternalCache* internal_cache_;
  Meta* meta_;
  Monitor* mtr_;
  LogRecord* log_obj;
  Checkpoint* chpt;
  std::thread BackGround_Glog;
};



class BPlusNewOrdersTree {
  struct Meta;
  struct Monitor;
  struct Index;
  struct Record;
  struct PAXKeyRecord;
  struct PAXColumn1Record;
  struct Per_page_Log;
  struct Node;
  struct IndexNode;
  struct LeafNode;
  struct PAXLeafNode;
  struct Plog;
  class BlockCache;
  class InternalCache;
  struct LogRecord;
  struct Checkpoint;
  
        

 public:
   //hkc-ap-00
  BPlusNewOrdersTree(const char* path, const char* log_path, const char* chpt_path, const int bg_id_, const bool enable_backthd, const char *shadow_plog_path);
  ~BPlusNewOrdersTree();

   //hkc-ap-01
  void ScanAP_impl(off_t &left, off_t &right, std::string &value);

  void  SetIOStaic();
  off_t GetChpt();

    //hkc-ap-02
  void APWorker();

  void SetLoadFlag(int flag);
  void DisableFLUOrNot(int flag);
  void LoadIOStatic(int &plog_insert, int &leaf_insert, int &leaf_split,int &phy_plog_write,int &phy_plog_read,int &phy_page_read,int &phy_page_write,int &logi_page_read,int &logi_page_write);
  void SetPlog();

  //hkc-ap-03
  void Getfd(int &fd);
  //hkc-ap-04
  int PrepareReadSetAP(off_t left, off_t right,void *scan_ptr);
  bool ScanAP(const std::string &start, const std::string &end, std::string &value) const;

  void SetBaseline();
  void GetStatic(int &hits_bp, int &hits_glog, int &hits_plog, int &disk_read, int &io_consume, int &error_cnt,int &dirtys, int &cleans);
  void ResetStatic();
  off_t GetLSN();
  void IncreLSN();

  
  void Put(const std::string& key, const std::string& value, off_t lsn);
  
  //hkc-gb-01
  off_t GetOnlyLeafOffset(const std::string& key);
  //hkc-gb-02
  char* GetLeafCopy(const std::string& key);
  void FlushAllPages();
  //hkc-gb-03
  void UpdateLeafCopy(const std::string& key, const std::string& value, off_t lsn, char* leaf_copy);
  //hkc-gb-03-1
  void GetRowRecord(const std::string &key,  std::string &value,  char* leaf_copy);
  //hkc-gb-03-2
  void TriggerFlush(const off_t* offsets, const int size, char* evict_buf);
  
  
  void RowToColumn(char* initial_page, char* converted_page);
  void ColumnToRow(char* initial_page, char* converted_page);
  //void Put_list(const std::string& key, const std::vector<std::string> value_list, off_t lsn);
  void InstantRecovery(off_t checkpoint);
  void NormalRecovery(off64_t checkpoint);
  void ApplyOneDiskToDataFile(int seg_id);
  void BackGround_GLog_Func();
  int  ApplyPendPages(std::map<off_t, std::pair<char*,char*>>* source, std::map<off_t, std::pair<char*,char*>>* target, std::map<off_t,std::pair<int,int>> *EmptyPages);
  void AIOFLushPlog_Demand(struct io_uring *ring);
  void InitBGGlog();
 
  bool Delete(const std::string& key);
  bool Get(const std::string& key, std::string& value) const;
  bool Scan(const std::string &start, const std::string &end, std::string &value) const;
  std::vector<std::pair<std::string, std::string>> GetRange(
      const std::string& left_key, const std::string& right_key) const;
  bool Empty() const;
  size_t Size() const;


 //private:

  template <typename T>
  T* AllocInMemPage()const;

  template <typename T>
  T* Map(off_t offset, int isleaf, int is_plog, char* &plog_copy) const; //is_plog: 1:leafnode; 1:plog; 3: Get both leaf and plog copy by one time  

 
  
  template <typename T>
  T* MapIndex(int page_id) const;

  template <typename T>
  T* MapLeaf(int page_id) const;

  
  template <typename T>
  void InsertDirty(off_t offset) const;
  template <typename T>
  void RelocateDirty(off_t offset_leaf, off_t offset_split) const;

  template <typename T>
  void DirectInsertDirty(off_t offset, off_t ori_oldset_lsn) const;

  
  template <typename T>
  void UnMap(T* map_obj, int isleaf, int is_plog) const;
  template <typename T>
  T* AllocLeaf();
  template <typename T>
  T* AllocIndex();
  template <typename T>
  void Dealloc(T* node);

  size_t GetMinKeys(int flag);
  size_t GetMaxKeys(int flag);
  size_t GetMinIndexKeys();
  size_t GetMaxIndexKeys();

  template <typename T>
  int UpperBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int UpperBoundIndex(T arr[], int n, const char* target) const;
  template <typename T>
  int LowerBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int LowerBoundIndex(T arr[], int n, const char* target) const;

  off_t GetLeafOffset(const char* key) const;
  void WarmUpVirtualPageCache(int x) const;
  LeafNode* SplitLeafNode(LeafNode* leaf_node);
  IndexNode* SplitIndexNode(IndexNode* index_node);
  size_t InsertKeyIntoIndexNode(IndexNode* index_node, const char* key,
                                Node* left_node, Node* right_node,off_t cur_lsn);
  size_t InsertKVIntoLeafNode(LeafNode* leaf_node, const char* key,
                              const char* value1, off_t cur_lsn);
  size_t InsertKVIntoPlog(Plog* plog, const char* key,
                              const char* value1, off_t cur_lsn);
  int GetIndexFromLeafNode(LeafNode* leaf_node, const char* key) const;
  int GetIndexFromPlog(Plog* plog, const char* key) const;
  IndexNode* GetOrCreateParent(Node* node);

  bool BorrowFromLeftLeafSibling(LeafNode* leaf_node);
  bool BorrowFromRightLeafSibling(LeafNode* leaf_node);
  bool BorrowFromLeafSibling(LeafNode* leaf_node);
  bool MergeLeftLeaf(LeafNode* leaf_node);
  bool MergeRightLeaf(LeafNode* leaf_node);
  LeafNode* MergeLeaf(LeafNode* leaf_node);

  bool BorrowFromLeftIndexSibling(IndexNode* index_node);
  bool BorrowFromRightIndexSibling(IndexNode* index_node);
  bool BorrowFromIndexSibling(IndexNode* index_node);
  bool MergeLeftIndex(IndexNode* index_node);
  bool MergeRightIndex(IndexNode* index_node);
  IndexNode* MergeIndex(IndexNode* index_node);

  int Check(off_t offset);

  char* GetMemCopy(off_t offset);
  Monitor* GetMtr();
  
  //hkc-ap-05
  int fd_shadow_plog_;
  off_t shadow_plog_off;


  int fd_;
  int log_fd_;
  int chpt_fd_;
  int bg_id;
  
  //hkc-ap-06
  bool scan_ap;

  BlockCache* block_cache_;
  InternalCache* internal_cache_;
  Meta* meta_;
  Monitor* mtr_;
  LogRecord* log_obj;
  Checkpoint* chpt;
  std::thread BackGround_Glog;
};




class BPlusStockTree {
  struct Meta;
  struct Monitor;
  struct Index;
  struct Record;
  struct PAXKeyRecord;
  struct PAXColumn1Record;
  struct PAXColumn2Record;
  struct PAXColumn3Record;
  struct PAXColumn4Record;
  struct PAXColumn5Record;
  struct PAXColumn6Record;
  struct Per_page_Log;
  struct Node;
  struct IndexNode;
  struct LeafNode;
  struct PAXLeafNode;
  struct Plog;
  class BlockCache;
  class InternalCache;
  struct LogRecord;
  struct Checkpoint;
  
        

 public:
 //hkc-ap-00
  BPlusStockTree(const char* path, const char* log_path, const char* chpt_path, const int bg_id_, const bool enable_backthd, const char *shadow_plog_path);
  ~BPlusStockTree();

  void  SetIOStaic();
  off_t GetChpt();

  //hkc-ap-01
  void ScanAP_impl(off_t &left, off_t &right, std::string &value);
  //hkc-ap-02
  void APWorker();
  //hkc-ap-03
  void Getfd(int &fd);
  //hkc-ap-04
  int PrepareReadSetAP(off_t left, off_t right,void *scan_ptr);
  bool ScanAP(const std::string &start, const std::string &end, std::string &value) const;
  
  void SetLoadFlag(int flag);
  void DisableFLUOrNot(int flag);
  void LoadIOStatic(int &plog_insert, int &leaf_insert, int &leaf_split,int &phy_plog_write,int &phy_plog_read,int &phy_page_read,int &phy_page_write,int &logi_page_read,int &logi_page_write);
  void SetPlog();
  void SetBaseline();
  void GetStatic(int &hits_bp, int &hits_glog, int &hits_plog, int &disk_read, int &io_consume, int &error_cnt,int &dirtys, int &cleans);
  void ResetStatic();
  off_t GetLSN();
  void IncreLSN();

  
  void Put(const std::string& key, const std::string& value, off_t lsn);

  //hkc-gb-01
  off_t GetOnlyLeafOffset(const std::string& key);
  //hkc-gb-02
  char* GetLeafCopy(const std::string& key);
  void FlushAllPages();
  //hkc-gb-03
  void UpdateLeafCopy(const std::string& key, const std::string& value, off_t lsn, char* leaf_copy);
  //hkc-gb-03-1
  void GetRowRecord(const std::string &key,  std::string &value,  char* leaf_copy);
  //hkc-gb-03-2
  void TriggerFlush(const off_t* offsets, const int size, char* evict_buf);
  
  
  void RowToColumn(char* initial_page, char* converted_page);
  void ColumnToRow(char* initial_page, char* converted_page);
  //void Put_list(const std::string& key, const std::vector<std::string> value_list, off_t lsn);
  void InstantRecovery(off_t checkpoint);
  void NormalRecovery(off64_t checkpoint);
  void ApplyOneDiskToDataFile(int seg_id);
  void BackGround_GLog_Func();
  int  ApplyPendPages(std::map<off_t, std::pair<char*,char*>>* source, std::map<off_t, std::pair<char*,char*>>* target, std::map<off_t,std::pair<int,int>> *EmptyPages);
  void AIOFLushPlog_Demand(struct io_uring *ring);
  void InitBGGlog();
 
  bool Delete(const std::string& key);
  bool Get(const std::string& key, std::string& value) const;
  bool Scan(const std::string &start, const std::string &end, std::string &value) const;
  std::vector<std::pair<std::string, std::string>> GetRange(
      const std::string& left_key, const std::string& right_key) const;
  bool Empty() const;
  size_t Size() const;


 //private:

  template <typename T>
  T* AllocInMemPage()const;

  template <typename T>
  T* Map(off_t offset, int isleaf, int is_plog, char* &plog_copy) const; //is_plog: 1:leafnode; 1:plog; 3: Get both leaf and plog copy by one time  

 
  
  template <typename T>
  T* MapIndex(int page_id) const;

  template <typename T>
  T* MapLeaf(int page_id) const;

  
  template <typename T>
  void InsertDirty(off_t offset) const;
  template <typename T>
  void RelocateDirty(off_t offset_leaf, off_t offset_split) const;

  template <typename T>
  void DirectInsertDirty(off_t offset, off_t ori_oldset_lsn) const;

  
  template <typename T>
  void UnMap(T* map_obj, int isleaf, int is_plog) const;
  template <typename T>
  T* AllocLeaf();
  template <typename T>
  T* AllocIndex();
  template <typename T>
  void Dealloc(T* node);

  size_t GetMinKeys(int flag);
  size_t GetMaxKeys(int flag);
  size_t GetMinIndexKeys();
  size_t GetMaxIndexKeys();

  template <typename T>
  int UpperBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int UpperBoundIndex(T arr[], int n, const char* target) const;
  template <typename T>
  int LowerBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int LowerBoundIndex(T arr[], int n, const char* target) const;

  off_t GetLeafOffset(const char* key) const;
  void WarmUpVirtualPageCache(int x) const;
  LeafNode* SplitLeafNode(LeafNode* leaf_node);
  IndexNode* SplitIndexNode(IndexNode* index_node);
  size_t InsertKeyIntoIndexNode(IndexNode* index_node, const char* key,
                                Node* left_node, Node* right_node,off_t cur_lsn);
  size_t InsertKVIntoLeafNode(LeafNode* leaf_node, const char* key,
                              const char* value1, const char* value2,const char* value3,const char* value4,const char* value5,const char* value6, off_t cur_lsn);
  size_t InsertKVIntoPlog(Plog* plog, const char* key,
                              const char* value1, const char* value2,const char* value3,const char* value4, const char* value5,const char* value6,off_t cur_lsn);
  
  //hkc-plog-0
  size_t InsertDeltaIntoPlog(Plog* plog, const char* key,const char* value_quality, off_t cur_lsn);
  size_t InsertDeltaKVIntoLeafNode(LeafNode *leaf_node, const char *key,const char* value_balance, off_t cur_lsn);
  
  int GetIndexFromLeafNode(LeafNode* leaf_node, const char* key) const;
  int GetIndexFromPlog(Plog* plog, const char* key) const;
  IndexNode* GetOrCreateParent(Node* node);

  bool BorrowFromLeftLeafSibling(LeafNode* leaf_node);
  bool BorrowFromRightLeafSibling(LeafNode* leaf_node);
  bool BorrowFromLeafSibling(LeafNode* leaf_node);
  bool MergeLeftLeaf(LeafNode* leaf_node);
  bool MergeRightLeaf(LeafNode* leaf_node);
  LeafNode* MergeLeaf(LeafNode* leaf_node);

  bool BorrowFromLeftIndexSibling(IndexNode* index_node);
  bool BorrowFromRightIndexSibling(IndexNode* index_node);
  bool BorrowFromIndexSibling(IndexNode* index_node);
  bool MergeLeftIndex(IndexNode* index_node);
  bool MergeRightIndex(IndexNode* index_node);
  IndexNode* MergeIndex(IndexNode* index_node);

  int Check(off_t offset);

  char* GetMemCopy(off_t offset);
  Monitor* GetMtr();
  

//hkc-ap-05
  int fd_shadow_plog_;
  off_t shadow_plog_off;
  //hkc-ap-06
  bool scan_ap;


  int fd_;
  int log_fd_;
  int chpt_fd_;
  int bg_id;
  BlockCache* block_cache_;
  InternalCache* internal_cache_;
  Meta* meta_;
  Monitor* mtr_;
  LogRecord* log_obj;
  Checkpoint* chpt;
  std::thread BackGround_Glog;
};



class BPlusOrdersTree {
  struct Meta;
  struct Monitor;
  struct Index;
  struct Record;
  struct PAXKeyRecord;
  struct PAXColumn1Record;
  struct PAXColumn2Record;
  struct PAXColumn3Record;
  struct PAXColumn4Record;
  struct PAXColumn5Record;
  struct Per_page_Log;
  struct Node;
  struct IndexNode;
  struct LeafNode;
  struct PAXLeafNode;
  struct Plog;
  class BlockCache;
  class InternalCache;
  struct LogRecord;
  struct Checkpoint;
  
        

 public:
 //hkc-ap-00
  BPlusOrdersTree(const char* path, const char* log_path, const char* chpt_path, const int bg_id_, const bool enable_backthd, const char *shadow_plog_path);
  ~BPlusOrdersTree();

  void  SetIOStaic();
  off_t GetChpt();

  //hkc-ap-01
  void ScanAP_impl(off_t &left, off_t &right, std::string &value);
  //hkc-ap-02
  void APWorker();
  //hkc-ap-03
  void Getfd(int &fd);
  //hkc-ap-04
  int PrepareReadSetAP(off_t left, off_t right,void *scan_ptr);
  bool ScanAP(const std::string &start, const std::string &end, std::string &value) const;
  
  void SetLoadFlag(int flag);
  void DisableFLUOrNot(int flag);
  void LoadIOStatic(int &plog_insert, int &leaf_insert, int &leaf_split,int &phy_plog_write,int &phy_plog_read,int &phy_page_read,int &phy_page_write,int &logi_page_read,int &logi_page_write);
  void SetPlog();
  void SetBaseline();
  void GetStatic(int &hits_bp, int &hits_glog, int &hits_plog, int &disk_read, int &io_consume, int &error_cnt,int &dirtys, int &cleans);
  void ResetStatic();
  off_t GetLSN();
  void IncreLSN();

  
  void Put(const std::string& key, const std::string& value, off_t lsn);
  
  //hkc-gb-01
  off_t GetOnlyLeafOffset(const std::string& key);
  //hkc-gb-02
  char* GetLeafCopy(const std::string& key);
  void FlushAllPages();
  //hkc-gb-03
  void UpdateLeafCopy(const std::string& key, const std::string& value, off_t lsn, char* leaf_copy);
  //hkc-gb-03-1
  void GetRowRecord(const std::string &key,  std::string &value,  char* leaf_copy);
  //hkc-gb-03-2
  void TriggerFlush(const off_t* offsets, const int size, char* evict_buf);
  
  
  
  void RowToColumn(char* initial_page, char* converted_page);
  void ColumnToRow(char* initial_page, char* converted_page);
  //void Put_list(const std::string& key, const std::vector<std::string> value_list, off_t lsn);
  void InstantRecovery(off_t checkpoint);
  void NormalRecovery(off64_t checkpoint);
  void ApplyOneDiskToDataFile(int seg_id);
  void BackGround_GLog_Func();
  int  ApplyPendPages(std::map<off_t, std::pair<char*,char*>>* source, std::map<off_t, std::pair<char*,char*>>* target, std::map<off_t,std::pair<int,int>> *EmptyPages);
  void AIOFLushPlog_Demand(struct io_uring *ring);
  void InitBGGlog();
 
  bool Delete(const std::string& key);
  bool Get(const std::string& key, std::string& value) const;
  bool Scan(const std::string &start, const std::string &end, std::string &value) const;
  std::vector<std::pair<std::string, std::string>> GetRange(
      const std::string& left_key, const std::string& right_key) const;
  bool Empty() const;
  size_t Size() const;


 //private:

  template <typename T>
  T* AllocInMemPage()const;

  template <typename T>
  T* Map(off_t offset, int isleaf, int is_plog, char* &plog_copy) const; //is_plog: 1:leafnode; 1:plog; 3: Get both leaf and plog copy by one time  

 
  
  template <typename T>
  T* MapIndex(int page_id) const;

  template <typename T>
  T* MapLeaf(int page_id) const;

  
  template <typename T>
  void InsertDirty(off_t offset) const;
  template <typename T>
  void RelocateDirty(off_t offset_leaf, off_t offset_split) const;

  template <typename T>
  void DirectInsertDirty(off_t offset, off_t ori_oldset_lsn) const;

  
  template <typename T>
  void UnMap(T* map_obj, int isleaf, int is_plog) const;
  template <typename T>
  T* AllocLeaf();
  template <typename T>
  T* AllocIndex();
  template <typename T>
  void Dealloc(T* node);

  size_t GetMinKeys(int flag);
  size_t GetMaxKeys(int flag);
  size_t GetMinIndexKeys();
  size_t GetMaxIndexKeys();

  template <typename T>
  int UpperBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int UpperBoundIndex(T arr[], int n, const char* target) const;
  template <typename T>
  int LowerBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int LowerBoundIndex(T arr[], int n, const char* target) const;

  off_t GetLeafOffset(const char* key) const;
  void WarmUpVirtualPageCache(int x) const;
  LeafNode* SplitLeafNode(LeafNode* leaf_node);
  IndexNode* SplitIndexNode(IndexNode* index_node);
  size_t InsertKeyIntoIndexNode(IndexNode* index_node, const char* key,
                                Node* left_node, Node* right_node,off_t cur_lsn);
  size_t InsertKVIntoLeafNode(LeafNode* leaf_node, const char* key,
                              const char* value1, const char* value2,const char* value3,const char* value4,const char* value5, off_t cur_lsn);
  size_t InsertKVIntoPlog(Plog* plog, const char* key,
                              const char* value1, const char* value2,const char* value3,const char* value4, const char* value5, off_t cur_lsn);
  int GetIndexFromLeafNode(LeafNode* leaf_node, const char* key) const;
  int GetIndexFromPlog(Plog* plog, const char* key) const;
  IndexNode* GetOrCreateParent(Node* node);

  //hkc-plog-0
  size_t InsertDeltaIntoPlog(Plog* plog, const char* key,const char* value_carrier, off_t cur_lsn);
  size_t InsertDeltaKVIntoLeafNode(LeafNode *leaf_node, const char *key, const char* value_carrier, off_t cur_lsn);

  bool BorrowFromLeftLeafSibling(LeafNode* leaf_node);
  bool BorrowFromRightLeafSibling(LeafNode* leaf_node);
  bool BorrowFromLeafSibling(LeafNode* leaf_node);
  bool MergeLeftLeaf(LeafNode* leaf_node);
  bool MergeRightLeaf(LeafNode* leaf_node);
  LeafNode* MergeLeaf(LeafNode* leaf_node);

  bool BorrowFromLeftIndexSibling(IndexNode* index_node);
  bool BorrowFromRightIndexSibling(IndexNode* index_node);
  bool BorrowFromIndexSibling(IndexNode* index_node);
  bool MergeLeftIndex(IndexNode* index_node);
  bool MergeRightIndex(IndexNode* index_node);
  IndexNode* MergeIndex(IndexNode* index_node);

  int Check(off_t offset);

  char* GetMemCopy(off_t offset);
  Monitor* GetMtr();
  

  int fd_;
  int log_fd_;
  int chpt_fd_;
  int bg_id;

  //hkc-ap-05
  int fd_shadow_plog_;
  off_t shadow_plog_off;
  //hkc-ap-06
  bool scan_ap;

  BlockCache* block_cache_;
  InternalCache* internal_cache_;
  Meta* meta_;
  Monitor* mtr_;
  LogRecord* log_obj;
  Checkpoint* chpt;
  std::thread BackGround_Glog;
};



class BPlusHistoryfk1Tree {
  struct Meta;
  struct Monitor;
  struct Index;
  struct Record;
  struct PAXKeyRecord;
  struct PAXColumn1Record;
  struct PAXColumn2Record;
  struct PAXColumn3Record;
  struct PAXColumn4Record;
  struct PAXColumn5Record;
  struct Per_page_Log;
  struct Node;
  struct IndexNode;
  struct LeafNode;
  struct PAXLeafNode;
  struct Plog;
  class BlockCache;
  class InternalCache;
  struct LogRecord;
  struct Checkpoint;
  
        

 public:
   //hkc-ap-00
  BPlusHistoryfk1Tree(const char* path, const char* log_path, const char* chpt_path, const int bg_id_, const bool enable_backthd, const char *shadow_plog_path);
  ~BPlusHistoryfk1Tree();

  void  SetIOStaic();
  off_t GetChpt();

  //hkc-ap-01
  void ScanAP_impl(off_t &left, off_t &right, std::string &value);
  //hkc-ap-02
  void APWorker();
  //hkc-ap-03
  void Getfd(int &fd);
  //hkc-ap-04
  int PrepareReadSetAP(off_t left, off_t right,void *scan_ptr);
  bool ScanAP(const std::string &start, const std::string &end, std::string &value) const;

  void SetLoadFlag(int flag);
  void DisableFLUOrNot(int flag);
  void LoadIOStatic(int &plog_insert, int &leaf_insert, int &leaf_split,int &phy_plog_write,int &phy_plog_read,int &phy_page_read,int &phy_page_write,int &logi_page_read,int &logi_page_write);
  void SetPlog();
  void SetBaseline();
  void GetStatic(int &hits_bp, int &hits_glog, int &hits_plog, int &disk_read, int &io_consume, int &error_cnt,int &dirtys, int &cleans);
  void ResetStatic();
  off_t GetLSN();
  void IncreLSN();

  
  void Put(const std::string& key, const std::string& value, off_t lsn);
  
  //hkc-gb-01
  off_t GetOnlyLeafOffset(const std::string& key);
  //hkc-gb-02
  char* GetLeafCopy(const std::string& key);
  void FlushAllPages();
  //hkc-gb-03
  void UpdateLeafCopy(const std::string& key, const std::string& value, off_t lsn, char* leaf_copy);
  //hkc-gb-03-1
  void GetRowRecord(const std::string &key,  std::string &value,  char* leaf_copy);
  //hkc-gb-03-2
  void TriggerFlush(const off_t* offsets, const int size, char* evict_buf);
  
  
  void RowToColumn(char* initial_page, char* converted_page);
  void ColumnToRow(char* initial_page, char* converted_page);
  //void Put_list(const std::string& key, const std::vector<std::string> value_list, off_t lsn);
  void InstantRecovery(off_t checkpoint);
  void NormalRecovery(off64_t checkpoint);
  void ApplyOneDiskToDataFile(int seg_id);
  void BackGround_GLog_Func();
  int  ApplyPendPages(std::map<off_t, std::pair<char*,char*>>* source, std::map<off_t, std::pair<char*,char*>>* target, std::map<off_t,std::pair<int,int>> *EmptyPages);
  void AIOFLushPlog_Demand(struct io_uring *ring);
  void InitBGGlog();
 
  bool Delete(const std::string& key);
  bool Get(const std::string& key, std::string& value) const;
  bool Scan(const std::string &start, const std::string &end, std::string &value) const;
  std::vector<std::pair<std::string, std::string>> GetRange(
      const std::string& left_key, const std::string& right_key) const;
  bool Empty() const;
  size_t Size() const;


 //private:

  template <typename T>
  T* AllocInMemPage()const;

  template <typename T>
  T* Map(off_t offset, int isleaf, int is_plog, char* &plog_copy) const; //is_plog: 1:leafnode; 1:plog; 3: Get both leaf and plog copy by one time  

 
  
  template <typename T>
  T* MapIndex(int page_id) const;

  template <typename T>
  T* MapLeaf(int page_id) const;

  
  template <typename T>
  void InsertDirty(off_t offset) const;
  template <typename T>
  void RelocateDirty(off_t offset_leaf, off_t offset_split) const;

  template <typename T>
  void DirectInsertDirty(off_t offset, off_t ori_oldset_lsn) const;

  
  template <typename T>
  void UnMap(T* map_obj, int isleaf, int is_plog) const;
  template <typename T>
  T* AllocLeaf();
  template <typename T>
  T* AllocIndex();
  template <typename T>
  void Dealloc(T* node);

  size_t GetMinKeys(int flag);
  size_t GetMaxKeys(int flag);
  size_t GetMinIndexKeys();
  size_t GetMaxIndexKeys();

  template <typename T>
  int UpperBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int UpperBoundIndex(T arr[], int n, const char* target) const;
  template <typename T>
  int LowerBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int LowerBoundIndex(T arr[], int n, const char* target) const;

  off_t GetLeafOffset(const char* key) const;
  void WarmUpVirtualPageCache(int x) const;
  LeafNode* SplitLeafNode(LeafNode* leaf_node);
  IndexNode* SplitIndexNode(IndexNode* index_node);
  size_t InsertKeyIntoIndexNode(IndexNode* index_node, const char* key,
                                Node* left_node, Node* right_node,off_t cur_lsn);
  size_t InsertKVIntoLeafNode(LeafNode* leaf_node, const char* key,
                              const char* value1, const char* value2,const char* value3,const char* value4,const char* value5, off_t cur_lsn);
  size_t InsertKVIntoPlog(Plog* plog, const char* key,
                              const char* value1, const char* value2,const char* value3,const char* value4, const char* value5, off_t cur_lsn);
  int GetIndexFromLeafNode(LeafNode* leaf_node, const char* key) const;
  int GetIndexFromPlog(Plog* plog, const char* key) const;
  IndexNode* GetOrCreateParent(Node* node);

  bool BorrowFromLeftLeafSibling(LeafNode* leaf_node);
  bool BorrowFromRightLeafSibling(LeafNode* leaf_node);
  bool BorrowFromLeafSibling(LeafNode* leaf_node);
  bool MergeLeftLeaf(LeafNode* leaf_node);
  bool MergeRightLeaf(LeafNode* leaf_node);
  LeafNode* MergeLeaf(LeafNode* leaf_node);

  bool BorrowFromLeftIndexSibling(IndexNode* index_node);
  bool BorrowFromRightIndexSibling(IndexNode* index_node);
  bool BorrowFromIndexSibling(IndexNode* index_node);
  bool MergeLeftIndex(IndexNode* index_node);
  bool MergeRightIndex(IndexNode* index_node);
  IndexNode* MergeIndex(IndexNode* index_node);

  int Check(off_t offset);

  char* GetMemCopy(off_t offset);
  Monitor* GetMtr();
  

  int fd_;
  int log_fd_;
  int chpt_fd_;
  int bg_id;

  //hkc-ap-05
  int fd_shadow_plog_;
  off_t shadow_plog_off;
  //hkc-ap-06
  bool scan_ap;

  BlockCache* block_cache_;
  InternalCache* internal_cache_;
  Meta* meta_;
  Monitor* mtr_;
  LogRecord* log_obj;
  Checkpoint* chpt;
  std::thread BackGround_Glog;
};


class BPlusHistoryfk2Tree {
  struct Meta;
  struct Monitor;
  struct Index;
  struct Record;
  struct PAXKeyRecord;
  struct PAXColumn1Record;
  struct PAXColumn2Record;
  struct PAXColumn3Record;
  struct PAXColumn4Record;
  struct PAXColumn5Record;
  struct PAXColumn6Record;
  struct Per_page_Log;
  struct Node;
  struct IndexNode;
  struct LeafNode;
  struct PAXLeafNode;
  struct Plog;
  class BlockCache;
  class InternalCache;
  struct LogRecord;
  struct Checkpoint;
  
        

 public:
   //hkc-ap-00
  BPlusHistoryfk2Tree(const char* path, const char* log_path, const char* chpt_path, const int bg_id_, const bool enable_backthd, const char *shadow_plog_path);
  ~BPlusHistoryfk2Tree();

  void  SetIOStaic();
  off_t GetChpt();

  //hkc-ap-01
  void ScanAP_impl(off_t &left, off_t &right, std::string &value);
  //hkc-ap-02
  void APWorker();
  //hkc-ap-03
  void Getfd(int &fd);
  //hkc-ap-04
  int PrepareReadSetAP(off_t left, off_t right,void *scan_ptr);
  bool ScanAP(const std::string &start, const std::string &end, std::string &value) const;
  
  void SetLoadFlag(int flag);
  void DisableFLUOrNot(int flag);
  void LoadIOStatic(int &plog_insert, int &leaf_insert, int &leaf_split,int &phy_plog_write,int &phy_plog_read,int &phy_page_read,int &phy_page_write,int &logi_page_read,int &logi_page_write);
  void SetPlog();
  void SetBaseline();
  void GetStatic(int &hits_bp, int &hits_glog, int &hits_plog, int &disk_read, int &io_consume, int &error_cnt,int &dirtys, int &cleans);
  void ResetStatic();
  off_t GetLSN();
  void IncreLSN();

  
  void Put(const std::string& key, const std::string& value, off_t lsn);
  
  //hkc-gb-01
  off_t GetOnlyLeafOffset(const std::string& key);
  //hkc-gb-02
  char* GetLeafCopy(const std::string& key);
  void FlushAllPages();
  //hkc-gb-03
  void UpdateLeafCopy(const std::string& key, const std::string& value, off_t lsn, char* leaf_copy);
  //hkc-gb-03-1
  void GetRowRecord(const std::string &key,  std::string &value,  char* leaf_copy);
  //hkc-gb-03-2
  void TriggerFlush(const off_t* offsets, const int size, char* evict_buf);
  
  
  void RowToColumn(char* initial_page, char* converted_page);
  void ColumnToRow(char* initial_page, char* converted_page);
  //void Put_list(const std::string& key, const std::vector<std::string> value_list, off_t lsn);
  void InstantRecovery(off_t checkpoint);
  void NormalRecovery(off64_t checkpoint);
  void ApplyOneDiskToDataFile(int seg_id);
  void BackGround_GLog_Func();
  int  ApplyPendPages(std::map<off_t, std::pair<char*,char*>>* source, std::map<off_t, std::pair<char*,char*>>* target, std::map<off_t,std::pair<int,int>> *EmptyPages);
  void AIOFLushPlog_Demand(struct io_uring *ring);
  void InitBGGlog();
 
  bool Delete(const std::string& key);
  bool Get(const std::string& key, std::string& value) const;
  bool Scan(const std::string &start, const std::string &end, std::string &value) const;
  std::vector<std::pair<std::string, std::string>> GetRange(
      const std::string& left_key, const std::string& right_key) const;
  bool Empty() const;
  size_t Size() const;


 //private:

  template <typename T>
  T* AllocInMemPage()const;

  template <typename T>
  T* Map(off_t offset, int isleaf, int is_plog, char* &plog_copy) const; //is_plog: 1:leafnode; 1:plog; 3: Get both leaf and plog copy by one time  

 
  
  template <typename T>
  T* MapIndex(int page_id) const;

  template <typename T>
  T* MapLeaf(int page_id) const;

  
  template <typename T>
  void InsertDirty(off_t offset) const;
  template <typename T>
  void RelocateDirty(off_t offset_leaf, off_t offset_split) const;

  template <typename T>
  void DirectInsertDirty(off_t offset, off_t ori_oldset_lsn) const;

  
  template <typename T>
  void UnMap(T* map_obj, int isleaf, int is_plog) const;
  template <typename T>
  T* AllocLeaf();
  template <typename T>
  T* AllocIndex();
  template <typename T>
  void Dealloc(T* node);

  size_t GetMinKeys(int flag);
  size_t GetMaxKeys(int flag);
  size_t GetMinIndexKeys();
  size_t GetMaxIndexKeys();

  template <typename T>
  int UpperBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int UpperBoundIndex(T arr[], int n, const char* target) const;
  template <typename T>
  int LowerBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int LowerBoundIndex(T arr[], int n, const char* target) const;

  off_t GetLeafOffset(const char* key) const;
  void WarmUpVirtualPageCache(int x) const;
  LeafNode* SplitLeafNode(LeafNode* leaf_node);
  IndexNode* SplitIndexNode(IndexNode* index_node);
  size_t InsertKeyIntoIndexNode(IndexNode* index_node, const char* key,
                                Node* left_node, Node* right_node,off_t cur_lsn);
  size_t InsertKVIntoLeafNode(LeafNode* leaf_node, const char* key,
                              const char* value1, const char* value2,const char* value3,const char* value4,const char* value5,const char* value6, off_t cur_lsn);
  size_t InsertKVIntoPlog(Plog* plog, const char* key,
                              const char* value1, const char* value2,const char* value3,const char* value4, const char* value5, const char* value6, off_t cur_lsn);
  int GetIndexFromLeafNode(LeafNode* leaf_node, const char* key) const;
  int GetIndexFromPlog(Plog* plog, const char* key) const;
  IndexNode* GetOrCreateParent(Node* node);

  bool BorrowFromLeftLeafSibling(LeafNode* leaf_node);
  bool BorrowFromRightLeafSibling(LeafNode* leaf_node);
  bool BorrowFromLeafSibling(LeafNode* leaf_node);
  bool MergeLeftLeaf(LeafNode* leaf_node);
  bool MergeRightLeaf(LeafNode* leaf_node);
  LeafNode* MergeLeaf(LeafNode* leaf_node);

  bool BorrowFromLeftIndexSibling(IndexNode* index_node);
  bool BorrowFromRightIndexSibling(IndexNode* index_node);
  bool BorrowFromIndexSibling(IndexNode* index_node);
  bool MergeLeftIndex(IndexNode* index_node);
  bool MergeRightIndex(IndexNode* index_node);
  IndexNode* MergeIndex(IndexNode* index_node);

  int Check(off_t offset);

  char* GetMemCopy(off_t offset);
  Monitor* GetMtr();
  
//hkc-ap-05
  int fd_shadow_plog_;
  off_t shadow_plog_off;
    //hkc-ap-06
  bool scan_ap;


  int fd_;
  int log_fd_;
  int chpt_fd_;
  int bg_id;
  BlockCache* block_cache_;
  InternalCache* internal_cache_;
  Meta* meta_;
  Monitor* mtr_;
  LogRecord* log_obj;
  Checkpoint* chpt;
  std::thread BackGround_Glog;
};



class BPlusOrderlineTree {
  struct Meta;
  struct Monitor;
  struct Index;
  struct Record;
  struct PAXKeyRecord;
  struct PAXColumn1Record;
  struct PAXColumn2Record;
  struct PAXColumn3Record;
  struct PAXColumn4Record;
  struct PAXColumn5Record;
  struct PAXColumn6Record;
  struct Per_page_Log;
  struct Node;
  struct IndexNode;
  struct LeafNode;
  struct PAXLeafNode;
  struct Plog;
  class BlockCache;
  class InternalCache;
  struct LogRecord;
  struct Checkpoint;
  
        

 public:
 //hkc-ap-00
  BPlusOrderlineTree(const char* path, const char* log_path, const char* chpt_path, const int bg_id_, const bool enable_backthd, const char *shadow_plog_path);
  ~BPlusOrderlineTree();

  void  SetIOStaic();
  //hkc-ap-01
  void ScanAP_impl(off_t &left, off_t &right, std::string &value);

  off_t GetChpt();
  off_t GetAPTime();
  off_t GetAPOff();
  //hkc-ap-02
  void APWorker();
  void SetLoadFlag(int flag);
  void DisableFLUOrNot(int flag);
  void LoadIOStatic(int &plog_insert, int &leaf_insert, int &leaf_split,int &phy_plog_write,int &phy_plog_read,int &phy_page_read,int &phy_page_write,int &logi_page_read,int &logi_page_write);
  void SetPlog();
  //hkc-ap-03
  void Getfd(int &fd);
  //hkc-ap-04
  int PrepareReadSetAP(off_t left, off_t right,void *scan_ptr);
  void SetBaseline();
  void GetStatic(int &hits_bp, int &hits_glog, int &hits_plog, int &disk_read, int &io_consume, int &error_cnt,int &dirtys, int &cleans);
  void ResetStatic();
  off_t GetLSN();
  void IncreLSN();

  
  void Put(const std::string& key, const std::string& value, off_t lsn);
  
  //hkc-gb-01
  off_t GetOnlyLeafOffset(const std::string& key);
  //hkc-gb-02
  char* GetLeafCopy(const std::string& key);
  void FlushAllPages();
  //hkc-gb-03
  void UpdateLeafCopy(const std::string& key, const std::string& value, off_t lsn, char* leaf_copy);
  //hkc-gb-03-1
  void GetRowRecord(const std::string &key,  std::string &value,  char* leaf_copy);
  //hkc-gb-03-2
  void TriggerFlush(const off_t* offsets, const int size, char* evict_buf);
  
  


  void RowToColumn(char* initial_page, char* converted_page);
  void ColumnToRow(char* initial_page, char* converted_page);
  //void Put_list(const std::string& key, const std::vector<std::string> value_list, off_t lsn);
  void InstantRecovery(off_t checkpoint);
  void NormalRecovery(off64_t checkpoint);
  void ApplyOneDiskToDataFile(int seg_id);
  void BackGround_GLog_Func();
  int  ApplyPendPages(std::map<off_t, std::pair<char*,char*>>* source, std::map<off_t, std::pair<char*,char*>>* target, std::map<off_t,std::pair<int,int>> *EmptyPages);
  void AIOFLushPlog_Demand(struct io_uring *ring);
  void InitBGGlog();
 
  bool Delete(const std::string& key);
  bool Get(const std::string& key, std::string& value) const;
  bool Scan(const std::string &start, const std::string &end, std::string &value) const;
  bool ScanAP(const std::string &start, const std::string &end, std::string &value) const;
  std::vector<std::pair<std::string, std::string>> GetRange(
      const std::string& left_key, const std::string& right_key) const;
  bool Empty() const;
  size_t Size() const;


 //private:

  template <typename T>
  T* AllocInMemPage()const;

  template <typename T>
  T* Map(off_t offset, int isleaf, int is_plog, char* &plog_copy) const; //is_plog: 1:leafnode; 1:plog; 3: Get both leaf and plog copy by one time  

 
  
  template <typename T>
  T* MapIndex(int page_id) const;

  template <typename T>
  T* MapLeaf(int page_id) const;

  
  template <typename T>
  void InsertDirty(off_t offset) const;
  template <typename T>
  void RelocateDirty(off_t offset_leaf, off_t offset_split) const;

  template <typename T>
  void DirectInsertDirty(off_t offset, off_t ori_oldset_lsn) const;

  
  template <typename T>
  void UnMap(T* map_obj, int isleaf, int is_plog) const;
  template <typename T>
  T* AllocLeaf();
  template <typename T>
  T* AllocIndex();
  template <typename T>
  void Dealloc(T* node);

  size_t GetMinKeys(int flag);
  size_t GetMaxKeys(int flag);
  size_t GetMinIndexKeys();
  size_t GetMaxIndexKeys();

  template <typename T>
  int UpperBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int UpperBoundIndex(T arr[], int n, const char* target) const;
  template <typename T>
  int LowerBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int LowerBoundIndex(T arr[], int n, const char* target) const;

  off_t GetLeafOffset(const char* key) const;
  void WarmUpVirtualPageCache(int x) const;
  LeafNode* SplitLeafNode(LeafNode* leaf_node);
  IndexNode* SplitIndexNode(IndexNode* index_node);
  size_t InsertKeyIntoIndexNode(IndexNode* index_node, const char* key,
                                Node* left_node, Node* right_node,off_t cur_lsn);
  size_t InsertKVIntoLeafNode(LeafNode* leaf_node, const char* key,
                              const char* value1, const char* value2,const char* value3,const char* value4,const char* value5,const char* value6, off_t cur_lsn);
  size_t InsertKVIntoPlog(Plog* plog, const char* key,
                              const char* value1, const char* value2,const char* value3,const char* value4, const char* value5,const char* value6, off_t cur_lsn);
  int GetIndexFromLeafNode(LeafNode* leaf_node, const char* key) const;

  //hkc-plog-0
  size_t InsertDeltaIntoPlog(Plog* plog, const char* key,const char* value_ts, off_t cur_lsn);
  size_t InsertDeltaKVIntoLeafNode(LeafNode *leaf_node, const char *key,const char* value_ts, off_t cur_lsn);
  int GetIndexFromPlog(Plog* plog, const char* key) const;
  IndexNode* GetOrCreateParent(Node* node);

  bool BorrowFromLeftLeafSibling(LeafNode* leaf_node);
  bool BorrowFromRightLeafSibling(LeafNode* leaf_node);
  bool BorrowFromLeafSibling(LeafNode* leaf_node);
  bool MergeLeftLeaf(LeafNode* leaf_node);
  bool MergeRightLeaf(LeafNode* leaf_node);
  LeafNode* MergeLeaf(LeafNode* leaf_node);

  bool BorrowFromLeftIndexSibling(IndexNode* index_node);
  bool BorrowFromRightIndexSibling(IndexNode* index_node);
  bool BorrowFromIndexSibling(IndexNode* index_node);
  bool MergeLeftIndex(IndexNode* index_node);
  bool MergeRightIndex(IndexNode* index_node);
  IndexNode* MergeIndex(IndexNode* index_node);

  int Check(off_t offset);

  char* GetMemCopy(off_t offset);
  Monitor* GetMtr();
  

  //hkc-ap-05
  int fd_shadow_plog_;
  off_t shadow_plog_off;
    //hkc-ap-06
  bool scan_ap;

  int fd_;
  int log_fd_;
  int chpt_fd_;
  int bg_id;
  



  BlockCache* block_cache_;
  InternalCache* internal_cache_;
  Meta* meta_;
  Monitor* mtr_;
  LogRecord* log_obj;
  Checkpoint* chpt;
  std::thread BackGround_Glog;
};


class BPlusCustomerTree {
  struct Meta;
  struct Monitor;
  struct Index;
  struct Record;
  struct PAXKeyRecord;
  struct PAXColumn1Record;
  struct PAXColumn2Record;
  struct PAXColumn3Record;
  struct PAXColumn4Record;
  struct PAXColumn5Record;
  struct PAXColumn6Record;
  struct PAXColumn7Record;
  struct PAXColumn8Record;
  struct PAXColumn9Record;
  struct PAXColumn10Record;
  struct PAXColumn11Record;
  struct PAXColumn12Record;
  struct PAXColumn13Record;
  struct PAXColumn14Record;
  struct PAXColumn15Record;
  struct PAXColumn16Record;
  struct PAXColumn17Record;
  struct PAXColumn18Record;
  struct Per_page_Log;
  struct Node;
  struct IndexNode;
  struct LeafNode;
  struct PAXLeafNode;
  struct Plog;
  class BlockCache;
  class InternalCache;
  struct LogRecord;
  struct Checkpoint;
  
        

 public:
   //hkc-ap-00
  BPlusCustomerTree(const char* path, const char* log_path, const char* chpt_path, const int bg_id_, const bool enable_backthd, const char *shadow_plog_path);
  ~BPlusCustomerTree();

  void  SetIOStaic();
  off_t GetChpt();

  //hkc-ap-01
  void ScanAP_impl(off_t &left, off_t &right, std::string &value);
  //hkc-ap-02
  void APWorker();
  //hkc-ap-03
  void Getfd(int &fd);
  //hkc-ap-04
  int PrepareReadSetAP(off_t left, off_t right,void *scan_ptr);
  bool ScanAP(const std::string &start, const std::string &end, std::string &value) const;

  void SetLoadFlag(int flag);
  void DisableFLUOrNot(int flag);
  void LoadIOStatic(int &plog_insert, int &leaf_insert, int &leaf_split,int &phy_plog_write,int &phy_plog_read,int &phy_page_read,int &phy_page_write,int &logi_page_read,int &logi_page_write);
  void SetPlog();
  void SetBaseline();
  void GetStatic(int &hits_bp, int &hits_glog, int &hits_plog, int &disk_read, int &io_consume, int &error_cnt,int &dirtys, int &cleans);
  void ResetStatic();
  off_t GetLSN();
  void IncreLSN();

  
  void Put(const std::string& key, const std::string& value, off_t lsn);
  //hkc-gb-01
  off_t GetOnlyLeafOffset(const std::string& key);
  //hkc-gb-02
  char* GetLeafCopy(const std::string& key);
  void FlushAllPages();
  //hkc-gb-03
  void UpdateLeafCopy(const std::string& key, const std::string& value, off_t lsn, char* leaf_copy);
  //hkc-gb-03-1
  void GetRowRecord(const std::string &key,  std::string &value,  char* leaf_copy);
  //hkc-gb-03-2
  void TriggerFlush(const off_t* offsets, const int size, char* evict_buf);
  
  void RowToColumn(char* initial_page, char* converted_page);
  void ColumnToRow(char* initial_page, char* converted_page);
  //void Put_list(const std::string& key, const std::vector<std::string> value_list, off_t lsn);
  void InstantRecovery(off_t checkpoint);
  void NormalRecovery(off64_t checkpoint);
  void ApplyOneDiskToDataFile(int seg_id);
  void BackGround_GLog_Func();
  int  ApplyPendPages(std::map<off_t, std::pair<char*,char*>>* source, std::map<off_t, std::pair<char*,char*>>* target, std::map<off_t,std::pair<int,int>> *EmptyPages);
  void AIOFLushPlog_Demand(struct io_uring *ring);
  void InitBGGlog();
 
  bool Delete(const std::string& key);
  bool Get(const std::string& key, std::string& value) const;
  bool Scan(const std::string& start, const std::string& end, std::string& value) const;
  std::vector<std::pair<std::string, std::string>> GetRange(
      const std::string& left_key, const std::string& right_key) const;
  bool Empty() const;
  size_t Size() const;


 //private:

  template <typename T>
  T* AllocInMemPage()const;

  template <typename T>
  T* Map(off_t offset, int isleaf, int is_plog, char* &plog_copy) const; //is_plog: 1:leafnode; 1:plog; 3: Get both leaf and plog copy by one time  

 
  
  template <typename T>
  T* MapIndex(int page_id) const;

  template <typename T>
  T* MapLeaf(int page_id) const;

  
  template <typename T>
  void InsertDirty(off_t offset) const;
  template <typename T>
  void RelocateDirty(off_t offset_leaf, off_t offset_split) const;

  template <typename T>
  void DirectInsertDirty(off_t offset, off_t ori_oldset_lsn) const;

  
  template <typename T>
  void UnMap(T* map_obj, int isleaf, int is_plog) const;
  template <typename T>
  T* AllocLeaf();
  template <typename T>
  T* AllocIndex();
  template <typename T>
  void Dealloc(T* node);

  size_t GetMinKeys(int flag);
  size_t GetMaxKeys(int flag);
  size_t GetMinIndexKeys();
  size_t GetMaxIndexKeys();

  template <typename T>
  int UpperBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int UpperBoundIndex(T arr[], int n, const char* target) const;
  template <typename T>
  int LowerBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int LowerBoundIndex(T arr[], int n, const char* target) const;

  off_t GetLeafOffset(const char* key) const;
  void WarmUpVirtualPageCache(int x) const;
  LeafNode* SplitLeafNode(LeafNode* leaf_node);
  IndexNode* SplitIndexNode(IndexNode* index_node);
  size_t InsertKeyIntoIndexNode(IndexNode* index_node, const char* key,
                                Node* left_node, Node* right_node,off_t cur_lsn);
  size_t InsertKVIntoLeafNode(LeafNode* leaf_node, const char* key,
                              const char* value1, const char* value2,const char* value3,const char* value4,const char* value5,const char* value6,
                              const char* value7, const char* value8,const char* value9,const char* value10,const char* value11,const char* value12,
                              const char* value13, const char* value14,const char* value15,const char* value16,const char* value17,const char* value18,
                               off_t cur_lsn);
  
  
  size_t InsertKVIntoPlog(Plog* plog, const char* key,
                              const char* value1, const char* value2,const char* value3,const char* value4,const char* value5,const char* value6,
                              const char* value7, const char* value8,const char* value9,const char* value10,const char* value11,const char* value12,
                              const char* value13, const char* value14,const char* value15,const char* value16,const char* value17,const char* value18,
                              off_t cur_lsn);
  //hkc-plog-0
  size_t InsertDeltaIntoPlog(Plog* plog, const char* key,const char* value_balance, const char* value_data,off_t cur_lsn);
  size_t InsertDeltaKVIntoLeafNode(LeafNode *leaf_node, const char *key,const char* value_balance, const char* value_data,off_t cur_lsn);
  int GetIndexFromLeafNode(LeafNode* leaf_node, const char* key) const;
  int GetIndexFromPlog(Plog* plog, const char* key) const;
  IndexNode* GetOrCreateParent(Node* node);

  bool BorrowFromLeftLeafSibling(LeafNode* leaf_node);
  bool BorrowFromRightLeafSibling(LeafNode* leaf_node);
  bool BorrowFromLeafSibling(LeafNode* leaf_node);
  bool MergeLeftLeaf(LeafNode* leaf_node);
  bool MergeRightLeaf(LeafNode* leaf_node);
  LeafNode* MergeLeaf(LeafNode* leaf_node);

  bool BorrowFromLeftIndexSibling(IndexNode* index_node);
  bool BorrowFromRightIndexSibling(IndexNode* index_node);
  bool BorrowFromIndexSibling(IndexNode* index_node);
  bool MergeLeftIndex(IndexNode* index_node);
  bool MergeRightIndex(IndexNode* index_node);
  IndexNode* MergeIndex(IndexNode* index_node);

  int Check(off_t offset);

  char* GetMemCopy(off_t offset);
  Monitor* GetMtr();
  

  //hkc-ap-05
  int fd_shadow_plog_;
  off_t shadow_plog_off;
  //hkc-ap-06
  bool scan_ap;

  int fd_;
  int log_fd_;
  int chpt_fd_;
  int bg_id;
  BlockCache* block_cache_;
  InternalCache* internal_cache_;
  Meta* meta_;
  Monitor* mtr_;
  LogRecord* log_obj;
  Checkpoint* chpt;
  std::thread BackGround_Glog;

};


class BPlusDistrictTree {
  struct Meta;
  struct Monitor;
  struct Index;
  struct Record;
  struct PAXKeyRecord;
  struct PAXColumn1Record;
  struct PAXColumn2Record;
  struct PAXColumn3Record;
  struct PAXColumn4Record;
  struct PAXColumn5Record;
  struct PAXColumn6Record;
  struct PAXColumn7Record;
  struct PAXColumn8Record;
  struct PAXColumn9Record;
  struct Per_page_Log;
  struct Node;
  struct IndexNode;
  struct LeafNode;
  struct PAXLeafNode;
  struct Plog;
  class BlockCache;
  class InternalCache;
  struct LogRecord;
  struct Checkpoint;
  
        

 public:
  BPlusDistrictTree(const char* path, const char* log_path, const char* chpt_path, const int bg_id_, const bool enable_backthd);
  ~BPlusDistrictTree();

  void  SetIOStaic();
  off_t GetChpt();
  void SetLoadFlag(int flag);
  void DisableFLUOrNot(int flag);
  void LoadIOStatic(int &plog_insert, int &leaf_insert, int &leaf_split,int &phy_plog_write,int &phy_plog_read,int &phy_page_read,int &phy_page_write,int &logi_page_read,int &logi_page_write);
  void SetPlog();
  void SetBaseline();
  void GetStatic(int &hits_bp, int &hits_glog, int &hits_plog, int &disk_read, int &io_consume, int &error_cnt,int &dirtys, int &cleans);
  void ResetStatic();
  off_t GetLSN();
  void IncreLSN();

  
  void Put(const std::string& key, const std::string& value, off_t lsn);
  
  //hkc-gb-01
  off_t GetOnlyLeafOffset(const std::string& key);
  //hkc-gb-02
  char* GetLeafCopy(const std::string& key);
  void FlushAllPages();
  //hkc-gb-03
  void UpdateLeafCopy(const std::string& key, const std::string& value, off_t lsn, char* leaf_copy);
  //hkc-gb-03-1
  void GetRowRecord(const std::string &key,  std::string &value,  char* leaf_copy);
  //hkc-gb-03-2
  void TriggerFlush(const off_t* offsets, const int size, char* evict_buf);
  
  


  void RowToColumn(char* initial_page, char* converted_page);
  void ColumnToRow(char* initial_page, char* converted_page);
  //void Put_list(const std::string& key, const std::vector<std::string> value_list, off_t lsn);
  void InstantRecovery(off_t checkpoint);
  void NormalRecovery(off64_t checkpoint);
  void ApplyOneDiskToDataFile(int seg_id);
  void BackGround_GLog_Func();
  int  ApplyPendPages(std::map<off_t, std::pair<char*,char*>>* source, std::map<off_t, std::pair<char*,char*>>* target, std::map<off_t,std::pair<int,int>> *EmptyPages);
  void AIOFLushPlog_Demand(struct io_uring *ring);
  void InitBGGlog();
 
  bool Delete(const std::string& key);
  bool Get(const std::string& key, std::string& value) const;
  bool Scan(const std::string &start, const std::string &end, std::string &value) const;
  std::vector<std::pair<std::string, std::string>> GetRange(
      const std::string& left_key, const std::string& right_key) const;
  bool Empty() const;
  size_t Size() const;


 //private:

  template <typename T>
  T* AllocInMemPage()const;

  template <typename T>
  T* Map(off_t offset, int isleaf, int is_plog, char* &plog_copy) const; //is_plog: 1:leafnode; 1:plog; 3: Get both leaf and plog copy by one time  

 
  
  template <typename T>
  T* MapIndex(int page_id) const;

  template <typename T>
  T* MapLeaf(int page_id) const;

  
  template <typename T>
  void InsertDirty(off_t offset) const;
  template <typename T>
  void RelocateDirty(off_t offset_leaf, off_t offset_split) const;

  template <typename T>
  void DirectInsertDirty(off_t offset, off_t ori_oldset_lsn) const;

  
  template <typename T>
  void UnMap(T* map_obj, int isleaf, int is_plog) const;
  template <typename T>
  T* AllocLeaf();
  template <typename T>
  T* AllocIndex();
  template <typename T>
  void Dealloc(T* node);

  size_t GetMinKeys(int flag);
  size_t GetMaxKeys(int flag);
  size_t GetMinIndexKeys();
  size_t GetMaxIndexKeys();

  template <typename T>
  int UpperBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int UpperBoundIndex(T arr[], int n, const char* target) const;
  template <typename T>
  int LowerBound(T arr[], int n, const char* target, int flag) const;
  template <typename T>
  int LowerBoundIndex(T arr[], int n, const char* target) const;

  off_t GetLeafOffset(const char* key) const;
  void WarmUpVirtualPageCache(int x) const;
  LeafNode* SplitLeafNode(LeafNode* leaf_node);
  IndexNode* SplitIndexNode(IndexNode* index_node);
  size_t InsertKeyIntoIndexNode(IndexNode* index_node, const char* key,
                                Node* left_node, Node* right_node,off_t cur_lsn);
  size_t InsertKVIntoLeafNode(LeafNode* leaf_node, const char* key,
                              const char* value1, const char* value2,const char* value3,const char* value4,const char* value5,const char* value6,
                              const char* value7, const char* value8,const char* value9,off_t cur_lsn);
  size_t InsertKVIntoPlog(Plog* plog, const char* key,
                              const char* value1, const char* value2,const char* value3,const char* value4,const char* value5,const char* value6,
                              const char* value7, const char* value8,const char* value9, off_t cur_lsn);
  int GetIndexFromLeafNode(LeafNode* leaf_node, const char* key) const;
  int GetIndexFromPlog(Plog* plog, const char* key) const;
  IndexNode* GetOrCreateParent(Node* node);

  bool BorrowFromLeftLeafSibling(LeafNode* leaf_node);
  bool BorrowFromRightLeafSibling(LeafNode* leaf_node);
  bool BorrowFromLeafSibling(LeafNode* leaf_node);
  bool MergeLeftLeaf(LeafNode* leaf_node);
  bool MergeRightLeaf(LeafNode* leaf_node);
  LeafNode* MergeLeaf(LeafNode* leaf_node);

  bool BorrowFromLeftIndexSibling(IndexNode* index_node);
  bool BorrowFromRightIndexSibling(IndexNode* index_node);
  bool BorrowFromIndexSibling(IndexNode* index_node);
  bool MergeLeftIndex(IndexNode* index_node);
  bool MergeRightIndex(IndexNode* index_node);
  IndexNode* MergeIndex(IndexNode* index_node);

  int Check(off_t offset);

  char* GetMemCopy(off_t offset);
  Monitor* GetMtr();
  

  int fd_;
  int log_fd_;
  int chpt_fd_;
  int bg_id;
  BlockCache* block_cache_;
  InternalCache* internal_cache_;
  Meta* meta_;
  Monitor* mtr_;
  LogRecord* log_obj;
  Checkpoint* chpt;
  std::thread BackGround_Glog;
};




class GlobalBuffer {
    struct PageNode;
    public:
        
        GlobalBuffer(const int plog_flag_);
        ~GlobalBuffer();
        void InsertIntoGlobalBuffer(int tab_id,off_t leaf_of,char* leaf_buf);
        
        bool DoLRUEviction();
        void Read_LRU();
        char* GetPageCopy(int tab_id, off_t leaf_of);
        void UpdateLRUOrder(int tab_id, off_t leaf_of, char* leaf_buf);
        char* GetEvictPageNode(int index, off_t &offset, int &tab_id);

        std::list<PageNode*> freelist;
        std::vector<PageNode*> evictlist;
        int budget;
        int cur_usage;
        int evict_batch_size;
        int plog_flag;
        PageNode* LRU_head;



    private:
        std::unordered_map<off_t,char*> gb_map[8];
        //only lru list need locking
        //std::list<std::pair<int,std::pair<off_t,char*>>> LRU_List; //<tableid,offset,memory_ptr>
        //hashmap for each table for each table
         
        /*
        std::unordered_map<off_t,char*> map_customer; //00
        std::unordered_map<off_t,char*> map_stock; //01
        std::unordered_map<off_t,char*> map_item; //02
        std::unordered_map<off_t,char*> map_orderline; //03
        std::unordered_map<off_t,char*> map_orders; //04
        std::unordered_map<off_t,char*> map_neworders; //05
        std::unordered_map<off_t,char*> map_history_1; //06
        std::unordered_map<off_t,char*> map_history_2; //07
        */
        char* Insert_LRU_Head();
        void DoReclaim();
        void Reinsert_LRU_Head(PageNode* pnode);
        void Delete_LRU_Tail(off_t &offset, int &tab_id, char* buf); 
        void readLock_LRU();
        void readUnlock_LRU();
        void writeLock_LRU();
        void writeUnlock_LRU();
        std::shared_timed_mutex readMtx_lru; //new c++20 standard
        std::mutex writeMtx_lru;
        int readCnt_lru; // 已加读锁个数

};

