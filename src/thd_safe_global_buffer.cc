#include "bplus_tree.h"

#include <iostream>
#include <mutex>  
#include <shared_mutex>
#include <thread>
#include <list>
#include <unordered_map>
#include <memory.h>

const int KSize_budget = 1000;
const int KSize_evict = 200;
const int kPageNodeSize = 1024*(16+128);
const int kLeafNodeSize = 1024*128;
int lru_cnt=0;

struct GlobalBuffer::PageNode{
    PageNode(){
        table_id = offset = 0;
        left = right =nullptr;
        state_ = 0;
        memset(page_buf,0,kPageNodeSize);
    }
    int table_id;
    off_t offset;
    int state_; //0:free; 1:in lru; 2:evicted
    
    PageNode* left;
    PageNode* right;
    char page_buf[kPageNodeSize];
};

GlobalBuffer::GlobalBuffer(const int plog_flag_) : readCnt_lru(0) {
    budget = KSize_budget;
    evict_batch_size = KSize_evict;
    cur_usage =0;
    plog_flag = plog_flag_;
    LRU_head = new PageNode();
    //init freelist
    for(int i=0;i<budget;i++){
        freelist.push_back(new PageNode());
    }
    for(int i = 0; i<evict_batch_size;i++){
        PageNode* pnode = new PageNode();
        pnode->state_=2;
        evictlist.push_back(pnode);    
    }
}

char* GlobalBuffer::GetEvictPageNode(int index, off_t &offset, int &tab_id){
    //readLock_LRU();
    offset =  evictlist[index]->offset;
    if(offset == 0){
        int x=1;
    }
    tab_id = evictlist[index]->table_id;
    if(evictlist[index]->state_ !=2){
        int x=1;
    }
    //readUnlock_LRU();
    return evictlist[index]->page_buf;
}

void GlobalBuffer::InsertIntoGlobalBuffer(int tab_id, off_t leaf_of, char* leaf_buf){
    writeLock_LRU();
    //readLock_LRU();
    if(gb_map[tab_id].find(leaf_of)!=gb_map[tab_id].end()){
        int x=1;
    }
    //readUnlock_LRU();
    char* pnode_buf = Insert_LRU_Head();
    if(LRU_head->left ==nullptr || LRU_head->right == nullptr || pnode_buf == nullptr){
        int x=1;
    }
    
    lru_cnt++;
    gb_map[tab_id].emplace(leaf_of,pnode_buf);
    
    PageNode* pnode = reinterpret_cast<PageNode*>(&pnode_buf[0]);
    pnode->table_id = tab_id;
    pnode->offset = leaf_of;
    if(pnode->state_!=1 || pnode->offset == 0){
        int x=0;
    }
    memcpy(pnode->page_buf,leaf_buf,kLeafNodeSize);
    writeUnlock_LRU();
    return;    
}



char* GlobalBuffer::GetPageCopy(int tab_id, off_t leaf_of){
    readLock_LRU();
    if(gb_map[tab_id].find(leaf_of)!=gb_map[tab_id].end()){
        char* pnode_buf = gb_map[tab_id][leaf_of];
        if(leaf_of ==0){
            int x=1;
        }
        PageNode* pnode = reinterpret_cast<PageNode*>(&pnode_buf[0]);
        if(pnode->state_!=1 || pnode->offset!=leaf_of || pnode->offset ==0){
            int x=1;
        }
        
        char* leaf_buf = (char*)malloc(sizeof(char)*(128+16)*1024);
        memcpy(leaf_buf,pnode->page_buf,(128+16)*1024);
        readUnlock_LRU();
        return leaf_buf;
    }else{
        readUnlock_LRU();
        return nullptr;
    }
    
}

void GlobalBuffer::UpdateLRUOrder(int tab_id, off_t leaf_of, char* leaf_buf){
    //readLock_LRU();
    writeLock_LRU();
    if(gb_map[tab_id].find(leaf_of)==gb_map[tab_id].end()){
        //the page is not locked since during read, 
        //this page has been flushed to disk and removed from gb
        writeUnlock_LRU();
        InsertIntoGlobalBuffer(tab_id, leaf_of, leaf_buf);
        return;

    }
    PageNode* pnode = reinterpret_cast<PageNode*>(gb_map[tab_id][leaf_of]);
    
    if(pnode->state_!=1 || pnode->offset!=leaf_of || pnode->offset ==0){
        int x=1;
    }
    memcpy(pnode->page_buf,leaf_buf,kLeafNodeSize);
    //readUnlock_LRU();
    Reinsert_LRU_Head(pnode);
    writeUnlock_LRU();
    return;
}


char* GlobalBuffer::Insert_LRU_Head(){
    while(cur_usage>budget*0.9){
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
    //writeLock_LRU();
    PageNode* pnode = freelist.front();
    freelist.pop_front();
    pnode->state_ =1;
    bool firstinsert = cur_usage == 0 ? true:false;
    if(firstinsert){
        LRU_head->left = LRU_head->right = pnode;
        pnode->left = pnode->right = LRU_head;
    }else{
        PageNode* firstnode=LRU_head->right;
        firstnode->left = pnode;
        pnode->right = firstnode;
        pnode->left = LRU_head;
        LRU_head->right = pnode;
    }
    if(LRU_head->left == nullptr || LRU_head->right == nullptr){
        int x=1;
    }
    cur_usage++;
    //writeUnlock_LRU();
    return reinterpret_cast<char*>(pnode);
}

void GlobalBuffer::Reinsert_LRU_Head(PageNode* pnode) {
    //writeLock_LRU();
    //action
    //1.1 trim pnode
    PageNode* neighbor_left = pnode->left;
    PageNode* neighbor_right = pnode->right;
    neighbor_left->right = neighbor_right;
    neighbor_right->left = neighbor_left;

    //1.2 reinsert pnode
    PageNode* firstnode=LRU_head->right;
    firstnode->left = pnode;
    pnode->right = firstnode;
    pnode->left = LRU_head;
    LRU_head->right = pnode;
    if(pnode->state_!=1){
        int x=1;
    }
        if(LRU_head->left == nullptr || LRU_head->right == nullptr){
        int x=1;
    }
    //writeUnlock_LRU();
    return;
}



void GlobalBuffer::Delete_LRU_Tail(off_t &offset, int &tab_id, char* buf) {
    writeLock_LRU();
    lru_cnt--;
    if(LRU_head->left == nullptr || LRU_head->right == nullptr){
        int x=1;
    }
    if(LRU_head == LRU_head->left || LRU_head == LRU_head->right){
        int x=1;
    }
    //action
    PageNode* tail = LRU_head->left;

    PageNode* neighbor_left = tail->left;
    PageNode* neighbor_right = tail->right;
    neighbor_left->right = neighbor_right;
    neighbor_right->left = neighbor_left;
    if(LRU_head->left == LRU_head || LRU_head->right == LRU_head){
        int x=1;
    }
    tail->left = tail->right = nullptr;
    cur_usage--;
    offset = tail->offset;
    if(offset == 0){
        int x=1;
    }
    tab_id = tail->table_id;
    
    memcpy(&buf[0],&tail->page_buf[0],128*1024);
    memset(&tail->page_buf[0],0,1024*(128+16));
    
    if(plog_flag == 1){
        freelist.push_back(tail);
        tail->state_=0;//back to free
        if(gb_map[tab_id].find(offset) == gb_map[tab_id].end()){
            int x=1;
        }else{
            PageNode* pp = reinterpret_cast<PageNode*>(gb_map[tab_id][offset]);
            if(pp->offset!=offset){
                int x=1;
            }
            pp->offset = pp->table_id =0;
            gb_map[tab_id].erase(offset);
        }
        

    }else{
        //do the flush for different tables;
        //pending
        freelist.push_back(tail);
        tail->state_=0;//back to free
        if(gb_map[tab_id].find(offset) == gb_map[tab_id].end()){
            int x=1;
        }else{
            PageNode* pp = reinterpret_cast<PageNode*>(gb_map[tab_id][offset]);
            if(pp->offset!=offset){
                int x=1;
            }
            pp->offset = pp->table_id =0;
            gb_map[tab_id].erase(offset);
        }
    
    }
    writeUnlock_LRU();
    return;
    
}

bool GlobalBuffer::DoLRUEviction(){
    //for plog approach, we do nothing except return the page node to freelist
    if(plog_flag == 1){
        //readLock_LRU();
        //action
        bool needflush = cur_usage > budget*0.8 ? true : false;
        //readUnlock_LRU();
        if(needflush){
            for (int i=0;i<evict_batch_size;i++){
                if(evictlist[i]->offset == 0){
                    int x=1;
                }
                Delete_LRU_Tail(evictlist[i]->offset,evictlist[i]->table_id,evictlist[i]->page_buf);
            }
            return true;
        }else{
            return false;
        }
        
    }else{
        //readLock_LRU();
        //action
        bool needflush = cur_usage > budget*0.8 ? true : false;
        //readUnlock_LRU();
        if(needflush){
            for (int i=0;i<evict_batch_size;i++){
                Delete_LRU_Tail(evictlist[i]->offset,evictlist[i]->table_id,evictlist[i]->page_buf);
            }
            return true;
        }else{
            return false;
        }
        
    }
    return false;
}

void GlobalBuffer::Read_LRU() {
    readLock_LRU();
    //action
    //std::cout << "read var : " << var << std::endl;
    readUnlock_LRU();
    return;
}


void GlobalBuffer::readLock_LRU()
{
    readMtx_lru.lock();
    if (++readCnt_lru == 1) {
        writeMtx_lru.lock();  // 存在线程读操作时，写加锁（只加一次）
    }
    readMtx_lru.unlock();
}
void GlobalBuffer::readUnlock_LRU()
{
    readMtx_lru.lock();
    if (--readCnt_lru == 0) { // 没有线程读操作时，释放写锁
        writeMtx_lru.unlock();
    }
    readMtx_lru.unlock();
}
void GlobalBuffer::writeLock_LRU()
{
    writeMtx_lru.lock();
}
void GlobalBuffer::writeUnlock_LRU()
{
    writeMtx_lru.unlock();
}


/*
class GlobalBuffer {
    public:
        GlobalBuffer(int size_budget, int batch_size, int plog_flag_) : readCnt_lru(0) {
            budget = size_budget;
            evict_batch_size = batch_size;
            cur_usage =0;
            plog_flag = plog_flag_;
            LRU_head = new PageNode();
            //init freelist
            for(int i=0;i<budget;i++){
                freelist.push_back(new PageNode());
            }
        }

        void Insert_LRU_Head() {
            writeLock_LRU();
            cur_usage++;
            PageNode* pnode = freelist.back();
            freelist.pop_back();
            LRU_head->left = LRU_head->right = pnode;
            pnode->left = pnode->right = LRU_head;
            //action
            writeUnlock_LRU();
            return;
        }
        
        void Reinsert_LRU_Head(PageNode* pnode) {
            writeLock_LRU();
            //action
            PageNode* firstnode=LRU_head->right;
            firstnode->left = pnode;
            pnode->right = firstnode;
            pnode->left = LRU_head;
            LRU_head->right = pnode;
            writeUnlock_LRU();
            cur_usage++;
            return;
        }

        void Delete_LRU_Tail() {
            writeLock_LRU();
            //action
            PageNode* tail = LRU_head->left;
            PageNode* last = tail->left;
            LRU_head->left = last;
            last->right = LRU_head;
            tail->left = tail->right = 0;
            cur_usage--;
            if(plog_flag == 1){
                tail->offset = tail->table_id =0;
                memset(tail->page_buf,0,1024*(128+16));
                freelist.push_front(tail);
                writeUnlock_LRU();
                return ;
            }else{
                //do the flush for different tables;
                //pending
                writeUnlock_LRU();            
                return; 
            }
            
        }

        void DoLRUEviction(){
            //for plog approach, we do nothing except return the page node to freelist
            if(plog_flag == 1){
                readLock_LRU();
                //action
                bool needflush = cur_usage > budget*0.8 ? true : false;
                readUnlock_LRU();
                if(needflush){
                    for (int i=0;i<evict_batch_size;i++){
                        Delete_LRU_Tail();
                    }
                }
            }else{
                //for baseline approach
            }
            return ;
        }

        void Read_LRU() {
            readLock_LRU();
            //action
            //std::cout << "read var : " << var << std::endl;
            readUnlock_LRU();
            return;
        }


        std::list<PageNode*> freelist;
        int budget;
        int cur_usage;
        int evict_batch_size;
        int plog_flag;
        PageNode* LRU_head;

        //only lru list need locking
        std::list<std::pair<int,std::pair<off_t,char*>>> LRU_List; //<tableid,offset,memory_ptr>
        //hashmap for each table for each table
        std::unordered_map<off_t,char*> map_customer; //01
        std::unordered_map<off_t,char*> map_stock; //02
        std::unordered_map<off_t,char*> map_item; //03
        std::unordered_map<off_t,char*> map_orderline; //04
        std::unordered_map<off_t,char*> map_orders; //05
        std::unordered_map<off_t,char*> map_neworders; //06
        std::unordered_map<off_t,char*> map_history_1; //07
        std::unordered_map<off_t,char*> map_history_2; //08

    private:

        void readLock_LRU()
        {
            readMtx_lru.lock();
            if (++readCnt_lru == 1) {
                writeMtx_lru.lock();  // 存在线程读操作时，写加锁（只加一次）
            }
            readMtx_lru.unlock();
        }
        void readUnlock_LRU()
        {
            readMtx_lru.lock();
            if (--readCnt_lru == 0) { // 没有线程读操作时，释放写锁
                writeMtx_lru.unlock();
            }
            readMtx_lru.unlock();
        }
        void writeLock_LRU()
        {
            writeMtx_lru.lock();
        }
        void writeUnlock_LRU()
        {
            writeMtx_lru.unlock();
        }

        std::shared_timed_mutex readMtx_lru; //new c++20 standard
        std::mutex writeMtx_lru;
        int readCnt_lru; // 已加读锁个数

};
*/

