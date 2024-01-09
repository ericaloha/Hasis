#include <chrono>
#include <iostream>
#include <fstream>
#include <random>
#include <string.h>
#include <deque>

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
#include <map>

// for async IO
#include <algorithm>
#include <thread>
// for ycsb
#include <limits.h>
#include <math.h>
#include <unistd.h>


#include <mysql.h>  




std::mutex mtx;






// extern off_t global_lsn;
//+++++++++++++++++++++YCSB Generator+++++++++++++++++++++++++++++++++++++++++++++++++
int afterLoad[64] = {0};
int aftertest[64] = {0};
extern off_t malloc_cnt[30];
extern off_t free_cnt[30];
int testphase[64][50] = {0};
extern int kMaxPages;


namespace static_tree{
  
  auto history_put_time =0;
  int history_put_cnt=0;
  auto history_get_time =0;
  int history_get_cnt=0;
  auto history_update_time =0;
  int history_update_cnt=0;
  auto history_scan_time_c =0;
  auto history_scan_time_h =0;
  int history_scan_cnt_c=0;
  int history_scan_cnt_h=0;
  int history_get_not_found=0;

  auto orders_put_time =0;
  int orders_put_cnt=0;
  auto orders_get_time =0;
  int orders_get_cnt=0;
  auto orders_update_time =0;
  int orders_update_cnt=0;
  auto orders_scan_time_c =0;
  auto orders_scan_time_h =0;
  int orders_scan_cnt_c=0;
  int orders_scan_cnt_h=0;
  int orders_get_not_found=0;

  auto neworders_put_time =0;
  int neworders_put_cnt=0;
  auto neworders_get_time =0;
  int neworders_get_cnt=0;
  auto neworders_update_time =0;
  int neworders_update_cnt=0;
  auto neworders_scan_time_c =0;
  auto neworders_scan_time_h =0;
  int neworders_scan_cnt_c=0;
  int neworders_scan_cnt_h=0;
  int neworders_get_not_found=0;

  auto customer_put_time =0;
  int customer_put_cnt=0;
  auto customer_get_time =0;
  int customer_get_cnt=0;
  auto customer_update_time =0;
  int customer_update_cnt=0;
  auto customer_scan_time_c =0;
  auto customer_scan_time_h =0;
  int customer_scan_cnt_c=0;
  int customer_scan_cnt_h=0;
  int customer_get_not_found=0;

  auto orderline_put_time =0;
  int orderline_put_cnt=0;
  auto orderline_get_time =0;
  int orderline_get_cnt=0;
  auto orderline_update_time =0;
  int orderline_update_cnt=0;
  auto orderline_scan_time_c =0;
  auto orderline_scan_time_h =0;
  int orderline_scan_cnt_c=0;
  int orderline_scan_cnt_h=0;
  int orderline_get_not_found=0;


  auto stock_put_time =0;
  int stock_put_cnt=0;
  auto stock_get_time =0;
  int stock_get_cnt=0;
  auto stock_update_time =0;
  int stock_update_cnt=0;
  auto stock_scan_time_c =0;
  auto stock_scan_time_h =0;
  int stock_scan_cnt_c=0;
  int stock_scan_cnt_h=0;
  int stock_get_not_found =0;

  auto item_put_time =0;
  int item_put_cnt=0;
  auto item_get_time =0;
  int item_get_cnt=0;
  auto item_update_time =0;
  int item_update_cnt=0;
  auto item_scan_time_c =0;
  auto item_scan_time_h =0;
  int item_scan_cnt_c=0;
  int item_scan_cnt_h=0;
  int item_get_not_found=0;


  void reset_static(){
    history_put_time =0;
    history_put_cnt=0;
    history_get_time =0;
    history_get_cnt=0;
    history_update_time =0;
    history_update_cnt=0;
    history_scan_time_c =0;
    history_scan_time_h =0;
    history_scan_cnt_c=0;
    history_scan_cnt_h=0;
    history_get_not_found=0;

    orders_put_time =0;
    orders_put_cnt=0;
    orders_get_time =0;
    orders_get_cnt=0;
    orders_update_time =0;
    orders_update_cnt=0;
    orders_scan_time_c =0;
    orders_scan_time_h =0;
    orders_scan_cnt_c=0;
    orders_scan_cnt_h=0;
    orders_get_not_found=0;

    neworders_put_time =0;
    neworders_put_cnt=0;
    neworders_get_time =0;
    neworders_get_cnt=0;
    neworders_update_time =0;
    neworders_update_cnt=0;
    neworders_scan_time_c =0;
    neworders_scan_time_h =0;
    neworders_scan_cnt_c=0;
    neworders_scan_cnt_h=0;
    neworders_get_not_found=0;

    customer_put_time =0;
    customer_put_cnt=0;
    customer_get_time =0;
    customer_get_cnt=0;
    customer_update_time =0;
    customer_update_cnt=0;
    customer_scan_time_c =0;
    customer_scan_time_h =0;
    customer_scan_cnt_c=0;
    customer_scan_cnt_h=0;
    customer_get_not_found=0;

    orderline_put_time =0;
    orderline_put_cnt=0;
    orderline_get_time =0;
    orderline_get_cnt=0;
    orderline_update_time =0;
    orderline_update_cnt=0;
    orderline_scan_time_c =0;
    orderline_scan_time_h =0;
    orderline_scan_cnt_c=0;
    orderline_scan_cnt_h=0;
    orderline_get_not_found=0;


    stock_put_time =0;
    stock_put_cnt=0;
    stock_get_time =0;
    stock_get_cnt=0;
    stock_update_time =0;
    stock_update_cnt=0;
    stock_scan_time_c =0;
    stock_scan_time_h =0;
    stock_scan_cnt_c=0;
    stock_scan_cnt_h=0;
    stock_get_not_found =0;

    item_put_time =0;
    item_put_cnt=0;
    item_get_time =0;
    item_get_cnt=0;
    item_update_time =0;
    item_update_cnt=0;
    item_scan_time_c =0;
    item_scan_time_h =0;
    item_scan_cnt_c=0;
    item_scan_cnt_h=0;
    item_get_not_found=0;

    
  }
}


void RandStrNum(char *str, const int len)
  {
      std::random_device rd;					
      std::default_random_engine random(rd());
      int i;
      for (i = 0; i < len; ++i)
      {
          switch ((rand() % 3))
          {
          case 1:
              str[i] = 'A' + random() % 26;
              break;
          case 2:
              str[i] = 'a' + random() % 26;
              break;
          default:
              str[i] = '1' + random() % 10;
              break;
          }
      }
      
      return;
  }

void RandStrBig(char *str, const int len)
  {
      srand(time(NULL));
      int i;
      for (i = 0; i < len; ++i)
      {
        str[i] = 'A' + rand() % 26;
      }
      
      return;
  }

void RandNum(char *str, const int len)
  {
      srand(time(NULL));
      int i;
      for (i = 0; i < len; ++i)
      {
        str[i] = '0' + rand() % 10;
      }
      if (str[0] == '0'){
        str[0] = '1';
      }
      return;
  }

void GenTimeStamp(char* str){
    time_t t = time(0);
    strftime(str, 64, "%Y-%m-%d-%H-%M-%S", localtime(&t)); 
    return ;
}




std::string* alloc_cmd(std::string &cmd){
    std::string* new_cmd=new std::string(cmd);
    return new_cmd;
}


bool KeyWordExist(std::string* cmd, int key_word){
  //key word:
  //1: put
  //2: get
  //3:scan
  //4.update
  if(key_word==1){
    if ((*cmd)[0]=='p' && (*cmd)[1]=='u' && (*cmd)[2]=='t'){
      return true;
    }else{
      return false;
    }
  }else if (key_word ==2){
    if ((*cmd)[0]=='g' && (*cmd)[1]=='e' && (*cmd)[2]=='t'){
      return true;
    }else{
      return false;
    }
  }else if (key_word ==3){
    if ((*cmd)[0]=='s' && (*cmd)[1]=='c' && (*cmd)[2]=='a'&& (*cmd)[3]=='n'){
      return true;
    }else{
      return false;
    }
  }else if (key_word ==5){
    if ((*cmd)[0]=='s' && (*cmd)[1]=='c' && (*cmd)[2]=='A'&& (*cmd)[3]=='P'){
      return true;
    }else{
      return false;
    }
  }
  else{
    //4: update
    if ((*cmd)[0]=='u' && (*cmd)[1]=='p' && (*cmd)[2]=='d'&& (*cmd)[3]=='a'&& (*cmd)[4]=='t'&& (*cmd)[5]=='e'){
      return true;
    }else{
      return false;
    }
  }
}

  
void ErrorDetect(){
  std::cout<<"error detect\n";
  return;

}


//==============================TPCC-KV-generator======================================================================
namespace tpcch_kv_generator{

  
  class tpcc_gen{
    public:

      tpcc_gen(int no_of_warehouse){
      w = no_of_warehouse;
      cur_kvs_customer = cur_kvs_history = cur_kvs_stock = cur_kvs_orderline = 0;
      cur_w_id_customer = 1;
      cur_d_id_customer = 1;
      cur_c_id_customer = 1;
      cur_w_id_neworders = 1;
      cur_d_id_neworders = 1;
      cur_c_id_neworders = 1;
      cur_w_id_orders = 1;
      cur_d_id_orders = 1;
      cur_c_id_orders = 1;
      cur_w_id_stock = 1;
      cur_id_stock = 1;
      cur_w_id_orderline = 1;
      cur_d_id_orderline = 1;
      cur_o_id_orderline = 1;
      cur_number_orderline = 1;

      cur_w_id_history = 1;
      cur_d_id_history = 1;
      cur_c_id_history = 1;



      cur_kvs_orders = cur_kvs_neworders = cur_kvs_item = 0;
      
      kvs_customer = 3000 * 10 * w;
      kvs_orderline = 3000 * 10 * w * 20;
      
      kvs_item = 100000;
      cur_id_item =1;
      
      kvs_stock = kvs_item * w;
      kvs_neworders = 3000 * 10 * w ;
      kvs_orders = 3000 * 10 * w ; 
      kvs_history = 3000 * 10 * w;
    }


      std::string* generateLoad_Orderline(){

        //INSERT INTO order_line values(
        //      3000, (1-3000) pk3
        //      10, (district) pk2
        //      2, (w id) pk1
        //      9, (1-20) pk4
        //      62180,
        //      2, NULL,5,30.040000915527344,'xQcMFRP9Bvc8tZtVecUujSgf')

        if(cur_number_orderline >10){
          cur_number_orderline=1;
          cur_o_id_orderline+=1;    
          if(cur_o_id_orderline >3000){
            cur_o_id_orderline = 1;
            cur_d_id_orderline +=1;
            if(cur_d_id_orderline >10){
              cur_d_id_orderline =1;
              cur_w_id_orderline+=1;
              if(cur_w_id_orderline > w){
                std::string cmd = "-1";      
                return alloc_cmd(cmd);
              }
            }
          }
        }

        std::string cmd = "put,orderline,";
        cmd+=std::to_string(cur_o_id_orderline); //3000
        cmd+=",";
        cmd+=std::to_string(cur_d_id_orderline); //10
        cmd+=",";
        cmd+=std::to_string(cur_w_id_orderline); //20
        cmd+=",";
        cmd+=std::to_string(cur_number_orderline++); //20
        cmd+=",";
        
        char buf[100]={'\0'};
        
        RandNum(buf,5);
        for(int i =0; i<5;i++) cmd+=buf[i];
        cmd+=",2,";
        memset(buf,0,100); 

        char timebuf[64]={'\0'};
        GenTimeStamp(timebuf);
        int i=0;
        while(timebuf[i]!='\0') cmd+=timebuf[i++];
        cmd+=",";  

        RandNum(buf,2);
        for(int i =0; i<2;i++) cmd+=buf[i];
        cmd+=",";
        memset(buf,0,100); //0.9389580549295925,-10, 10.0, 1,0,

        RandNum(buf,8);
        for(int i =0; i<8;i++) cmd+=buf[i];
        cmd+=",";
        memset(buf,0,100); 

        RandStrNum(buf,24);
        for(int i =0; i<24;i++) cmd+=buf[i];
        memset(buf,0,100); 

        return alloc_cmd(cmd);

      }

      std::string* generateLoad_Item(){
        //put,item,
        //999994,
        //84285,
        //R70iawo4a0nkzprZmNHmtgtb,
        //581584,
        //jbGB3pxToEmXF6CfY5t5viqpRs9OJUpSanoJvssWL8zzuc0XD0
        
        if(cur_id_item > kvs_item){
          std::string cmd_ = "-1";
          return alloc_cmd(cmd_);
        }
        
          std::string cmd = "put,item,";
          cmd+=std::to_string(cur_id_item++); 
          cmd+=",";
          char buf[100]={'\0'};
        
          RandNum(buf,5);
          for(int i =0; i<5;i++) cmd+=buf[i];
          cmd+=",";
          memset(buf,0,100); 
          
          RandStrNum(buf,24);
          for(int i =0; i<24;i++) cmd+=buf[i];
          cmd+=",";
          memset(buf,0,100); 

          RandNum(buf,6);
          for(int i =0; i<56;i++) cmd+=buf[i];
          cmd+=",";
          memset(buf,0,100); 

          RandStrNum(buf,50);
          for(int i =0; i<50;i++) cmd+=buf[i];
          memset(buf,0,100); 

          return alloc_cmd(cmd);
          
        
      }

      std::string* generateLoad_Stock(){
        //INSERT INTO stock values(
        //   1,2,3,... 100000 (item_id)
        //   1,2,3,... w (physically, wid then item id)
        //23,'bm4cdxlSRMRh7dFbFfF8TbKk','tUiCgklSoL3l6VCQvuXW96lm','sGuCy83SoD6YzYMKZ8R5KryC','UJTD6p5KjHIsIigWoFTKZnth','w04uCRMUsaahfuPvAHDsvhg8','yiwrN8oNAgYZ32Sba9GUYBen','Uh2ApD6V7xtFjEQOkzPmP0MX','o3QQrluWn2Jy8p54gJlsj9dg','N4WiblUe9vTunzfqJew6h0k0','chtDMQZQuFzfjdoc5gQ2MGJy',0,0,0,'NfB7eviJotfCS37h0kPeZ1hDRj')

        if(cur_id_stock > kvs_item){
          cur_id_stock=1;
          cur_w_id_stock+=1;
          if(cur_w_id_stock > w){
            std::string cmd = "-1";      
            return alloc_cmd(cmd);
          }
        }

        std::string cmd = "put,stock,";
        cmd+=std::to_string(cur_id_stock++); //100000
        cmd+=",";
        cmd+=std::to_string(cur_w_id_stock); //w
        cmd+=",";

        char buf[100]={'\0'};
        
        RandNum(buf,3);
        for(int i =0; i<3;i++) cmd+=buf[i];
        cmd+=",";
        memset(buf,0,100); 

        for(int j=0;j<10;j++){
          RandStrNum(buf,24);
          for(int i =0; i<24;i++) cmd+=buf[i];
          cmd+=",";
          memset(buf,0,100); //eBl42WoDyZb,OE,
        }
        cmd+="0,0,0,";
        RandStrNum(buf,50);
        for(int i =0; i<50;i++) cmd+=buf[i];
        memset(buf,0,100); //eBl42WoDyZb,OE,
        return alloc_cmd(cmd);





      }
      
      std::string* generateLoad_Customer(){


        if(cur_c_id_customer >3000){
          cur_c_id_customer = 1;
          cur_d_id_customer +=1;
          if(cur_d_id_customer >10){
            cur_d_id_customer =1;
            cur_w_id_customer+=1;
            if(cur_w_id_customer > w){
              std::string cmd = "-1";      
              return alloc_cmd(cmd);
            }
          }
        }

        std::string cmd = "put,customer,";
        cmd+=std::to_string(cur_c_id_customer++); //3000
        cmd+=",";
        cmd+=std::to_string(cur_d_id_customer); //10
        cmd+=",";
        cmd+=std::to_string(cur_w_id_customer); //20
        cmd+=",";
        
        char buf[100]={'\0'};
        
        RandStrNum(buf,12);
        for(int i =0; i<12;i++) cmd+=buf[i];
        cmd+=",OE,";
        memset(buf,0,100); //eBl42WoDyZb,OE,
        
        RandStrBig(buf,14);
        for(int i =0; i<14;i++) cmd+=buf[i];
        cmd+=","; //GMUUDXRMCZASEB,
        memset(buf,0,100);

        RandStrNum(buf,14);
        for(int i =0; i<14;i++) cmd+=buf[i];
        cmd+=",";
        memset(buf,0,100); //lksPRYp7HHuB2


        RandStrNum(buf,14);
        for(int i =0; i<12;i++) cmd+=buf[i];
        cmd+=",";
        memset(buf,0,100); //ADoSzCdMTXts

        RandStrNum(buf,14);
        for(int i =0; i<14;i++) cmd+=buf[i];
        cmd+=",fc,";
        memset(buf,0,100); //0qCbmXbPTw11cJdi,fc

        RandNum(buf,9);
        for(int i =0; i<9;i++) cmd+=buf[i];
        cmd+=",";
        memset(buf,0,100); //603645925

        RandNum(buf,15);
        for(int i =0; i<15;i++) cmd+=buf[i];
        cmd+=",";
        memset(buf,0,100); //7036785914322627

        char timebuf[64]={'\0'};
        GenTimeStamp(timebuf);
        int i=0;
        while(timebuf[i]!='\0') cmd+=timebuf[i++];
        cmd+=",BC,50000,";  //2023-04-18 16:04:56,BC,50000,

        RandNum(buf,14);
        for(int i =0; i<14;i++) cmd+=buf[i];
        cmd+=",-10, 10.0, 1,0,";
        memset(buf,0,100); //0.9389580549295925,-10, 10.0, 1,0,

        RandStrNum(buf,100);
        for(int i =0; i<100;i++) cmd+=buf[i];
        memset(buf,0,100); //last

        //put,customer,
        //  3000,10,20,
        //  eBl42WoDyZb,OE,GMUUDXRMCZASEB,
        //  lksPRYp7HHuB2,ADoSzCdMTXts,0qCbmXbPTw11cJdi,
        //  fC,603645925,7036785914322627,2023-04-18 16:04:56,BC,50000,0.9389580549295925,-10, 10.0, 1,0,vOXHDzgCn1kSLxgFmMw2iRGESBezRKezZwBgZ36ebpEHtHxZ6MA56oRCUwkMHYH2m0iw8ZXtfo6IlQFJquVlP64CJ1ADsu1lmUFL
        
        return alloc_cmd(cmd);

      }

      std::string* generateLoad_History(){

        if(cur_c_id_history >3000){
          cur_c_id_history = 1;
          cur_d_id_history +=1;
          if(cur_d_id_history >10){
            cur_d_id_history =1;
            cur_w_id_history+=1;
            if(cur_w_id_history > w){
              std::string cmd = "-1";      
              return alloc_cmd(cmd);
            }
          }
        }

        std::string cmd = "put,history,";
        cmd+=std::to_string(cur_c_id_history++); //3000
        cmd+=",";
        cmd+=std::to_string(cur_d_id_history); //10
        cmd+=",";
        cmd+=std::to_string(cur_w_id_history); //20
        cmd+=",";
        
        char buf[100]={'\0'};
        
        RandStrNum(buf,4);
        for(int i =0; i<4;i++) cmd+=buf[i];
        cmd+=",";
        memset(buf,0,100);

        RandStrNum(buf,4);
        for(int i =0; i<4;i++) cmd+=buf[i];
        cmd+=",";
        memset(buf,0,100);

        char timebuf[64]={'\0'};
        GenTimeStamp(timebuf);
        int i=0;
        while(timebuf[i]!='\0') cmd+=timebuf[i++];
        cmd+=","; 

        RandNum(buf,8);
        for(int i =0; i<8;i++) cmd+=buf[i];
        cmd+=",";
        memset(buf,0,100); 

        RandStrNum(buf,100);
        for(int i =0; i<24;i++) cmd+=buf[i];
        memset(buf,0,100); 
        
        return alloc_cmd(cmd);

      }

      std::string* generatePut_History(){
        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
        std::string cmd = "put,history,";
        cmd+=std::to_string((random()%3000)+1); //cid
        cmd+=",";
        cmd+=std::to_string((random()%10)+1); //district
        cmd+=",";
        cmd+=std::to_string((random()%w)+1); //warehouse
        cmd+=",";
        
        char buf[100]={'\0'};
        
        RandStrNum(buf,4);
        for(int i =0; i<4;i++) cmd+=buf[i];
        cmd+=",";
        memset(buf,0,100);

        RandStrNum(buf,4);
        for(int i =0; i<4;i++) cmd+=buf[i];
        cmd+=",";
        memset(buf,0,100);

        char timebuf[64]={'\0'};
        GenTimeStamp(timebuf);
        int i=0;
        while(timebuf[i]!='\0') cmd+=timebuf[i++];
        cmd+=","; 

        RandNum(buf,8);
        for(int i =0; i<8;i++) cmd+=buf[i];
        cmd+=",";
        memset(buf,0,100); 

        RandStrNum(buf,100);
        for(int i =0; i<24;i++) cmd+=buf[i];
        memset(buf,0,100); 
        
        return alloc_cmd(cmd);

      }
  

      std::string* generateLoad_NewOrders(){


        if(cur_c_id_neworders >3000){
          cur_c_id_neworders = 1;
          cur_d_id_neworders +=1;
          if(cur_d_id_neworders >10){
            cur_d_id_neworders =1;
            cur_w_id_neworders+=1;
            if(cur_w_id_neworders > w){
              std::string cmd = "-1";      
              return alloc_cmd(cmd);
            }
          }
        }

        std::string cmd = "put,neworders,";
        cmd+=std::to_string(cur_c_id_neworders++); //3000
        cmd+=",";
        cmd+=std::to_string(cur_d_id_neworders); //10
        cmd+=",";
        cmd+=std::to_string(cur_w_id_neworders); //20
        cmd+=",";
        
        char buf[100]={'\0'};
        
        RandStrNum(buf,4);
        for(int i =0; i<4;i++) cmd+=buf[i];
        memset(buf,0,100);
        
        return alloc_cmd(cmd);

      }

      std::string* generateLoad_Orders(){


        if(cur_c_id_orders >3000){
          cur_c_id_orders = 1;
          cur_d_id_orders +=1;
          if(cur_d_id_orders >10){
            cur_d_id_orders =1;
            cur_w_id_orders+=1;
            if(cur_w_id_orders > w){
              std::string cmd = "-1";      
              return alloc_cmd(cmd);
            }
          }
        }

        std::string cmd = "put,orders,";
        cmd+=std::to_string(cur_c_id_orders++); //3000
        cmd+=",";
        cmd+=std::to_string(cur_d_id_orders); //10
        cmd+=",";
        cmd+=std::to_string(cur_w_id_orders); //20
        cmd+=",";
        
        char buf[100]={'\0'};
        
        RandStrNum(buf,8);
        for(int i =0; i<8;i++) cmd+=buf[i];
        memset(buf,0,100);
        cmd+=",";

        char timebuf[64]={'\0'};
        GenTimeStamp(timebuf);
        int i=0;
        while(timebuf[i]!='\0') cmd+=timebuf[i++];
        cmd+=","; 

        RandStrNum(buf,4);
        for(int i =0; i<4;i++) cmd+=buf[i];
        memset(buf,0,100);
        cmd+=",";

        RandStrNum(buf,4);
        for(int i =0; i<4;i++) cmd+=buf[i];
        memset(buf,0,100);
        cmd+=",";

        RandStrNum(buf,4);
        for(int i =0; i<4;i++) cmd+=buf[i];
        memset(buf,0,100);
        
        return alloc_cmd(cmd);

      }



      std::string* generateGet_Item(){
        //get,item,all,195285
        std::string cmd= "get,item,all,";
        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
        cmd+=std::to_string((random()%100000)+1); //cid
        return alloc_cmd(cmd);

      }
      

      std::string* generateScan_Orderline(){
        //scan,orderline,
        std::random_device rd;
        std::default_random_engine random(rd());	
        //scan,orderline,1,1,2,60,4,7,1,200,2
        std::string cmd = "scan,orderline,1,1,1,";
        //random generate scan range
        int w_start = (random()%w)+1;
        cmd+=std::to_string(w_start); //district
        cmd+=",1,15,1,";
        cmd+=std::to_string(w_start); //warehouse
        return alloc_cmd(cmd);
      }
      

      std::string* generateGet_Orderline(){
        
        // put,orderline,3000,10,2,20
        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
        //scan,customer,c_id,1,3000,6,2
        std::string cmd = "get,orderline,";
        cmd+=std::to_string((random()%3000)+1); //o-id
        cmd+=",";
        cmd+=std::to_string((random()%10)+1); //district
        cmd+=",";
        cmd+=std::to_string((random()%w)+1); //warehouse
        cmd+=",";
        cmd+=std::to_string((random()%10)+1); 
        return alloc_cmd(cmd);
      }
      

      std::string* generateUpdate_Orderline(){
        
        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
        //update,orderline,3000,10,2,9, time
        std::string cmd = "update,orderline,";
        cmd+=std::to_string((random()%3000)+1); //o-id
        cmd+=",";
        cmd+=std::to_string((random()%10)+1); //district
        cmd+=",";
        cmd+=std::to_string((random()%w)+1); //warehouse
        cmd+=",";
        cmd+=std::to_string((random()%10)+1); //warehouse
        cmd+=",";
        
        char timebuf[64]={'\0'};
        GenTimeStamp(timebuf);
        int i=0;
        while(timebuf[i]!='\0') cmd+=timebuf[i++];
        
        return alloc_cmd(cmd);
      }
      


      std::string* generatePut_Orderline(){
        
        // get,orderline,3000,10,2,20
        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
        //scan,customer,c_id,1,3000,6,2
        std::string cmd = "put,orderline,";
        cmd+=std::to_string((random()%3000)+1); //o-id
        cmd+=",";
        cmd+=std::to_string((random()%10)+1); //district
        cmd+=",";
        cmd+=std::to_string((random()%w)+1); //warehouse
        cmd+=",";
        cmd+=std::to_string((random()%10)+1); //1-20
        cmd+=",";
        
        
        char buf[100]={'\0'};
        
        RandNum(buf,5);
        for(int i =0; i<5;i++) cmd+=buf[i];
        cmd+=",2,";
        memset(buf,0,100); 

        char timebuf[64]={'\0'};
        GenTimeStamp(timebuf);
        int i=0;
        while(timebuf[i]!='\0') cmd+=timebuf[i++];
        cmd+=",";  

        RandNum(buf,2);
        for(int i =0; i<2;i++) cmd+=buf[i];
        cmd+=",";
        memset(buf,0,100); //0.9389580549295925,-10, 10.0, 1,0,

        RandNum(buf,8);
        for(int i =0; i<8;i++) cmd+=buf[i];
        cmd+=",";
        memset(buf,0,100); 

        RandStrNum(buf,24);
        for(int i =0; i<24;i++) cmd+=buf[i];
        memset(buf,0,100); 

        return alloc_cmd(cmd);
      }
      
      std::string* generatePut_Orders(){


        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
        std::string cmd = "put,orders,";
        cmd+=std::to_string((random()%3000)+1); //cid
        cmd+=",";
        cmd+=std::to_string((random()%10)+1); //district
        cmd+=",";
        cmd+=std::to_string((random()%w)+1); //warehouse
        cmd+=",";
        
        char buf[100]={'\0'};
        
        RandStrNum(buf,8);
        for(int i =0; i<8;i++) cmd+=buf[i];
        memset(buf,0,100);
        cmd+=",";

        char timebuf[64]={'\0'};
        GenTimeStamp(timebuf);
        int i=0;
        while(timebuf[i]!='\0') cmd+=timebuf[i++];
        cmd+=","; 

        RandStrNum(buf,4);
        for(int i =0; i<4;i++) cmd+=buf[i];
        memset(buf,0,100);
        cmd+=",";

        RandStrNum(buf,4);
        for(int i =0; i<4;i++) cmd+=buf[i];
        memset(buf,0,100);
        cmd+=",";

        RandStrNum(buf,4);
        for(int i =0; i<4;i++) cmd+=buf[i];
        memset(buf,0,100);
        
        return alloc_cmd(cmd);

      }

      std::string* generateGet_Orders(){


        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
        std::string cmd = "get,orders,";
        cmd+=std::to_string((random()%3000)+1); //cid
        cmd+=",";
        cmd+=std::to_string((random()%10)+1); //district
        cmd+=",";
        cmd+=std::to_string((random()%w)+1); //warehouse
        
        return alloc_cmd(cmd);

      }

      std::string* generateUpdate_Orders(){


        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
        std::string cmd = "update,orders,";
        cmd+=std::to_string((random()%3000)+1); //cid
        cmd+=",";
        cmd+=std::to_string((random()%10)+1); //district
        cmd+=",";
        cmd+=std::to_string((random()%w)+1); //warehouse
        cmd+=",";

        char buf[100]={'\0'};
        
        RandStrNum(buf,8);
        for(int i =0; i<8;i++) cmd+=buf[i];
        memset(buf,0,100);


        
        return alloc_cmd(cmd);

      }



      std::string* generatePut_NewOrders(){
        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
        std::string cmd = "put,neworders,";
        cmd+=std::to_string((random()%3000)+1); //cid
        cmd+=",";
        cmd+=std::to_string((random()%10)+1); //district
        cmd+=",";
        cmd+=std::to_string((random()%w)+1); //warehouse
        cmd+=",";
       
        char buf[100]={'\0'};
        
        RandStrNum(buf,4);
        for(int i =0; i<4;i++) cmd+=buf[i];
        memset(buf,0,100);
        
        return alloc_cmd(cmd);

      }

      std::string* generateScan_NewOrders(){
        std::random_device rd;	
        std::default_random_engine random(rd());	
        std::string cmd = "scan,neworders,";
        //random generate scan range
        int w_start = (random()%w)+1;
        cmd+="1,1,";
        cmd+=std::to_string(w_start); //district
        cmd+=",3000,1,";
        cmd+=std::to_string(w_start); //warehouse
        return alloc_cmd(cmd);

      }



      std::string* generateScan_Customer(){
        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
        //scan,customer,c_id,1,3000,6,2
        std::string cmd = "scan,customer,c_id,1,1,";
        int k1_end = (random()%w)+1;
        cmd+=std::to_string(k1_end); //district
        cmd+=",";
        cmd+=",3000,1,";
        cmd+=std::to_string(k1_end); //district
        return alloc_cmd(cmd);
      }
      
      
      std::string* generateGet_Customer(){
        //get,customer,c_data,249,5,2
        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
        std::string cmd = "get,customer,c_data,";
        cmd+=std::to_string((random()%3000)+1); //cid
        cmd+=",";
        cmd+=std::to_string((random()%10)+1); //district
        cmd+=",";
        cmd+=std::to_string((random()%w)+1); //warehouse
        return alloc_cmd(cmd);

      }

     
      std::string* generateUpdate_Stock(){
        //update,stock,249(itemid),5(wid),2(quality)
        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
                std::string cmd = "update,stock,";
        cmd+=std::to_string((random()%kvs_item)+1); //item id
        cmd+=",";
        cmd+=std::to_string((random()%w)+1); //w id
        cmd+=",";

        cmd+=std::to_string((random()%99)+1); //quality
        return alloc_cmd(cmd);
        
      }
     

      std::string* generateGet_Stock(){
        //get,stock,249(itemid),5(wid),2(quality)
        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
        std::string cmd = "get,stock,";
        cmd+=std::to_string((random()%kvs_item)+1); //item id
        cmd+=",";
        cmd+=std::to_string((random()%w)+1); //w id
        return alloc_cmd(cmd);
        
      }
     
     
      std::string* generateUpdate_Customer(){
        //update,customer,249,5,2,-15403,flvQjsz7ADcUgKul18DtmQq9BwuMaKOF5KfmCdtKoToVohGnnSgZqUh1QbMyukBz2rkFSdXG7Kjt1ygoQKMGp3FDcZCWINUKCEXc
        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
        std::string cmd = "update,customer,";
        cmd+=std::to_string((random()%3000)+1); //cid
        cmd+=",";
        cmd+=std::to_string((random()%10)+1); //district
        cmd+=",";
        cmd+=std::to_string((random()%w)+1); //warehouse
        cmd+=",";
        char buf[100]={'\0'};
        RandNum(buf,14);
        for (int i=0;i<14;i++) cmd+=buf[i];
        memset(buf,0,100);
        cmd+=",";
        RandStrNum(buf,100);
        for (int i=0;i<100;i++) cmd+=buf[i];
        memset(buf,0,100);
        return alloc_cmd(cmd);
      }


      std::string* GenerateTEST_Customer(){
        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
        int seed= random() % 15000 ;
        std::string* cmd;
        if(seed<500){
          cmd=generateScan_Customer();
          cur_kvs_customer++;
        }else if(seed<1000){
          cmd=generateScan_NewOrders();
          cur_kvs_neworders++;
        }else if(seed<2000){
          cmd=generateGet_Stock();
          cur_kvs_stock++;
        }else if(seed<3000){
          cmd=generatePut_NewOrders();
          cur_kvs_neworders++;
        }else if(seed<4000){
          cmd=generateUpdate_Stock();
          cur_kvs_stock++;
        }
        else if(seed<5000){
          cmd=generatePut_Orderline();  
          cur_kvs_orderline++;
        }
        else if(seed<6000){
          cmd=generateGet_Customer();
          cur_kvs_customer++;
        }else if(seed<7000){
          cmd=generateScan_Orderline();
          cur_kvs_orderline++;
        }else if (seed<8000){
          cmd=generateGet_Item();
        }else if(seed<9000){
          cmd=generateUpdate_Orderline();
          cur_kvs_orderline++;
        }else if(seed<10000){
          cmd=generateUpdate_Orders();
          cur_kvs_orders++;
        }else if(seed<11000){
          cmd=generateGet_Orders();
          cur_kvs_orders++;
        }else if(seed<12000){
          cmd=generatePut_Orders();
          cur_kvs_orders++;
        }else if(seed<13000){
          cmd=generatePut_History();
          cur_kvs_orders++;
        }else {
          cmd=generateUpdate_Customer();
          cur_kvs_customer++;
        }
        
        return cmd;
      }
   
      std::string* gentpcc_cmd_Orderline_TEST(){
        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());
        int seed= random() % 1000 ;
        std::string* cmd;
        if (seed < 10){
          cmd=generateScan_Orderline();
          cur_kvs_orderline++;
        }else if (seed < 600){
          cmd=generateUpdate_Orderline();
          cur_kvs_orderline++;
        }else{
          cmd=generatePut_Orderline();
          cur_kvs_orderline++;
        }
        return cmd;
      }

      std::string* gentpcc_cmd(){
        
        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());
        int seed= random() % 10000 ;
        std::string* cmd;
        if(seed<customer_scan*10000){
          cmd=generateScan_Customer();
          cur_kvs_customer++;
        }else if(seed<(customer_scan+customer_get)*10000){
          cmd=generateGet_Customer();
          cur_kvs_customer++;
        }else if(seed<(customer_scan+customer_get+customer_update)*10000){
          cmd=generateUpdate_Customer();
          cur_kvs_customer++;
        }else if (seed < (customer_ratio+orderline_scan)*10000){
          cmd=generateScan_Orderline();
          cur_kvs_orderline++;
        }else if (seed < (customer_ratio+orderline_scan+orderline_update)*10000){
          cmd=generateUpdate_Orderline();
          cur_kvs_orderline++;
        }else if (seed < (customer_ratio+orderline_scan+orderline_update+orderline_put)*10000){
          cmd=generatePut_Orderline();
          cur_kvs_orderline++;
        }else if (seed < (customer_ratio+orderline_ratio+stock_scan)*10000){
          cmd=generateGet_Stock();
          cur_kvs_stock++;
        }else if (seed < (customer_ratio+orderline_ratio+stock_scan+stock_update)*10000){
          cmd=generateUpdate_Stock();
          cur_kvs_stock++;
        }else if (seed < (customer_ratio+orderline_ratio+stock_ratio+neworders_get)*10000){
          cmd=generateScan_NewOrders();
          cur_kvs_neworders++;
        }else if (seed < (customer_ratio+orderline_ratio+stock_ratio+neworders_get+neworders_put)*10000){
          cmd=generatePut_NewOrders();
          cur_kvs_neworders++;
        }else if (seed < (customer_ratio+orderline_ratio+stock_ratio+neworders_ratio+orders_get)*10000){
          cmd=generateGet_Orders();
          cur_kvs_orders++;
        }else if (seed < (customer_ratio+orderline_ratio+stock_ratio+neworders_ratio+orders_get+orders_put)*10000){
          cmd=generatePut_Orders();
          cur_kvs_orders++;
        }else if (seed < (customer_ratio+orderline_ratio+stock_ratio+neworders_ratio+orders_get+orders_put+orders_update)*10000){
          cmd=generateUpdate_Orders();
          cur_kvs_orders++;
        }else if (seed < (customer_ratio+orderline_ratio+stock_ratio+neworders_ratio+orders_ratio+history_put)*10000){
          cmd=generatePut_History();
          cur_kvs_history++;
        }else{
          cmd=generateGet_Item();
          cur_kvs_item++;
        }

        return cmd;


      }
      std::string* gentpcc_cmd_TEST(){
        
        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());
        int seed= random() % 10000 ;
        std::string* cmd;
        if(seed<customer_scan*10000){
          cmd=generateScan_Customer();
          cur_kvs_customer++;
        }else if(seed<(customer_scan+customer_get)*10000){
          cmd=generateGet_Customer();
          cur_kvs_customer++;
        }else if(seed<(customer_scan+customer_get+customer_update)*10000){
          cmd=generateUpdate_Customer();
          cur_kvs_customer++;
        }else if (seed < (customer_ratio+orderline_scan)*10000){
          cmd=generateScan_Orderline();
          cur_kvs_orderline++;
        }else if (seed < (customer_ratio+orderline_scan+orderline_update)*10000){
          cmd=generateUpdate_Orderline();
          cur_kvs_orderline++;
        }else if (seed < (customer_ratio+orderline_scan+orderline_update+orderline_put)*10000){
          cmd=generatePut_Orderline();
          cur_kvs_orderline++;
        }else if (seed < (customer_ratio+orderline_ratio+stock_scan)*10000){
          cmd=generateGet_Stock();
          cur_kvs_stock++;
        }else if (seed < (customer_ratio+orderline_ratio+stock_scan+stock_update)*10000){
          cmd=generateUpdate_Stock();
          cur_kvs_stock++;
        }
        else if (seed < (customer_ratio+orderline_ratio+stock_ratio+neworders_get)*10000){
          cmd=generateScan_NewOrders();
          cur_kvs_neworders++;
        }else if (seed < (customer_ratio+orderline_ratio+stock_ratio+neworders_get+neworders_put)*10000){
          cmd=generatePut_NewOrders();
          cur_kvs_neworders++;
        }
        else if (seed < (customer_ratio+orderline_ratio+stock_ratio+neworders_ratio+orders_get)*10000){
          cmd=generateGet_Orders();
          cur_kvs_orders++;
        }else if (seed < (customer_ratio+orderline_ratio+stock_ratio+neworders_ratio+orders_get+orders_put)*10000){
          cmd=generatePut_Orders();
          cur_kvs_orders++;
        }else if (seed < (customer_ratio+orderline_ratio+stock_ratio+neworders_ratio+orders_get+orders_put+orders_update)*10000){
          cmd=generateUpdate_Orders();
          cur_kvs_orders++;
        }
        else if (seed < (customer_ratio+orderline_ratio+stock_ratio+neworders_ratio+orders_ratio+history_put)*10000){
          cmd=generatePut_History();
          cur_kvs_history++;
        }
        else{
          cmd=generateGet_Item();
          cur_kvs_item++;
        }

        return cmd;


      }
      
      private:


      int w; //num of warehouse

      //access ratio for different table
      const float customer_ratio= 0.078;
      const float customer_scan = 0.078 * 0.01;
      const float customer_get = 0.078 * 0.65;
      const float customer_update = 0.078 * 0.34;

      const float orderline_ratio= 0.162;
      const float orderline_scan = 0.162*0.01;
      const float orderline_update = 0.162*0.072;
      const float orderline_put = 0.162*0.918;

      const float stock_ratio= 0.53;
      const float stock_scan = 0.53*0.74;
      const float stock_update = 0.53*0.26;

      const float neworders_ratio= 0.04;
      const float neworders_get = 0.04*0.5;
      const float neworders_put = 0.04*0.5;
      
      const float orders_ratio= 0.043;
      const float orders_get = 0.043*0.4;
      const float orders_put = 0.043*0.3;
      const float orders_update = 0.043*0.3;

      const float history_put = 0.013;

      const float item_get = 0.133;
      
      
      int cur_kvs_customer;
      int cur_w_id_customer;
      int cur_d_id_customer;
      int cur_c_id_customer;



      int cur_kvs_orderline;
      int cur_w_id_orderline;
      int cur_d_id_orderline;
      int cur_o_id_orderline;
      int cur_number_orderline;

      


      int cur_kvs_stock;
      int cur_w_id_stock;
      int cur_id_stock;

      int cur_kvs_neworders;
      int cur_kvs_orders;
      int cur_kvs_history;
      
      int cur_kvs_item;
      int cur_id_item;

      int kvs_customer;
      int kvs_orderline;
      int kvs_stock;
      
      int kvs_neworders;
      int cur_w_id_neworders;
      int cur_d_id_neworders;
      int cur_c_id_neworders;


      int kvs_orders;
      int cur_w_id_orders;
      int cur_d_id_orders;
      int cur_c_id_orders;


      int kvs_history;
      int cur_w_id_history;
      int cur_d_id_history;
      int cur_c_id_history;

      
      int kvs_item;



  };
  
  
  
  class tpch_gen{

    public:
      tpch_gen(int warehouse,int item_size){
        kvs_item = item_size;
        w = warehouse;
      }
      std::string* gentpch_cmd_orderline_TEST(){
        return genQ3_3_AP();
      }

      std::string* gentpch_cmd_Q1_1(){
        int range = 96;
        return genorderline_impl(range);
      }

      std::string* gentpch_cmd_Q2_2(int seq){
        if(seq == 1){
          int range = 42;
          return genitem_impl(range);
        }else{
          int range = 53;
          return genneworder_impl(range);
        }
        
      }

      std::string* gentpch_cmd_Q3_3(int seq){
        if(seq == 1){
          int range = 37;
          return gencustomer_impl(range);
        }else if(seq==2){
          int range = 44;
          return genorder_impl(range);
        }else{
          int range = 42;
          return genorderline_impl(range);
        }
      }

      std::string* gentpch_cmd_Q5_3(int seq){
        if(seq == 1){
          int range = 49;
          return gencustomer_impl(range);
        }else if(seq==2){
          int range = 56;
          return genorder_impl(range);
        }else{
          int range = 50;
          return genorderline_impl(range);
        }
      }

      std::string* gentpch_cmd_Q6_1(){
        int range = 88;
        return genorderline_impl(range);
      }

      std::string* gentpch_cmd_Q7_3(int seq){
        if(seq == 1){
          int range = 43;
          return gencustomer_impl(range);
        }else if(seq==2){
          int range = 49;
          return genorder_impl(range);
        }else{
          int range = 43;
          return genorderline_impl(range);
        }
      }

      std::string* gentpch_cmd_Q8_4(int seq){
        if(seq == 1){
          int range = 46;
          return gencustomer_impl(range);
        }else if(seq==2){
          int range = 47;
          return genorderline_impl(range);
        }else if (seq==3){
          int range = 32;
          return genorder_impl(range);
        }else{
          int range =75;
          return genitem_impl(range);
        }
      }

      std::string* gentpch_cmd_Q9_4(int seq){
        if(seq == 1){
          int range = 51;
          return genorderline_impl(range);
        }else if(seq==2){
          int range = 45;
          return genorder_impl(range);
        }else if (seq==3){
          int range = 39;
          return genitem_impl(range);
        }else{
          int range =56;
          return genneworder_impl(range);
        }
      }

      std::string* gentpch_cmd_Q10_3(int seq){
        if(seq == 1){
          int range = 98;
          return gencustomer_impl(range);
        }else if(seq==2){
          int range = 90;
          return genorder_impl(range);
        }else{
          int range = 87;
          return genorderline_impl(range);
        }
      }

      std::string* gentpch_cmd_Q12_2(int seq){
        if(seq==1){
          int range = 75;
          return genorder_impl(range);
        }else{
          int range = 65;
          return genorderline_impl(range);
        }
      }

      std::string* gentpch_cmd_Q14_2(int seq){
        if(seq==1){
          int range = 56;
          return genitem_impl(range);
        }else{
          int range = 50;
          return genorderline_impl(range);
        }
      }

      std::string* gentpch_cmd_Q16_2(int seq){
        if(seq==1){
          int range = 57;
          return genitem_impl(range);
        }else{
          int range = 52;
          return genneworder_impl(range);
        }
      }

      std::string* gentpch_cmd_Q17_2(int seq){
        if(seq==1){
          int range = 92;
          return genorderline_impl(range);
        }else{
          int range = 87;
          return genitem_impl(range);
        }
      }


      std::string* gentpch_cmd_Q18_3(int seq){
        if(seq == 1){
          int range = 90;
          return gencustomer_impl(range);
        }else if(seq==2){
          int range = 90;
          return genorder_impl(range);
        }else{
          int range = 90;
          return genorderline_impl(range);
        }
      }

      std::string* gentpch_cmd_Q20_3(int seq){
        if(seq == 1){
          int range = 76;
          return genitem_impl(range);
        }else if(seq==2){
          int range = 90;
          return genneworder_impl(range);
        }else{
          int range = 54;
          return genorderline_impl(range);
        }
      }
      


      
      std::string* gentpch_cmd(){
        std::random_device rd;
        std::default_random_engine random(rd());
        int seed = random()%6000;
        if(seed<1000){
          return genQ1();
        }else if (seed<2000){
          return genQ2_1();
        }else if (seed<3000){
          return genQ2_2();
        }else if (seed<4000){
          return genQ3_1();
        }else if (seed<5000){
          return genQ3_2();
        }else{
          return genQ3_3();
        }



      }

    private:
      //query_1
      /*
      select   ol_number,
      sum(ol_quantity) as sum_qty,
      sum(ol_amount) as sum_amount,
      avg(ol_quantity) as avg_qty,
      avg(ol_amount) as avg_amount,
      count(*) as count_order
      from	 orderline
      where	 ol_delivery_d > '2007-01-02 00:00:00.000000'
      group by ol_number order by ol_number
      */
      //we synthesis scan operations that only touches customer table, the range of scan is given by primary keys
      std::string* genQ1(){
        std::random_device rd;
        std::default_random_engine random(rd());	
        ////scan,customer,c_id,100,5,2,60,7,4
        std::string cmd = "scAP,customer,c_id,";
        //random generate scan range
        
        //int w_range=(random()%10)+1;
        
        
        int w_start = (random()%w)+1;
        int w_end=w_start+(random()%w)+1;
        if(w_end>=w){
          w_end = w;
        }
        cmd+="1,1,";
        cmd+=std::to_string(w_start); //district
        cmd+=",1,1,";
        cmd+=std::to_string(w_end); //warehouse
        return alloc_cmd(cmd);
      }

        //query_2
        /*
        select 	 su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment
        from	 item, supplier, stock, nation, region,
          (select s_i_id as m_i_id,
            min(s_quantity) as m_s_quantity
          from	 stock, supplier, nation, region
          where	 mod((s_w_id*s_i_id),10000)=su_suppkey
            and su_nationkey=n_nationkey
            and n_regionkey=r_regionkey
            and r_name like 'Europ%'
          group by s_i_id) m
        where 	 i_id = s_i_id
          and mod((s_w_id * s_i_id), 10000) = su_suppkey
          and su_nationkey = n_nationkey
          and n_regionkey = r_regionkey
          and i_data like '%b'
          and r_name like 'Europ%'
          and i_id=m_i_id
          and s_quantity = m_s_quantity
        order by n_name, su_name, i_id
        */
       std::string* genQ2_1(){
        //retrieve stock
        //scan,item,1,300
        std::random_device rd;
        std::default_random_engine random(rd());	
        std::string cmd = "scAP,item,";
        //random generate scan range
        int item_start=(random()%kvs_item)+1;
        int item_end=item_start+(random()%kvs_item)+1;
        if(item_end>kvs_item){
          item_end = kvs_item;
        }

        cmd+=std::to_string(item_start); //district
        cmd+=",";
        cmd+=std::to_string(item_end); //district
        return alloc_cmd(cmd);

      }

       std::string* genQ2_2(){
        //retrieve stock
        //scan,stock,2957,1, 1212,3
        std::random_device rd;
        std::default_random_engine random(rd());	
        std::string cmd = "scAP,stock,";
        //random generate scan range
        
        
        int w_start = (random()%w)+1;
        int w_end=w_start+ (random()%w)+1;
        if(w_end>=w){
          w_end = w;
        }
        int kvs = random()%kvs_item;

        cmd+="1,";
        cmd+=std::to_string(w_start); //district
        cmd+=",";
        cmd+=std::to_string(kvs_item); //district
        cmd+=",";
        cmd+=std::to_string(w_end); //warehouse
        return alloc_cmd(cmd);

      }

       //query 3
       /*
        select   ol_o_id, ol_w_id, ol_d_id,
          sum(ol_amount) as revenue, o_entry_d
        from 	 customer, neworder, orders, orderline
        where 	 c_state like 'A%'
          and c_id = o_c_id
          and c_w_id = o_w_id
          and c_d_id = o_d_id
          and no_w_id = o_w_id
          and no_d_id = o_d_id
          and no_o_id = o_id
          and ol_w_id = o_w_id
          and ol_d_id = o_d_id
          and ol_o_id = o_id
          and o_entry_d > '2007-01-02 00:00:00.000000'
        group by ol_o_id, ol_w_id, ol_d_id, o_entry_d
        order by revenue desc, o_entry_d
       
       */

      std::string* genQ3_1(){
        //scan,orders,
        std::random_device rd;
        std::default_random_engine random(rd());	
        ////scan,orders,100,5,2,60,7,4
        std::string cmd = "scAP,orders,";
        //random generate scan range
        
        
        int w_start = (random()%w)+1;
        int w_end=w_start+(random()%w)+1;
        if(w_end>=w){
          w_end = w;
        }
        cmd+="1,1,";
        cmd+=std::to_string(w_start); //district
        cmd+=",1,1,";
        cmd+=std::to_string(w_end); //warehouse
        return alloc_cmd(cmd);

      }
      std::string* genQ3_2(){
        //scan,neworders,
        std::random_device rd;
        std::default_random_engine random(rd());	
        //scan,neworders,100,5,2,60,7,4
        std::string cmd = "scAP,neworders,";
        //random generate scan range
        
        
        int w_start = (random()%w)+1;
        int w_end=w_start+(random()%w)+1;
        if(w_end>=w){
          w_end = w;
        }
        cmd+="1,1,";
        cmd+=std::to_string(w_start); //district
        cmd+=",1,1,";
        cmd+=std::to_string(w_end); //warehouse
        return alloc_cmd(cmd);

       }
      std::string* genQ3_3(){
        //scan,orderline,
        std::random_device rd;
        std::default_random_engine random(rd());	
        //scan,orderline,1,1,2,60,4,7,1,200,2
        std::string cmd = "scAP,orderline,1,1,1,";
        //random generate scan range
        
        
        int w_start = (random()%w)+1;
        int w_end=w_start+((random()%w)+1)*0.4;
        if(w_end>=w){
          w_end = w;
        }
        cmd+=std::to_string(w_start); //district
        cmd+=",1,1,1,";
        cmd+=std::to_string(w_end); //warehouse
        return alloc_cmd(cmd);

       }

      std::string* genQ3_3_AP(){
        //scan,orderline,
        std::random_device rd;
        std::default_random_engine random(rd());	
        //scan,orderline,1,1,2,60,4,7,1,200,2
        std::string cmd = "scAP,orderline,1,1,1,";
        //random generate scan range
        
        
        int w_start = 1;
        int w_end=8;
        if(w_end>=w){
          w_end = w;
        }
        cmd+=std::to_string(w_start); //district
        cmd+=",1,1,1,";
        cmd+=std::to_string(w_end); //warehouse
        return alloc_cmd(cmd);

       }
      
      
      std::string* genorderline_impl(int range){ //0-100
        //scan,orderline,	
        //scan,orderline,1,1,2,60,4,7,1,200,2
        std::string cmd = "scAP,orderline,1,1,1,";
        //random generate scan range
        
        
        int w_start = 1;
        int w_end = range/10*w/10;
        int tail = range%10;
        if(w_end>=w){
          w_end = w;
        }
        cmd+=std::to_string(w_start); //district
        cmd+=",1,1,";
        cmd+=std::to_string(tail); //district
        cmd+=",";
        cmd+=std::to_string(w_end); //warehouse
        return alloc_cmd(cmd);

       }

      std::string* genitem_impl(int range){ //0-100
        //scan,item,1,300
        std::string cmd = "scAP,item,";
        //random generate scan range
        int item_start=1;
        int item_end=range/10*kvs_item/10;
        if(item_end>kvs_item){
          item_end = kvs_item;
        }

        cmd+=std::to_string(item_start); //district
        cmd+=",";
        cmd+=std::to_string(item_end); //district
        return alloc_cmd(cmd);

       }

      std::string* genneworder_impl(int range){
        //scan,neworders,
        //scan,neworders,100,5,2,60,7,4
        std::string cmd = "scAP,neworders,";
        //random generate scan range
        
        
        int w_start = 1;
        int w_end=range/10*w/10;
        int tail = range%10;
        if(w_end>=w){
          w_end = w;
        }
        cmd+="1,1,";
        cmd+=std::to_string(w_start); //district
        cmd+=",1,";
        cmd+=std::to_string(tail); //warehouse
        cmd+=",";
        cmd+=std::to_string(w_end); //warehouse
        return alloc_cmd(cmd);
      }


      std::string* gencustomer_impl(int range){

        ////scan,customer,c_id,100,5,2,60,7,4
        std::string cmd = "scAP,customer,c_id,";
        //random generate scan range
        
        //int w_range=(random()%10)+1;
        
        
        int w_start = 11;
        int w_end=range/10*w/10;
        int tail = range%10;
        if(w_end>=w){
          w_end = w;
        }
        cmd+="1,1,";
        cmd+=std::to_string(w_start); //district
        cmd+=",1,";
        cmd+=std::to_string(tail);
        cmd+=",";
        cmd+=std::to_string(w_end); //warehouse
        
        return alloc_cmd(cmd);
      }
    
      std::string* genorder_impl(int range){
        std::string cmd = "scAP,orders,";
        //random generate scan range
        
        
        int w_start = 1;
        int w_end=range/10*w/10;
        int tail = range%10;
        if(w_end>=w){
          w_end = w;
        }
        cmd+="1,1,";
        cmd+=std::to_string(w_start); //district
        cmd+=",1,";
        cmd+=std::to_string(tail); 
        cmd+=",";
        cmd+=std::to_string(w_end); //warehouse
        return alloc_cmd(cmd);
      }
    
    
    
    
    
    
    
    
    
    
    
    
    
    int repeat_time;
    int kvs_item;
    int w;










};



}







//==============================End of TPCC-KV-generator================================================================





//==============================HTAP Begin======================================================================

//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// global variance
std::map<int, std::vector<std::string> *> *warehouse; // key:value list //0
//BPlusItemTree *ItemTree;                              // 1
//BPlusStockTree *StockTree;                            // 2
//BPlusDistrictTree *DistrictTree;                              // 3
//BPlusHistoryfk1Tree *Historyfk1Tree;                         // 4
//BPlusHistoryfk2Tree *Historyfk2Tree;                         // 5
//BPlusCustomerTree *CustomerTree;                              // 6
//BPlusNewOrdersTree *NewOrdersTree;                              // 7
//BPlusOrdersTree *OrdersTree;                                // 8
//BPlusOrderlineTree *OrderLineTree;                             // 9

std::thread BackGround_ItemTree;
std::thread BackGround_StockTree;
//std::thread BackGround_DistrictTree;
std::thread BackGround_HistoryTree;
std::thread BackGround_CustomerTree;
std::thread BackGround_NewOrderTree;
std::thread BackGround_OrdersTree;
std::thread BackGround_OrderLineTree;
int bg_id = 0;

//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// fuction tools

// 1. split string
std::vector<std::string> split(std::string s, std::string delimiter)
{
  size_t pos_start = 0, pos_end, delim_len = delimiter.length();
  std::string token;
  std::vector<std::string> res;

  while ((pos_end = s.find(delimiter, pos_start)) != std::string::npos)
  {
    token = s.substr(pos_start, pos_end - pos_start);
    pos_start = pos_end + delim_len;
    if (token == "")
    {
      continue;
    }
    res.push_back(token);
  }
  if (s.substr(pos_start) != "")
    res.push_back(s.substr(pos_start));
  return res;
}

//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// benchmark & warmup



// multithread model

class ProAndCon {
  private:
    std::deque<std::string*> m_queue;
    std::mutex m_mutex;//全局互斥锁
    std::condition_variable_any m_notEmpty;//全局条件变量（不为空）
    std::condition_variable_any m_notFull;//全局条件变量（不为满）
    int m_maxSize;//队列最大容量

  private:
    //队列为空
    bool isEmpty() const {
        return m_queue.empty();
    }
    //队列已满
    bool isFull() const {
        return m_queue.size() == m_maxSize;
    }

public:
    ProAndCon(int maxSize) {
        this->m_maxSize = maxSize;
        
        
    }

    ~ProAndCon();

    void product(std::string* cmd) {
      
        std::unique_lock<std::mutex> locker(m_mutex);
        while(isFull()) {
            //cout<<"队列已满，请等待"<<endl;
            //生产者等待"产品队列缓冲区不为满"这一条件发生.
            m_notFull.wait(m_mutex);
        }
        //往队列里面生产一个元素,同时通知不为空这个信号量
        m_queue.push_back(cmd);
        m_notEmpty.notify_one();
    }
    std::string* consumption() {
        std::unique_lock<std::mutex> locker(m_mutex);
        while(isEmpty()) {
           // cout<<"队列已空，请等待"<<endl;
            // 消费者等待"产品队列缓冲区不为空"这一条件发生.
            m_notEmpty.wait(m_mutex);
        }
        //在队列里面消费一个元素,同时通知队列不满这个信号量
        std::string* v = m_queue.front();
        m_queue.pop_front();
        m_notFull.notify_one();
        return v;
    }
};




void Consumer_ItemTree_MVCC_TEST(ProAndCon* itemQ_, const int load_flag_){
  std::string skt= "/home/client/mysql-base/data/mysql.sock"; 
  MYSQL mysql;     
  mysql_init(&mysql);     
  mysql_options(&mysql, MYSQL_SET_CHARSET_NAME, "utf8");     
  mysql_options(&mysql, MYSQL_INIT_COMMAND, "SET NAMES utf8");      
  mtx.lock(); 
  if(!mysql_real_connect(&mysql, "127.0.0.1", "root", "","pbtest", 4000, NULL, 0)) {         
      std::cout<<"mysql connect error item init : "<<mysql_error(&mysql)<<" "<<mysql_errno(&mysql);     
  }    
  mtx.unlock();
  //"create table pbtest.item(
    //  pk char(16) primary key, 
    //  lsn char(8), 
    //value1 char(16),value2 char(24),value3 char(8),value4 char(50))";   
  if(load_flag_ == 1){
    auto t1 = std::chrono::steady_clock::now();
    while(1){
      std::string* cmd = itemQ_->consumption();
      if(cmd->size()==0){
        ErrorDetect(); 
        delete cmd;
        continue;
      }
      if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        delete cmd;
        break;
      }

      //key word://1: put//2: get//3:scan//4.update
      if(KeyWordExist(cmd,1)){
        std::vector<std::string> res = split(*cmd, ",");
        
        if(res.size()!=7){
          ErrorDetect();
        }
        //int pk = atoi(res[2].c_str());
        std::string str = "insert into item(pk, lsn, value1, value2, value3, value4 ) values(";
        str+=res[2];
        str+=",\'lsn\',\'";
        str+=res[3];
        str+="\', \'";
        str+=res[4]+"\', \'";
        str+=std::to_string(atoi(res[5].c_str()))+"\', \'";
        str+=res[6];
        str+="\');";     
        
        mysql_real_query(&mysql, str.c_str(), str.size());  
        int check_flag=0;
        if(check_flag == 1){
          std::string str = "select * from item where pk = ";
          str+=res[2]+";";
          mysql_real_query(&mysql, str.c_str(), str.size());       
          MYSQL_RES *result = mysql_store_result(&mysql);   
          MYSQL_ROW row;    
          while (row = mysql_fetch_row(result)) {         
            std::cout<<"id: "<<row[0]<<" name: "<<row[1]<<" age: "<<row[2]<<std::endl;     
          } 
          mysql_free_result(result);
        }   
      }
      delete cmd;
    }
    auto t2 = std::chrono::steady_clock::now();
    
    static_tree::item_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
    std::cout<< "load time used: "<<static_tree::item_put_time<<" ms\n";
    mysql_close(&mysql);
    return;
  }else{
    static_tree::item_put_time=0;
    while(1){
      std::string* cmd = itemQ_->consumption();
      if(cmd->size()==0){
        ErrorDetect(); 
        delete cmd;
        continue;
      }
      if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        mysql_close(&mysql);
        break;
      }

      



      //key word:
      //1: put
      //2: get
      //3:scan
      //4.update
      if(KeyWordExist(cmd,1)){
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=7){
          ErrorDetect();
        }
        std::string str = "insert into item(pk, lsn, value1, value2, value3, value4 ) values( 100";
        str+=res[2];
        str+=",\'lsn\',\'";
        str+=res[3];
        str+="\', \'";
        str+=res[4]+"\', \'";
        str+=std::to_string(atoi(res[5].c_str()))+"\', \'";
        str+=res[6];
        str+="\');"; 
          auto t1 = std::chrono::steady_clock::now();
          mysql_real_query(&mysql, str.c_str(), str.size());     
          auto t2 = std::chrono::steady_clock::now();
          static_tree::item_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
          static_tree::item_put_cnt++;
      }else if(KeyWordExist(cmd,2)){
        std::vector<std::string> res = split(*cmd, ",");
        int pk = atoi(res[3].c_str());
        std::string str = "select * from item where pk = "+res[3]+";";
        auto t1 = std::chrono::steady_clock::now();
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::item_get_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
        static_tree::item_get_cnt++;
      }else if (KeyWordExist(cmd,3)){
        //scan,item,1,300
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=4){
          ErrorDetect();
        }
        int pk1 = atoi(res[2].c_str()); //w id
        int pk2 = atoi(res[3].c_str()); //item d
        if(pk2<pk1){
          int tmp=pk2;
          pk2=pk1;
          pk1=tmp;
          int x=1;
        }
        auto t1_i = std::chrono::steady_clock::now();
        std::string str = "select * from item where pk >= ";
        str+=res[2];
        str+=" and pk <= ";
        str+=res[3];
        str+=";";
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);
        auto t2 = std::chrono::steady_clock::now();   
        auto t2_i = std::chrono::steady_clock::now();     
        static_tree::item_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count();
        static_tree::item_scan_cnt_c++;
        //value[0] = value[1];
      }else if (KeyWordExist(cmd,5)){
        //scan,item,1,300
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=4){
          ErrorDetect();
        }
        int pk1 = atoi(res[2].c_str()); //w id
        int pk2 = atoi(res[3].c_str()); //item d
        if(pk2<pk1){
          int tmp=pk2;
          pk2=pk1;
          pk1=tmp;
          int x=1;
        }
        auto t1_i = std::chrono::steady_clock::now();
        std::string str = "select * from item where pk >= ";
        str+=res[2];
        str+=" and pk <= ";
        str+=res[3];
        str+=";";
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);
        auto t2 = std::chrono::steady_clock::now();   
        auto t2_i = std::chrono::steady_clock::now();     
        static_tree::item_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count();
        static_tree::item_scan_cnt_h++;
      }else{
        ErrorDetect();
      }
      delete cmd;
    }
    
  
  }
    
  return;
}


void Consumer_OrderlineTree_MVCC_TEST(ProAndCon* itemQ_, const int load_flag_){
  std::string skt= "/home/client/mysql-base/data/mysql.sock"; 
  MYSQL mysql;     
  mysql_init(&mysql);     
  mysql_options(&mysql, MYSQL_SET_CHARSET_NAME, "utf8");     
  mysql_options(&mysql, MYSQL_INIT_COMMAND, "SET NAMES utf8");    
  mtx.lock();    
  if(!mysql_real_connect(&mysql, "127.0.0.1", "root", "","pbtest", 4000, NULL, 0)) {         
      std::cout<<"mysql connect error item init : "<<mysql_error(&mysql)<<" "<<mysql_errno(&mysql);     
  }  
  mtx.unlock(); 
  if(load_flag_ == 1){
    auto t1 = std::chrono::steady_clock::now();
    while(1){
      std::string* cmd = itemQ_->consumption();
      if(cmd->size()==0){
        ErrorDetect(); 
        delete cmd;
        continue;
      }
      if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        delete cmd;
        break;
      }

      //key word://1: put//2: get//3:scan//4.update
      if(KeyWordExist(cmd,1)){
        std::vector<std::string> res = split(*cmd, ",");
        
        if(res.size()!=12){
          ErrorDetect();
        }
        //put,orderline,[0-3000],[0,10],[0-w],[0-20],num5,num2, ts,num2,num8,strnum24
        std::string str = "insert into orderline(pk1, pk2, pk3, pk4, lsn, value1, value2, value3, value4, value5, value6) values(";
        str+=res[4]+",";
        str+=res[3]+",";
        str+=res[2]+",";
        str+=res[5];
        str+=",\'lsn\',\'";
        str+=res[6];
        str+="\', \'";
        str+=res[7];
        str+="\', \'";
        str+=res[8];
        str+="\', \'";
        str+=res[9];
        str+="\', \'";
        str+=res[10];
        str+="\', \'";
        str+=res[11];
        str+="\');";   
        mysql_real_query(&mysql, str.c_str(), str.size());  
        int check_flag=0;
        if(check_flag == 1){
          std::string str = "select * from orderline where pk1 = ";
          str+=res[4]+" and pk2 = ";
          str+=res[3]+" and pk3 = ";
          str+=res[2]+" and pk4 = ";
          str+=res[5]+" ;";
          mysql_real_query(&mysql, str.c_str(), str.size());       
          MYSQL_RES *result = mysql_store_result(&mysql);   
          MYSQL_ROW row;    
          while (row = mysql_fetch_row(result)) {         
            std::cout<<"id: "<<row[0]<<" name: "<<row[1]<<" age: "<<row[2]<<std::endl;     
          } 
          mysql_free_result(result);
        }   
      }

    }
    auto t2 = std::chrono::steady_clock::now();
    
    static_tree::orderline_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
    std::cout<< "load orderline used: "<<static_tree::orderline_put_time<<" ms\n";
    mysql_close(&mysql);
    return;
  }else{
    static_tree::orderline_put_time=0;
    while(1){
      std::string* cmd = itemQ_->consumption();
      if(cmd->size()==0){
        ErrorDetect(); 
        delete cmd;
        continue;
      }
      if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        delete cmd;
        mysql_close(&mysql);
        break;
      }
      //key word:
      //1: put
      //2: get
      //3:scan
      //4.update
      if(KeyWordExist(cmd,1)){
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=12){
          ErrorDetect();
        }
        //put,orderline,[0-3000],[0,10],[0-w],[0-20],num5,num2, ts,num2,num8,strnum24
        std::string str = "insert into orderline(pk1, pk2, pk3, pk4, lsn, value1, value2, value3, value4, value5, value6) values(";
        str+=res[4]+",";
        str+=res[3]+",";
        str+=res[2]+",";
        str+=res[5];
        str+=",\'lsn\',\'";
        str+=res[6];
        str+="\', \'";
        str+=res[7];
        str+="\', \'";
        str+=res[8];
        str+="\', \'";
        str+=res[9];
        str+="\', \'";
        str+=res[10];
        str+="\', \'";
        str+=res[11];
        str+="\');";   
          auto t1 = std::chrono::steady_clock::now();
          mysql_real_query(&mysql, str.c_str(), str.size());     
          auto t2 = std::chrono::steady_clock::now();
          static_tree::orderline_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
          static_tree::orderline_put_cnt++;
        int check_flag=0;
        if(check_flag == 1){
          std::string str = "select * from orderline where pk1 = ";
          str+=res[4]+" and pk2 = ";
          str+=res[3]+" and pk3 = ";
          str+=res[2]+" and pk4 = ";
          str+=res[5]+";";
          mysql_real_query(&mysql, str.c_str(), str.size());       
          MYSQL_RES *result = mysql_store_result(&mysql);   
          MYSQL_ROW row;    
          while (row = mysql_fetch_row(result)) {         
            std::cout<<"id: "<<row[0]<<" name: "<<row[1]<<" age: "<<row[2]<<std::endl;     
          } 

        }   
      }else if(KeyWordExist(cmd,4)){
        //update,orderline,[0-3000],[0,10],[0-w],[0-20],ts
        std::vector<std::string> res = split(*cmd, ",");
        std::string str = "update orderline set value6=\'";
        str+=res[6]+"\' where pk1=";
        str+=res[4]+" and pk2=";
        str+=res[3]+" and pk3=";
        str+=res[2]+" and pk4=";
        str+=res[5]+";";
        auto t1 = std::chrono::steady_clock::now();
        mysql_real_query(&mysql, str.c_str(), str.size());       
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::orderline_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
        static_tree::orderline_update_cnt++;
      }else if (KeyWordExist(cmd,3)){
        //scan,orderline,1,1,2,60,4,7,1,200,2
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=10){
          ErrorDetect();
        }
        auto t1_i = std::chrono::steady_clock::now();
        std::string str = "select * from orderline where pk1 >= ";
        str+=res[5];
        str+=" and pk1 <= ";
        str+=res[9]+" and pk2 >= ";
        str+=res[4]+" and pk2 <= ";
        str+=res[8]+" and pk3 >= ";
        str+=res[3]+" and pk3 <= ";
        str+=res[7]+" and pk4 >= ";
        str+=res[2]+" and pk4 <= ";
        str+=res[6]+";";
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);
         
        auto t2_i = std::chrono::steady_clock::now();     
        static_tree::orderline_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count();
        static_tree::orderline_scan_cnt_c++;
        //value[0] = value[1];
      }else if (KeyWordExist(cmd,5)){
        //scap,orderline,1,1,2,60,4,7,1,200,2
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=10){
          ErrorDetect();
        }
        auto t1_i = std::chrono::steady_clock::now();
        std::string str = "select * from orderline where pk1 >= ";
        str+=res[5];
        str+=" and pk1 <= ";
        str+=res[9]+" and pk2 >= ";
        str+=res[4]+" and pk2 <= ";
        str+=res[8]+" and pk3 >= ";
        str+=res[3]+" and pk3 <= ";
        str+=res[7]+" and pk4 >= ";
        str+=res[2]+" and pk4 <= ";
        str+=res[6]+";";
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);
        auto t2 = std::chrono::steady_clock::now();   
        auto t2_i = std::chrono::steady_clock::now();     
        static_tree::orderline_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count();
        static_tree::orderline_scan_cnt_h++;
      }else{
        ErrorDetect();
      }
      delete cmd;
    }
    
  
  }
  return;

}


void Consumer_CustomerTree_MVCC_TEST(ProAndCon* itemQ_, const int load_flag_){
  std::string skt= "/home/client/mysql-base/data/mysql.sock"; 
  MYSQL mysql;     
  mysql_init(&mysql);     
  mysql_options(&mysql, MYSQL_SET_CHARSET_NAME, "utf8");     
  mysql_options(&mysql, MYSQL_INIT_COMMAND, "SET NAMES utf8");  
  mtx.lock();      
  if(!mysql_real_connect(&mysql, "127.0.0.1", "root", "","pbtest", 4000, NULL, 0)) {         
      std::cout<<"mysql connect error item init : "<<mysql_error(&mysql)<<" "<<mysql_errno(&mysql);     
  }  
   mtx.unlock(); 
  if(load_flag_ == 1){
    auto t1 = std::chrono::steady_clock::now();
    while(1){
      std::string* cmd = itemQ_->consumption();
      if(cmd->size()==0){
        ErrorDetect(); 
        delete cmd;
        continue;
      }
      if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        delete cmd;
        break;
      }

      //key word://1: put//2: get//3:scan//4.update
      if(KeyWordExist(cmd,1)){
        std::vector<std::string> res = split(*cmd, ",");
        
        if(res.size()!=23){
          ErrorDetect();
        }
        
        std::string str = "insert into customer(pk1, pk2, pk3, lsn, value1, value2, value3, value4, value5, value6,value7, value8, value9, value10, value11, value12, value13, value14,value15, value16, value17, value18) values(1";
        str+=res[4]+",";
        str+=res[3]+",";
        str+=res[2];
        str+=",\'lsn\',\'";
        str+=res[5];
        str+="\', \'";
        str+=res[6];
        str+="\', \'";
        str+=res[7];
        str+="\', \'";
        str+=res[8];
        str+="\', \'";
        str+=res[9];
        str+="\', \'";
        str+=res[10];
        str+="\', \'";
        str+=res[11];
        str+="\', \'";
        str+=res[12];
        str+="\', \'";
        str+=res[13];
        str+="\', \'";
        str+=res[14];
        str+="\', \'";
        str+=res[15];
        str+="\', \'";
        str+=res[16];
        str+="\', \'";
        str+=res[17];
        str+="\', \'";
        str+=res[18];
        str+="\', \'";
        str+=res[19].substr(1);
        str+="\', \'";
        str+=res[20].substr(1);
        str+="\', \'";
        str+=res[21];
        str+="\', \'";
        str+=res[22];
        str+="\');";   
        mysql_real_query(&mysql, str.c_str(), str.size());  
        int check_flag=0;
        if(check_flag == 1){
          std::string str = "select * from customer where pk1 = ";
          str+=res[4]+" and pk2 = ";
          str+=res[3]+" and pk3 = ";
          str+=res[2]+" ;";
          mysql_real_query(&mysql, str.c_str(), str.size());       
          MYSQL_RES *result = mysql_store_result(&mysql);   
          MYSQL_ROW row;    
          while (row = mysql_fetch_row(result)) {         
            std::cout<<"id: "<<row[0]<<" name: "<<row[1]<<" age: "<<row[2]<<std::endl;     
          } 
          mysql_free_result(result);
        }   
      }

    }
    auto t2 = std::chrono::steady_clock::now();
    
    static_tree::customer_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
    std::cout<< "load customer used: "<<static_tree::customer_put_time<<" ms\n";
    mysql_close(&mysql);
    return;
  }else{
    static_tree::customer_put_time=0;
    while(1){
      std::string* cmd = itemQ_->consumption();
      if(cmd->size()==0){
        ErrorDetect(); 
        delete cmd;
        continue;
      }
      if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        delete cmd;
        mysql_close(&mysql);
        break;
      }
      //key word:
      //1: put
      //2: get
      //3:scan
      //4.update
      if(KeyWordExist(cmd,1)){
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=23){
          ErrorDetect();
        }
        //put,orderline,[0-3000],[0,10],[0-w],[0-20],num5,num2, ts,num2,num8,strnum24
        std::string str = "insert into customer(pk1, pk2, pk3, lsn, value1, value2, value3, value4, value5, value6,value7, value8, value9, value10, value11, value12, value13, value14,value15, value16, value17, value18) values(";
        str+=res[4]+",";
        str+=res[3]+",";
        str+=res[2];
        str+=",\'lsn\',\'";
        str+=res[5];
        str+="\', \'";
        str+=res[6];
        str+="\', \'";
        str+=res[7];
        str+="\', \'";
        str+=res[8];
        str+="\', \'";
        str+=res[9];
        str+="\', \'";
        str+=res[10];
        str+="\', \'";
        str+=res[11];
        str+="\', \'";
        str+=res[12];
        str+="\', \'";
        str+=res[13];
        str+="\', \'";
        str+=res[14];
        str+="\', \'";
        str+=res[15];
        str+="\', \'";
        str+=res[16];
        str+="\', \'";
        str+=res[17];
        str+="\', \'";
        str+=res[18];
        str+="\', \'";
        str+=res[19];
        str+="\', \'";
        str+=res[20];
        str+="\', \'";
        str+=res[21];
        str+="\', \'";
        str+=res[22];
        str+="\');";   
          auto t1 = std::chrono::steady_clock::now();
          mysql_real_query(&mysql, str.c_str(), str.size());     
          auto t2 = std::chrono::steady_clock::now();
          static_tree::customer_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
          static_tree::customer_put_cnt++; 
        int check_flag=0;
        if(check_flag == 1){
          std::string str = "select * from customer where pk1 = ";
          str+=res[4]+" and pk2 = ";
          str+=res[3]+" and pk3 = ";
          
          str+=res[2]+";";
          mysql_real_query(&mysql, str.c_str(), str.size());       
          MYSQL_RES *result = mysql_store_result(&mysql);   
          MYSQL_ROW row;    
          while (row = mysql_fetch_row(result)) {         
            std::cout<<"id: "<<row[0]<<" name: "<<row[1]<<" age: "<<row[2]<<std::endl;     
          } 

        }   
      }else if(KeyWordExist(cmd,4)){
        std::vector<std::string> res = split(*cmd, ",");
        std::string str = "update customer set value17=\'";
        str+=res[5]+"\', value18=\'";
        str+=res[6]+"\' where pk1=";
        str+=res[4]+" and pk2=";
        str+=res[3]+" and pk3=";
        str+=res[2]+";";
        auto t1 = std::chrono::steady_clock::now();
        mysql_real_query(&mysql, str.c_str(), str.size());       
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::customer_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
        static_tree::customer_update_cnt++;
      }else if (KeyWordExist(cmd,3)){
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=9){
          ErrorDetect();
        }
        auto t1_i = std::chrono::steady_clock::now();
        std::string str = "select * from customer where pk1 >= ";
        str+=res[5];
        str+=" and pk1 <= ";
        str+=res[8]+" and pk2 >= ";
        str+=res[4]+" and pk2 <= ";
        str+=res[7]+" and pk3 >= ";
        str+=res[3]+" and pk3 <= ";
        str+=res[6]+";";
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);
         
        auto t2_i = std::chrono::steady_clock::now();     
        static_tree::customer_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count();
        static_tree::customer_scan_cnt_c++;
        //value[0] = value[1];
      }else if (KeyWordExist(cmd,5)){
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=9){
          ErrorDetect();
        }
        
        std::string str = "select * from customer where pk1 >= ";
        str+=res[5];
        str+=" and pk1 <= ";
        str+=res[8]+" and pk2 >= ";
        str+=res[4]+" and pk2 <= ";
        str+=res[7]+" and pk3 >= ";
        str+=res[3]+" and pk3 <= ";
        str+=res[6]+";";
        auto t1_i = std::chrono::steady_clock::now();
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);
         
          
        auto t2_i = std::chrono::steady_clock::now();     
        static_tree::customer_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count();
        static_tree::customer_scan_cnt_h++;
      }else if (KeyWordExist(cmd,2)){
        //scap,orderline,1,1,2,60,4,7,1,200,2
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=6){
          ErrorDetect();
        }
        auto t1_i = std::chrono::steady_clock::now();
        std::string str = "select * from customer where pk1 = ";
        str+=res[5]+" and pk2 = ";
        str+=res[4]+" and pk3 = ";
        str+=res[3]+";";
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);
        auto t2 = std::chrono::steady_clock::now();   
        auto t2_i = std::chrono::steady_clock::now();     
        static_tree::customer_get_time += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count();
        static_tree::customer_get_cnt++;
      }else{
        ErrorDetect();
      }
      delete cmd;
    }
    
  
  }
  return;

}


void Consumer_StockTree_MVCC_TEST(ProAndCon* itemQ_, const int load_flag_){
  std::string skt= "/home/client/mysql-base/data/mysql.sock"; 
  MYSQL mysql;     
  mysql_init(&mysql);     
  mysql_options(&mysql, MYSQL_SET_CHARSET_NAME, "utf8");     
  mysql_options(&mysql, MYSQL_INIT_COMMAND, "SET NAMES utf8");  
  mtx.lock();      
  if(!mysql_real_connect(&mysql, "127.0.0.1", "root", "","pbtest", 4000, NULL, 0)) {         
      std::cout<<"mysql connect error item init : "<<mysql_error(&mysql)<<" "<<mysql_errno(&mysql);     
  }  
mtx.unlock(); 
  if(load_flag_ == 1){
    auto t1 = std::chrono::steady_clock::now();
    while(1){
      std::string* cmd = itemQ_->consumption();
      if(cmd->size()==0){
        ErrorDetect(); 
        delete cmd;
        continue;
      }
      if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        delete cmd;
        break;
      }

      //key word://1: put//2: get//3:scan//4.update
      if(KeyWordExist(cmd,1)){
        std::vector<std::string> res = split(*cmd, ",");
        
        if(res.size()!=19){
          ErrorDetect();
        }
        //str = "create table pbtest.stock(pk1 int(16),pk2 int(16),lsn char(8), value1 char(16),value2 char(240),value3 char(8),value4 char(16),value5 char(8),value5 char(16),value6 char(50),PRIMARY KEY(pk1,pk2));";
        std::string str = "insert into stock(pk1, pk2, lsn, value1, value2, value3, value4, value5, value6) values(1";
        str+=res[3]+",";
        str+=res[2];
        str+=",\'lsn\',\'";
        str+=res[4];
        str+="\', \'";
        str+=res[5];
        str+=res[6];
        str+=res[7];
        str+=res[8];
        str+=res[9];
        str+=res[10];
        str+=res[11];
        str+=res[12];
        str+=res[13];
        str+=res[14];
        str+="\', \'";
        str+=res[15];
        str+="\', \'";
        str+=res[16];
        str+="\', \'";
        str+=res[17];
        str+="\', \'";
        str+=res[18];
        str+="\');";   
        mysql_real_query(&mysql, str.c_str(), str.size());  
        int check_flag=0;
        if(check_flag == 1){
          std::string str = "select * from stock where pk1 = ";
          str+=res[3]+" and pk2 = ";
          str+=res[2]+";";
          
          mysql_real_query(&mysql, str.c_str(), str.size());       
          MYSQL_RES *result = mysql_store_result(&mysql);   
          MYSQL_ROW row;    
          while (row = mysql_fetch_row(result)) {         
            std::cout<<"id: "<<row[0]<<" name: "<<row[1]<<" age: "<<row[2]<<std::endl;     
          } 
          mysql_free_result(result);
        }   
      }

    }
    auto t2 = std::chrono::steady_clock::now();
    
    static_tree::stock_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
    std::cout<< "load stock used: "<<static_tree::stock_put_time<<" ms \n";
    mysql_close(&mysql);
    return;
  }else{
    static_tree::stock_put_time=0;
    while(1){
      std::string* cmd = itemQ_->consumption();
      if(cmd->size()==0){
        ErrorDetect(); 
        delete cmd;
        continue;
      }
      if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        delete cmd;
        mysql_close(&mysql);
        break;
      }
      //key word:
      //1: put
      //2: get
      //3:scan
      //4.update
      if(KeyWordExist(cmd,1)){
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=19){
          ErrorDetect();
        }
        
        std::string str = "insert into stock(pk1, pk2, lsn, value1, value2, value3, value4, value5, value6) values(1";
        str+=res[3]+",";
        str+=res[2];
        str+=",\'lsn\',\'";
        str+=res[4];
        str+="\', \'";
        str+=res[5];
        str+=res[6];
        str+=res[7];
        str+=res[8];
        str+=res[9];
        str+=res[10];
        str+=res[11];
        str+=res[12];
        str+=res[13];
        str+=res[14];
        str+="\', \'";
        str+=res[15];
        str+="\', \'";
        str+=res[16];
        str+="\', \'";
        str+=res[17];
        str+="\', \'";
        str+=res[18];
        str+="\');";     
           
          auto t1 = std::chrono::steady_clock::now();
          mysql_real_query(&mysql, str.c_str(), str.size());     
          auto t2 = std::chrono::steady_clock::now();
          static_tree::stock_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
          static_tree::stock_put_cnt++;
        int check_flag=0;
        if(check_flag == 1){
          std::string str = "select * from stock where pk1 = ";
          str+=res[3]+" and pk2 = ";
          str+=res[2]+";";
          mysql_real_query(&mysql, str.c_str(), str.size());       
          MYSQL_RES *result = mysql_store_result(&mysql);   
          MYSQL_ROW row;    
          while (row = mysql_fetch_row(result)) {         
            std::cout<<"id: "<<row[0]<<" name: "<<row[1]<<" age: "<<row[2]<<std::endl;     
          } 

        }   
      }else if(KeyWordExist(cmd,4)){
        std::vector<std::string> res = split(*cmd, ",");
        std::string str = "update stock set value1=\'";
        str+=res[4]+"\' where pk1=";
        str+=res[3]+" and pk2=";
        str+=res[2]+";";
        auto t1 = std::chrono::steady_clock::now();
        mysql_real_query(&mysql, str.c_str(), str.size());       
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::stock_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
        static_tree::stock_update_cnt++;
      }else if (KeyWordExist(cmd,3)){
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=6){
          ErrorDetect();
        }
        auto t1_i = std::chrono::steady_clock::now();
        std::string str = "select * from stock where pk1 >= ";
        str+=res[3];
        str+=" and pk1 <= ";
        str+=res[5]+" and pk2 >= ";
        str+=res[2]+" and pk2 <= ";
        str+=res[4]+";";
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);
         
        auto t2_i = std::chrono::steady_clock::now();     
        static_tree::stock_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count();
        static_tree::stock_scan_cnt_c++;
        //value[0] = value[1];
      }else if (KeyWordExist(cmd,5)){
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=6){
          ErrorDetect();
        }
        std::string str = "select * from stock where pk1 >= ";
        str+=res[3];
        str+=" and pk1 <= ";
        str+=res[5]+" and pk2 >= ";
        str+=res[2]+" and pk2 <= ";
        str+=res[4]+";";
        auto t1_i = std::chrono::steady_clock::now();
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);
         
          
        auto t2_i = std::chrono::steady_clock::now();     
        static_tree::stock_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count();
        static_tree::stock_scan_cnt_h++;
      }else if (KeyWordExist(cmd,2)){
        //scap,orderline,1,1,2,60,4,7,1,200,2
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=4){
          ErrorDetect();
        }
        auto t1_i = std::chrono::steady_clock::now();
        std::string str = "select * from stock where pk1 = ";
        str+=res[3]+" and pk2 = ";
        str+=res[2]+";";
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);  
        auto t2_i = std::chrono::steady_clock::now();     
        static_tree::stock_get_time += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count();
        static_tree::stock_get_cnt++;
      }else{
        ErrorDetect();
      }
      delete cmd;
    }
    
  
  }
  return;

}



void Consumer_NewordersTree_MVCC_TEST(ProAndCon* itemQ_, const int load_flag_){
  std::string skt= "/home/client/mysql-base/data/mysql.sock"; 
  MYSQL mysql;     
  mysql_init(&mysql);     
  mysql_options(&mysql, MYSQL_SET_CHARSET_NAME, "utf8");     
  mysql_options(&mysql, MYSQL_INIT_COMMAND, "SET NAMES utf8");  
  mtx.lock();      
  if(!mysql_real_connect(&mysql, "127.0.0.1", "root", "","pbtest", 4000, NULL, 0)) {         
      std::cout<<"mysql connect error item init : "<<mysql_error(&mysql)<<" "<<mysql_errno(&mysql);     
  }  
mtx.unlock(); 
  if(load_flag_ == 1){
    auto t1 = std::chrono::steady_clock::now();
    while(1){
      std::string* cmd = itemQ_->consumption();
      if(cmd->size()==0){
        ErrorDetect(); 
        delete cmd;
        continue;
      }
      if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        delete cmd;
        break;
      }

      //key word://1: put//2: get//3:scan//4.update
      if(KeyWordExist(cmd,1)){
        std::vector<std::string> res = split(*cmd, ",");
        
        if(res.size()!=6){
          ErrorDetect();
        }
        //str = "create table pbtest.neworders(pk1 int(16),pk2 int(16),
        //                          pk3 int(16),lsn char(8), 
        //          value1 char(16),PRIMARY KEY(pk1,pk2,pk3));";
        std::string str = "insert into neworders(pk1, pk2, pk3, lsn, value1) values(1";
        str+=res[4]+",";
        str+=res[3]+",";
        str+=res[2];
        str+=",\'lsn\',\'";
        str+=res[5];
        str+="\');";   
        mysql_real_query(&mysql, str.c_str(), str.size());  
        int check_flag=0;
        if(check_flag == 1){
          std::string str = "select * from neworders where pk1 = ";
          str+=res[4]+" and pk2 = ";
          str+=res[3]+" and pk3 = ";
          str+=res[2]+";";
          
          mysql_real_query(&mysql, str.c_str(), str.size());       
          MYSQL_RES *result = mysql_store_result(&mysql);   
          MYSQL_ROW row;    
          while (row = mysql_fetch_row(result)) {         
            std::cout<<"id: "<<row[0]<<" name: "<<row[1]<<" age: "<<row[2]<<std::endl;     
          } 
          mysql_free_result(result);
        }   
      }

    }
    auto t2 = std::chrono::steady_clock::now();
    
    static_tree::neworders_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
    std::cout<< "load neworders used: "<<static_tree::neworders_put_time<<" ms\n";
    mysql_close(&mysql);
    return;
  }else{
    static_tree::neworders_put_time=0;
    while(1){
      std::string* cmd = itemQ_->consumption();
      if(cmd->size()==0){
        ErrorDetect(); 
        delete cmd;
        continue;
      }
      if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        delete cmd;
        mysql_close(&mysql);
        break;
      }
      //key word:
      //1: put
      //2: get
      //3:scan
      //4.update
      if(KeyWordExist(cmd,1)){
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=6){
          ErrorDetect();
        }
        
        std::string str = "insert into neworders(pk1, pk2, pk3, lsn, value1) values(1";
        str+=res[4]+",";
        str+=res[3]+",";
        str+=res[2];
        str+=",\'lsn\',\'";
        str+=res[5];
        str+="\');";     
          auto t1 = std::chrono::steady_clock::now();
          mysql_real_query(&mysql, str.c_str(), str.size());     
          auto t2 = std::chrono::steady_clock::now();
          static_tree::neworders_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
          static_tree::neworders_put_cnt++;
        int check_flag=0;
        if(check_flag == 1){
          std::string str = "select * from neworders where pk1 = ";
          str+=res[4]+" and pk2 = ";
          str+=res[3]+" and pk3 = ";
          str+=res[2]+";";
          mysql_real_query(&mysql, str.c_str(), str.size());       
          MYSQL_RES *result = mysql_store_result(&mysql);   
          MYSQL_ROW row;    
          while (row = mysql_fetch_row(result)) {         
            std::cout<<"id: "<<row[0]<<" name: "<<row[1]<<" age: "<<row[2]<<std::endl;     
          } 

        }   
      }else if (KeyWordExist(cmd,3)){
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=8){
          ErrorDetect();
        }
        auto t1_i = std::chrono::steady_clock::now();
        std::string str = "select * from neworders where pk1 >= ";
        str+=res[4];
        str+=" and pk1 <= ";
        str+=res[7]+" and pk2 >= ";
        str+=res[3]+" and pk2 <= ";
        str+=res[6]+" and pk3 >= ";
        str+=res[2]+" and pk3 <= ";
        str+=res[5]+";";
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);
         
        auto t2_i = std::chrono::steady_clock::now();     
        static_tree::neworders_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count();
        static_tree::neworders_scan_cnt_c++;
        //value[0] = value[1];
      }else if (KeyWordExist(cmd,5)){
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=8){
          ErrorDetect();
        }
        std::string str = "select * from orders where pk1 >= ";
        str+=res[4];
        str+=" and pk1 <= ";
        str+=res[7]+" and pk2 >= ";
        str+=res[3]+" and pk2 <= ";
        str+=res[6]+" and pk3 >= ";
        str+=res[2]+" and pk3 <= ";
        str+=res[5]+";";
        auto t1_i = std::chrono::steady_clock::now();
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);
         
          
        auto t2_i = std::chrono::steady_clock::now();     
        static_tree::neworders_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count();
        static_tree::neworders_scan_cnt_h++;
      }else{
        ErrorDetect();
      }
      delete cmd;
    }
    
  
  }
  return;

}


void Consumer_OrdersTree_MVCC_TEST(ProAndCon* itemQ_, const int load_flag_){
  std::string skt= "/home/client/mysql-base/data/mysql.sock"; 
  MYSQL mysql;     
  mysql_init(&mysql);     
  mysql_options(&mysql, MYSQL_SET_CHARSET_NAME, "utf8");     
  mysql_options(&mysql, MYSQL_INIT_COMMAND, "SET NAMES utf8");  
  mtx.lock();      
  if(!mysql_real_connect(&mysql, "127.0.0.1", "root", "","pbtest", 4000, NULL, 0)) {         
      std::cout<<"mysql connect error item init : "<<mysql_error(&mysql)<<" "<<mysql_errno(&mysql);     
  }  
   mtx.unlock(); 
  if(load_flag_ == 1){
    auto t1 = std::chrono::steady_clock::now();
    while(1){
      std::string* cmd = itemQ_->consumption();
      if(cmd->size()==0){
        ErrorDetect(); 
        delete cmd;
        continue;
      }
      if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        delete cmd;
        break;
      }

      //key word://1: put//2: get//3:scan//4.update
      if(KeyWordExist(cmd,1)){
        std::vector<std::string> res = split(*cmd, ",");
        
        if(res.size()!=10){
          ErrorDetect();
        }
        //str = "create table pbtest.orders(pk1 int(16),pk2 int(16),pk3 int(16),
        //lsn char(8), value1 char(16),value2 char(20),value3 char(16),
        //value4 char(16),value5 char(16),PRIMARY KEY(pk1,pk2,pk3));";/
        
        std::string str = "insert into orders(pk1, pk2, pk3, lsn, value1, value2, value3, value4, value5) values(1";
        str+=res[4]+",";
        str+=res[3]+",";
        str+=res[2];
        str+=",\'lsn\',\'";
        str+=res[5];
        str+="\', \'";
        str+=res[6];
        str+="\', \'";
        str+=res[7];
        str+="\', \'";
        str+=res[8];
        str+="\', \'";
        str+=res[9];
        str+="\');";   
        mysql_real_query(&mysql, str.c_str(), str.size());  
        int check_flag=0;
        if(check_flag == 1){
          std::string str = "select * from orders where pk1 = ";
          str+=res[4]+" and pk2 = ";
          str+=res[3]+" and pk3 = ";
          str+=res[2]+";";
          
          mysql_real_query(&mysql, str.c_str(), str.size());       
          MYSQL_RES *result = mysql_store_result(&mysql);   
          MYSQL_ROW row;    
          while (row = mysql_fetch_row(result)) {         
            std::cout<<"id: "<<row[0]<<" name: "<<row[1]<<" age: "<<row[2]<<std::endl;     
          } 
          mysql_free_result(result);
        }   
      }

    }
    auto t2 = std::chrono::steady_clock::now();
    
    static_tree::orders_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
    std::cout<< "load orders used: "<<static_tree::orders_put_time<<" ms\n";
    mysql_close(&mysql);
    return;
  }else{
    static_tree::orders_put_time=0;
    while(1){
      std::string* cmd = itemQ_->consumption();
      if(cmd->size()==0){
        ErrorDetect(); 
        delete cmd;
        continue;
      }
      if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        delete cmd;
        mysql_close(&mysql);
        break;
      }
      //key word:
      //1: put
      //2: get
      //3:scan
      //4.update
      if(KeyWordExist(cmd,1)){
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=10){
          ErrorDetect();
        }
        
        std::string str = "insert into orders(pk1, pk2, pk3, lsn, value1, value2, value3, value4, value5) values(1";
        str+=res[4]+",";
        str+=res[3]+",";
        str+=res[2];
        str+=",\'lsn\',\'";
        str+=res[5];
        str+="\', \'";
        str+=res[6];
        str+="\', \'";
        str+=res[7];
        str+="\', \'";
        str+=res[8];
        str+="\', \'";
        str+=res[9];
        str+="\');";     
          auto t1 = std::chrono::steady_clock::now();
          mysql_real_query(&mysql, str.c_str(), str.size());     
          auto t2 = std::chrono::steady_clock::now();
          static_tree::orders_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
          static_tree::orders_put_cnt++; 
        int check_flag=0;
        if(check_flag == 1){
          std::string str = "select * from orders where pk1 = ";
          str+=res[4]+" and pk2 = ";
          str+=res[3]+" and pk3 = ";
          str+=res[2]+";";
          mysql_real_query(&mysql, str.c_str(), str.size());       
          MYSQL_RES *result = mysql_store_result(&mysql);   
          MYSQL_ROW row;    
          while (row = mysql_fetch_row(result)) {         
            std::cout<<"id: "<<row[0]<<" name: "<<row[1]<<" age: "<<row[2]<<std::endl;     
          } 

        }   
      }else if(KeyWordExist(cmd,4)){
        std::vector<std::string> res = split(*cmd, ",");
        std::string str = "update orders set value1=\'";
        str+=res[5]+"\' where pk1=";
        str+=res[4]+" and pk2=";
        str+=res[3]+" and pk3=";
        str+=res[2]+";";
        auto t1 = std::chrono::steady_clock::now();
        mysql_real_query(&mysql, str.c_str(), str.size());       
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::orders_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
        static_tree::orders_update_cnt++;
      }else if (KeyWordExist(cmd,3)){
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=8){
          ErrorDetect();
        }
        auto t1_i = std::chrono::steady_clock::now();
        std::string str = "select * from orders where pk1 >= ";
        str+=res[4];
        str+=" and pk1 <= ";
        str+=res[7]+" and pk2 >= ";
        str+=res[3]+" and pk2 <= ";
        str+=res[6]+" and pk3 >= ";
        str+=res[2]+" and pk3 <= ";
        str+=res[5]+";";
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);
         
        auto t2_i = std::chrono::steady_clock::now();     
        static_tree::orders_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count();
        static_tree::orders_scan_cnt_c++;
        //value[0] = value[1];
      }else if (KeyWordExist(cmd,5)){
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=8){
          ErrorDetect();
        }
        std::string str = "select * from orders where pk1 >= ";
        str+=res[4];
        str+=" and pk1 <= ";
        str+=res[7]+" and pk2 >= ";
        str+=res[3]+" and pk2 <= ";
        str+=res[6]+" and pk3 >= ";
        str+=res[2]+" and pk3 <= ";
        str+=res[5]+";";
        auto t1_i = std::chrono::steady_clock::now();
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);
         
          
        auto t2_i = std::chrono::steady_clock::now();     
        static_tree::orders_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count();
        static_tree::orders_scan_cnt_h++;
      }else if (KeyWordExist(cmd,2)){
        //scap,orderline,1,1,2,60,4,7,1,200,2
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=5){
          ErrorDetect();
        }
        auto t1_i = std::chrono::steady_clock::now();
        std::string str = "select * from orders where pk1 = ";
        str+=res[4]+" and pk2 = ";
        str+=res[3]+" and pk3 = ";
        str+=res[2]+";";
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row = mysql_fetch_row(result);
        mysql_free_result(result);  
        auto t2_i = std::chrono::steady_clock::now();     
        static_tree::orders_get_time += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count();
        static_tree::orders_get_cnt++;
      }else{
        ErrorDetect();
      }
      delete cmd;
    }
    
  
  }
  return;

}






//str = "create table pbtest.history1(pk1 int(16),pk2 int(16),pk3 int(16),lsn char(8), value1 char(16),value2 char(16),value3 char(20),value4 char(8),value5 char(24),PRIMARY KEY(pk1,pk2,pk3));";
//str = "create table pbtest.history2(pk1 int(16),pk2 int(16),lsn char(8), value1 char(16),value2 char(16),value3 char(16),value4 char(20),value5 char(8),value6 char(24),PRIMARY KEY(pk1,pk2));";




void dispatcher_TEST(int load_flag_){
    
    
  tpcch_kv_generator::tpcc_gen tpcc_gen_(100);
  
  tpcch_kv_generator::tpch_gen tpch_gen_(100,100000);


  ProAndCon* itemQ = new ProAndCon(50);
  std::thread CItemTree = std::thread(Consumer_ItemTree_MVCC_TEST,itemQ, load_flag_);
  CItemTree.detach();

  ProAndCon* orderlineQ = new ProAndCon(50);
  std::thread COrderlineTree = std::thread(Consumer_OrderlineTree_MVCC_TEST,orderlineQ, load_flag_);
  COrderlineTree.detach();  
  
  
  ProAndCon* stockQ = new ProAndCon(50);
  std::thread CStockTree = std::thread(Consumer_StockTree_MVCC_TEST,stockQ, load_flag_);
  CStockTree.detach();
  
 
 
  
  ProAndCon* ordersQ = new ProAndCon(50);
  std::thread COrdersTree = std::thread(Consumer_OrdersTree_MVCC_TEST,ordersQ, load_flag_);
  COrdersTree.detach();
  
  //ProAndCon* historyQ = new ProAndCon(50);
  //std::thread CHistoryTree = std::thread(Consumer_HistoryTree,historyQ, load_flag_);
  //CHistoryTree.detach();
  


  ProAndCon* newordersQ = new ProAndCon(50);
  std::thread CNewordersTree = std::thread(Consumer_NewordersTree_MVCC_TEST,newordersQ, load_flag_);
  CNewordersTree.detach();

  ProAndCon* customerQ = new ProAndCon(50);
  std::thread CCustomerTree = std::thread(Consumer_CustomerTree_MVCC_TEST,customerQ, load_flag_);
  CCustomerTree.detach();
 
std::string* cmdLine = nullptr;
if(load_flag_==1){
  auto t1 = std::chrono::steady_clock::now();
  auto t2 = std::chrono::steady_clock::now(); 
  auto tmp = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
  
  
  cmdLine = tpcc_gen_.generateLoad_Item();
  while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  {
    itemQ->product(cmdLine);
    cmdLine = tpcc_gen_.generateLoad_Item();
 
  }
  itemQ->product(cmdLine);
  t2 = std::chrono::steady_clock::now(); 
  tmp = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
  std::cout<<"finish load item, exe time: "<<tmp<<" ms\n";

  t1 = std::chrono::steady_clock::now(); 
  cmdLine = tpcc_gen_.generateLoad_Orderline();
  while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  {
    orderlineQ->product(cmdLine);
    cmdLine = tpcc_gen_.generateLoad_Orderline();
 
  }
  orderlineQ->product(cmdLine);
  t2 = std::chrono::steady_clock::now(); 
  tmp = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
  std::cout<<"finish load orderline, exe time: "<<tmp<<" ms\n";
  
  
  t1 = std::chrono::steady_clock::now(); 
  
  cmdLine = tpcc_gen_.generateLoad_Customer();
  while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  {
    customerQ->product(cmdLine); 
    cmdLine = tpcc_gen_.generateLoad_Customer();
 
  }
  //cmd_file.close();
  customerQ->product(cmdLine); 
  t2 = std::chrono::steady_clock::now(); 
  tmp = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
  std::cout<<"finish load customer, exe time: "<<tmp<<" ms\n";
  
  
  t1 = std::chrono::steady_clock::now(); 
  cmdLine = tpcc_gen_.generateLoad_Orders();
  while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  {
    ordersQ->product(cmdLine);
    cmdLine = tpcc_gen_.generateLoad_Orders();
 
  }
  ordersQ->product(cmdLine);
  t2 = std::chrono::steady_clock::now(); 
  tmp = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
  std::cout<<"finish load orders, exe time: "<<tmp<<" ms\n";  
  
  //t1 = std::chrono::steady_clock::now(); 
  //cmdLine = tpcc_gen_.generateLoad_History();
  //while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  //{
  //  historyQ->product(cmdLine);
  //  cmdLine = tpcc_gen_.generateLoad_History();
 
  //}
  //historyQ->product(cmdLine);
  //t2 = std::chrono::steady_clock::now(); 
  //tmp = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
  //std::cout<<"finish load history, exe time: "<<tmp<<" us\n";  

  t1 = std::chrono::steady_clock::now(); 
  cmdLine = tpcc_gen_.generateLoad_NewOrders();
  while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  {
    newordersQ->product(cmdLine);
    cmdLine = tpcc_gen_.generateLoad_NewOrders();
 
  }
  newordersQ->product(cmdLine);
  t2 = std::chrono::steady_clock::now(); 
  tmp = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
  std::cout<<"finish load neworders, exe time: "<<tmp<<" ms\n";



  t1 = std::chrono::steady_clock::now(); 
  cmdLine = tpcc_gen_.generateLoad_Stock();
  while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  {
    stockQ->product(cmdLine);
    cmdLine = tpcc_gen_.generateLoad_Stock();
 
  }
  stockQ->product(cmdLine);
  t2 = std::chrono::steady_clock::now(); 
  tmp = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
  std::cout<<"finish load stock, exe time: "<<tmp<<" ms\n";




  
    return;
  }


  // feed a huge ap query
  int tp_cnt=0;
  auto t3 = std::chrono::steady_clock::now();
  cmdLine = tpcc_gen_.gentpcc_cmd_TEST();;
  while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  //while(getline(cmd_file_test,cmdLine))
  {
    
    if(cmdLine->find(",item,")!=cmdLine->npos){
      itemQ->product(cmdLine);
      tp_cnt+=1;
    }else if(cmdLine->find(",orderline,")!=cmdLine->npos){
      orderlineQ->product(cmdLine);
      tp_cnt+=1;
    }else if(cmdLine->find(",stock,")!=cmdLine->npos){
      //char* cmd = (char*)malloc((cmdLine.length())*sizeof(char));
      //strcpy(cmd,cmdLine.c_str());
      stockQ->product(cmdLine);
      tp_cnt+=1;
    }else if(cmdLine->find(",customer,")!=cmdLine->npos){
      //char* cmd = (char*)malloc((cmdLine.length())*sizeof(char));
      //strcpy(cmd,cmdLine.c_str());
      customerQ->product(cmdLine);
      tp_cnt+=1;
    }else if(cmdLine->find(",neworders,")!=cmdLine->npos){
      newordersQ->product(cmdLine);
      tp_cnt+=1;
    }else if(cmdLine->find(",orders,")!=cmdLine->npos){
      ordersQ->product(cmdLine);
      tp_cnt+=1;
    }
    //else if(cmdLine->find(",history,")!=cmdLine->npos){
    //  historyQ->product(cmdLine);
    //  tp_cnt+=1;
     // }
    else{
      if(cmdLine->find(",history,")==cmdLine->npos){
        ErrorDetect();
      }
      
    }
    
    if(tp_cnt == 10000){
      tp_cnt=0;
      cmdLine = tpch_gen_.gentpch_cmd();
      //report performance
      std::cout<<"orderline time/cnt:[put,get,update,scan_c,scan_h]--> "
               <<static_tree::orderline_put_time<<"/"<<static_tree::orderline_put_cnt<<","
               <<static_tree::orderline_get_time<<"/"<<static_tree::orderline_get_cnt<<","
               <<static_tree::orderline_update_time<<"/"<<static_tree::orderline_update_cnt<<","
               <<static_tree::orderline_scan_time_c<<"/"<<static_tree::orderline_scan_cnt_c<<","
               <<static_tree::orderline_scan_time_h<<"/"<<static_tree::orderline_scan_cnt_h<<"\n";
      std::cout<<"stock time/cnt:[put,get,update,scan_c,scan_h]--> "
               <<static_tree::stock_put_time<<"/"<<static_tree::stock_put_cnt<<","
               <<static_tree::stock_get_time<<"/"<<static_tree::stock_get_cnt<<","
               <<static_tree::stock_update_time<<"/"<<static_tree::stock_update_cnt<<","
               <<static_tree::stock_scan_time_c<<"/"<<static_tree::stock_scan_cnt_c<<","
               <<static_tree::stock_scan_time_h<<"/"<<static_tree::stock_scan_cnt_h<<"\n";
      std::cout<<"customer time/cnt:[put,get,update,scan_c,scan_h]--> "
               <<static_tree::customer_put_time<<"/"<<static_tree::customer_put_cnt<<","
               <<static_tree::customer_get_time<<"/"<<static_tree::customer_get_cnt<<","
               <<static_tree::customer_update_time<<"/"<<static_tree::customer_update_cnt<<","
               <<static_tree::customer_scan_time_c<<"/"<<static_tree::customer_scan_cnt_c<<","
               <<static_tree::customer_scan_time_h<<"/"<<static_tree::customer_scan_cnt_h<<"\n";
      std::cout<<"orders time/cnt:[put,get,update,scan_c,scan_h]--> "
               <<static_tree::orders_put_time<<"/"<<static_tree::orders_put_cnt<<","
               <<static_tree::orders_get_time<<"/"<<static_tree::orders_get_cnt<<","
               <<static_tree::orders_update_time<<"/"<<static_tree::orders_update_cnt<<","
               <<static_tree::orders_scan_time_c<<"/"<<static_tree::orders_scan_cnt_c<<","
               <<static_tree::orders_scan_time_h<<"/"<<static_tree::orders_scan_cnt_h<<"\n";
      std::cout<<"neworders time/cnt:[put,get,update,scan_c,scan_h]--> "
               <<static_tree::neworders_put_time<<"/"<<static_tree::neworders_put_cnt<<","
               <<static_tree::neworders_get_time<<"/"<<static_tree::neworders_get_cnt<<","
               <<static_tree::neworders_update_time<<"/"<<static_tree::neworders_update_cnt<<","
               <<static_tree::neworders_scan_time_c<<"/"<<static_tree::neworders_scan_cnt_c<<","
               <<static_tree::neworders_scan_time_h<<"/"<<static_tree::neworders_scan_cnt_h<<"\n";      
      std::cout<<"item time/cnt:[put,get,update,scan_c,scan_h]--> "
               <<static_tree::item_put_time<<"/"<<static_tree::item_put_cnt<<","
               <<static_tree::item_get_time<<"/"<<static_tree::item_get_cnt<<","
               <<static_tree::item_update_time<<"/"<<static_tree::item_update_cnt<<","
               <<static_tree::item_scan_time_c<<"/"<<static_tree::item_scan_cnt_c<<","
               <<static_tree::item_scan_time_h<<"/"<<static_tree::item_scan_cnt_h<<"\n\n";
      
    }else{
      cmdLine = tpcc_gen_.gentpcc_cmd_TEST();
    }
  }
  return;










}


/*
void dispatcher_MVCC_TEST(){
    
    
  tpcch_kv_generator::tpcc_gen tpcc_gen_(10);
  
  tpcch_kv_generator::tpch_gen tpch_gen_(10,100000);

  int plog_flag=1;
  GlobalBuffer* gb_ = new GlobalBuffer(plog_flag);
  std::thread thd_gb = std::thread(GlobalBufferRunner,gb_);

  thd_gb.detach();

  ProAndCon* itemQ = new ProAndCon(50);
  std::thread CItemTree = std::thread(Consumer_ItemTree_MVCC_TEST,itemQ,gb_,plog_flag);
  CItemTree.detach();
  
  ProAndCon* stockQ = new ProAndCon(50);
  std::thread CStockTree = std::thread(Consumer_StockTree_MVCC_TEST,stockQ,gb_,plog_flag);
  CStockTree.detach();
  
  
  ProAndCon* ordersQ = new ProAndCon(50);
  std::thread COrdersTree = std::thread(Consumer_OrdersTree_MVCC_TEST,ordersQ,gb_,plog_flag);
  COrdersTree.detach();
  
  ProAndCon* historyQ = new ProAndCon(50);
  std::thread CHistoryTree = std::thread(Consumer_HistoryTree,historyQ,gb_,plog_flag);
  CHistoryTree.detach();
  
  ProAndCon* orderlineQ = new ProAndCon(50);
  std::thread COrderlineTree = std::thread(Consumer_OrderlineTree_MVCC_TEST,orderlineQ,gb_,plog_flag);
  COrderlineTree.detach();

  ProAndCon* newordersQ = new ProAndCon(50);
  std::thread CNewordersTree = std::thread(Consumer_NewordersTree_MVCC_TEST,newordersQ,gb_,plog_flag);
  CNewordersTree.detach();

  ProAndCon* customerQ = new ProAndCon(50);
  std::thread CCustomerTree = std::thread(Consumer_CustomerTree_MVCC_TEST,customerQ,gb_,plog_flag);
  CCustomerTree.detach();


  auto t1 = std::chrono::steady_clock::now();
  std::string* cmdLine = nullptr;

  cmdLine = tpcc_gen_.generateLoad_Customer();
  while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  {
    customerQ->product(cmdLine); 
    cmdLine = tpcc_gen_.generateLoad_Customer();
 
  }
  //cmd_file.close();
  customerQ->product(cmdLine); 
  auto t2 = std::chrono::steady_clock::now(); 
  auto tmp = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
  std::cout<<"finish load customer, exe time: "<<tmp<<" us\n";

  t1 = std::chrono::steady_clock::now(); 
  cmdLine = tpcc_gen_.generateLoad_Orders();
  while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  {
    ordersQ->product(cmdLine);
    cmdLine = tpcc_gen_.generateLoad_Orders();
 
  }
  ordersQ->product(cmdLine);
  t2 = std::chrono::steady_clock::now(); 
  tmp = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
  std::cout<<"finish load orders, exe time: "<<tmp<<" us\n";  

  t1 = std::chrono::steady_clock::now(); 
  cmdLine = tpcc_gen_.generateLoad_History();
  while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  {
    historyQ->product(cmdLine);
    cmdLine = tpcc_gen_.generateLoad_History();
 
  }
  historyQ->product(cmdLine);
  t2 = std::chrono::steady_clock::now(); 
  tmp = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
  std::cout<<"finish load history, exe time: "<<tmp<<" us\n";  

  t1 = std::chrono::steady_clock::now(); 
  cmdLine = tpcc_gen_.generateLoad_NewOrders();
  while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  {
    newordersQ->product(cmdLine);
    cmdLine = tpcc_gen_.generateLoad_NewOrders();
 
  }
  newordersQ->product(cmdLine);
  t2 = std::chrono::steady_clock::now(); 
  tmp = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
  std::cout<<"finish load neworders, exe time: "<<tmp<<" us\n";

  t1 = std::chrono::steady_clock::now(); 
  cmdLine = tpcc_gen_.generateLoad_Item();
  while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  {
    itemQ->product(cmdLine);
    cmdLine = tpcc_gen_.generateLoad_Item();
 
  }
  itemQ->product(cmdLine);
  t2 = std::chrono::steady_clock::now(); 
  tmp = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
  std::cout<<"finish load item, exe time: "<<tmp<<" us\n";

  t1 = std::chrono::steady_clock::now(); 
  cmdLine = tpcc_gen_.generateLoad_Stock();
  while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  {
    stockQ->product(cmdLine);
    cmdLine = tpcc_gen_.generateLoad_Stock();
 
  }
  stockQ->product(cmdLine);
  t2 = std::chrono::steady_clock::now(); 
  tmp = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
  std::cout<<"finish load stock, exe time: "<<tmp<<" us\n";

  t1 = std::chrono::steady_clock::now(); 
  cmdLine = tpcc_gen_.generateLoad_Orderline();
  while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  {
    orderlineQ->product(cmdLine);
    cmdLine = tpcc_gen_.generateLoad_Orderline();
 
  }
  orderlineQ->product(cmdLine);
  t2 = std::chrono::steady_clock::now(); 
  tmp = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
  std::cout<<"finish load orderline, exe time: "<<tmp<<" us\n";








  // feed a huge ap query
  int tp_cnt=0;
  auto t3 = std::chrono::steady_clock::now();
  cmdLine = tpch_gen_.gentpch_cmd_Q1_1();
  while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  //while(getline(cmd_file_test,cmdLine))
  {
    if(cmdLine->find(",orderline,")!=cmdLine->npos){
      orderlineQ->product(cmdLine);
      tp_cnt+=1;
    }else if(cmdLine->find(",item,")!=cmdLine->npos){
      //char* cmd = (char*)malloc((cmdLine.length())*sizeof(char));
      //strcpy(cmd,cmdLine.c_str());
      itemQ->product(cmdLine);
      tp_cnt+=1;
    }else if(cmdLine->find(",stock,")!=cmdLine->npos){
      //char* cmd = (char*)malloc((cmdLine.length())*sizeof(char));
      //strcpy(cmd,cmdLine.c_str());
      stockQ->product(cmdLine);
      tp_cnt+=1;
    }else if(cmdLine->find(",customer,")!=cmdLine->npos){
      //char* cmd = (char*)malloc((cmdLine.length())*sizeof(char));
      //strcpy(cmd,cmdLine.c_str());
      customerQ->product(cmdLine);
      tp_cnt+=1;
    }else if(cmdLine->find(",neworders,")!=cmdLine->npos){
      newordersQ->product(cmdLine);
      tp_cnt+=1;
    }else if(cmdLine->find(",orders,")!=cmdLine->npos){
      ordersQ->product(cmdLine);
      tp_cnt+=1;
    }else if(cmdLine->find(",history,")!=cmdLine->npos){
      historyQ->product(cmdLine);
      tp_cnt+=1;
    }else{
      ErrorDetect();
    }
    cmdLine = tpcc_gen_.gentpcc_cmd_TEST();
    if(tp_cnt == 200000){
      auto t4 = std::chrono::steady_clock::now();
      auto tmp1 = std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count()/1000;
      off_t ap_time = OrderLineTree->GetAPTime();
      
      off_t ap_off = OrderLineTree->GetAPOff();

      std::cout<<"finish test: "<<tp_cnt<<" tp requests, exe time: "<<tmp1<<" ms;"
              <<" finish ap query 1 with "<< ap_time << "ms and "<<ap_off<<" extra space usage\n";
      t3 = t4;
      cmdLine = tpch_gen_.gentpch_cmd_Q2_2(1); //item
      itemQ->product(cmdLine);
      cmdLine = tpch_gen_.gentpch_cmd_Q2_2(2); //neworder
      newordersQ->product(cmdLine);
    }
  }
  return;










}
*/


// main
int main(int argc, char const *argv[])
{
  (void)argc;
  (void)argv;

  int load = 0;
  //dispatcher();
  if(load==1){
    //1. init db instance
    MYSQL mysql;     
    mysql_init(&mysql);
    std::string skt= "/home/client/mysql-base/data/mysql.sock"; 
    if(!mysql_real_connect(&mysql, "127.0.0.1", "root", "","mysql", 4000, NULL, 0)) {         
        std::cout<<"mysql connect error1: "<<mysql_error(&mysql)<<" "<<mysql_errno(&mysql)<<std::endl;     
    } 
    std::string str = "create database pbtest;";       
    mysql_real_query(&mysql, str.c_str(),  str.size()); 
    str = "alter database pbtest charset=utf8;";      
    mysql_real_query(&mysql, str.c_str(), str.size());
    //2. create tables
    str = "create table pbtest.item(pk int(16) primary key, lsn char(8), value1 char(16),value2 char(24),value3 char(8),value4 char(50))";
    mysql_real_query(&mysql, str.c_str(), str.size());
    str = "create table pbtest.orderline(pk1 int(16),pk2 int(16),pk3 int(16),pk4 int(16), lsn char(8), value1 char(16),value2 char(16),value3 char(20),value4 char(16),value5 char(8),value6 char(24),PRIMARY KEY(pk1,pk2,pk3,pk4))";
    mysql_real_query(&mysql, str.c_str(), str.size()); 
    str = "create table pbtest.customer(pk1 int(16),pk2 int(16),pk3 int(16),lsn char(8), value1 char(16),value2 char(2),value3 char(16),value4 char(20),value5 char(20),value6 char(20),value7 char(2),value8 char(9),value9 char(16),value10 char(20),value11 char(2),value12 char(16),value13 char(60),value14 char(14),value15 char(14),value16 char(16),value17 char(16),value18 char(100),PRIMARY KEY(pk1,pk2,pk3));";
    mysql_real_query(&mysql, str.c_str(), str.size()); 
    str = "create table pbtest.history1(pk1 int(16),pk2 int(16),pk3 int(16),lsn char(8), value1 char(16),value2 char(16),value3 char(20),value4 char(8),value5 char(24),PRIMARY KEY(pk1,pk2,pk3));";
    mysql_real_query(&mysql, str.c_str(), str.size()); 
    str = "create table pbtest.history2(pk1 int(16),pk2 int(16),lsn char(8), value1 char(16),value2 char(16),value3 char(16),value4 char(20),value5 char(8),value6 char(24),PRIMARY KEY(pk1,pk2));";
    mysql_real_query(&mysql, str.c_str(), str.size());  
    str = "create table pbtest.neworders(pk1 int(16),pk2 int(16),pk3 int(16),lsn char(8), value1 char(16),PRIMARY KEY(pk1,pk2,pk3));";
    mysql_real_query(&mysql, str.c_str(), str.size()); 
    str = "create table pbtest.orders(pk1 int(16),pk2 int(16),pk3 int(16),lsn char(8), value1 char(16),value2 char(20),value3 char(16),value4 char(16),value5 char(16),PRIMARY KEY(pk1,pk2,pk3));";
    mysql_real_query(&mysql, str.c_str(), str.size()); 
    str = "create table pbtest.stock(pk1 int(16),pk2 int(16),lsn char(8), value1 char(16),value2 char(240),value3 char(8),value4 char(16),value5 char(16),value6 char(50),PRIMARY KEY(pk1,pk2));";
    mysql_real_query(&mysql, str.c_str(), str.size()); 
    
    /*


    */
    mysql_close(&mysql);
    std::cout<<"start load\n";
    dispatcher_TEST(load);
  }else{
    std::cout<<"start test\n";
    dispatcher_TEST(load);
  }
  return 1;
}