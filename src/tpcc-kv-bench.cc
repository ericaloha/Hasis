#include <chrono>
#include <iostream>
#include <fstream>
#include <random>
#include <string.h>
// for async IO
#include <algorithm>
#include <thread>
// for ycsb
#include <limits.h>
#include <math.h>
#include <unistd.h>
#include "bplus_tree.h"

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
  int x=2;
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
        std::string cmd = "scan,customer,c_id,";
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
        std::string cmd = "scan,item,";
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
        std::string cmd = "scan,stock,";
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
        std::string cmd = "scan,orders,";
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
        std::string cmd = "scan,neworders,";
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
        std::string cmd = "scan,orderline,1,1,1,";
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
BPlusItemTree *ItemTree;                              // 1
BPlusStockTree *StockTree;                            // 2
//BPlusDistrictTree *DistrictTree;                              // 3
BPlusHistoryfk1Tree *Historyfk1Tree;                         // 4
BPlusHistoryfk2Tree *Historyfk2Tree;                         // 5
BPlusCustomerTree *CustomerTree;                              // 6
BPlusNewOrdersTree *NewOrdersTree;                              // 7
BPlusOrdersTree *OrdersTree;                                // 8
BPlusOrderlineTree *OrderLineTree;                             // 9

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


void Consumer_StockTree(ProAndCon* itemQ_, GlobalBuffer* gb_, const int plog_flag_){
  //INSERT INTO stock values(
  //          2567,1,76,'u43OyncoVY9HLUbSyOnziusa','BAvu1Lo5MB37npdLxuePU6Nq',
  //          'LZk
  //          'cIc9jvqjZaDBmbqnlPxDbT4i','awyzkHZ1riklYRI6eTsDHeye','AE5EwnoqjlZ0Lap3IaajsgLJ','YY9UDieRlsLYeAFOhFjH5kXd',
  //          'EWzZpU7OdbZLEy74HdhRt586',0,0,0,'N9Ysnt6Jqf6KkSxK1GWzLaHxHg')
  //std::cout << "+++++++++++++++ TEST Stock Tree BEGIN ++++++++++++++++++++\n";
  std::string StockDir = "/media/hkc/csd-7.7T/htap/test";
  std::string DB_path = StockDir + "Stock/";
  std::string db = DB_path + "testStock.db";
  std::string log = DB_path + "testStock.log";
  std::string chpt = DB_path + "chptStock.log";
  std::string sha_plog = DB_path + "shaPlogStock.log";
  StockTree = new BPlusStockTree(db.c_str(), log.c_str(), chpt.c_str(), bg_id++, true,sha_plog.c_str());
  // load
  StockTree->SetBaseline();
  StockTree->SetLoadFlag(1);
  char k[33] = {'\0'};
  char v[16 + 240 + 8 + 16 + 16 + 50+1] = {'\0'};
  char updated_value[16 + 240 + 8 + 16 + 16 + 50+1] = {'\0'};
  int load_flag =1;
  int plog_flag = plog_flag_;  
  int gb_flag =0;
  int map_id = 1;
    while(1){
    
      std::string* cmd = itemQ_->consumption();
      //std::string cmd(cmd_char, cmd_char + strlen(cmd_char));
      if(cmd->size()==0){
        delete cmd;
        ErrorDetect(); static_tree::stock_get_not_found++;
        continue;
      }
      if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        if(load_flag==1){
          load_flag=0;
          gb_flag =1;
          StockTree->WarmUpVirtualPageCache(1);
        }
        if(plog_flag==1){
          StockTree->SetLoadFlag(0);
          StockTree->SetPlog();
          plog_flag =2;
        }
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
        int pk1 = atoi(res[3].c_str()); //cwid
        int pk2 = atoi(res[2].c_str()); //item id
        snprintf(k, 33, "%16d%16d", pk1,pk2);
        off_t lsn = StockTree->GetLSN();
        snprintf(v, 16 + 240 + 8 + 16 + 16 + 50+1, 
                    "%16s%24s%24s%24s%24s%24s%24s%24s%24s%24s%24s%8s%16s%16s%50s", 
            res[4].c_str(), 
            res[5].c_str(),
            res[6].c_str(),
            res[7].c_str(),
            res[8].c_str(),
            res[9].c_str(),
            res[10].c_str(),
            res[11].c_str(),
            res[12].c_str(),
            res[13].c_str(),
            res[14].c_str(),
            res[15].c_str(),
            res[16].c_str(),
            res[17].c_str(),
            res[18].c_str()
            );
        auto t1 = std::chrono::steady_clock::now();
        if(gb_flag == 0 || plog_flag == 2){//blind update
          StockTree->Put(k, v, lsn);
        }else{
          off_t leaf_of = StockTree->GetOnlyLeafOffset(k);
          //map id: stock 01
          //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
          char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
          if(leaf_buf!=nullptr){
            //can be found in gb
            //char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
            StockTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
            //update LRU order
            gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }else{
            //register the page to gb
            char* leaf_buf = StockTree->GetLeafCopy(k);
            StockTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
            //this page is newly inserted into gb
            gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }


        }
        auto t2 = std::chrono::steady_clock::now(); 
        if(load_flag ==0){
          static_tree::stock_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
          static_tree::stock_put_cnt++;
        }
        StockTree->IncreLSN();
      }else if (KeyWordExist(cmd,4)){
        //UPDATE stock SET s_quantity = ? WHERE s_i_id = ? AND s_w_id = ?
        //update,stock, item_id, w_id, ...
        if(plog_flag==0){
            //baseline, read modify write
            //get,c_id,2957,5,1
            std::vector<std::string> res = split(*cmd, ",");
            if(res.size()!=5){
              ErrorDetect();
            }
            int pk1 = atoi(res[3].c_str()); //cw_id
            int pk2 = atoi(res[2].c_str()); //item id
            snprintf(k, 33, "%16d%16d", pk1,pk2);

            auto t1 = std::chrono::steady_clock::now();
            std::string value;
            if(gb_flag == 0){
              StockTree->Get(k,value);
              memcpy(&updated_value[0],value.c_str(),value.size());
              char quality[17]={'\0'};
              snprintf(quality,17,"%16s",res[4]);
              memcpy(&updated_value[0],&quality[0],16);

              off_t lsn = StockTree->GetLSN();
              std::string tmp_value(&updated_value[0],&updated_value[0]+346);
              StockTree->Put(k, tmp_value, lsn);
              StockTree->IncreLSN();
            }else{
              off_t leaf_of = StockTree->GetOnlyLeafOffset(k);
              //map id: stock 01
              //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
              char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
              if(leaf_buf!=nullptr){
                //can be found in gb
               
                StockTree->GetRowRecord(k,value,leaf_buf);
                memcpy(&updated_value[0],value.c_str(),value.size());
                char quality[17]={'\0'};
                snprintf(quality,17,"%16s",res[4]);
                memcpy(&updated_value[0],&quality[0],16);

                off_t lsn = StockTree->GetLSN();
                std::string tmp_value(&updated_value[0],&updated_value[0]+346);

                StockTree->UpdateLeafCopy(k,tmp_value,lsn,leaf_buf);
                StockTree->IncreLSN();
                gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
                free(leaf_buf);
              }else{
                char* leaf_buf = StockTree->GetLeafCopy(k);
                StockTree->GetRowRecord(k,value,leaf_buf);
                memcpy(&updated_value[0],value.c_str(),value.size());
                char quality[17]={'\0'};
                snprintf(quality,17,"%16s",res[4]);
                memcpy(&updated_value[0],&quality[0],16);

                off_t lsn = StockTree->GetLSN();
                std::string tmp_value(&updated_value[0],&updated_value[0]+346);

                StockTree->UpdateLeafCopy(k,tmp_value,lsn,leaf_buf);
                StockTree->IncreLSN();
                gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
                free(leaf_buf);

              }
            }
            auto t2 = std::chrono::steady_clock::now(); 
            static_tree::stock_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
            static_tree::stock_update_cnt++;
            //continue;
            

          }else{
            //blind update
            std::vector<std::string> res = split(*cmd, ",");
            if(res.size()!=5){
              ErrorDetect();
            } 
            int pk1 = atoi(res[3].c_str()); //cw_id
            int pk2 = atoi(res[2].c_str()); //item id
            snprintf(k, 33, "%16d%16d", pk1,pk2);
            
            char quality[17]={'\0'};
            snprintf(quality,17,"%16s",res[4]);
            updated_value[0] = 'U';
            updated_value[1] = ',';
            memcpy(&updated_value[2],&quality,16);
            std::string tmp_value(&updated_value[0],&updated_value[0]+346); 
            int lsn = StockTree->GetLSN();
            auto t1 = std::chrono::steady_clock::now();
            StockTree->Put(k, tmp_value, lsn);
            StockTree->IncreLSN();
            auto t2 = std::chrono::steady_clock::now(); 
            static_tree::stock_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
            static_tree::stock_update_cnt++;
          }

      
      }else if (KeyWordExist(cmd,3)){
        //scan,stock,2957,1, 1212,3
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=6){
          ErrorDetect();
        }
        char k_start[33]={'\0'};
        char k_end[33]={'\0'};
        int pk1_1 = atoi(res[3].c_str()); //w id
        int pk1_2 = atoi(res[2].c_str()); //item d
        
        int pk2_1 = atoi(res[5].c_str()); //w id
        int pk2_2 = atoi(res[4].c_str()); //item d
        


        snprintf(k_start, 33, "%16d%16d", pk1_1,pk1_2);
        snprintf(k_end, 33, "%16d%16d", pk2_1,pk2_2);
        std::string value;
        auto t1_s = std::chrono::steady_clock::now();
        StockTree->Scan(k_start,k_end,value);
        auto t2_s = std::chrono::steady_clock::now(); 
        if(pk2_1 == pk1_1){
          static_tree::stock_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2_s - t1_s).count()/1000;
          static_tree::stock_scan_cnt_c++;
        }else{
          static_tree::stock_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_s - t1_s).count()/1000;
          static_tree::stock_scan_cnt_h++;
        }
        
        //value[0] = value[1];
      }else if (KeyWordExist(cmd,2)){

        //get,stock,2957,1
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=4){
          ErrorDetect();
        }
        int pk1 = atoi(res[3].c_str()); //w id
        int pk2 = atoi(res[2].c_str()); //item d
        
        snprintf(k, 33, "%16d%16d", pk1,pk2);
        std::string value;
        auto t1 = std::chrono::steady_clock::now();
        if(gb_flag == 0){
          StockTree->Get(k,value);
        }else{ //the same for both baseline and plog
          off_t leaf_of = StockTree->GetOnlyLeafOffset(k);
          //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
          char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of); 
          if(leaf_buf!=nullptr){
            //can be found in gb
            
            StockTree->GetRowRecord(k,value,leaf_buf);
            //update LRU order
            gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }else{
            //register the page to gb
            char* leaf_buf = StockTree->GetLeafCopy(k);
            StockTree->GetRowRecord(k,value,leaf_buf);
            //this page is newly inserted into gb
            gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }
        }
        
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::stock_get_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::stock_get_cnt++;
        //value[0] = value[1];
      }else{
        ErrorDetect();
      }
   

    

      delete cmd;
    }
    
    return;


}

void Consumer_StockTree_MVCC_TEST(ProAndCon* itemQ_, GlobalBuffer* gb_, const int plog_flag_){
  //INSERT INTO stock values(
  //          2567,1,76,'u43OyncoVY9HLUbSyOnziusa','BAvu1Lo5MB37npdLxuePU6Nq',
  //          'LZk
  //          'cIc9jvqjZaDBmbqnlPxDbT4i','awyzkHZ1riklYRI6eTsDHeye','AE5EwnoqjlZ0Lap3IaajsgLJ','YY9UDieRlsLYeAFOhFjH5kXd',
  //          'EWzZpU7OdbZLEy74HdhRt586',0,0,0,'N9Ysnt6Jqf6KkSxK1GWzLaHxHg')
  //std::cout << "+++++++++++++++ TEST Stock Tree BEGIN ++++++++++++++++++++\n";
  std::string StockDir = "/media/hkc/csd-7.7T/htap/test";
  std::string DB_path = StockDir + "Stock/";
  std::string db = DB_path + "testStock.db";
  std::string log = DB_path + "testStock.log";
  std::string chpt = DB_path + "chptStock.log";
  std::string sha_plog = DB_path + "shaPlogStock.log";
  StockTree = new BPlusStockTree(db.c_str(), log.c_str(), chpt.c_str(), bg_id++, true,sha_plog.c_str());
  // load
  StockTree->SetBaseline();
  StockTree->SetLoadFlag(1);
  char k[33] = {'\0'};
  char v[16 + 240 + 8 + 16 + 16 + 50+1] = {'\0'};
  char updated_value[16 + 240 + 8 + 16 + 16 + 50+1] = {'\0'};
  int load_flag =1;
  int plog_flag = plog_flag_;  
  int gb_flag =0;
  int map_id = 1;
    while(1){
    
      std::string* cmd = itemQ_->consumption();
      //std::string cmd(cmd_char, cmd_char + strlen(cmd_char));
      if(cmd->size()==0){
        delete cmd;
        ErrorDetect(); static_tree::stock_get_not_found++;
        continue;
      }
      if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        if(load_flag==1){
          load_flag=0;
          gb_flag =1;
          StockTree->WarmUpVirtualPageCache(1);
        }
        if(plog_flag==1){
          StockTree->SetLoadFlag(0);
          StockTree->SetPlog();
          plog_flag =2;
        }
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
        int pk1 = atoi(res[3].c_str()); //cwid
        int pk2 = atoi(res[2].c_str()); //item id
        snprintf(k, 33, "%16d%16d", pk1,pk2);
        off_t lsn = StockTree->GetLSN();
        snprintf(v, 16 + 240 + 8 + 16 + 16 + 50+1, 
                    "%16s%24s%24s%24s%24s%24s%24s%24s%24s%24s%24s%8s%16s%16s%50s", 
            res[4].c_str(), 
            res[5].c_str(),
            res[6].c_str(),
            res[7].c_str(),
            res[8].c_str(),
            res[9].c_str(),
            res[10].c_str(),
            res[11].c_str(),
            res[12].c_str(),
            res[13].c_str(),
            res[14].c_str(),
            res[15].c_str(),
            res[16].c_str(),
            res[17].c_str(),
            res[18].c_str()
            );
        auto t1 = std::chrono::steady_clock::now();
        if(gb_flag == 0 || plog_flag == 2){//blind update
          StockTree->Put(k, v, lsn);
        }else{
          off_t leaf_of = StockTree->GetOnlyLeafOffset(k);
          //map id: stock 01
          //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
          char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
          if(leaf_buf!=nullptr){
            //can be found in gb
            //char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
            StockTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
            //update LRU order
            gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }else{
            //register the page to gb
            char* leaf_buf = StockTree->GetLeafCopy(k);
            StockTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
            //this page is newly inserted into gb
            gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }


        }
        auto t2 = std::chrono::steady_clock::now(); 
        if(load_flag ==0){
          static_tree::stock_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
          static_tree::stock_put_cnt++;
        }
        StockTree->IncreLSN();
      }else if (KeyWordExist(cmd,4)){
        //UPDATE stock SET s_quantity = ? WHERE s_i_id = ? AND s_w_id = ?
        //update,stock, item_id, w_id, ...
        if(plog_flag==0){
            //baseline, read modify write
            //get,c_id,2957,5,1
            std::vector<std::string> res = split(*cmd, ",");
            if(res.size()!=5){
              ErrorDetect();
            }
            int pk1 = atoi(res[3].c_str()); //cw_id
            int pk2 = atoi(res[2].c_str()); //item id
            snprintf(k, 33, "%16d%16d", pk1,pk2);

            auto t1 = std::chrono::steady_clock::now();
            std::string value;
            if(gb_flag == 0){
              StockTree->Get(k,value);
              memcpy(&updated_value[0],value.c_str(),value.size());
              char quality[17]={'\0'};
              snprintf(quality,17,"%16s",res[4]);
              memcpy(&updated_value[0],&quality[0],16);

              off_t lsn = StockTree->GetLSN();
              std::string tmp_value(&updated_value[0],&updated_value[0]+346);
              StockTree->Put(k, tmp_value, lsn);
              StockTree->IncreLSN();
            }else{
              off_t leaf_of = StockTree->GetOnlyLeafOffset(k);
              //map id: stock 01
              //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
              char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
              if(leaf_buf!=nullptr){
                //can be found in gb
               
                StockTree->GetRowRecord(k,value,leaf_buf);
                memcpy(&updated_value[0],value.c_str(),value.size());
                char quality[17]={'\0'};
                snprintf(quality,17,"%16s",res[4]);
                memcpy(&updated_value[0],&quality[0],16);

                off_t lsn = StockTree->GetLSN();
                std::string tmp_value(&updated_value[0],&updated_value[0]+346);

                StockTree->UpdateLeafCopy(k,tmp_value,lsn,leaf_buf);
                StockTree->IncreLSN();
                gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
                free(leaf_buf);
              }else{
                char* leaf_buf = StockTree->GetLeafCopy(k);
                StockTree->GetRowRecord(k,value,leaf_buf);
                memcpy(&updated_value[0],value.c_str(),value.size());
                char quality[17]={'\0'};
                snprintf(quality,17,"%16s",res[4]);
                memcpy(&updated_value[0],&quality[0],16);

                off_t lsn = StockTree->GetLSN();
                std::string tmp_value(&updated_value[0],&updated_value[0]+346);

                StockTree->UpdateLeafCopy(k,tmp_value,lsn,leaf_buf);
                StockTree->IncreLSN();
                gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
                free(leaf_buf);

              }
            }
            auto t2 = std::chrono::steady_clock::now(); 
            static_tree::stock_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
            static_tree::stock_update_cnt++;
            //continue;
            

          }else{
            //blind update
            std::vector<std::string> res = split(*cmd, ",");
            if(res.size()!=5){
              ErrorDetect();
            } 
            int pk1 = atoi(res[3].c_str()); //cw_id
            int pk2 = atoi(res[2].c_str()); //item id
            snprintf(k, 33, "%16d%16d", pk1,pk2);
            
            char quality[17]={'\0'};
            snprintf(quality,17,"%16s",res[4]);
            updated_value[0] = 'U';
            updated_value[1] = ',';
            memcpy(&updated_value[2],&quality,16);
            std::string tmp_value(&updated_value[0],&updated_value[0]+346); 
            int lsn = StockTree->GetLSN();
            auto t1 = std::chrono::steady_clock::now();
            StockTree->Put(k, tmp_value, lsn);
            StockTree->IncreLSN();
            auto t2 = std::chrono::steady_clock::now(); 
            static_tree::stock_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
            static_tree::stock_update_cnt++;
          }

      
      }else if (KeyWordExist(cmd,3)){
        //scan,stock,2957,1, 1212,3
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=6){
          ErrorDetect();
        }
        char k_start[33]={'\0'};
        char k_end[33]={'\0'};
        int pk1_1 = atoi(res[3].c_str()); //w id
        int pk1_2 = atoi(res[2].c_str()); //item d
        
        int pk2_1 = atoi(res[5].c_str()); //w id
        int pk2_2 = atoi(res[4].c_str()); //item d
        


        snprintf(k_start, 33, "%16d%16d", pk1_1,pk1_2);
        snprintf(k_end, 33, "%16d%16d", pk2_1,pk2_2);
        std::string value;
        auto t1_s = std::chrono::steady_clock::now();
        StockTree->Scan(k_start,k_end,value);
        auto t2_s = std::chrono::steady_clock::now(); 
        if(pk2_1 == pk1_1){
          static_tree::stock_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2_s - t1_s).count()/1000;
          static_tree::stock_scan_cnt_c++;
        }else{
          static_tree::stock_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_s - t1_s).count()/1000;
          static_tree::stock_scan_cnt_h++;
        }
        
        //value[0] = value[1];
      }else if (KeyWordExist(cmd,5)){
        //scan,stock,2957,1, 1212,3
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=6){
          ErrorDetect();
        }
        char k_start[33]={'\0'};
        char k_end[33]={'\0'};
        int pk1_1 = atoi(res[3].c_str()); //w id
        int pk1_2 = atoi(res[2].c_str()); //item d
        
        int pk2_1 = atoi(res[5].c_str()); //w id
        int pk2_2 = atoi(res[4].c_str()); //item d
        


        snprintf(k_start, 33, "%16d%16d", pk1_1,pk1_2);
        snprintf(k_end, 33, "%16d%16d", pk2_1,pk2_2);
        std::string value;
        StockTree->ScanAP(k_start,k_end,value);

      }else if (KeyWordExist(cmd,2)){

        //get,stock,2957,1
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=4){
          ErrorDetect();
        }
        int pk1 = atoi(res[3].c_str()); //w id
        int pk2 = atoi(res[2].c_str()); //item d
        
        snprintf(k, 33, "%16d%16d", pk1,pk2);
        std::string value;
        auto t1 = std::chrono::steady_clock::now();
        if(gb_flag == 0){
          StockTree->Get(k,value);
        }else{ //the same for both baseline and plog
          off_t leaf_of = StockTree->GetOnlyLeafOffset(k);
          //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
          char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of); 
          if(leaf_buf!=nullptr){
            //can be found in gb
            
            StockTree->GetRowRecord(k,value,leaf_buf);
            //update LRU order
            gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }else{
            //register the page to gb
            char* leaf_buf = StockTree->GetLeafCopy(k);
            StockTree->GetRowRecord(k,value,leaf_buf);
            //this page is newly inserted into gb
            gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }
        }
        
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::stock_get_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::stock_get_cnt++;
        //value[0] = value[1];
      }else{
        ErrorDetect();
      }
   

    

      delete cmd;
    }
    
    return;


}



void Consumer_OrdersTree(ProAndCon* itemQ_,GlobalBuffer* gb_, const int plog_flag_){
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


  //std::cout << "+++++++++++++++ TEST Orders Tree BEGIN ++++++++++++++++++++\n";
  std::string OrdersDir = "/media/hkc/csd-7.7T/htap/test";
  std::string DB_path = OrdersDir + "Orders/";
  std::string db = DB_path + "testOrders.db";
  std::string log = DB_path + "testOrders.log";
  std::string chpt = DB_path + "chptOrders.log";
  std::string sha_plog = DB_path + "shaPlogOrders.log";
  OrdersTree = new BPlusOrdersTree(db.c_str(), log.c_str(), chpt.c_str(), bg_id++, true,sha_plog.c_str());
  // load
  OrdersTree->SetBaseline();
  OrdersTree->SetLoadFlag(1);
  int load_flag =1;
  int plog_flag = plog_flag_;
  int gb_flag =0;
  int map_id = 4;
  char k[49] = {'\0'};
  char v[16 + 20 + 16 + 16 + 16 +1] = {'\0'};
  char updated_value[85] = {'\0'};
  char tmp_v[17] = {'\0'};
  while(1){
    std::string* cmd = itemQ_->consumption();
    if(cmd->size()==0){
        ErrorDetect();
        delete cmd;
        continue;
    }

    if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        if(load_flag==1){
          load_flag=0;
          gb_flag =1;
          OrdersTree->WarmUpVirtualPageCache(1);
        }
        if(plog_flag==1){
          OrdersTree->SetLoadFlag(0);
          OrdersTree->SetPlog();
          plog_flag =2;
        }
    }
      

      //key word:
      //1: put
      //2: get
      //3:scan
      //4.update
    if(KeyWordExist(cmd,1)){
      //put,orders,1(cid),1(cd),1(cw),num1,ts,num2,num3,num4
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=10){
        ErrorDetect();
      }
      int pk1 = atoi(res[4].c_str()); //cw
      int pk2 = atoi(res[3].c_str()); //cd
      int pk3 = atoi(res[2].c_str()); //cid
      snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);
      off_t lsn = OrdersTree->GetLSN();
      snprintf(v, 85,"%16d%20d%16d%16d%16d", 
            res[5].c_str(),
            res[6].c_str(),
            res[7].c_str(),
            res[8].c_str(),
            res[9].c_str());
      auto t1 = std::chrono::steady_clock::now();
      if(gb_flag == 0 || plog_flag == 2){//blind update
        OrdersTree->Put(k, v, lsn); //this interface implements blind update for plog
      }else{
        off_t leaf_of = OrdersTree->GetOnlyLeafOffset(k);
        //map id: stock 01
        //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
        char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
        if(leaf_buf!=nullptr){
          //can be found in gb
          //char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
          OrdersTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
          //update LRU order
          gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }else{
          //register the page to gb
          char* leaf_buf = OrdersTree->GetLeafCopy(k);
          OrdersTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
          //this page is newly inserted into gb
          gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }
      }
      
      auto t2 = std::chrono::steady_clock::now(); 
      if(load_flag ==0){
        static_tree::orders_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::orders_put_cnt++;
      }
      OrdersTree->IncreLSN();
    }else if (KeyWordExist(cmd,2)){

      //get,orders,2957,5,1
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=5){
        ErrorDetect();
      }
      int pk1 = atoi(res[4].c_str()); //cw
      int pk2 = atoi(res[3].c_str()); //cd
      int pk3 = atoi(res[2].c_str()); //cid
      snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);
      std::string value;
      auto t1 = std::chrono::steady_clock::now();
      if(plog_flag ==2){
        //1.1 get from gb
        off_t leaf_of  = OrdersTree->GetOnlyLeafOffset(k);
        char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
        if(leaf_buf!=nullptr){
          //can be found
          OrdersTree->GetRowRecord(k,value,leaf_buf);
          gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }else{
          //register to gb
          char* leaf_buf = OrdersTree->GetLeafCopy(k);
          OrdersTree->GetRowRecord(k,value,leaf_buf);
          gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
          free(leaf_buf);

        }

      }else if(gb_flag == 0){
        OrdersTree->Get(k,value);
      }else{
        off_t leaf_of = OrdersTree->GetOnlyLeafOffset(k);
        //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
        char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of); 
        if(leaf_buf!=nullptr){
          //can be found in gb
          
          OrdersTree->GetRowRecord(k,value,leaf_buf);
          //update LRU order
          gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }else{
          //register the page to gb
          char* leaf_buf = OrdersTree->GetLeafCopy(k);
          OrdersTree->GetRowRecord(k,value,leaf_buf);
          //this page is newly inserted into gb
          gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }
      }
      
      if(value.size()!=84){
        ErrorDetect();
      }
      auto t2 = std::chrono::steady_clock::now(); 
      static_tree::orders_get_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
      static_tree::orders_get_cnt++;
      
    }else if (KeyWordExist(cmd,3)){
      //scan,orders,100,5,2,60,7,4
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=8){
        ErrorDetect();
      }
      int pk1_1 = atoi(res[4].c_str()); //cw
      int pk1_2 = atoi(res[3].c_str()); //cd
      int pk1_3 = atoi(res[2].c_str()); //cid
      
      int pk2_1 = atoi(res[7].c_str()); //cw
      int pk2_2 = atoi(res[6].c_str()); //cd
      int pk2_3 = atoi(res[5].c_str()); //cid
      
      char k_start[49]={'\0'};
      char k_end[49]={'\0'};
      
      snprintf(k_start, 49, "%16d%16d%16d", pk1_1,pk1_2,pk1_3);
      snprintf(k_end, 49, "%16d%16d%16d", pk2_1,pk2_2,pk2_3);
      std::string value;
      auto t1 = std::chrono::steady_clock::now();
      OrdersTree->Scan(k_start,k_end,value);
      auto t2 = std::chrono::steady_clock::now(); 
      if(pk1_1==pk2_1 && pk1_2 == pk2_2){
        static_tree::orders_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::orders_scan_cnt_c++;
      }else{
        static_tree::orders_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::orders_scan_cnt_h++;
      }
      
      
    }else if (KeyWordExist(cmd,4)){
      //update,orders,249,5,2,5403
      if(plog_flag==0){
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=6){
          ErrorDetect();
        }
        int pk1 = atoi(res[4].c_str()); //cw
        int pk2 = atoi(res[3].c_str()); //cd
        int pk3 = atoi(res[2].c_str()); //cid
        snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);

        auto t1 = std::chrono::steady_clock::now();
        std::string value;
        if(gb_flag == 0){
          OrdersTree->Get(k,value);
          if(value.size()!=84){
            static_tree::orders_get_not_found++;
            ErrorDetect();
          }
                
          memcpy(&updated_value[0],value.c_str(),value.size());
          
          snprintf(tmp_v,17,"%16s",res[5]);
          memcpy(&updated_value[16+20],&tmp_v[0],16);
          
          off_t lsn = OrdersTree->GetLSN();
          std::string tmp_value(&updated_value[0],&updated_value[0]+84);
          OrdersTree->Put(k, tmp_value, lsn);
          OrdersTree->IncreLSN();
          
        }else{
          off_t leaf_of = OrdersTree->GetOnlyLeafOffset(k);
          //map id: orders 04
          char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);

          if(leaf_buf!=nullptr){
            OrdersTree->GetRowRecord(k,value,leaf_buf);
            memcpy(&updated_value[0],value.c_str(),value.size());
            snprintf(tmp_v,17,"%16s",res[5]);
            memcpy(&updated_value[16+20],&tmp_v[0],16);
            off_t lsn = OrdersTree->GetLSN();
            std::string tmp_value(&updated_value[0],&updated_value[0]+84);
            OrdersTree->UpdateLeafCopy(k, tmp_value,lsn,leaf_buf);
            OrdersTree->IncreLSN();
            gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }else{
            char* leaf_buf = OrdersTree->GetLeafCopy(k);
            OrdersTree->GetRowRecord(k,value,leaf_buf);
            memcpy(&updated_value[0],value.c_str(),value.size());
            snprintf(tmp_v,17,"%16s",res[5]);
            memcpy(&updated_value[16+20],&tmp_v[0],16);
            off_t lsn = OrdersTree->GetLSN();
            std::string tmp_value(&updated_value[0],&updated_value[0]+84);
            OrdersTree->UpdateLeafCopy(k, tmp_value,lsn,leaf_buf);
            OrdersTree->IncreLSN();
            gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }
        }
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::orders_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::orders_update_cnt++;
        //continue;
        

      }else{
        //blind update
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=6){
          ErrorDetect();
        }
        int pk1 = atoi(res[4].c_str()); //cw
        int pk2 = atoi(res[3].c_str()); //cd
        int pk3 = atoi(res[2].c_str()); //cid
        snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);
        
        
        snprintf(tmp_v,17,"%16s",res[5]);
        updated_value[0] = 'U';
        updated_value[1] = ',';
        memcpy(&updated_value[2],&tmp_v[0],16);
        std::string tmp_value(&updated_value[0],&updated_value[0]+84); 
        int lsn = OrdersTree->GetLSN();
        auto t1 = std::chrono::steady_clock::now();
        OrdersTree->Put(k, tmp_value, lsn);
        OrdersTree->IncreLSN();
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::orders_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::orders_update_cnt++;
      }

      
    }else{
      ErrorDetect();
    }
    
      
    delete cmd;
  }
  return;

}

void Consumer_OrdersTree_MVCC_TEST(ProAndCon* itemQ_,GlobalBuffer* gb_, const int plog_flag_){
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


  //std::cout << "+++++++++++++++ TEST Orders Tree BEGIN ++++++++++++++++++++\n";
  std::string OrdersDir = "/media/hkc/csd-7.7T/htap/test";
  std::string DB_path = OrdersDir + "Orders/";
  std::string db = DB_path + "testOrders.db";
  std::string log = DB_path + "testOrders.log";
  std::string chpt = DB_path + "chptOrders.log";
  std::string sha_plog = DB_path + "shaPlogOrders.log";
  OrdersTree = new BPlusOrdersTree(db.c_str(), log.c_str(), chpt.c_str(), bg_id++, true,sha_plog.c_str());
  // load
  OrdersTree->SetBaseline();
  OrdersTree->SetLoadFlag(1);
  int load_flag =1;
  int plog_flag = plog_flag_;
  int gb_flag =0;
  int map_id = 4;
  char k[49] = {'\0'};
  char v[16 + 20 + 16 + 16 + 16 +1] = {'\0'};
  char updated_value[85] = {'\0'};
  char tmp_v[17] = {'\0'};
  while(1){
    std::string* cmd = itemQ_->consumption();
    if(cmd->size()==0){
        ErrorDetect();
        delete cmd;
        continue;
    }

    if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        if(load_flag==1){
          load_flag=0;
          gb_flag =1;
          OrdersTree->WarmUpVirtualPageCache(1);
        }
        if(plog_flag==1){
          OrdersTree->SetLoadFlag(0);
          OrdersTree->SetPlog();
          plog_flag =2;
        }
    }
      

      //key word:
      //1: put
      //2: get
      //3:scan
      //4.update
    if(KeyWordExist(cmd,1)){
      //put,orders,1(cid),1(cd),1(cw),num1,ts,num2,num3,num4
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=10){
        ErrorDetect();
      }
      int pk1 = atoi(res[4].c_str()); //cw
      int pk2 = atoi(res[3].c_str()); //cd
      int pk3 = atoi(res[2].c_str()); //cid
      snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);
      off_t lsn = OrdersTree->GetLSN();
      snprintf(v, 85,"%16d%20d%16d%16d%16d", 
            res[5].c_str(),
            res[6].c_str(),
            res[7].c_str(),
            res[8].c_str(),
            res[9].c_str());
      auto t1 = std::chrono::steady_clock::now();
      if(gb_flag == 0 || plog_flag == 2){//blind update
        OrdersTree->Put(k, v, lsn); //this interface implements blind update for plog
      }else{
        off_t leaf_of = OrdersTree->GetOnlyLeafOffset(k);
        //map id: stock 01
        //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
        char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
        if(leaf_buf!=nullptr){
          //can be found in gb
          //char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
          OrdersTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
          //update LRU order
          gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }else{
          //register the page to gb
          char* leaf_buf = OrdersTree->GetLeafCopy(k);
          OrdersTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
          //this page is newly inserted into gb
          gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }
      }
      
      auto t2 = std::chrono::steady_clock::now(); 
      if(load_flag ==0){
        static_tree::orders_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::orders_put_cnt++;
      }
      OrdersTree->IncreLSN();
    }else if (KeyWordExist(cmd,2)){

      //get,orders,2957,5,1
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=5){
        ErrorDetect();
      }
      int pk1 = atoi(res[4].c_str()); //cw
      int pk2 = atoi(res[3].c_str()); //cd
      int pk3 = atoi(res[2].c_str()); //cid
      snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);
      std::string value;
      auto t1 = std::chrono::steady_clock::now();
      if(plog_flag ==2){
        //1.1 get from gb
        off_t leaf_of  = OrdersTree->GetOnlyLeafOffset(k);
        char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
        if(leaf_buf!=nullptr){
          //can be found
          OrdersTree->GetRowRecord(k,value,leaf_buf);
          gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }else{
          //register to gb
          char* leaf_buf = OrdersTree->GetLeafCopy(k);
          OrdersTree->GetRowRecord(k,value,leaf_buf);
          gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
          free(leaf_buf);

        }

      }else if(gb_flag == 0){
        OrdersTree->Get(k,value);
      }else{
        off_t leaf_of = OrdersTree->GetOnlyLeafOffset(k);
        //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
        char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of); 
        if(leaf_buf!=nullptr){
          //can be found in gb
          
          OrdersTree->GetRowRecord(k,value,leaf_buf);
          //update LRU order
          gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }else{
          //register the page to gb
          char* leaf_buf = OrdersTree->GetLeafCopy(k);
          OrdersTree->GetRowRecord(k,value,leaf_buf);
          //this page is newly inserted into gb
          gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }
      }
      
      if(value.size()!=84){
        ErrorDetect();
      }
      auto t2 = std::chrono::steady_clock::now(); 
      static_tree::orders_get_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
      static_tree::orders_get_cnt++;
      
    }else if (KeyWordExist(cmd,3)){
      //scan,orders,100,5,2,60,7,4
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=8){
        ErrorDetect();
      }
      int pk1_1 = atoi(res[4].c_str()); //cw
      int pk1_2 = atoi(res[3].c_str()); //cd
      int pk1_3 = atoi(res[2].c_str()); //cid
      
      int pk2_1 = atoi(res[7].c_str()); //cw
      int pk2_2 = atoi(res[6].c_str()); //cd
      int pk2_3 = atoi(res[5].c_str()); //cid
      
      char k_start[49]={'\0'};
      char k_end[49]={'\0'};
      
      snprintf(k_start, 49, "%16d%16d%16d", pk1_1,pk1_2,pk1_3);
      snprintf(k_end, 49, "%16d%16d%16d", pk2_1,pk2_2,pk2_3);
      std::string value;
      auto t1 = std::chrono::steady_clock::now();
      OrdersTree->Scan(k_start,k_end,value);
      auto t2 = std::chrono::steady_clock::now(); 
      if(pk1_1==pk2_1 && pk1_2 == pk2_2){
        static_tree::orders_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::orders_scan_cnt_c++;
      }else{
        static_tree::orders_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::orders_scan_cnt_h++;
      }
      
      
    }else if (KeyWordExist(cmd,5)){
      //scan,orders,100,5,2,60,7,4
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=8){
        ErrorDetect();
      }
      int pk1_1 = atoi(res[4].c_str()); //cw
      int pk1_2 = atoi(res[3].c_str()); //cd
      int pk1_3 = atoi(res[2].c_str()); //cid
      
      int pk2_1 = atoi(res[7].c_str()); //cw
      int pk2_2 = atoi(res[6].c_str()); //cd
      int pk2_3 = atoi(res[5].c_str()); //cid
      
      char k_start[49]={'\0'};
      char k_end[49]={'\0'};
      
      snprintf(k_start, 49, "%16d%16d%16d", pk1_1,pk1_2,pk1_3);
      snprintf(k_end, 49, "%16d%16d%16d", pk2_1,pk2_2,pk2_3);
      std::string value;
      OrdersTree->ScanAP(k_start,k_end,value);
      
      
    }else if (KeyWordExist(cmd,4)){
      //update,orders,249,5,2,5403
      if(plog_flag==0){
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=6){
          ErrorDetect();
        }
        int pk1 = atoi(res[4].c_str()); //cw
        int pk2 = atoi(res[3].c_str()); //cd
        int pk3 = atoi(res[2].c_str()); //cid
        snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);

        auto t1 = std::chrono::steady_clock::now();
        std::string value;
        if(gb_flag == 0){
          OrdersTree->Get(k,value);
          if(value.size()!=84){
            static_tree::orders_get_not_found++;
            ErrorDetect();
          }
                
          memcpy(&updated_value[0],value.c_str(),value.size());
          
          snprintf(tmp_v,17,"%16s",res[5]);
          memcpy(&updated_value[16+20],&tmp_v[0],16);
          
          off_t lsn = OrdersTree->GetLSN();
          std::string tmp_value(&updated_value[0],&updated_value[0]+84);
          OrdersTree->Put(k, tmp_value, lsn);
          OrdersTree->IncreLSN();
          
        }else{
          off_t leaf_of = OrdersTree->GetOnlyLeafOffset(k);
          //map id: orders 04
          char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);

          if(leaf_buf!=nullptr){
            OrdersTree->GetRowRecord(k,value,leaf_buf);
            memcpy(&updated_value[0],value.c_str(),value.size());
            snprintf(tmp_v,17,"%16s",res[5]);
            memcpy(&updated_value[16+20],&tmp_v[0],16);
            off_t lsn = OrdersTree->GetLSN();
            std::string tmp_value(&updated_value[0],&updated_value[0]+84);
            OrdersTree->UpdateLeafCopy(k, tmp_value,lsn,leaf_buf);
            OrdersTree->IncreLSN();
            gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }else{
            char* leaf_buf = OrdersTree->GetLeafCopy(k);
            OrdersTree->GetRowRecord(k,value,leaf_buf);
            memcpy(&updated_value[0],value.c_str(),value.size());
            snprintf(tmp_v,17,"%16s",res[5]);
            memcpy(&updated_value[16+20],&tmp_v[0],16);
            off_t lsn = OrdersTree->GetLSN();
            std::string tmp_value(&updated_value[0],&updated_value[0]+84);
            OrdersTree->UpdateLeafCopy(k, tmp_value,lsn,leaf_buf);
            OrdersTree->IncreLSN();
            gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }
        }
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::orders_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::orders_update_cnt++;
        //continue;
        

      }else{
        //blind update
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=6){
          ErrorDetect();
        }
        int pk1 = atoi(res[4].c_str()); //cw
        int pk2 = atoi(res[3].c_str()); //cd
        int pk3 = atoi(res[2].c_str()); //cid
        snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);
        
        
        snprintf(tmp_v,17,"%16s",res[5]);
        updated_value[0] = 'U';
        updated_value[1] = ',';
        memcpy(&updated_value[2],&tmp_v[0],16);
        std::string tmp_value(&updated_value[0],&updated_value[0]+84); 
        int lsn = OrdersTree->GetLSN();
        auto t1 = std::chrono::steady_clock::now();
        OrdersTree->Put(k, tmp_value, lsn);
        OrdersTree->IncreLSN();
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::orders_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::orders_update_cnt++;
      }

      
    }else{
      ErrorDetect();
    }
    
      
    delete cmd;
  }
  return;

}




void Consumer_ItemTree(ProAndCon* itemQ_,GlobalBuffer* gb_, const int plog_flag_){
// 2023-02-26T13:56:04.057992Z	    3 Query	create table item (
  // i_id int not null,
  // i_im_id int,
  // i_name varchar(24),
  // i_price decimal(5,2),
  // i_data varchar(50),
  // PRIMARY KEY(i_id) ) Engine=InnoDB


    // init a btree
    std::string ItemDir = "/media/hkc/csd-7.7T/htap/test";
    std::string DB_path = ItemDir + "Item/";
    std::string db = DB_path + "testItem.db";
    std::string log = DB_path + "testItem.log";
    std::string chpt = DB_path + "chptItem.log";
    std::string sha_plog = DB_path + "shaPlogItem.log";
    ItemTree = new BPlusItemTree(db.c_str(), log.c_str(), chpt.c_str(), bg_id++, true,sha_plog.c_str());

    // INSERT INTO item values(
    //   1,2498,'8TA69fNTQqhRD7KLKkV',77.2699966430664,
    //   '87LD1FH6AFFbPLl2hCSXsukzC3IxW23eAOqBb7')
    //

    int load_flag =1;
    int plog_flag = plog_flag_;
    int gb_flag =0;
    int map_id = 2;

    ItemTree->SetBaseline();
    ItemTree->SetLoadFlag(1);
    char k[17] = {'\0'};
    char v[16 + 24 + 8 + 50+1] = {'\0'};
    while(1){


      std::string* cmd = itemQ_->consumption();
      //std::string cmd(cmd_char, cmd_char + strlen(cmd_char));
      if(cmd->size()==0){
        ErrorDetect(); 
        delete cmd;
        static_tree::item_get_not_found++; 
        continue;
      }


      if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
          if(load_flag==1){
            load_flag=0;
            gb_flag=1;
            ItemTree->WarmUpVirtualPageCache(1);
          }
          if(plog_flag==1){
            ItemTree->SetLoadFlag(0);
            ItemTree->SetPlog();
            plog_flag =2;
          }
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
          int pk = atoi(res[2].c_str());
          snprintf(k, 17, "%16d", pk);
          snprintf(v, 16 + 24 + 8 + 50+1, "%16s%24s%8s%50s", 
            res[3].c_str(), 
            res[4].c_str(), 
            res[5].c_str(), 
            res[6].c_str()
            );
          off_t lsn = ItemTree->GetLSN();
          auto t1 = std::chrono::steady_clock::now();
          if(gb_flag == 0 || plog_flag == 2){
            ItemTree->Put(k, v, lsn);
          }else{
            off_t leaf_of = ItemTree->GetOnlyLeafOffset(k);
            char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
            if(leaf_buf!=nullptr){
              //can be found in gb
              //char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
              ItemTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
              //update LRU order
              gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
              free(leaf_buf);
            }else{
              //register the page to gb
              char* leaf_buf = ItemTree->GetLeafCopy(k);
              ItemTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
              //this page is newly inserted into gb
              gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
              free(leaf_buf);
            }
          }
          auto t2 = std::chrono::steady_clock::now();
          if(load_flag ==0){
            static_tree::item_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
            static_tree::item_put_cnt++;
          }
          ItemTree->IncreLSN();
      }else if(KeyWordExist(cmd,2)){
        std::vector<std::string> res = split(*cmd, ",");
        int pk = atoi(res[3].c_str());
        snprintf(k, 17, "%16d", pk);
        std::string value;
        auto t1 = std::chrono::steady_clock::now();
        if(gb_flag == 0){
          ItemTree->Get(k,value);
        }else{
          off_t leaf_of = ItemTree->GetOnlyLeafOffset(k);
          //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
          char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of); 
          if(leaf_buf!=nullptr){
            //can be found in gb
            
            ItemTree->GetRowRecord(k,value,leaf_buf);
            //update LRU order
            gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }else{
            //register the page to gb
            char* leaf_buf = ItemTree->GetLeafCopy(k);
            ItemTree->GetRowRecord(k,value,leaf_buf);
            //this page is newly inserted into gb
            gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }
        }
        
        value[0]= value[1];
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::item_get_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::item_get_cnt++;

      }else if (KeyWordExist(cmd,3)){
        //scan,item,1,300
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=4){
          ErrorDetect();
        }
        char k_start[17]={'\0'};
        char k_end[17]={'\0'};
        int pk1 = atoi(res[2].c_str()); //w id
        int pk2 = atoi(res[3].c_str()); //item d
        

        if(pk2<pk1){
          int tmp=pk2;
          pk2=pk1;
          pk1=tmp;
          int x=1;
        }
        snprintf(k_start, 17, "%16d", pk1);
        snprintf(k_end, 17, "%16d", pk2);
        std::string value;
        auto t1_i = std::chrono::steady_clock::now();
        ItemTree->Scan(k_start,k_end,value);
        auto t2_i = std::chrono::steady_clock::now(); 
        
        static_tree::item_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count()/1000;
        static_tree::item_scan_cnt_h++;
        //value[0] = value[1];
      }else{
        ErrorDetect();
      }
      delete cmd;
    }
    return;
}


void Consumer_ItemTree_MVCC_TEST(ProAndCon* itemQ_,GlobalBuffer* gb_, const int plog_flag_){
// 2023-02-26T13:56:04.057992Z	    3 Query	create table item (
  // i_id int not null,
  // i_im_id int,
  // i_name varchar(24),
  // i_price decimal(5,2),
  // i_data varchar(50),
  // PRIMARY KEY(i_id) ) Engine=InnoDB


    // init a btree
    std::string ItemDir = "/media/hkc/csd-7.7T/htap/test";
    std::string DB_path = ItemDir + "Item/";
    std::string db = DB_path + "testItem.db";
    std::string log = DB_path + "testItem.log";
    std::string chpt = DB_path + "chptItem.log";
    std::string sha_plog = DB_path + "shaPlogItem.log";
    ItemTree = new BPlusItemTree(db.c_str(), log.c_str(), chpt.c_str(), bg_id++, true,sha_plog.c_str());

    // INSERT INTO item values(
    //   1,2498,'8TA69fNTQqhRD7KLKkV',77.2699966430664,
    //   '87LD1FH6AFFbPLl2hCSXsukzC3IxW23eAOqBb7')
    //

    int load_flag =1;
    int plog_flag = plog_flag_;
    int gb_flag =0;
    int map_id = 2;

    ItemTree->SetBaseline();
    ItemTree->SetLoadFlag(1);
    char k[17] = {'\0'};
    char v[16 + 24 + 8 + 50+1] = {'\0'};
    while(1){


      std::string* cmd = itemQ_->consumption();
      //std::string cmd(cmd_char, cmd_char + strlen(cmd_char));
      if(cmd->size()==0){
        ErrorDetect(); 
        delete cmd;
        static_tree::item_get_not_found++; 
        continue;
      }


      if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
          if(load_flag==1){
            load_flag=0;
            gb_flag=1;
            ItemTree->WarmUpVirtualPageCache(1);
          }
          if(plog_flag==1){
            ItemTree->SetLoadFlag(0);
            ItemTree->SetPlog();
            plog_flag =2;
          }
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
          int pk = atoi(res[2].c_str());
          snprintf(k, 17, "%16d", pk);
          snprintf(v, 16 + 24 + 8 + 50+1, "%16s%24s%8s%50s", 
            res[3].c_str(), 
            res[4].c_str(), 
            res[5].c_str(), 
            res[6].c_str()
            );
          off_t lsn = ItemTree->GetLSN();
          auto t1 = std::chrono::steady_clock::now();
          if(gb_flag == 0 || plog_flag == 2){
            ItemTree->Put(k, v, lsn);
          }else{
            off_t leaf_of = ItemTree->GetOnlyLeafOffset(k);
            char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
            if(leaf_buf!=nullptr){
              //can be found in gb
              //char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
              ItemTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
              //update LRU order
              gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
              free(leaf_buf);
            }else{
              //register the page to gb
              char* leaf_buf = ItemTree->GetLeafCopy(k);
              ItemTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
              //this page is newly inserted into gb
              gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
              free(leaf_buf);
            }
          }
          auto t2 = std::chrono::steady_clock::now();
          if(load_flag ==0){
            static_tree::item_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
            static_tree::item_put_cnt++;
          }
          ItemTree->IncreLSN();
      }else if(KeyWordExist(cmd,2)){
        std::vector<std::string> res = split(*cmd, ",");
        int pk = atoi(res[3].c_str());
        snprintf(k, 17, "%16d", pk);
        std::string value;
        auto t1 = std::chrono::steady_clock::now();
        if(gb_flag == 0){
          ItemTree->Get(k,value);
        }else{
          off_t leaf_of = ItemTree->GetOnlyLeafOffset(k);
          //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
          char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of); 
          if(leaf_buf!=nullptr){
            //can be found in gb
            
            ItemTree->GetRowRecord(k,value,leaf_buf);
            //update LRU order
            gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }else{
            //register the page to gb
            char* leaf_buf = ItemTree->GetLeafCopy(k);
            ItemTree->GetRowRecord(k,value,leaf_buf);
            //this page is newly inserted into gb
            gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }
        }
        
        value[0]= value[1];
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::item_get_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::item_get_cnt++;

      }else if (KeyWordExist(cmd,3)){
        //scan,item,1,300
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=4){
          ErrorDetect();
        }
        char k_start[17]={'\0'};
        char k_end[17]={'\0'};
        int pk1 = atoi(res[2].c_str()); //w id
        int pk2 = atoi(res[3].c_str()); //item d
        

        if(pk2<pk1){
          int tmp=pk2;
          pk2=pk1;
          pk1=tmp;
          int x=1;
        }
        snprintf(k_start, 17, "%16d", pk1);
        snprintf(k_end, 17, "%16d", pk2);
        std::string value;
        auto t1_i = std::chrono::steady_clock::now();
        ItemTree->Scan(k_start,k_end,value);
        auto t2_i = std::chrono::steady_clock::now(); 
        
        static_tree::item_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_i - t1_i).count()/1000;
        static_tree::item_scan_cnt_h++;
        //value[0] = value[1];
      }else if (KeyWordExist(cmd,5)){
        //scan,item,1,300
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=4){
          ErrorDetect();
        }
        char k_start[17]={'\0'};
        char k_end[17]={'\0'};
        int pk1 = atoi(res[2].c_str()); //w id
        int pk2 = atoi(res[3].c_str()); //item d
        

        if(pk2<pk1){
          int tmp=pk2;
          pk2=pk1;
          pk1=tmp;
          int x=1;
        }
        snprintf(k_start, 17, "%16d", pk1);
        snprintf(k_end, 17, "%16d", pk2);
        std::string value;
        ItemTree->ScanAP(k_start,k_end,value);

        //value[0] = value[1];
      }else{
        ErrorDetect();
      }
      delete cmd;
    }
    return;
}



/*
void Consumer_DistrictTree(ProAndCon* itemQ_){
  // Schema  
  create table district (
  d_id tinyint not null, 
  d_w_id smallint not null, 

  d_name varchar(10), 
  d_street_1 varchar(20), 
  d_street_2 varchar(20), 
  d_city varchar(20), 
  d_state char(2), 
  d_zip char(9), 
  d_tax decimal(4,2), 
  d_ytd decimal(12,2), 
  d_next_o_id int,
  primary key (d_w_id, d_id) ) Engine=InnoDB
  // end of Schema  


  //std::cout << "+++++++++++++++ TEST District Tree BEGIN ++++++++++++++++++++\n";
  std::string DistrictDir = "/media/hkc/csd-7.7T/htap/test";
  std::string DB_path = DistrictDir + "District/";
  std::string db = DB_path + "testDistrict.db";
  std::string log = DB_path + "testDistrict.log";
  std::string chpt = DB_path + "chptDistrict.log";
  DistrictTree = new BPlusDistrictTree(db.c_str(), log.c_str(), chpt.c_str(), bg_id++, true);
  // load
  DistrictTree->SetBaseline();
  DistrictTree->SetLoadFlag(1);
  char k[32] = {'\0'};
  char v[10+20+20+20+2+9+6+14+16] = {'\0'};
  while(1){
    std::string* cmd = itemQ_->consumption();

    delete cmd; 
  }
  return;

}
*/


void Consumer_CustomerTree(ProAndCon* itemQ_, GlobalBuffer* gb_, const int plog_flag_){
  // Schema  
/*
create table customer (
c_id int not null, 16
c_d_id tinyint not null,  16
c_w_id smallint not null, 16

c_first varchar(16),  16
c_middle char(2),   2
c_last varchar(16), 16
c_street_1 varchar(20), 20 
c_street_2 varchar(20),  20
c_city varchar(20), 20
c_state char(2),        2
c_zip char(9),          9
c_phone char(16),       16
c_since datetime,       20
c_credit char(2),       2
c_credit_lim bigint,    16
c_discount decimal(4,2), 6
c_balance decimal(12,2), 14
c_ytd_payment decimal(12,2), 14
c_payment_cnt smallint, 16
c_delivery_cnt smallint, 16
c_data text,  100 // for text >100, simply discard
PRIMARY KEY(c_w_id, c_d_id, c_id) ) Engine=InnoDB
*/

//FROM customer, warehouse WHERE 
//w_id = 1 AND c_w_id = w_id AND c_d_id = 5 AND c_id = 1026


  //std::cout << "+++++++++++++++ TEST Customer Tree BEGIN ++++++++++++++++++++\n";
  std::string CustomerDir = "/media/hkc/csd-7.7T/htap/test";
  std::string DB_path = CustomerDir + "Customer/";
  std::string db = DB_path + "testCustomer.db";
  std::string log = DB_path + "testCustomer.log";
  std::string chpt = DB_path + "chptCustomer.log";
  std::string sha_plog = DB_path + "shaPlogCustomer.log";
  CustomerTree = new BPlusCustomerTree(db.c_str(), log.c_str(), chpt.c_str(), bg_id++, true,sha_plog.c_str());
  // load
  CustomerTree->SetBaseline();
  CustomerTree->SetLoadFlag(1);
  //bpt->SetLoadFlag(0);
  //bpt->SetPlog();
  //
  int load_flag =1;
  int plog_flag = plog_flag_;
  char k[49] = {'\0'};
  char v[326] = {'\0'};
  char updated_value[326] = {'\0'};
  char balance[15]={'\0'};
  char data[101]={'\0'};
  int gb_flag =0;
  int map_id =0;
  while(1){
    std::string* cmd = itemQ_->consumption();
    //std::string cmd(cmd_char, cmd_char + strlen(cmd_char));
    if(cmd->size()==0){
        ErrorDetect();
        delete cmd;
        continue;
    }

    if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
          if(load_flag==1){
            load_flag=0;
            gb_flag = 1;
            CustomerTree->WarmUpVirtualPageCache(1);
          }
          if(plog_flag==1){
            CustomerTree->SetLoadFlag(0);
            CustomerTree->SetPlog();
            plog_flag =2;
          }
    }
      

        //key word:
      //1: put
      //2: get
      //3:scan
      //4.update


    if(KeyWordExist(cmd,1)){
      //put,customer,1(cid),1(cd),1(cw),rk4o0bDasnuUY,OE,BARBARBAR,drcT5CErowB5yMEs,3arraS38Kq0mOp,GPrLa5AO0LSwfgpShNK,wm,098594856,6775361462581630,2023-02-26 21:56:14,BC,50000,0.2199999988079071,-10, 10.0, 1, 0,47F7XKxNCCSRGpHPNTXF7bnNUlMi3rt7y8DTQ8raIH1Y4GvRjSho3SBVDVCGN3LJBYnZec9wr8UtNQL6q0tsS2NfYXttaECjlxIP
      //std::string contents = cmd->substr((*cmd).find("(") + 1, (*cmd).find(")") - 2);
      //contents = contents.replace(contents.find(")"), 1, "");
      //replace(contents.begin(), contents.end(), '\'', ',');
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=23){
        ErrorDetect();
      }
      int pk1 = atoi(res[4].c_str()); //cw
      int pk2 = atoi(res[3].c_str()); //cd
      int pk3 = atoi(res[2].c_str()); //cid
      snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);
      //snprintf(v, 16 + 20 + 16 + 16 +16, "%16s%20s%16s%16s%16s", i, rand() % 10000000, rand() % 10000000, rand() % 10000, rand() % 100000000,rand()%10000000);
      off_t lsn = CustomerTree->GetLSN();
      snprintf(v, 16+2+16+20+20+20+2+9+16+20+2+16+6+14+14+16+16+100+1, 
                  "%16s%2s%16s%20s%20s%20s%2s%9s%16s%20s%2s%16s%6s%14s%14s%16s%16s%100s", 
          res[5].c_str(), 
          res[6].c_str(),
          res[7].c_str(),
          res[8].c_str(),
          res[9].c_str(),
          res[10].c_str(),
          res[11].c_str(),
          res[12].c_str(),
          res[13].c_str(),
          res[14].c_str(),
          res[15].c_str(),
          res[16].c_str(),
          res[17].c_str(),
          res[18].c_str(),
          res[19].c_str(),
          res[20].c_str(),
          res[21].c_str(),
          res[22].c_str()
          );
      auto t1 = std::chrono::steady_clock::now();
      if(gb_flag == 0 || plog_flag == 2){
        CustomerTree->Put(k, v, lsn);
      }else{
        off_t leaf_of = CustomerTree->GetOnlyLeafOffset(k);
        //map id: customer 00
        //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
        char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of); 
        if(leaf_buf!=nullptr){
          //can be found in gb
          
          CustomerTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
          //update LRU order
          gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }else{
          //register the page to gb
          char* leaf_buf = CustomerTree->GetLeafCopy(k);
          CustomerTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
          //this page is newly inserted into gb
          gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }
      }
      auto t2 = std::chrono::steady_clock::now(); 
      if(load_flag ==0){
        static_tree::customer_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::customer_put_cnt++;
      }
      CustomerTree->IncreLSN();


      
    }else if (KeyWordExist(cmd,4)){
      //update,customer,249,5,2,-15403,,flvQjsz7ADcUgKul18DtmQq9BwuMaKOF5KfmCdtKoToVohGnnSgZqUh1QbMyukBz2rkFSdXG7Kjt1ygoQKMGp3FDcZCWINUKCEXc
      if(plog_flag==0){
        //baseline, read modify write
        //get,c_id,2957,5,1
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=7){
          ErrorDetect();
        }
        int pk1 = atoi(res[4].c_str()); //cw
        int pk2 = atoi(res[3].c_str()); //cd
        int pk3 = atoi(res[2].c_str()); //cid
        snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);

        auto t1 = std::chrono::steady_clock::now();
        std::string value;
        if(gb_flag == 0){
          CustomerTree->Get(k,value);
          if(value.size()!=325){
          static_tree::customer_get_not_found++;
            ErrorDetect();
          }
                
          memcpy(&updated_value[0],value.c_str(),value.size());
          
          snprintf(balance,15,"%14s",res[5]);
          memcpy(&updated_value[16+2+16+20+20+20+2+9+16+20+2+16+6],&balance[0],14);
          
          
          snprintf(data,101,"%100s",res[6].c_str());
          memcpy(&updated_value[16+2+16+20+20+20+2+9+16+20+2+16+6+14+14+16+16],data,100);

          off_t lsn = CustomerTree->GetLSN();
          std::string tmp_value(&updated_value[0],&updated_value[0]+325);
          CustomerTree->Put(k, tmp_value, lsn);
          CustomerTree->IncreLSN();

          //continue;
        }else{
          //update conducted with gb involving
          //1.1 get the involved page
          off_t leaf_of = CustomerTree->GetOnlyLeafOffset(k);
          //map id: customer 00
          //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
          char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of); 
          if(leaf_buf!=nullptr){
            //can be found in gb
            
            CustomerTree->GetRowRecord(k,value,leaf_buf);
            if(value.size()!=325){
            static_tree::customer_get_not_found++;
              ErrorDetect();
            }      
            memcpy(&updated_value[0],value.c_str(),value.size());
            snprintf(balance,15,"%14s",res[5]);
            memcpy(&updated_value[16+2+16+20+20+20+2+9+16+20+2+16+6],&balance[0],14);
            snprintf(data,101,"%100s",res[6].c_str());
            memcpy(&updated_value[16+2+16+20+20+20+2+9+16+20+2+16+6+14+14+16+16],data,100);
            off_t lsn = CustomerTree->GetLSN();
            std::string tmp_value(&updated_value[0],&updated_value[0]+325);
            CustomerTree->UpdateLeafCopy(k,tmp_value,lsn,leaf_buf);
            //CustomerTree->Put(k, tmp_value, lsn);
            CustomerTree->IncreLSN();
            //update LRU order
            gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }else{
            //register the page to gb
            char* leaf_buf = CustomerTree->GetLeafCopy(k);
            CustomerTree->GetRowRecord(k,value,leaf_buf);
            if(value.size()!=325){
            static_tree::customer_get_not_found++;
              ErrorDetect();
            }      
            memcpy(&updated_value[0],value.c_str(),value.size());
            snprintf(balance,15,"%14s",res[5]);
            memcpy(&updated_value[16+2+16+20+20+20+2+9+16+20+2+16+6],&balance[0],14);
            snprintf(data,101,"%100s",res[6].c_str());
            memcpy(&updated_value[16+2+16+20+20+20+2+9+16+20+2+16+6+14+14+16+16],data,100);
            off_t lsn = CustomerTree->GetLSN();
            std::string tmp_value(&updated_value[0],&updated_value[0]+325);
            CustomerTree->UpdateLeafCopy(k,tmp_value,lsn,leaf_buf);
            //CustomerTree->Put(k, tmp_value, lsn);
            CustomerTree->IncreLSN();
            //this page is newly inserted into gb
            gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }
        }
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::customer_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::customer_update_cnt++;
      
      
      }else{
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=7){
          ErrorDetect();
        }
        int pk1 = atoi(res[4].c_str()); //cw
        int pk2 = atoi(res[3].c_str()); //cd
        int pk3 = atoi(res[2].c_str()); //cid
        snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);
        
        
        snprintf(balance,15,"%14s",res[5]);
        updated_value[0] = 'U';
        updated_value[1] = ',';
        memcpy(&updated_value[2],&balance[0],14);
        snprintf(data,101,"%100s",res[6].c_str());
        memcpy(&updated_value[16],data,100);
        std::string tmp_value(&updated_value[0],&updated_value[0]+325); 
        int lsn = CustomerTree->GetLSN();
        auto t1 = std::chrono::steady_clock::now();
        CustomerTree->Put(k, tmp_value, lsn);
        CustomerTree->IncreLSN();
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::customer_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::customer_update_cnt++;
      }

      
    }else if (KeyWordExist(cmd,2)){
      //get,customer,c_id,2957,5,1
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=6){
        ErrorDetect();
      }
      int pk1 = atoi(res[5].c_str()); //cw
      int pk2 = atoi(res[4].c_str()); //cd
      int pk3 = atoi(res[3].c_str()); //cid
      snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);
      std::string value;
      auto t1 = std::chrono::steady_clock::now();
      if(gb_flag ==0){
        CustomerTree->Get(k,value);
        if(value.size()!=325){
          int x=0;
        }
      }else{
        off_t leaf_of = CustomerTree->GetOnlyLeafOffset(k);
        //map id: customer 00
        //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
        char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
        if(leaf_buf!=nullptr){
          //can be found in gb
          
          CustomerTree->GetRowRecord(k,value,leaf_buf);
          //update LRU order
          gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }else{
          //register the page to gb
          char* leaf_buf = CustomerTree->GetLeafCopy(k);
          CustomerTree->GetRowRecord(k,value,leaf_buf);
          //this page is newly inserted into gb
          gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }

      }
      auto t2 = std::chrono::steady_clock::now(); 
      static_tree::customer_get_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
      static_tree::customer_get_cnt++;
      //value[0] = value[1];
    }else if (KeyWordExist(cmd,3)){

      //scan,customer,c_id,100,5,2,60,7,4
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=9){
        ErrorDetect();
      }
      int pk1_1 = atoi(res[5].c_str()); //cw
      int pk1_2 = atoi(res[4].c_str()); //cd
      int pk1_start = atoi(res[3].c_str()); //end
      int pk2_1 = atoi(res[8].c_str()); //cw
      int pk2_2 = atoi(res[7].c_str()); //cd
      int pk2_end = atoi(res[6].c_str()); //end
      
      char start[49] = {'\0'};
      char end[49] = {'\0'};
      snprintf(start, 49, "%16d%16d%16d", pk1_1,pk1_2,pk1_start);
      snprintf(end, 49, "%16d%16d%16d", pk2_1,pk2_2,pk2_end);
      std::string value;
      auto t1_c = std::chrono::steady_clock::now();
      CustomerTree->Scan(start,end,value);
      //value[0] = value[1];
      auto t2_c = std::chrono::steady_clock::now(); 
      if(pk1_1==pk2_1 && pk1_2 == pk2_2){
        static_tree::customer_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2_c - t1_c).count()/1000;
        static_tree::customer_scan_cnt_c++;
      }else{
        static_tree::customer_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_c - t1_c).count()/1000;
        static_tree::customer_scan_cnt_h++;
      }
    }else{
      ErrorDetect();
    }
    delete cmd;
    // SELECT c_data FROM customer WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 777
    //SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 777 FOR UPDATE
  }
  return;

}

void Consumer_CustomerTree_MVCC_TEST(ProAndCon* itemQ_, GlobalBuffer* gb_, const int plog_flag_){
  // Schema  
/*
create table customer (
c_id int not null, 16
c_d_id tinyint not null,  16
c_w_id smallint not null, 16

c_first varchar(16),  16
c_middle char(2),   2
c_last varchar(16), 16
c_street_1 varchar(20), 20 
c_street_2 varchar(20),  20
c_city varchar(20), 20
c_state char(2),        2
c_zip char(9),          9
c_phone char(16),       16
c_since datetime,       20
c_credit char(2),       2
c_credit_lim bigint,    16
c_discount decimal(4,2), 6
c_balance decimal(12,2), 14
c_ytd_payment decimal(12,2), 14
c_payment_cnt smallint, 16
c_delivery_cnt smallint, 16
c_data text,  100 // for text >100, simply discard
PRIMARY KEY(c_w_id, c_d_id, c_id) ) Engine=InnoDB
*/

//FROM customer, warehouse WHERE 
//w_id = 1 AND c_w_id = w_id AND c_d_id = 5 AND c_id = 1026


  //std::cout << "+++++++++++++++ TEST Customer Tree BEGIN ++++++++++++++++++++\n";
  std::string CustomerDir = "/media/hkc/csd-7.7T/htap/test";
  std::string DB_path = CustomerDir + "Customer/";
  std::string db = DB_path + "testCustomer.db";
  std::string log = DB_path + "testCustomer.log";
  std::string chpt = DB_path + "chptCustomer.log";
  std::string sha_plog = DB_path + "shaPlogCustomer.log";
  CustomerTree = new BPlusCustomerTree(db.c_str(), log.c_str(), chpt.c_str(), bg_id++, true,sha_plog.c_str());
  // load
  CustomerTree->SetBaseline();
  CustomerTree->SetLoadFlag(1);
  //bpt->SetLoadFlag(0);
  //bpt->SetPlog();
  //
  int load_flag =1;
  int plog_flag = plog_flag_;
  char k[49] = {'\0'};
  char v[326] = {'\0'};
  char updated_value[326] = {'\0'};
  char balance[15]={'\0'};
  char data[101]={'\0'};
  int gb_flag =0;
  int map_id =0;
  while(1){
    std::string* cmd = itemQ_->consumption();
    //std::string cmd(cmd_char, cmd_char + strlen(cmd_char));
    if(cmd->size()==0){
        ErrorDetect();
        delete cmd;
        continue;
    }

    if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
          if(load_flag==1){
            load_flag=0;
            gb_flag = 1;
            CustomerTree->WarmUpVirtualPageCache(1);
          }
          if(plog_flag==1){
            CustomerTree->SetLoadFlag(0);
            CustomerTree->SetPlog();
            plog_flag =2;
          }
    }
      

        //key word:
      //1: put
      //2: get
      //3:scan
      //4.update


    if(KeyWordExist(cmd,1)){
      //put,customer,1(cid),1(cd),1(cw),rk4o0bDasnuUY,OE,BARBARBAR,drcT5CErowB5yMEs,3arraS38Kq0mOp,GPrLa5AO0LSwfgpShNK,wm,098594856,6775361462581630,2023-02-26 21:56:14,BC,50000,0.2199999988079071,-10, 10.0, 1, 0,47F7XKxNCCSRGpHPNTXF7bnNUlMi3rt7y8DTQ8raIH1Y4GvRjSho3SBVDVCGN3LJBYnZec9wr8UtNQL6q0tsS2NfYXttaECjlxIP
      //std::string contents = cmd->substr((*cmd).find("(") + 1, (*cmd).find(")") - 2);
      //contents = contents.replace(contents.find(")"), 1, "");
      //replace(contents.begin(), contents.end(), '\'', ',');
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=23){
        ErrorDetect();
      }
      int pk1 = atoi(res[4].c_str()); //cw
      int pk2 = atoi(res[3].c_str()); //cd
      int pk3 = atoi(res[2].c_str()); //cid
      snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);
      //snprintf(v, 16 + 20 + 16 + 16 +16, "%16s%20s%16s%16s%16s", i, rand() % 10000000, rand() % 10000000, rand() % 10000, rand() % 100000000,rand()%10000000);
      off_t lsn = CustomerTree->GetLSN();
      snprintf(v, 16+2+16+20+20+20+2+9+16+20+2+16+6+14+14+16+16+100+1, 
                  "%16s%2s%16s%20s%20s%20s%2s%9s%16s%20s%2s%16s%6s%14s%14s%16s%16s%100s", 
          res[5].c_str(), 
          res[6].c_str(),
          res[7].c_str(),
          res[8].c_str(),
          res[9].c_str(),
          res[10].c_str(),
          res[11].c_str(),
          res[12].c_str(),
          res[13].c_str(),
          res[14].c_str(),
          res[15].c_str(),
          res[16].c_str(),
          res[17].c_str(),
          res[18].c_str(),
          res[19].c_str(),
          res[20].c_str(),
          res[21].c_str(),
          res[22].c_str()
          );
      auto t1 = std::chrono::steady_clock::now();
      if(gb_flag == 0 || plog_flag == 2){
        CustomerTree->Put(k, v, lsn);
      }else{
        off_t leaf_of = CustomerTree->GetOnlyLeafOffset(k);
        //map id: customer 00
        //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
        char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of); 
        if(leaf_buf!=nullptr){
          //can be found in gb
          
          CustomerTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
          //update LRU order
          gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }else{
          //register the page to gb
          char* leaf_buf = CustomerTree->GetLeafCopy(k);
          CustomerTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
          //this page is newly inserted into gb
          gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }
      }
      auto t2 = std::chrono::steady_clock::now(); 
      if(load_flag ==0){
        static_tree::customer_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::customer_put_cnt++;
      }
      CustomerTree->IncreLSN();


      
    }else if (KeyWordExist(cmd,4)){
      //update,customer,249,5,2,-15403,,flvQjsz7ADcUgKul18DtmQq9BwuMaKOF5KfmCdtKoToVohGnnSgZqUh1QbMyukBz2rkFSdXG7Kjt1ygoQKMGp3FDcZCWINUKCEXc
      if(plog_flag==0){
        //baseline, read modify write
        //get,c_id,2957,5,1
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=7){
          ErrorDetect();
        }
        int pk1 = atoi(res[4].c_str()); //cw
        int pk2 = atoi(res[3].c_str()); //cd
        int pk3 = atoi(res[2].c_str()); //cid
        snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);

        auto t1 = std::chrono::steady_clock::now();
        std::string value;
        if(gb_flag == 0){
          CustomerTree->Get(k,value);
          if(value.size()!=325){
          static_tree::customer_get_not_found++;
            ErrorDetect();
          }
                
          memcpy(&updated_value[0],value.c_str(),value.size());
          
          snprintf(balance,15,"%14s",res[5]);
          memcpy(&updated_value[16+2+16+20+20+20+2+9+16+20+2+16+6],&balance[0],14);
          
          
          snprintf(data,101,"%100s",res[6].c_str());
          memcpy(&updated_value[16+2+16+20+20+20+2+9+16+20+2+16+6+14+14+16+16],data,100);

          off_t lsn = CustomerTree->GetLSN();
          std::string tmp_value(&updated_value[0],&updated_value[0]+325);
          CustomerTree->Put(k, tmp_value, lsn);
          CustomerTree->IncreLSN();

          //continue;
        }else{
          //update conducted with gb involving
          //1.1 get the involved page
          off_t leaf_of = CustomerTree->GetOnlyLeafOffset(k);
          //map id: customer 00
          //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
          char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of); 
          if(leaf_buf!=nullptr){
            //can be found in gb
            
            CustomerTree->GetRowRecord(k,value,leaf_buf);
            if(value.size()!=325){
            static_tree::customer_get_not_found++;
              ErrorDetect();
            }      
            memcpy(&updated_value[0],value.c_str(),value.size());
            snprintf(balance,15,"%14s",res[5]);
            memcpy(&updated_value[16+2+16+20+20+20+2+9+16+20+2+16+6],&balance[0],14);
            snprintf(data,101,"%100s",res[6].c_str());
            memcpy(&updated_value[16+2+16+20+20+20+2+9+16+20+2+16+6+14+14+16+16],data,100);
            off_t lsn = CustomerTree->GetLSN();
            std::string tmp_value(&updated_value[0],&updated_value[0]+325);
            CustomerTree->UpdateLeafCopy(k,tmp_value,lsn,leaf_buf);
            //CustomerTree->Put(k, tmp_value, lsn);
            CustomerTree->IncreLSN();
            //update LRU order
            gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }else{
            //register the page to gb
            char* leaf_buf = CustomerTree->GetLeafCopy(k);
            CustomerTree->GetRowRecord(k,value,leaf_buf);
            if(value.size()!=325){
            static_tree::customer_get_not_found++;
              ErrorDetect();
            }      
            memcpy(&updated_value[0],value.c_str(),value.size());
            snprintf(balance,15,"%14s",res[5]);
            memcpy(&updated_value[16+2+16+20+20+20+2+9+16+20+2+16+6],&balance[0],14);
            snprintf(data,101,"%100s",res[6].c_str());
            memcpy(&updated_value[16+2+16+20+20+20+2+9+16+20+2+16+6+14+14+16+16],data,100);
            off_t lsn = CustomerTree->GetLSN();
            std::string tmp_value(&updated_value[0],&updated_value[0]+325);
            CustomerTree->UpdateLeafCopy(k,tmp_value,lsn,leaf_buf);
            //CustomerTree->Put(k, tmp_value, lsn);
            CustomerTree->IncreLSN();
            //this page is newly inserted into gb
            gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }
        }
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::customer_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::customer_update_cnt++;
      
      
      }else{
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=7){
          ErrorDetect();
        }
        int pk1 = atoi(res[4].c_str()); //cw
        int pk2 = atoi(res[3].c_str()); //cd
        int pk3 = atoi(res[2].c_str()); //cid
        snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);
        
        
        snprintf(balance,15,"%14s",res[5]);
        updated_value[0] = 'U';
        updated_value[1] = ',';
        memcpy(&updated_value[2],&balance[0],14);
        snprintf(data,101,"%100s",res[6].c_str());
        memcpy(&updated_value[16],data,100);
        std::string tmp_value(&updated_value[0],&updated_value[0]+325); 
        int lsn = CustomerTree->GetLSN();
        auto t1 = std::chrono::steady_clock::now();
        CustomerTree->Put(k, tmp_value, lsn);
        CustomerTree->IncreLSN();
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::customer_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::customer_update_cnt++;
      }

      
    }else if (KeyWordExist(cmd,2)){
      //get,customer,c_id,2957,5,1
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=6){
        ErrorDetect();
      }
      int pk1 = atoi(res[5].c_str()); //cw
      int pk2 = atoi(res[4].c_str()); //cd
      int pk3 = atoi(res[3].c_str()); //cid
      snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);
      std::string value;
      auto t1 = std::chrono::steady_clock::now();
      if(gb_flag ==0){
        CustomerTree->Get(k,value);
        if(value.size()!=325){
          int x=0;
        }
      }else{
        off_t leaf_of = CustomerTree->GetOnlyLeafOffset(k);
        //map id: customer 00
        //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
        char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
        if(leaf_buf!=nullptr){
          //can be found in gb
          
          CustomerTree->GetRowRecord(k,value,leaf_buf);
          //update LRU order
          gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }else{
          //register the page to gb
          char* leaf_buf = CustomerTree->GetLeafCopy(k);
          CustomerTree->GetRowRecord(k,value,leaf_buf);
          //this page is newly inserted into gb
          gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }

      }
      auto t2 = std::chrono::steady_clock::now(); 
      static_tree::customer_get_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
      static_tree::customer_get_cnt++;
      //value[0] = value[1];
    }else if (KeyWordExist(cmd,3)){

      //scan,customer,c_id,100,5,2,60,7,4
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=9){
        ErrorDetect();
      }
      int pk1_1 = atoi(res[5].c_str()); //cw
      int pk1_2 = atoi(res[4].c_str()); //cd
      int pk1_start = atoi(res[3].c_str()); //end
      int pk2_1 = atoi(res[8].c_str()); //cw
      int pk2_2 = atoi(res[7].c_str()); //cd
      int pk2_end = atoi(res[6].c_str()); //end
      
      char start[49] = {'\0'};
      char end[49] = {'\0'};
      snprintf(start, 49, "%16d%16d%16d", pk1_1,pk1_2,pk1_start);
      snprintf(end, 49, "%16d%16d%16d", pk2_1,pk2_2,pk2_end);
      std::string value;
      auto t1_c = std::chrono::steady_clock::now();
      CustomerTree->Scan(start,end,value);
      //value[0] = value[1];
      auto t2_c = std::chrono::steady_clock::now(); 
      if(pk1_1==pk2_1 && pk1_2 == pk2_2){
        static_tree::customer_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2_c - t1_c).count()/1000;
        static_tree::customer_scan_cnt_c++;
      }else{
        static_tree::customer_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_c - t1_c).count()/1000;
        static_tree::customer_scan_cnt_h++;
      }
    }else if (KeyWordExist(cmd,5)){

      //scan,customer,c_id,100,5,2,60,7,4
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=9){
        ErrorDetect();
      }
      int pk1_1 = atoi(res[5].c_str()); //cw
      int pk1_2 = atoi(res[4].c_str()); //cd
      int pk1_start = atoi(res[3].c_str()); //end
      int pk2_1 = atoi(res[8].c_str()); //cw
      int pk2_2 = atoi(res[7].c_str()); //cd
      int pk2_end = atoi(res[6].c_str()); //end
      
      char start[49] = {'\0'};
      char end[49] = {'\0'};
      snprintf(start, 49, "%16d%16d%16d", pk1_1,pk1_2,pk1_start);
      snprintf(end, 49, "%16d%16d%16d", pk2_1,pk2_2,pk2_end);
      std::string value;
      CustomerTree->ScanAP(start,end,value);
    }else{
      ErrorDetect();
    }
    delete cmd;
    // SELECT c_data FROM customer WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 777
    //SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 777 FOR UPDATE
  }
  return;

}



void Consumer_HistoryTree(ProAndCon* itemQ_,GlobalBuffer* gb_, const int plog_flag_){
  //  create table history (
  //  h_c_id int,
  //  h_c_d_id tinyint,
  //  h_c_w_id smallint,

  //  h_d_id tinyint,
  //  h_w_id smallint,
  //  h_date datetime,
  //  h_amount decimal(6,2),
  //  h_data varchar(24) ) Engine=InnoDB
  //  ADD CONSTRAINT fkey_history_1 FOREIGN KEY(h_c_w_id,h_c_d_id,h_c_id) REFERENCES customer(c_w_id,c_d_id,c_id)
  //	ALTER TABLE history  ADD CONSTRAINT fkey_history_2 FOREIGN KEY(h_w_id,h_d_id) REFERENCES district(d_w_id,d_id)
  //std::cout << "+++++++++++++++ TEST Historyfk1 Tree BEGIN ++++++++++++++++++++\n";
  
  //init fk1
  std::string Historyfk1Dir1 = "/media/hkc/csd-7.7T/htap/test";
  std::string DB_path1 = Historyfk1Dir1 + "Historyfk1/";
  std::string db1 = DB_path1 + "testHistoryfk1.db";
  std::string log1 = DB_path1 + "testHistoryfk1.log";
  std::string chpt1 = DB_path1 + "chptHistoryfk1.log";
  std::string sha_plog_1 = DB_path1 + "shaPlogHistoryfk1.log";
  Historyfk1Tree = new BPlusHistoryfk1Tree(db1.c_str(), log1.c_str(), chpt1.c_str(), bg_id++, true,sha_plog_1.c_str());
  // load
  Historyfk1Tree->SetBaseline();
  Historyfk1Tree->SetLoadFlag(1);
  
  //init fk2
  std::string Historyfk2Dir2 = "/media/hkc/csd-7.7T/htap/test";
  std::string DB_path2 = Historyfk2Dir2 + "Historyfk2/";
  std::string db2 = DB_path2 + "testHistoryfk2.db";
  std::string log2 = DB_path2 + "testHistoryfk2.log";
  std::string chpt2 = DB_path2 + "chptHistoryfk2.log";
  std::string sha_plog_2 = DB_path2 + "shaPlogHistoryfk2.log";
  Historyfk2Tree = new BPlusHistoryfk2Tree(db2.c_str(), log2.c_str(), chpt2.c_str(), bg_id++, true,sha_plog_2.c_str());
  // load
  Historyfk2Tree->SetBaseline();
  Historyfk2Tree->SetLoadFlag(1);



  int load_flag =1;
  int plog_flag = plog_flag_;
  int gb_flag =0;
  int map_id_1 = 6;
  int map_id_2 = 7;
  char k1[49] = {'\0'};
  char v1[85] = {'\0'};
  char k2[33] = {'\0'};
  char v2[101] = {'\0'};
  while(1){
    std::string* cmd = itemQ_->consumption();
    if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        load_flag=0;
        gb_flag =1;
        Historyfk1Tree->WarmUpVirtualPageCache(1);
        Historyfk2Tree->WarmUpVirtualPageCache(1);
        if(plog_flag==1){
          Historyfk1Tree->SetLoadFlag(0);
          Historyfk2Tree->SetLoadFlag(0);
          Historyfk1Tree->SetPlog();
          Historyfk2Tree->SetPlog();
          plog_flag =2;
        }
    }
    if(cmd->size()==0){
        ErrorDetect();
        delete cmd;
        continue;
    }
    if(KeyWordExist(cmd,1)){
      //put,history,1888,3,8,num1,num2,ts,num3,num4
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=10){
        ErrorDetect();
      }
      int pk1 = atoi(res[4].c_str()); //cw
      int pk2 = atoi(res[3].c_str()); //cd
      int pk3 = atoi(res[2].c_str()); //cid
      snprintf(k1, 49, "%16d%16d%16d", pk1,pk2,pk3);
      snprintf(k2, 33, "%16d%16d", pk1,pk2);
      off_t lsn1 = Historyfk1Tree->GetLSN();
      off_t lsn2 = Historyfk2Tree->GetLSN();
      snprintf(v1, 16+16+20+8+24+1, 
                  "%16s%16s%20s%8s%24s", 
          res[5].c_str(), 
          res[6].c_str(),
          res[7].c_str(),
          res[8].c_str(),
          res[9].c_str()
          );
      snprintf(v2, 16+16+16+20+8+24+1, 
                  "%16s%16s%16s%20s%8s%24s", 
          res[4].c_str(), 
          res[5].c_str(), 
          res[6].c_str(),
          res[7].c_str(),
          res[8].c_str(),
          res[9].c_str()
          );
      auto t1 = std::chrono::steady_clock::now();
      if(gb_flag == 0 || plog_flag == 2){
        Historyfk1Tree->Put(k1, v1, lsn1);
        Historyfk2Tree->Put(k2, v2, lsn2);
      }else{
        off_t leaf_of_1 = Historyfk1Tree->GetOnlyLeafOffset(k1);
        //map id: shitory1 06
        //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
        char* leaf_buf_1 = gb_->GetPageCopy(map_id_1,leaf_of_1);
        if(leaf_buf_1!=nullptr){
          //can be found in gb
          //char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
          Historyfk1Tree ->UpdateLeafCopy(k1,v1,lsn1,leaf_buf_1);
          //update LRU order
          gb_->UpdateLRUOrder(map_id_1,leaf_of_1,leaf_buf_1);
          free(leaf_buf_1);
        }else{
          //register the page to gb
          char* leaf_buf_1 = Historyfk1Tree->GetLeafCopy(k1);
          Historyfk1Tree->UpdateLeafCopy(k1,v1,lsn1,leaf_buf_1);
          //this page is newly inserted into gb
          gb_->InsertIntoGlobalBuffer(map_id_1,leaf_of_1,leaf_buf_1);
          free(leaf_buf_1);
        }

        off_t leaf_of_2 = Historyfk2Tree->GetOnlyLeafOffset(k2);
        //map id: history2 07
        //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
        char* leaf_buf_2 = gb_->GetPageCopy(map_id_2,leaf_of_2);
        if(leaf_buf_2!=nullptr){
          //can be found in gb
          //char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
          Historyfk2Tree->UpdateLeafCopy(k2,v2,lsn2,leaf_buf_2);
          //update LRU order
          gb_->UpdateLRUOrder(map_id_2,leaf_of_2,leaf_buf_2);
          free(leaf_buf_2);
        }else{
          //register the page to gb
          char* leaf_buf_2 = Historyfk2Tree->GetLeafCopy(k2);
          Historyfk2Tree->UpdateLeafCopy(k2,v2,lsn2,leaf_buf_2);
          //this page is newly inserted into gb
          gb_->InsertIntoGlobalBuffer(map_id_2,leaf_of_2,leaf_buf_2);
          free(leaf_buf_2);
        }
      }
      
      auto t2 = std::chrono::steady_clock::now(); 
      if(load_flag ==0){
        static_tree::history_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::history_put_cnt++;
      }
      Historyfk1Tree->IncreLSN();
      Historyfk2Tree->IncreLSN();
    }else{
      ErrorDetect();
    }
    delete cmd;
  }
  return;

}

/*
void Consumer_Historyfk2Tree(ProAndCon* itemQ_){
  //  create table history (
  //  h_c_id int,
  //  h_c_d_id tinyint,
  //  h_c_w_id smallint,

  //  h_d_id tinyint,
  //  h_w_id smallint,
  //  h_date datetime,
  //  h_amount decimal(6,2),
  //  h_data varchar(24) ) Engine=InnoDB
  //  ADD CONSTRAINT fkey_history_1 FOREIGN KEY(h_c_w_id,h_c_d_id,h_c_id) REFERENCES customer(c_w_id,c_d_id,c_id)
  //	ALTER TABLE history  ADD CONSTRAINT fkey_history_2 FOREIGN KEY(h_w_id,h_d_id) REFERENCES district(d_w_id,d_id)
  //std::cout << "+++++++++++++++ TEST Historyfk2 Tree BEGIN ++++++++++++++++++++\n";
  std::string Historyfk2Dir = "/media/hkc/csd-7.7T/htap/test";
  std::string DB_path = Historyfk2Dir + "Historyfk2/";
  std::string db = DB_path + "testHistoryfk2.db";
  std::string log = DB_path + "testHistoryfk2.log";
  std::string chpt = DB_path + "chptHistoryfk2.log";
  Historyfk2Tree = new BPlusHistoryfk2Tree(db.c_str(), log.c_str(), chpt.c_str(), bg_id++, true);
  // load
  Historyfk2Tree->SetBaseline();
  Historyfk2Tree->SetLoadFlag(1);
  char k[32] = {'\0'};
  char v[16+16+16+20+8+24] = {'\0'};
  while(1){
    std::string* cmd = itemQ_->consumption();
    
    delete cmd;
  }
  return;

}
*/
void Consumer_OrderlineTree(ProAndCon* itemQ_,GlobalBuffer* gb_, const int plog_flag_){
  //  create table order_line (
  //  ol_o_id int not null, 16
  //  ol_d_id tinyint not null, 16
  //  ol_w_id smallint not null, 16
  //  ol_number tinyint not null, 16
  
  //  ol_i_id int, 16
  //  ol_supply_w_id smallint, 16
  //  ol_delivery_d datetime, 20
  //  ol_quantity tinyint, 16
  //  ol_amount decimal(6,2), 8
  //  ol_dist_info char(24), 24
  //  PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number) ) Engine=InnoDB

  //std::cout << "+++++++++++++++ TEST OrderLine Tree BEGIN ++++++++++++++++++++\n";
  std::string OrderLineDir = "/media/hkc/csd-7.7T/htap/test";
  std::string DB_path = OrderLineDir + "OrderLine/";
  std::string db = DB_path + "testOrderLine.db";
  std::string log = DB_path + "testOrderLine.log";
  std::string chpt = DB_path + "chptOrderLine.log";
  std::string sha_plog = DB_path + "shaPlogOrderLine.log";
  OrderLineTree = new BPlusOrderlineTree(db.c_str(), log.c_str(), chpt.c_str(), bg_id++, true, sha_plog.c_str());
  // load
  OrderLineTree->SetBaseline();
  OrderLineTree->SetLoadFlag(1);

  int load_flag =1;
  int plog_flag = plog_flag_;
  int gb_flag =0;
  int map_id = 3;
  char k[65] = {'\0'};
  char v[16 + 16+ 20 + 16 + 8 + 24 + 1] = {'\0'};
  char updated_value[101] = {'\0'};
  char tmp_time[21]={'\0'};
  while(1){
    std::string* cmd = itemQ_->consumption();
    if(cmd->size()==0){
        ErrorDetect();
        delete cmd;
        continue;
    }
    if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        if(load_flag==1){
          load_flag=0;
          gb_flag = 1;
          OrderLineTree->WarmUpVirtualPageCache(1);
        }
        if(plog_flag==1){
          OrderLineTree->SetLoadFlag(0);
          OrderLineTree->SetPlog();
          plog_flag =2;
        }
    }

      //key word:
      //1: put
      //2: get
      //3:scan
      //4.update
    if(KeyWordExist(cmd,1)){
      //put,orderline,[0-3000],[0,10],[0-w],[0-20],num5,num2, ts,num2,num8,strnum24
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=12){
        ErrorDetect();
      }
      int pk1 = atoi(res[4].c_str()); //cw
      int pk2 = atoi(res[3].c_str()); //cd id
      int pk3 = atoi(res[2].c_str()); //cid
      int pk4 = atoi(res[5].c_str()); //id
      snprintf(k, 65, "%16d%16d%16d%16d", pk1,pk2,pk3,pk4);
      off_t lsn = OrderLineTree->GetLSN();
      snprintf(v, 16+16+20+16+8+24+1, 
                  "%16s%16s%20s%16s%8s%24s", 
          res[6].c_str(),
          res[7].c_str(),
          res[8].c_str(),
          res[9].c_str(),
          res[10].c_str(),
          res[11].c_str()
          );
      auto t1 = std::chrono::steady_clock::now();
      if(gb_flag == 0 || plog_flag == 2){
        OrderLineTree->Put(k, v, lsn);
      }else{
        off_t leaf_of = OrderLineTree->GetOnlyLeafOffset(k);
        //map id: orderline 03
        //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
        char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
        if(leaf_buf!=nullptr){
          //can be found in gb
          //char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
          OrderLineTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
          //update LRU order
          gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }else{
          //register the page to gb
          char* leaf_buf = OrderLineTree->GetLeafCopy(k);
          OrderLineTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
          //this page is newly inserted into gb
          gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }
      }
      
      auto t2 = std::chrono::steady_clock::now(); 
      if(load_flag ==0){
        static_tree::orderline_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::orderline_put_cnt++;
      }
      OrderLineTree->IncreLSN();
    }else if (KeyWordExist(cmd,4)){
      //update,orderline,[0-3000],[0,10],[0-w],[0-20],ts
      if(plog_flag==0){
        //baseline, read modify write
        //get,c_id,2957,5,1
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=7){
          ErrorDetect();
        }
        int pk1 = atoi(res[4].c_str()); //cw
        int pk2 = atoi(res[3].c_str()); //cd
        int pk3 = atoi(res[2].c_str()); //cid
        int pk4 = atoi(res[5].c_str()); //cid
        snprintf(k, 65, "%16d%16d%16d%16d", pk1,pk2,pk3,pk4);

        auto t1 = std::chrono::steady_clock::now();
        std::string value;

        if(gb_flag == 0){
          OrderLineTree->Get(k,value);
          if(value.size()!=100){
          static_tree::orderline_get_not_found++;
            ErrorDetect();
          }
                
          memcpy(&updated_value[0],value.c_str(),value.size());
          
          snprintf(tmp_time,21,"%20s",res[6]);
          memcpy(&updated_value[16+16],&tmp_time[0],20);
          
          
          
          off_t lsn = OrderLineTree->GetLSN();
          std::string tmp_value(&updated_value[0],&updated_value[0]+100);
          OrderLineTree->Put(k, tmp_value, lsn);
          OrderLineTree->IncreLSN();
        }else{
          off_t leaf_of = OrderLineTree->GetOnlyLeafOffset(k);
          char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
          if(leaf_buf!= nullptr){
            OrderLineTree->GetRowRecord(k,value,leaf_buf);
            memcpy(&updated_value[0],value.c_str(),value.size());
            snprintf(tmp_time,21,"%20s",res[6]);
            memcpy(&updated_value[16+16],&tmp_time[0],20);
            off_t lsn = OrderLineTree->GetLSN();
            std::string tmp_value(&updated_value[0],&updated_value[0]+100);
            OrderLineTree->UpdateLeafCopy(k,tmp_value,lsn,leaf_buf);
            OrderLineTree->IncreLSN();
            gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }else{
            char* leaf_buf = OrderLineTree->GetLeafCopy(k);
            OrderLineTree->GetRowRecord(k,value,leaf_buf);
            memcpy(&updated_value[0],value.c_str(),value.size());
            snprintf(tmp_time,21,"%20s",res[6]);
            memcpy(&updated_value[16+16],&tmp_time[0],20);
            off_t lsn = OrderLineTree->GetLSN();
            std::string tmp_value(&updated_value[0],&updated_value[0]+100);
            OrderLineTree->UpdateLeafCopy(k,tmp_value,lsn,leaf_buf);
            OrderLineTree->IncreLSN();
            gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }
        }

        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::orderline_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::orderline_update_cnt++;
        //continue;
        

      }else{
        //pending work!!! hkc 0418 for plog
        //plog blind update
        //update,customer,249,5,2,-15403,,flvQjsz7ADcUgKul18DtmQq9BwuMaKOF5KfmCdtKoToVohGnnSgZqUh1QbMyukBz2rkFSdXG7Kjt1ygoQKMGp3FDcZCWINUKCEXc
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=7){
          ErrorDetect();
        }
        int pk1 = atoi(res[4].c_str()); //cw
        int pk2 = atoi(res[3].c_str()); //cd
        int pk3 = atoi(res[2].c_str()); //cid
        int pk4 = atoi(res[5].c_str()); //cid
        snprintf(k, 65, "%16d%16d%16d%16d", pk1,pk2,pk3,pk4);
        
        
        snprintf(tmp_time,21,"%20s",res[6]);
        updated_value[0] = 'U';
        updated_value[1] = ',';
        memcpy(&updated_value[2],&tmp_time[0],20);
        std::string tmp_value(&updated_value[0],&updated_value[0]+100); 
        int lsn = OrderLineTree->GetLSN();
        auto t1 = std::chrono::steady_clock::now();
        OrderLineTree->Put(k, tmp_value, lsn);
        OrderLineTree->IncreLSN();
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::orderline_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::orderline_update_cnt++;
      }

      
    }else if (KeyWordExist(cmd,3)){
      //scan,orderline,1,1,2,60,4,7,1,200,2
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=10){
        ErrorDetect();
      }
      int pk1_1 = atoi(res[5].c_str()); //cw
      int pk1_2 = atoi(res[4].c_str()); //cd
      int pk1_3 = atoi(res[3].c_str()); //cd
      int pk1_4 = atoi(res[2].c_str()); //cd

      int pk2_1 = atoi(res[9].c_str()); //cw
      int pk2_2 = atoi(res[8].c_str()); //cd
      int pk2_3 = atoi(res[7].c_str()); //cd
      int pk2_4 = atoi(res[6].c_str()); //cd
      
      char start[65] = {'\0'};
      char end[65] = {'\0'};
      snprintf(start, 65, "%16d%16d%16d%16d", pk1_1,pk1_2,pk1_3,pk1_4);
      snprintf(end, 65, "%16d%16d%16d%16d", pk2_1,pk2_2,pk2_3,pk2_4);
      std::string value;
      auto t1_o = std::chrono::steady_clock::now();
      OrderLineTree->Scan(start,end,value);
      //value[0] = value[1];
      auto t2_o = std::chrono::steady_clock::now();
      if(pk1_1 == pk2_1 && pk2_1 == pk2_2){
        static_tree::orderline_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2_o - t1_o).count()/1000;
        static_tree::orderline_scan_cnt_c++;
      }else{
        static_tree::orderline_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_o - t1_o).count()/1000;
        static_tree::orderline_scan_cnt_h++;
      } 
      
    }else{
      ErrorDetect();
    }
    
    delete cmd;
  }
  return;

}



void Consumer_OrderlineTree_MVCC_TEST(ProAndCon* itemQ_,GlobalBuffer* gb_, const int plog_flag_){
  //  create table order_line (
  //  ol_o_id int not null, 16
  //  ol_d_id tinyint not null, 16
  //  ol_w_id smallint not null, 16
  //  ol_number tinyint not null, 16
  
  //  ol_i_id int, 16
  //  ol_supply_w_id smallint, 16
  //  ol_delivery_d datetime, 20
  //  ol_quantity tinyint, 16
  //  ol_amount decimal(6,2), 8
  //  ol_dist_info char(24), 24
  //  PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number) ) Engine=InnoDB

  //std::cout << "+++++++++++++++ TEST OrderLine Tree BEGIN ++++++++++++++++++++\n";
  std::string OrderLineDir = "/media/hkc/csd-7.7T/htap/test";
  std::string DB_path = OrderLineDir + "OrderLine/";
  std::string db = DB_path + "testOrderLine.db";
  std::string log = DB_path + "testOrderLine.log";
  std::string chpt = DB_path + "chptOrderLine.log";
  std::string sha_plog = DB_path + "shaPlogOrderLine.log";
  OrderLineTree = new BPlusOrderlineTree(db.c_str(), log.c_str(), chpt.c_str(), bg_id++, true, sha_plog.c_str());
  // load
  OrderLineTree->SetBaseline();
  OrderLineTree->SetLoadFlag(1);

  int load_flag =1;
  int plog_flag = plog_flag_;
  int gb_flag =0;
  int map_id = 3;
  char k[65] = {'\0'};
  char v[16 + 16+ 20 + 16 + 8 + 24 + 1] = {'\0'};
  char updated_value[101] = {'\0'};
  char tmp_time[21]={'\0'};
  while(1){
    std::string* cmd = itemQ_->consumption();
    if(cmd->size()==0){
        ErrorDetect();
        delete cmd;
        continue;
    }
    if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        if(load_flag==1){
          load_flag=0;
          gb_flag = 1;
          OrderLineTree->WarmUpVirtualPageCache(1);
        }
        if(plog_flag==1){
          OrderLineTree->SetLoadFlag(0);
          OrderLineTree->SetPlog();
          plog_flag =2;
        }
    }

      //key word:
      //1: put
      //2: get
      //3:scan
      //4.update
    if(KeyWordExist(cmd,1)){
      //put,orderline,[0-3000],[0,10],[0-w],[0-20],num5,num2, ts,num2,num8,strnum24
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=12){
        ErrorDetect();
      }
      int pk1 = atoi(res[4].c_str()); //cw
      int pk2 = atoi(res[3].c_str()); //cd id
      int pk3 = atoi(res[2].c_str()); //cid
      int pk4 = atoi(res[5].c_str()); //id
      snprintf(k, 65, "%16d%16d%16d%16d", pk1,pk2,pk3,pk4);
      off_t lsn = OrderLineTree->GetLSN();
      snprintf(v, 16+16+20+16+8+24+1, 
                  "%16s%16s%20s%16s%8s%24s", 
          res[6].c_str(),
          res[7].c_str(),
          res[8].c_str(),
          res[9].c_str(),
          res[10].c_str(),
          res[11].c_str()
          );
      auto t1 = std::chrono::steady_clock::now();
      if(gb_flag == 0 || plog_flag == 2){
        OrderLineTree->Put(k, v, lsn);
      }else{
        off_t leaf_of = OrderLineTree->GetOnlyLeafOffset(k);
        //map id: orderline 03
        //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
        char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
        if(leaf_buf!=nullptr){
          //can be found in gb
          //char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
          OrderLineTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
          //update LRU order
          gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }else{
          //register the page to gb
          char* leaf_buf = OrderLineTree->GetLeafCopy(k);
          OrderLineTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
          //this page is newly inserted into gb
          gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }
      }
      
      auto t2 = std::chrono::steady_clock::now(); 
      if(load_flag ==0){
        static_tree::orderline_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::orderline_put_cnt++;
      }
      OrderLineTree->IncreLSN();
    }else if (KeyWordExist(cmd,4)){
      //update,orderline,[0-3000],[0,10],[0-w],[0-20],ts
      if(plog_flag==0){
        //baseline, read modify write
        //get,c_id,2957,5,1
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=7){
          ErrorDetect();
        }
        int pk1 = atoi(res[4].c_str()); //cw
        int pk2 = atoi(res[3].c_str()); //cd
        int pk3 = atoi(res[2].c_str()); //cid
        int pk4 = atoi(res[5].c_str()); //cid
        snprintf(k, 65, "%16d%16d%16d%16d", pk1,pk2,pk3,pk4);

        auto t1 = std::chrono::steady_clock::now();
        std::string value;

        if(gb_flag == 0){
          OrderLineTree->Get(k,value);
          if(value.size()!=100){
          static_tree::orderline_get_not_found++;
            ErrorDetect();
          }
                
          memcpy(&updated_value[0],value.c_str(),value.size());
          
          snprintf(tmp_time,21,"%20s",res[6]);
          memcpy(&updated_value[16+16],&tmp_time[0],20);
          
          
          
          off_t lsn = OrderLineTree->GetLSN();
          std::string tmp_value(&updated_value[0],&updated_value[0]+100);
          OrderLineTree->Put(k, tmp_value, lsn);
          OrderLineTree->IncreLSN();
        }else{
          off_t leaf_of = OrderLineTree->GetOnlyLeafOffset(k);
          char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
          if(leaf_buf!= nullptr){
            OrderLineTree->GetRowRecord(k,value,leaf_buf);
            memcpy(&updated_value[0],value.c_str(),value.size());
            snprintf(tmp_time,21,"%20s",res[6]);
            memcpy(&updated_value[16+16],&tmp_time[0],20);
            off_t lsn = OrderLineTree->GetLSN();
            std::string tmp_value(&updated_value[0],&updated_value[0]+100);
            OrderLineTree->UpdateLeafCopy(k,tmp_value,lsn,leaf_buf);
            OrderLineTree->IncreLSN();
            gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }else{
            char* leaf_buf = OrderLineTree->GetLeafCopy(k);
            OrderLineTree->GetRowRecord(k,value,leaf_buf);
            memcpy(&updated_value[0],value.c_str(),value.size());
            snprintf(tmp_time,21,"%20s",res[6]);
            memcpy(&updated_value[16+16],&tmp_time[0],20);
            off_t lsn = OrderLineTree->GetLSN();
            std::string tmp_value(&updated_value[0],&updated_value[0]+100);
            OrderLineTree->UpdateLeafCopy(k,tmp_value,lsn,leaf_buf);
            OrderLineTree->IncreLSN();
            gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
            free(leaf_buf);
          }
        }

        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::orderline_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::orderline_update_cnt++;
        //continue;
        

      }else{
        //pending work!!! hkc 0418 for plog
        //plog blind update
        //update,customer,249,5,2,-15403,,flvQjsz7ADcUgKul18DtmQq9BwuMaKOF5KfmCdtKoToVohGnnSgZqUh1QbMyukBz2rkFSdXG7Kjt1ygoQKMGp3FDcZCWINUKCEXc
        std::vector<std::string> res = split(*cmd, ",");
        if(res.size()!=7){
          ErrorDetect();
        }
        int pk1 = atoi(res[4].c_str()); //cw
        int pk2 = atoi(res[3].c_str()); //cd
        int pk3 = atoi(res[2].c_str()); //cid
        int pk4 = atoi(res[5].c_str()); //cid
        snprintf(k, 65, "%16d%16d%16d%16d", pk1,pk2,pk3,pk4);
        
        
        snprintf(tmp_time,21,"%20s",res[6]);
        updated_value[0] = 'U';
        updated_value[1] = ',';
        memcpy(&updated_value[2],&tmp_time[0],20);
        std::string tmp_value(&updated_value[0],&updated_value[0]+100); 
        int lsn = OrderLineTree->GetLSN();
        auto t1 = std::chrono::steady_clock::now();
        OrderLineTree->Put(k, tmp_value, lsn);
        OrderLineTree->IncreLSN();
        auto t2 = std::chrono::steady_clock::now(); 
        static_tree::orderline_update_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::orderline_update_cnt++;
      }

      
    }else if (KeyWordExist(cmd,3)){
      //scan,orderline,1,1,2,60,4,7,1,200,2
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=10){
        ErrorDetect();
      }
      int pk1_1 = atoi(res[5].c_str()); //cw
      int pk1_2 = atoi(res[4].c_str()); //cd
      int pk1_3 = atoi(res[3].c_str()); //cd
      int pk1_4 = atoi(res[2].c_str()); //cd

      int pk2_1 = atoi(res[9].c_str()); //cw
      int pk2_2 = atoi(res[8].c_str()); //cd
      int pk2_3 = atoi(res[6].c_str()); //cd
      int pk2_4 = atoi(res[7].c_str()); //cd
      
      char start[65] = {'\0'};
      char end[65] = {'\0'};
      snprintf(start, 65, "%16d%16d%16d%16d", pk1_1,pk1_2,pk1_3,pk1_4);
      snprintf(end, 65, "%16d%16d%16d%16d", pk2_1,pk2_2,pk2_3,pk2_4);
      std::string value;
      auto t1_o = std::chrono::steady_clock::now();
      OrderLineTree->Scan(start,end,value);
      //value[0] = value[1];
      auto t2_o = std::chrono::steady_clock::now();
      if(pk1_1 == pk2_1 && pk2_1 == pk2_2){
        static_tree::orderline_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2_o - t1_o).count()/1000;
        static_tree::orderline_scan_cnt_c++;
      }else{
        static_tree::orderline_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_o - t1_o).count()/1000;
        static_tree::orderline_scan_cnt_h++;
      } 
      
    }else if (KeyWordExist(cmd,5)){
      //scan,orderline,1,1,2,60,4,7,1,200,2
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=10){
        ErrorDetect();
      }
      int pk1_1 = atoi(res[5].c_str()); //cw
      int pk1_2 = atoi(res[4].c_str()); //cd
      int pk1_3 = atoi(res[3].c_str()); //cd
      int pk1_4 = atoi(res[2].c_str()); //cd

      int pk2_1 = atoi(res[9].c_str()); //cw
      int pk2_2 = atoi(res[8].c_str()); //cd
      int pk2_3 = atoi(res[7].c_str()); //cd
      int pk2_4 = atoi(res[6].c_str()); //cd
      
      char start[65] = {'\0'};
      char end[65] = {'\0'};
      snprintf(start, 65, "%16d%16d%16d%16d", pk1_1,pk1_2,pk1_3,pk1_4);
      snprintf(end, 65, "%16d%16d%16d%16d", pk2_1,pk2_2,pk2_3,pk2_4);
      std::string value;
      //auto t1_o = std::chrono::steady_clock::now();
      //std::thread thd_ap = std::thread(ScanOrderLineAP, start,end,value);
      //thd_ap.detach();
      
      OrderLineTree->ScanAP(start,end,value);
      //value[0] = value[1];
      //auto t2_o = std::chrono::steady_clock::now();
      //if(pk1_1 == pk2_1 && pk2_1 == pk2_2){
      //  static_tree::orderline_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2_o - t1_o).count()/1000;
      //  static_tree::orderline_scan_cnt_c++;
      //}else{
      //  static_tree::orderline_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_o - t1_o).count()/1000;
      //  static_tree::orderline_scan_cnt_h++;
      //} 
      
    }else{
      ErrorDetect();
    }
    
    delete cmd;
  }
  return;

}



void Consumer_NewordersTree(ProAndCon* itemQ_,GlobalBuffer* gb_, const int plog_flag_){

  std::string NewOrdersDir = "/media/hkc/csd-7.7T/htap/test";
  std::string DB_path = NewOrdersDir + "NewOrders/";
  std::string db = DB_path + "testNewOrders.db";
  std::string log = DB_path + "testNewOrders.log";
  std::string chpt = DB_path + "chptNewOrders.log";
  std::string sha_plog = DB_path + "shaPlogNewOrders.log";
  NewOrdersTree = new BPlusNewOrdersTree(db.c_str(), log.c_str(), chpt.c_str(), bg_id++, true,sha_plog.c_str());
  // load
  NewOrdersTree->SetBaseline();
  NewOrdersTree->SetLoadFlag(1);
  char k[49] = {'\0'};
  char v[5] = {'\0'};
  int load_flag =1;
  int plog_flag = plog_flag_;
  int gb_flag =0;
  int map_id = 5;

  while(1){
    std::string* cmd = itemQ_->consumption();
    if(cmd->size()==0){
        ErrorDetect();
        delete cmd;
        continue;
    }

    if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        if(load_flag==1){
          load_flag=0;
          gb_flag =1;
          NewOrdersTree->WarmUpVirtualPageCache(1);
        }
        if(plog_flag==1){
          NewOrdersTree->SetLoadFlag(0);
          NewOrdersTree->SetPlog();
          plog_flag =2;
        }
    }
      
      
      //key word:
      //1: put
      //2: get
      //3:scan
      //4.update
  
    if(KeyWordExist(cmd,1)){
      //put,neworders,1(cid),1(cd),1(cw),rk4o
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=6){
        ErrorDetect();
      }
      int pk1 = atoi(res[4].c_str()); //cw
      int pk2 = atoi(res[3].c_str()); //cd
      int pk3 = atoi(res[2].c_str()); //cid
      snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);
      //snprintf(v, 16 + 20 + 16 + 16 +16, "%16s%20s%16s%16s%16s", i, rand() % 10000000, rand() % 10000000, rand() % 10000, rand() % 100000000,rand()%10000000);
      off_t lsn = NewOrdersTree->GetLSN();
      snprintf(v, 4+1,"%4s", res[5].c_str());
      auto t1 = std::chrono::steady_clock::now();
      if(gb_flag == 0 || plog_flag ==2){
        NewOrdersTree->Put(k, v, lsn);
      }else{
        off_t leaf_of = NewOrdersTree->GetOnlyLeafOffset(k);
        //map id: neworders 05
        //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
        char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
        if(leaf_buf!=nullptr){
          //can be found in gb
          //char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
          NewOrdersTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
          //update LRU order
          gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }else{
          //register the page to gb
          char* leaf_buf = NewOrdersTree->GetLeafCopy(k);
          NewOrdersTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
          //this page is newly inserted into gb
          gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }

      }
      
      auto t2 = std::chrono::steady_clock::now(); 
      if(load_flag ==0){
        static_tree::neworders_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::neworders_put_cnt++;
      }
      NewOrdersTree->IncreLSN();
    }else if (KeyWordExist(cmd,3)){
      //scan,neworders,100,5,2,60,7,4
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=8){
        ErrorDetect();
      }
      int pk1_1 = atoi(res[4].c_str()); //cw
      int pk1_2 = atoi(res[3].c_str()); //cd
      int pk1_3 = atoi(res[2].c_str()); //end
      int pk2_1 = atoi(res[7].c_str()); //cw
      int pk2_2 = atoi(res[6].c_str()); //cd
      int pk2_3 = atoi(res[5].c_str()); //end
      
      char start[49] = {'\0'};
      char end[49] = {'\0'};
      snprintf(start, 49, "%16d%16d%16d", pk1_1,pk1_2,pk1_3);
      snprintf(end, 49, "%16d%16d%16d", pk2_1,pk2_2,pk2_3);
      std::string value;
      auto t1_n = std::chrono::steady_clock::now();
      NewOrdersTree->Scan(start,end,value);
      //value[0] = value[1];
      auto t2_n = std::chrono::steady_clock::now();
      if(pk1_1 == pk2_1 && pk2_1 == pk2_2){ 
        static_tree::neworders_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2_n - t1_n).count()/1000;
        static_tree::neworders_scan_cnt_c++;
      }else{
        static_tree::neworders_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_n - t1_n).count()/1000;
        static_tree::neworders_scan_cnt_h++;
      }
    }else{
      ErrorDetect();
    }
    
    delete cmd;
  }
  return;

}


void Consumer_NewordersTree_MVCC_TEST(ProAndCon* itemQ_,GlobalBuffer* gb_, const int plog_flag_){

  std::string NewOrdersDir = "/media/hkc/csd-7.7T/htap/test";
  std::string DB_path = NewOrdersDir + "NewOrders/";
  std::string db = DB_path + "testNewOrders.db";
  std::string log = DB_path + "testNewOrders.log";
  std::string chpt = DB_path + "chptNewOrders.log";
  std::string sha_plog = DB_path + "shaPlogNewOrders.log";
  NewOrdersTree = new BPlusNewOrdersTree(db.c_str(), log.c_str(), chpt.c_str(), bg_id++, true,sha_plog.c_str());
  // load
  NewOrdersTree->SetBaseline();
  NewOrdersTree->SetLoadFlag(1);
  char k[49] = {'\0'};
  char v[5] = {'\0'};
  int load_flag =1;
  int plog_flag = plog_flag_;
  int gb_flag =0;
  int map_id = 5;

  while(1){
    std::string* cmd = itemQ_->consumption();
    if(cmd->size()==0){
        ErrorDetect();
        delete cmd;
        continue;
    }

    if((*cmd)[0]=='-' &&(*cmd)[1]=='1'){
        if(load_flag==1){
          load_flag=0;
          gb_flag =1;
          NewOrdersTree->WarmUpVirtualPageCache(1);
        }
        if(plog_flag==1){
          NewOrdersTree->SetLoadFlag(0);
          NewOrdersTree->SetPlog();
          plog_flag =2;
        }
    }
      
      
      //key word:
      //1: put
      //2: get
      //3:scan
      //4.update
  
    if(KeyWordExist(cmd,1)){
      //put,neworders,1(cid),1(cd),1(cw),rk4o
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=6){
        ErrorDetect();
      }
      int pk1 = atoi(res[4].c_str()); //cw
      int pk2 = atoi(res[3].c_str()); //cd
      int pk3 = atoi(res[2].c_str()); //cid
      snprintf(k, 49, "%16d%16d%16d", pk1,pk2,pk3);
      //snprintf(v, 16 + 20 + 16 + 16 +16, "%16s%20s%16s%16s%16s", i, rand() % 10000000, rand() % 10000000, rand() % 10000, rand() % 100000000,rand()%10000000);
      off_t lsn = NewOrdersTree->GetLSN();
      snprintf(v, 4+1,"%4s", res[5].c_str());
      auto t1 = std::chrono::steady_clock::now();
      if(gb_flag == 0 || plog_flag ==2){
        NewOrdersTree->Put(k, v, lsn);
      }else{
        off_t leaf_of = NewOrdersTree->GetOnlyLeafOffset(k);
        //map id: neworders 05
        //std::unordered_map<off_t,char*> cur_map = gb_->gb_map[map_id];
        char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
        if(leaf_buf!=nullptr){
          //can be found in gb
          //char* leaf_buf = gb_->GetPageCopy(map_id,leaf_of);
          NewOrdersTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
          //update LRU order
          gb_->UpdateLRUOrder(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }else{
          //register the page to gb
          char* leaf_buf = NewOrdersTree->GetLeafCopy(k);
          NewOrdersTree->UpdateLeafCopy(k,v,lsn,leaf_buf);
          //this page is newly inserted into gb
          gb_->InsertIntoGlobalBuffer(map_id,leaf_of,leaf_buf);
          free(leaf_buf);
        }

      }
      
      auto t2 = std::chrono::steady_clock::now(); 
      if(load_flag ==0){
        static_tree::neworders_put_time += std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count()/1000;
        static_tree::neworders_put_cnt++;
      }
      NewOrdersTree->IncreLSN();
    }else if (KeyWordExist(cmd,3)){
      //scan,neworders,100,5,2,60,7,4
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=8){
        ErrorDetect();
      }
      int pk1_1 = atoi(res[4].c_str()); //cw
      int pk1_2 = atoi(res[3].c_str()); //cd
      int pk1_3 = atoi(res[2].c_str()); //end
      int pk2_1 = atoi(res[7].c_str()); //cw
      int pk2_2 = atoi(res[6].c_str()); //cd
      int pk2_3 = atoi(res[5].c_str()); //end
      
      char start[49] = {'\0'};
      char end[49] = {'\0'};
      snprintf(start, 49, "%16d%16d%16d", pk1_1,pk1_2,pk1_3);
      snprintf(end, 49, "%16d%16d%16d", pk2_1,pk2_2,pk2_3);
      std::string value;
      auto t1_n = std::chrono::steady_clock::now();
      NewOrdersTree->Scan(start,end,value);
      //value[0] = value[1];
      auto t2_n = std::chrono::steady_clock::now();
      if(pk1_1 == pk2_1 && pk2_1 == pk2_2){ 
        static_tree::neworders_scan_time_c += std::chrono::duration_cast<std::chrono::microseconds>(t2_n - t1_n).count()/1000;
        static_tree::neworders_scan_cnt_c++;
      }else{
        static_tree::neworders_scan_time_h += std::chrono::duration_cast<std::chrono::microseconds>(t2_n - t1_n).count()/1000;
        static_tree::neworders_scan_cnt_h++;
      }
    }else if (KeyWordExist(cmd,5)){
      //scan,neworders,100,5,2,60,7,4
      std::vector<std::string> res = split(*cmd, ",");
      if(res.size()!=8){
        ErrorDetect();
      }
      int pk1_1 = atoi(res[4].c_str()); //cw
      int pk1_2 = atoi(res[3].c_str()); //cd
      int pk1_3 = atoi(res[2].c_str()); //end

      int pk2_1 = atoi(res[7].c_str()); //cw
      int pk2_2 = atoi(res[6].c_str()); //cd
      int pk2_3 = atoi(res[5].c_str()); //end
      
      char start[49] = {'\0'};
      char end[49] = {'\0'};
      snprintf(start, 49, "%16d%16d%16d", pk1_1,pk1_2,pk1_3);
      snprintf(end, 49, "%16d%16d%16d", pk2_1,pk2_2,pk2_3);
      std::string value;
      
      NewOrdersTree->ScanAP(start,end,value);
    }else{
      ErrorDetect();
    }
    
    delete cmd;
  }
  return;

}




void GBRunnerTest(GlobalBuffer*gb_){
  std::cout<<"test_flag";
  std::vector<off_t> evicted[8];
  for(int i=0;i<8;i++){
    for(int j=0;j<200;j++){
      evicted[i].push_back(0);
    }
  }
  std::vector<int> evict_size;
  for(int i=0;i<8;i++){
    evict_size.push_back(0);
  }
  //char customer_buf[200*128*1024] = {'\0'};
  while(1){
    bool need = gb_->DoLRUEviction();
    if(gb_->plog_flag == 0){
        // need flush
        int x=0;
    }else{
        //directly skip
        int x=1;
    }

  }
}
//hkc-gb-11
void GlobalBufferRunner(GlobalBuffer* gb_){
  std::cout<<"test_flag\n";
  //int plog_enable = gb_->plog_flag;
  off_t evicted[8][200] = {0};
  int evict_size[8] = {0};
  //0
  char* customer_buf = (char*)malloc(sizeof(char)*200*128*1024);
  memset(customer_buf,0,200*128*1024);
  
  //1
  char* stock_buf = (char*)malloc(sizeof(char)*200*128*1024);
  memset(stock_buf,0,200*128*1024);
  
  //2
  char* item_buf = (char*)malloc(sizeof(char)*200*128*1024);
  memset(item_buf,0,200*128*1024);
  //3
  char* orderline_buf = (char*)malloc(sizeof(char)*200*128*1024);
  memset(orderline_buf,0,200*128*1024);
  
  //4
  char* orders_buf = (char*)malloc(sizeof(char)*200*128*1024);
  memset(orders_buf,0,200*128*1024);
  
  //5
  char* neworders_buf = (char*)malloc(sizeof(char)*200*128*1024);
  memset(neworders_buf,0,200*128*1024);
  
  //6
  char* history1_buf = (char*)malloc(sizeof(char)*200*128*1024);
  memset(history1_buf,0,200*128*1024);
  
  //7
  char* history2_buf = (char*)malloc(sizeof(char)*200*128*1024);
  memset(history2_buf,0,200*128*1024);
  
  
  std::this_thread::sleep_for(std::chrono::seconds(10));

  while(1){
    //std::this_thread::sleep_for(std::chrono::milliseconds(1));
    if(gb_->DoLRUEviction()){
      if(gb_->plog_flag == 0){
        //need to flush pages to trees
        int tab_id =0;
        off_t offset = 0;
        char* buf = nullptr;
        for(int i=0; i<200;i++){
          buf = gb_->GetEvictPageNode(i, offset,tab_id);
          
          switch (tab_id)
          {
            case 0: //customer
              evicted[0][evict_size[0]] = offset;
              memcpy(&customer_buf[evict_size[0]*128*1024], buf,128*1024);
              evict_size[0]++;
              //customer tab
              break;
            case 1: //stock
              evicted[1][evict_size[1]] = offset;
              memcpy(&stock_buf[evict_size[1]*128*1024], buf,128*1024);
              evict_size[1]++;
              break;
            case 2: //item
              evicted[2][evict_size[2]] = offset;
              memcpy(&item_buf[evict_size[2]*128*1024], buf,128*1024);
              evict_size[2]++;
              break;
            case 3: //orderline
              evicted[3][evict_size[3]] = offset;
              memcpy(&orderline_buf[evict_size[3]*128*1024], buf,128*1024);
              evict_size[3]++;
              break;
            case 4: //orders
              evicted[4][evict_size[4]] = offset;
              memcpy(&orders_buf[evict_size[4]*128*1024], buf,128*1024);
              evict_size[4]++;
              break;
            case 5: //neworders
              evicted[5][evict_size[5]] = offset;
              memcpy(&neworders_buf[evict_size[5]*128*1024], buf,128*1024);
              evict_size[5]++;
              break;
            case 6: //history 1
              evicted[6][evict_size[6]] = offset;
              memcpy(&history1_buf[evict_size[6]*128*1024], buf,128*1024);
              evict_size[6]++;
              break;
            case 7: //history 2
              evicted[7][evict_size[7]] = offset;
              memcpy(&history2_buf[evict_size[7]*128*1024], buf,128*1024);
              evict_size[7]++;
              break;
            default:
              break;
          }
        }
        {
          if(evict_size[0]!=0){
            CustomerTree->TriggerFlush(evicted[0],evict_size[0],&customer_buf[0]);
            memset(customer_buf,0,200*128*1024);
            evict_size[0]=0;
          }
          if(evict_size[1]!=0){
            StockTree->TriggerFlush(evicted[1],evict_size[1],&stock_buf[0]);
            memset(stock_buf,0,200*128*1024);
            evict_size[1]=0;
          }
          if(evict_size[2]!=0){
            ItemTree->TriggerFlush(evicted[2],evict_size[2],&item_buf[0]);
            memset(item_buf,0,200*128*1024);
            evict_size[2]=0;
          }
          if(evict_size[3]!=0){
            OrderLineTree->TriggerFlush(evicted[3],evict_size[3],&orderline_buf[0]);
            memset(orderline_buf,0,200*128*1024);
            evict_size[3]=0;
          }
          if(evict_size[4]!=0){
            OrdersTree->TriggerFlush(evicted[4],evict_size[4],&orders_buf[0]);
            memset(orders_buf,0,200*128*1024);
            evict_size[4]=0;
          }
          if(evict_size[5]!=0){
            NewOrdersTree->TriggerFlush(evicted[5],evict_size[5],&neworders_buf[0]);
            memset(neworders_buf,0,200*128*1024);
            evict_size[5]=0;
          }
          if(evict_size[6]!=0){
            Historyfk1Tree->TriggerFlush(evicted[6],evict_size[6],&history1_buf[0]);
            memset(history1_buf,0,200*128*1024);
            evict_size[6]=0;
          }
          if(evict_size[7]!=0){
            Historyfk2Tree->TriggerFlush(evicted[7],evict_size[7],&history2_buf[0]);
            memset(history2_buf,0,200*128*1024);
            evict_size[7]=0;
          }
          

        }


      }else{
        //do nothing
        int x=1;
      }
    }
  }
  std::cout<<"finish gb\n";
  return;
  
  

}

void dispatcher(){
  // it is a procuder to load trace from file and push cmd to different queue
  tpcch_kv_generator::tpcc_gen tpcc_gen_(10);
  
  tpcch_kv_generator::tpch_gen tpch_gen_(10,100000);
  
  //char *filePath = "/home/hkc/HTAP/PB_HTAP/workloads/parsed/load_10_item.log";
  //std::ifstream cmd_file;
  //cmd_file.open(filePath,std::ios::in);
  //if(!cmd_file.is_open()) printf("Error in open trace file\n");
  int plog_flag=1;
  GlobalBuffer* gb_ = new GlobalBuffer(plog_flag);
  std::thread thd_gb = std::thread(GlobalBufferRunner,gb_);
  //std::thread thd_gb = std::thread(GBRunnerTest,gb_);
  thd_gb.detach();

  ProAndCon* itemQ = new ProAndCon(50);
  std::thread CItemTree = std::thread(Consumer_ItemTree,itemQ,gb_,plog_flag);
  CItemTree.detach();

  ProAndCon* stockQ = new ProAndCon(50);
  std::thread CStockTree = std::thread(Consumer_StockTree,stockQ,gb_,plog_flag);
  CStockTree.detach();

  ProAndCon* ordersQ = new ProAndCon(50);
  std::thread COrdersTree = std::thread(Consumer_OrdersTree,ordersQ,gb_,plog_flag);
  COrdersTree.detach();

  //ProAndCon* districtQ = new ProAndCon(50);
  //std::thread CDistrictTree = std::thread(Consumer_DistrictTree,districtQ);
  //CDistrictTree.detach();

  ProAndCon* historyQ = new ProAndCon(50);
  std::thread CHistoryTree = std::thread(Consumer_HistoryTree,historyQ,gb_,plog_flag);
  CHistoryTree.detach();

  ProAndCon* orderlineQ = new ProAndCon(50);
  std::thread COrderlineTree = std::thread(Consumer_OrderlineTree,orderlineQ,gb_,plog_flag);
  COrderlineTree.detach();

  ProAndCon* newordersQ = new ProAndCon(50);
  std::thread CNewordersTree = std::thread(Consumer_NewordersTree,newordersQ,gb_,plog_flag);
  CNewordersTree.detach();

  ProAndCon* customerQ = new ProAndCon(50);
  std::thread CCustomerTree = std::thread(Consumer_CustomerTree,customerQ,gb_,plog_flag);
  CCustomerTree.detach();
  
 
  
  std::string* cmdLine = nullptr;
  auto t1 = std::chrono::steady_clock::now();
          
  //while(getline(cmd_file,cmdLine))
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




  
  //test phase
  //char *filePath_test = "/home/hkc/HTAP/PB_HTAP/workloads/parsed/test_10_item.log";
  //std::ifstream cmd_file_test;
  //cmd_file_test.open(filePath_test,std::ios::in);
  //if(!cmd_file_test.is_open()) printf("Error in open test trace file\n");
  int cntt=0;
  int tpcc_cnt=0;
  int tpch_cnt=0;
  auto t3 = std::chrono::steady_clock::now(); 
  //cmdLine = tpcc_gen_.gentpcc_cmd_TEST();
  cmdLine = tpch_gen_.gentpch_cmd();
  int cycles =0;
  while((*cmdLine)[0]!='-' && (*cmdLine)[0]!='1')
  //while(getline(cmd_file_test,cmdLine))
  {
    //std::string cmd_str(cmdLine,cmdLine+strlen(cmdLine));
    if(cmdLine->find(",customer,")!=cmdLine->npos){
      //char* cmd = (char*)malloc((cmdLine.length())*sizeof(char));
      //strcpy(cmd,cmdLine.c_str());
      customerQ->product(cmdLine);
      cntt++;
    }else if(cmdLine->find(",item,")!=cmdLine->npos){
      //char* cmd = (char*)malloc((cmdLine.length())*sizeof(char));
      //strcpy(cmd,cmdLine.c_str());
      itemQ->product(cmdLine);
      cntt++;
      /*
      if(cntt==100000){
        cntt=0;
        int hits_bp =0; int hits_glog =0; int hits_plog = 0; int disk_read=0; int io_consume=0; int error_cnt=0; int dirtys =0; int cleans =0;
        //ItemTree->GetStatic(hits_bp, hits_glog, hits_plog, disk_read, io_consume, error_cnt, dirtys, cleans);
        std::cout<<"ItemTab. put[ "<<static_tree::item_put_cnt<<" , "<<static_tree::item_put_time<<" ] "
                 <<"get[ "<<static_tree::item_get_cnt<<" , "<<static_tree::item_get_time<<" ] "
                 <<"update[ "<<static_tree::item_update_cnt<<" , "<<static_tree::item_update_time<<" ] "
                 <<"scan[ "<<static_tree::item_scan_cnt<<" , "<<static_tree::item_scan_time<<" ] "
                 <<"io_consume,diskread[ "<<io_consume<<" , "<<disk_read<<" ] \n";
        
      }*/
    }else if(cmdLine->find(",stock,")!=cmdLine->npos){
      //char* cmd = (char*)malloc((cmdLine.length())*sizeof(char));
      //strcpy(cmd,cmdLine.c_str());
      stockQ->product(cmdLine);
      cntt++;
      /*
      if(cntt==100000){
        cntt=0;
        int hits_bp =0; int hits_glog =0; int hits_plog = 0; int disk_read=0; int io_consume=0; int error_cnt=0; int dirtys =0; int cleans =0;
        //StockTree->GetStatic(hits_bp, hits_glog, hits_plog, disk_read, io_consume, error_cnt, dirtys, cleans);
        std::cout<<"StockTab. put[ "<<static_tree::stock_put_cnt<<" , "<<static_tree::stock_put_time<<" ] "
                 <<"get[ "<<static_tree::stock_get_cnt<<" , "<<static_tree::stock_get_time<<" ] "
                 <<"update[ "<<static_tree::stock_update_cnt<<" , "<<static_tree::stock_update_time<<" ] "
                 <<"scan[ "<<static_tree::stock_scan_cnt<<" , "<<static_tree::stock_scan_time<<" ] "<<", get not found[ "<<static_tree::stock_get_not_found
                 <<" io_consume,diskread[ "<<io_consume<<" , "<<disk_read<<" ] \n";
      }
      */    
    }else if(cmdLine->find(",orderline,")!=cmdLine->npos){
      
      //TEST
      //cmdLine = tpcc_gen_.GenerateTEST_Customer();
      //continue;
      //END OF TEST

      //char* cmd = (char*)malloc((cmdLine.length())*sizeof(char));
      //strcpy(cmd,cmdLine.c_str());
      orderlineQ->product(cmdLine);
      cntt++;
      /*
      if(cntt==100000){
        cntt=0;
        int hits_bp =0; int hits_glog =0; int hits_plog = 0; int disk_read=0; int io_consume=0; int error_cnt=0; int dirtys =0; int cleans =0;
        //OrderLineTree->GetStatic(hits_bp, hits_glog, hits_plog, disk_read, io_consume, error_cnt, dirtys, cleans);
        std::cout<<"OrderlineTab. put[ "<<static_tree::orderline_put_cnt<<" , "<<static_tree::orderline_put_time<<" ] "
                 <<"get[ "<<static_tree::orderline_get_cnt<<" , "<<static_tree::orderline_get_time<<" ] "
                 <<"update[ "<<static_tree::orderline_update_cnt<<" , "<<static_tree::orderline_update_time<<" ] "<<", get not found[ "<<static_tree::orderline_get_not_found
                 <<"scan[ "<<static_tree::orderline_scan_cnt<<" , "<<static_tree::orderline_scan_time<<" ] "
                 <<" io_consume,diskread[ "<<io_consume<<" , "<<disk_read<<" ] \n";
      }
      */

    }else if(cmdLine->find(",neworders,")!=cmdLine->npos){
      newordersQ->product(cmdLine);
      cntt++;
    }else if(cmdLine->find(",orders,")!=cmdLine->npos){
      ordersQ->product(cmdLine);
      cntt++;
    }else if(cmdLine->find(",history,")!=cmdLine->npos){
      historyQ->product(cmdLine);
      cntt++;
    }else{
      ErrorDetect();
    }
    //cmdLine = tpcc_gen_.GenerateTEST();
    
    /*
    if(cntt==500000){
        cntt=0;
        cycles++;
        std::cout<<"CustomerTab. put[ "<<static_tree::customer_put_cnt<<" , "<<static_tree::customer_put_time<<" ] "
                 <<"get[ "<<static_tree::customer_get_cnt<<" , "<<static_tree::customer_get_time<<" ] "
                 <<"update[ "<<static_tree::customer_update_cnt<<" , "<<static_tree::customer_update_time<<" ] "
                 <<"scan_c[ "<<static_tree::customer_scan_cnt_c<<" , "<<static_tree::customer_scan_time_c<<" ] "
                 <<"scan_h[ "<<static_tree::customer_scan_cnt_h<<" , "<<static_tree::customer_scan_time_h<<" ] \n"
                 <<"ItemTab. put[ "<<static_tree::item_put_cnt<<" , "<<static_tree::item_put_time<<" ] "
                 <<"get[ "<<static_tree::item_get_cnt<<" , "<<static_tree::item_get_time<<" ] "
                 <<"update[ "<<static_tree::item_update_cnt<<" , "<<static_tree::item_update_time<<" ] "
                 <<"scan_c[ "<<static_tree::item_scan_cnt_c<<" , "<<static_tree::item_scan_time_c<<" ] "
                 <<"scan_h[ "<<static_tree::item_scan_cnt_h<<" , "<<static_tree::item_scan_time_h<<" ] \n"
                 <<"StockTab. put[ "<<static_tree::stock_put_cnt<<" , "<<static_tree::stock_put_time<<" ] "
                 <<"get[ "<<static_tree::stock_get_cnt<<" , "<<static_tree::stock_get_time<<" ] "
                 <<"update[ "<<static_tree::stock_update_cnt<<" , "<<static_tree::stock_update_time<<" ] "
                 <<"scan_c[ "<<static_tree::stock_scan_cnt_c<<" , "<<static_tree::stock_scan_time_c<<" ] "
                 <<"scan_h[ "<<static_tree::stock_scan_cnt_h<<" , "<<static_tree::stock_scan_time_h<<" ] \n"
                 <<"OrderlineTab. put[ "<<static_tree::orderline_put_cnt<<" , "<<static_tree::orderline_put_time<<" ] "
                 <<"get[ "<<static_tree::orderline_get_cnt<<" , "<<static_tree::orderline_get_time<<" ] "
                 <<"update[ "<<static_tree::orderline_update_cnt<<" , "<<static_tree::orderline_update_time<<" ] "
                 <<"scan_c[ "<<static_tree::orderline_scan_cnt_c<<" , "<<static_tree::orderline_scan_time_c<<" ] "
                 <<"scan_h[ "<<static_tree::orderline_scan_cnt_h<<" , "<<static_tree::orderline_scan_time_h<<" ] \n"
                 <<"NewOrdersTab. put[ "<<static_tree::neworders_put_cnt<<" , "<<static_tree::neworders_put_time<<" ] "
                 <<"get[ "<<static_tree::neworders_get_cnt<<" , "<<static_tree::neworders_get_time<<" ] "
                 <<"update[ "<<static_tree::neworders_update_cnt<<" , "<<static_tree::neworders_update_time<<" ] "
                 <<"scan_c[ "<<static_tree::neworders_scan_cnt_c<<" , "<<static_tree::neworders_scan_time_c<<" ] "
                 <<"scan_h[ "<<static_tree::neworders_scan_cnt_h<<" , "<<static_tree::neworders_scan_time_h<<" ] \n"
                 <<"OrdersTab. put[ "<<static_tree::orders_put_cnt<<" , "<<static_tree::orders_put_time<<" ] "
                 <<"get[ "<<static_tree::orders_get_cnt<<" , "<<static_tree::orders_get_time<<" ] "
                 <<"update[ "<<static_tree::orders_update_cnt<<" , "<<static_tree::orders_update_time<<" ] "
                 <<"scan_c[ "<<static_tree::orders_scan_cnt_c<<" , "<<static_tree::orders_scan_time_c<<" ] "
                 <<"scan_h[ "<<static_tree::orders_scan_cnt_h<<" , "<<static_tree::orders_scan_time_h<<" ] \n"
                 <<"HistoryTab. put[ "<<static_tree::history_put_cnt<<" , "<<static_tree::history_put_time<<" ] "
                 <<"get[ "<<static_tree::history_get_cnt<<" , "<<static_tree::history_get_time<<" ] "
                 <<"update[ "<<static_tree::history_update_cnt<<" , "<<static_tree::history_update_time<<" ] "
                 <<"scan_c[ "<<static_tree::history_scan_cnt_c<<" , "<<static_tree::history_scan_time_c<<" ] "
                 <<"scan_h[ "<<static_tree::history_scan_cnt_h<<" , "<<static_tree::history_scan_time_h<<" ] \n";
        static_tree::reset_static();

        auto t4 = std::chrono::steady_clock::now(); 
        auto tmp1 = std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count()/1000;
        std::cout<<"finish "<<cycles<<" cycles, exe time: "<<tmp1<<" tpcc/tpch:[ "<<tpcc_cnt<<" , "<<tpch_cnt<<" ]\n";
        t3=t4;
      }
    */
    
    if(cntt==200){
            cntt=0;
            cycles++;
            std::cout<<"CustomerTab. scan_ch[ "
                    <<static_tree::customer_scan_cnt_c<<" , "<<static_tree::customer_scan_cnt_h<<" , "
                    <<static_tree::customer_scan_time_c<<" , "<<static_tree::customer_scan_time_h<<" ] \n"
                    <<"ItemTab. scan_h[ "
                    <<static_tree::item_scan_cnt_c<<" , "<<static_tree::item_scan_cnt_h<<" , "
                    <<static_tree::item_scan_time_c<<" , "<<static_tree::item_scan_time_h<<" ] \n"
                    <<"StockTab. scan_h[ "
                    <<static_tree::stock_scan_cnt_c<<" , "<<static_tree::stock_scan_cnt_h<<" , "
                    <<static_tree::stock_scan_time_c<<" , "<<static_tree::stock_scan_time_h<<" ] \n"
                    <<"OrderlineTab. scan_h[ "
                    <<static_tree::orderline_scan_cnt_c<<" , "<<static_tree::orderline_scan_cnt_h<<" , "
                    <<static_tree::orderline_scan_time_c<<" , "<<static_tree::orderline_scan_time_h<<" ] \n"
                    <<"NewOrdersTab. scan_h[ "
                    <<static_tree::neworders_scan_cnt_c<<" , "<<static_tree::neworders_scan_cnt_h<<" , "
                    <<static_tree::neworders_scan_time_c<<" , "<<static_tree::neworders_scan_time_h<<" ] \n"
                    <<"OrdersTab. scan_h[ "
                    <<static_tree::orders_scan_cnt_c<<" , "<<static_tree::orders_scan_cnt_h<<" , "
                    <<static_tree::orders_scan_time_c<<" , "<<static_tree::orders_scan_time_h<<" ] \n"
                    <<"HistoryTab. scan_h[ "<<static_tree::history_scan_cnt_h<<" , "<<static_tree::history_scan_time_h<<" ] \n";
          static_tree::reset_static();

            auto t4 = std::chrono::steady_clock::now(); 
            auto tmp1 = std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count()/1000;
            std::cout<<"finish "<<cycles<<" cycles, exe time: "<<tmp1<<"\n";
            t3=t4;
          }
     cmdLine = tpch_gen_.gentpch_cmd();
    
    /*
      tpcc_cnt++;  
      if(tpcc_cnt % 9999 == 0){
        tpcc_cnt = 0;
        cmdLine = tpch_gen_.gentpch_cmd();
        
      }else{
        cmdLine = tpcc_gen_.gentpcc_cmd_TEST();
        tpch_cnt++;
      }
    */
      if(cycles == 100){
        break;
      }
    /*
    else if(cmdLine.find("?")!=cmdLine.npos){
      continue;
    }else if (cmdLine.find("Execute	SELECT")!=cmdLine.npos){
      //case for read
      // SELECT i_price, i_name, i_data FROM item WHERE i_id = 53126
      char* cmd = (char*)malloc((cmdLine.length()+1)*sizeof(char));
      strcpy(cmd,cmdLine.c_str());
      if(cmdLine.find("FROM item WHERE")!=cmdLine.npos){
        itemQ->product(cmd);
      }else if(cmdLine.find("FROM customer")!=cmdLine.npos){
        customerQ->product(cmd);
      }else if(cmdLine.find("FROM history WHERE")!=cmdLine.npos){
        //not happen, only for backup

      }else if(cmdLine.find("FROM district WHERE")!=cmdLine.npos){
        districtQ->product(cmd);
      }else if(cmdLine.find("FROM new_orders WHERE")!=cmdLine.npos){
        newordersQ->product(cmd);
      }else if(cmdLine.find("FROM order_line WHERE")!=cmdLine.npos){
        orderlineQ->product(cmd);
      }else if(cmdLine.find("FROM orders WHERE")!=cmdLine.npos){
        ordersQ->product(cmd);
      }else if(cmdLine.find("FROM stock WHERE")!=cmdLine.npos){
        stockQ->product(cmd);
      }
    }else if(cmdLine.find("Execute	INSERT")!=cmdLine.npos || cmdLine.find("Execute	UPDATE")!=cmdLine.npos){
      //case for insert
      if (cmdLine.find("INSERT INTO item values") != cmdLine.npos || cmdLine.find("UPDATE item")!=cmdLine.npos){
        //INSERT INTO item values(1,2498,'8TA69fNTQqhRD7KLKkV',77.2699966430664,'87LD1FH6AFFbPLl2hCSXsukzC3IxW23eAOqBb7')
        char* cmd = (char*)malloc((cmdLine.length()+1)*sizeof(char));
        strcpy(cmd,cmdLine.c_str());
        itemQ->product(cmd);
      }else if (cmdLine.find("INSERT INTO stock values") != cmdLine.npos || cmdLine.find("UPDATE stock")!=cmdLine.npos){
        char* cmd = (char*)malloc((cmdLine.length()+1)*sizeof(char));
        strcpy(cmd,cmdLine.c_str());
        stockQ->product(cmd);
      }else if (cmdLine.find("INSERT INTO orders values") != cmdLine.npos || cmdLine.find("UPDATE orders")!=cmdLine.npos){
        char* cmd = (char*)malloc((cmdLine.length()+1)*sizeof(char));
        strcpy(cmd,cmdLine.c_str());
        ordersQ->product(cmd);
      }else if (cmdLine.find("INSERT INTO district values") != cmdLine.npos || cmdLine.find("UPDATE district")!=cmdLine.npos){
        char* cmd = (char*)malloc((cmdLine.length()+1)*sizeof(char));
        strcpy(cmd,cmdLine.c_str());
        districtQ->product(cmd);
      }else if (cmdLine.find("INSERT INTO history values") != cmdLine.npos || cmdLine.find("UPDATE history")!=cmdLine.npos){
        char* cmd1 = (char*)malloc((cmdLine.length()+1)*sizeof(char));
        strcpy(cmd1,cmdLine.c_str());
        historyfk1Q->product(cmd1);
        char* cmd2 = (char*)malloc((cmdLine.length()+1)*sizeof(char));
        strcpy(cmd2,cmdLine.c_str());
        historyfk2Q->product(cmd2);
      }else if (cmdLine.find("INSERT INTO order_line values") != cmdLine.npos || cmdLine.find("UPDATE order_line")!=cmdLine.npos){
        char* cmd = (char*)malloc((cmdLine.length()+1)*sizeof(char));
        strcpy(cmd,cmdLine.c_str());
        orderlineQ->product(cmd);
      }else if (cmdLine.find("INSERT INTO new_orders values"|| cmdLine.find("UPDATE new_orders")!=cmdLine.npos) != cmdLine.npos){
        char* cmd = (char*)malloc((cmdLine.length()+1)*sizeof(char));
        strcpy(cmd,cmdLine.c_str());
        newordersQ->product(cmd);
      }else if (cmdLine.find("INSERT INTO customer values") != cmdLine.npos || cmdLine.find("UPDATE customer")!=cmdLine.npos){
        char* cmd = (char*)malloc((cmdLine.length()+1)*sizeof(char));
        strcpy(cmd,cmdLine.c_str());
        customerQ->product(cmd);
      }

    }
    */
  }
  
  auto t4 = std::chrono::steady_clock::now(); 
  auto tmp1 = std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count()/1000;
  std::cout<<"finish test, exe time: "<<tmp1<<"\n";
  
  
  return ;

}
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

// main
int main(int argc, char const *argv[])
{
  (void)argc;
  (void)argv;


  //dispatcher();
  dispatcher_MVCC_TEST();
  return 1;
}