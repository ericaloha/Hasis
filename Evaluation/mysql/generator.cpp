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
      int pk2_3 = atoi(res[6].c_str()); //cd
      int pk2_4 = atoi(res[7].c_str()); //cd
      
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
