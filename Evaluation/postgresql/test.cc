#include <iostream>
#include <pqxx/pqxx>
using namespace std;
using namespace pqxx;
 
int main(int argc,char* argv[]){
  const char* sql;
  try{
    connection conn("dbname=pbtest user=pgsql password=root \
    hostaddr=127.0.0.1 port=5432");
    if(conn.is_open()){
      cout<<"connect to postgresql successfully. dbname="<<conn.dbname()<<endl;
    }else{
      cout<<"connect to postgresql fail."<<endl;
      return 1;
    }
    work tnx(conn);
    sql = "insert into xx_user(id,name,mobile,birth) values (1,'aaa','15011186301',current_date),(2,'bbb','15910909520',current_date)";
    tnx.exec(sql);
    tnx.commit();
    cout<<"insert record ok."<<endl;
    conn.disconnect();
  }catch(const std::exception &e){
    cerr<<e.what()<<endl;
    return 1;
  }
  return 0;
}
