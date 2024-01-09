#include <iostream>
#include <mysql.h>   
using namespace std; 
/// ///sql_create 创建数据库 
/// 
string skt= "/media/hkc/csd-7/mysql-base/data/mysql.sock"; 
void sql_create() {     
    MYSQL mysql;     
    mysql_init(&mysql); 
    
    if(!mysql_real_connect(&mysql, "localhost", "root", "root", "mysql", 3306, skt.c_str(), 0)) {         
        cout<<"mysql connect error1: "<<mysql_error(&mysql)<<" "<<mysql_errno(&mysql)<<endl;     
    }       
    string str = "create database school;";       
    mysql_real_query(&mysql, str.c_str(), str.size());       
    str = "alter database school charset=utf8;";      
    mysql_real_query(&mysql, str.c_str(), str.size()); // 自动生成id add数据时去掉id //    
    str = "create table school.students(id int(10) primary key auto_increment, name varchar(20) not null, age int(3) not null);"; //手动添加id     
    str = "create table school.students(id int(10) primary key , name varchar(20) not null, age int(3) not null);";       
    mysql_real_query(&mysql, str.c_str(), str.size());       
    
    mysql_close(&mysql); } /// /// \brief sql_add 增加数据 /// 
    
    void sql_add() {     
        MYSQL mysql;     
        mysql_init(&mysql);     
        mysql_options(&mysql, MYSQL_SET_CHARSET_NAME, "utf8");     
        mysql_options(&mysql, MYSQL_INIT_COMMAND, "SET NAMES utf8");       
        if(!mysql_real_connect(&mysql, "localhost", "root", "root", "school", 3306, skt.c_str(), 0)) {         
            cout<<"mysql connect error2: "<<mysql_error(&mysql)<<" "<<mysql_errno(&mysql);     
        }       
        string str = "insert into students(id, name, age) values(1, \'张三\', 20)";     
        mysql_real_query(&mysql, str.c_str(), str.size());     
        str = "insert into students(id, name, age) values(2, \'李四\', 18)";     
        mysql_real_query(&mysql, str.c_str(), str.size());     
        str = "insert into students(id, name, age) values(3, \'王五\', 19)";     
        mysql_real_query(&mysql, str.c_str(), str.size());     
        str = "insert into students(id, name, age) values(4, \'赵六\', 21)";     
        mysql_real_query(&mysql, str.c_str(), str.size());     
        str = "insert into students(id, name, age) values(5, \'马七\', 20)";     
        mysql_real_query(&mysql, str.c_str(), str.size());       
        mysql_close(&mysql); 
    } /// 
    
    /// \brief sql_modify 修改数据 /// 
    void sql_modify() {     
        MYSQL mysql;     
        mysql_init(&mysql);     
        mysql_options(&mysql, MYSQL_SET_CHARSET_NAME, "utf8");    
        mysql_options(&mysql, MYSQL_INIT_COMMAND, "SET NAMES utf8");       
        if(!mysql_real_connect(&mysql, "localhost", "root", "root", "school", 3306, skt.c_str(), 0)) {         
            cout<<"mysql connect error3: "<<mysql_error(&mysql)<<" "<<mysql_errno(&mysql);     
        }       
        string str = "update students set age = 30 where name = \'王五\'";       
        mysql_real_query(&mysql, str.c_str(), str.size());       
        mysql_close(&mysql); 
    } /// 
    
    /// \brief sql_delete 删除数据 /// 
    void sql_delete() {     
        MYSQL mysql;     
        mysql_init(&mysql);     
        mysql_options(&mysql, MYSQL_SET_CHARSET_NAME, "utf8");     
        mysql_options(&mysql, MYSQL_INIT_COMMAND, "SET NAMES utf8");       
        if(!mysql_real_connect(&mysql, "localhost", "root", "root", "school", 3306, skt.c_str(), 0)) {         
            cout<<"mysql connect error4: "<<mysql_error(&mysql)<<" "<<mysql_errno(&mysql);     
        }       
        string str = "delete from students where name = \'赵六\'";       
        mysql_real_query(&mysql, str.c_str(), str.size());       
        mysql_close(&mysql); 
    } /// 
    
    /// \brief sql_query 查询数据 /// 
    void sql_query() {     
        MYSQL mysql;     
        mysql_init(&mysql);     
        mysql_options(&mysql, MYSQL_SET_CHARSET_NAME, "utf8");     
        mysql_options(&mysql, MYSQL_INIT_COMMAND, "SET NAMES utf8");       
        if(!mysql_real_connect(&mysql, "localhost", "root", "root", "school", 3306, skt.c_str(), 0)) {         
            cout<<"mysql connect error5: "<<mysql_error(&mysql)<<" "<<mysql_errno(&mysql);     
        }       
        string str = "select * from students;";       
        mysql_real_query(&mysql, str.c_str(), str.size());       
        MYSQL_RES *result = mysql_store_result(&mysql);       
        MYSQL_ROW row;     
        while (row = mysql_fetch_row(result)) {         
            cout<<"id: "<<row[0]<<" name: "<<row[1]<<" age: "<<row[2]<<endl;     
        }       
        mysql_free_result(result);       
        mysql_close(&mysql); 
        }   
        
        
        int main() {     
            cout << "Hello World!111" << endl;     
            sql_create();     
            cout << "Hello World!222" << endl;     
            sql_add();     
            cout << "Hello World!333" << endl;     
            sql_query();     
            cout << "Hello World!444" << endl;     
            sql_modify();     
            cout << "Hello World!555" << endl;     
            sql_query();     
            cout << "Hello World!666" << endl;     
            sql_delete();     
            cout << "Hello World!777" << endl;     
            sql_query();       
            return 0; 
        }
