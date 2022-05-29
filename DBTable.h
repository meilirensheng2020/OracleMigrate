
#include <condition_variable>
#include <vector>
#include <mutex>
#include <string>


#ifndef DBTable_H_
#define DBTable_H_


using namespace std;


    class DBTable {
    
    private:
      vector<string>  tmpObjects;

    public:

        mutex tabMutex;
        vector<string> tables; // table or view name
        vector<string> tabPartitions; // partition name
        string sourcedb;     
        string db_constr;
        string db_user;
        string db_password;  
        string targetdb;  
        string db_constr2;
        string db_user2;
        string db_password2;  
        string  tabStr;
        DBTable(const string sourcedb,const string targetdb,const string tabStr);
        void printTables();
        void initTables();
        ~DBTable();

    };

#endif
