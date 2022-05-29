

#include <iostream>
#include "DBTable.h"
#include "ocilib.hpp"
#include <boost/algorithm/string.hpp>

using namespace std;
using namespace ocilib;



DBTable::DBTable(const string sourcedb,const string targetdb,const string tabStr) :
    sourcedb(sourcedb),
    targetdb(targetdb),
    tabStr(tabStr) {
        boost::split(tmpObjects, tabStr,boost::is_any_of(","), boost::token_compress_on);

        vector <string> dbinfo;
        boost::split(dbinfo, sourcedb,boost::is_any_of(":"), boost::token_compress_on);
        if (dbinfo.size()==5) {

            //username:password:ip:port:dbname
            db_user     =  dbinfo[0];
            db_password =  dbinfo[1];
            db_constr   =  dbinfo[2]+":"+dbinfo[3]+"/" + dbinfo[4];

        }  else
        {
             cerr << "- ERROR: invalid source database information" << endl;
        }

        dbinfo.clear();
         boost::split(dbinfo, targetdb,boost::is_any_of(":"), boost::token_compress_on);
        if (dbinfo.size()==5) {

            //username:password:ip:port:dbname
            db_user2     =  dbinfo[0];
            db_password2 =  dbinfo[1];
            db_constr2   =  dbinfo[2]+":"+dbinfo[3]+"/" + dbinfo[4];

        }  else
        {
             cerr << "- ERROR: invalid target database information" << endl;
        }

        initTables();
}

DBTable::~DBTable() {
}


void DBTable::printTables(){
    for(size_t i=0; i< tmpObjects.size();i++) {
        cout << tmpObjects[i] << endl;
    }
}

void DBTable::initTables(){
        
    try
    {
        //Environment::Initialize();

        Connection con(db_constr, db_user, db_password);
        Statement st(con);

        for(size_t i=0; i< tmpObjects.size();i++) {

            string tmpName = tmpObjects[i];
            string sqltxt="SELECT TABLE_NAME||':'||PARTITION_NAME  \
                    FROM ALL_TAB_PARTITIONS   \
                    WHERE TABLE_NAME=upper('" + tmpName + "') and table_owner=upper('"+db_user+"')";

            st.Execute(sqltxt);
            Resultset rs = st.GetResultset();
            if (rs++)
            {
                tabPartitions.push_back(rs.Get<ostring>(1));  //partition tables
                while(rs++) {
                    tabPartitions.push_back(rs.Get<ostring>(1));  //partition tables
                }
            }   else
            {
                string::size_type pos = tmpName.find(":") ;
                if (pos >= 0)
                    tabPartitions.push_back(tmpName);   
                else     
                     tables.push_back(tmpName);
        
                     
            }
            
            
   
        }


    }
    catch (std::exception &ex)
    {
        std::cout << ex.what() << std::endl;
    }

}


