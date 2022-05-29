

#include <iostream>
#include <fstream>
#include <sstream>
#include <string.h>
#include <iomanip>
#include <unistd.h>
#include <pthread.h>
#include <mutex>
#include <sys/syscall.h>
#include "ocilib.hpp"
#include <boost/algorithm/string.hpp>
#include "OracleReader.h"



#define gettid() syscall(SYS_gettid)


using namespace std;
using namespace ocilib;

string  get_objname(const string tname,const string pname)  {

   if (pname.length() > 0)
      return tname+":"+pname;
    return tname;
}

string strLpad(string strSrc, int iLen, char cFill)
{
    stringstream strStream;
    strStream<<setw(iLen)<<setfill(cFill)<<strSrc;
    return strStream.str();

}

string replaceAll(string srcStr,const string s1,const string s2){
    int pos;
    pos = srcStr.find(s1);
    while(pos != -1){
        srcStr.replace(pos,s1.length(),s2);
        pos = srcStr.find(s1);
    }

    return srcStr;
}


OracleReader::OracleReader(DBTable *dbtable, const uint32_t threadID,const uint32_t loadsize,const uint32_t buffersize,const bool merge,const string logfile) :
    DBThread("YUNHUAN"),
    totalRows(0),
    dbtable(dbtable),
    threadID(threadID),
    loadsize(loadsize),
    buffersize(buffersize),
    merge(merge),
    logfile(logfile)
 
 {

    initOracleEnv();
    string filename = logfile + strLpad(to_string(threadID),4,'0') + ".log";
    outfile.open(filename.c_str(), ios::out | ios::trunc);

}

OracleReader::~OracleReader() {

    //mysqlWriter->closeMysql();

}



void *OracleReader::run(void) {

 
   string tmpTabName,tmpPartName; 
   Timestamp tm(Timestamp::NoTimeZone);


    try {

        while (true) {  // non partitions --view

            usleep(10000);
            
            if (this->shutdown) break;

            dbtable->tabMutex.lock();

            if (dbtable->tables.empty()) {
                dbtable->tabMutex.unlock();
                break;
            }
            //fetch table 
            string tmpTabName = dbtable->tables[0];
            dbtable->tables.erase(dbtable->tables.begin());            
            //cout <<"PID:" << getpid() << ", TID:" << gettid() << ", ThreadNO:" << threadID <<", table:" << tmpTabName << endl;
            dbtable->tabMutex.unlock();

            
            tm = Timestamp::SysTimestamp();
            cout << "- INFO Starting migrate  " << tmpTabName  << ", ThreadNO:" << threadID << "," << tm.ToString("YYYY-MM-DD HH24:MI:SS:FF3") << endl;
            dataMIgrate(srcCon,targetCon,tmpTabName,"","");
            tm = Timestamp::SysTimestamp();
            cout << "- INFO End      migrate  " << tmpTabName  << ", ThreadNO:" << threadID << "," << tm.ToString("YYYY-MM-DD HH24:MI:SS:FF3") << endl;


           
        } 

        tmpTabName.clear();
        while (true) {  //partition tables

            usleep(10000);
            if (this->shutdown) break;

            dbtable->tabMutex.lock();

            if (dbtable->tabPartitions.empty()) {
                dbtable->tabMutex.unlock();
                break;
            }
            //fetch tableName   
            string tmp_TabPartName = dbtable->tabPartitions[0];
            dbtable->tabPartitions.erase(dbtable->tabPartitions.begin());            
            //cout <<"PID:" << getpid() << ", TID:" << gettid() << ", ThreadNO:" << threadID <<", table:" << tmp_TabPartName << endl;
            dbtable->tabMutex.unlock();

            //EMP:P01
            int pos = tmp_TabPartName.find(":");
            //string tmpStr = 

            if ( pos >= 0){
                tmpPartName = tmp_TabPartName.substr(pos + 1);
                tmpTabName  = tmp_TabPartName.substr(0,pos);
            }
        
            
            tm = Timestamp::SysTimestamp();
            cout << "- INFO Starting migrate  " << tmp_TabPartName  << ", ThreadNO:" << threadID << "," << tm.ToString("YYYY-MM-DD HH24:MI:SS:FF3") << endl;
            if (! merge)
                 dataMIgrate(srcCon,targetCon,tmpTabName,tmpPartName,tmpPartName);
            else
                 dataMIgrate(srcCon,targetCon,tmpTabName,"",tmpPartName);
  
            
            tm = Timestamp::SysTimestamp();
            cout << "- INFO End      migrate  " << tmp_TabPartName  << ", ThreadNO:" << threadID << "," << tm.ToString("YYYY-MM-DD HH24:MI:SS:FF3") << endl;
           
        } 

        outfile.close();

    }
    catch (std::exception e)
    {
        dbtable->tabMutex.unlock();
        cout << e.what() << endl;

    }
    
    return EXIT_SUCCESS;
}





void OracleReader::dataMIgrate(Connection srcCon,Connection tarCon,string objname,string partitionname,string tmpname){
    try
    {
        
        Timestamp tm(Timestamp::NoTimeZone);
        //uint64_t totalLoadRows = 0;
        uint64_t rowNumber = 0;

        TypeInfo table(tarCon, objname, TypeInfo::Table);
        uint16_t columnCount = table.GetColumnCount();// column counts

      
        DirectPath dp(table, columnCount, loadsize,partitionname);
      
        /* optional attributes to set */
        dp.SetBufferSize(buffersize);
        dp.SetNoLog(true);
        dp.SetParallel(true);
        dp.SetCacheSize(buffersize);

        /* describe the target table */

        for (int i = 1; i <= columnCount; i++)
        {
            Column col = table.GetColumn(i);

            string colName      = col.GetName();
            string typeName     = col.GetSQLType();
            uint16_t colSize      = col.GetSize();

            if(typeName.compare("VARCHAR2")==0) {

                dp.SetColumn(i,colName,colSize);
                continue;
            }
            if(typeName.compare("NUMBER")==0) {

                dp.SetColumn(i,colName,20);
                continue;
            }
            if(typeName.compare("DATE")==0) {

                dp.SetColumn(i,colName,30);
                continue;
            }
            if(typeName.compare("TIMESTAMP")==0) {

                dp.SetColumn(i,colName,30);
                continue;
            }

            if(typeName.compare("CLOB")==0) {

                dp.SetColumn(i,colName,104857600);
                continue;
            }

            dp.SetColumn(i,colName,4000);

        }

        /*fetch data from the source table */
        Statement st(srcCon);
        st.SetFetchSize(500);
        st.SetPrefetchSize(500);

        string sqltxt="select * from " + objname;
        if (partitionname.length() >0)
            sqltxt="select * from " + objname + " partition ("+partitionname+")";
        st.Execute(sqltxt);
        Resultset rs = st.GetResultset();

        /* prepare the load */
        dp.Prepare();

        while (rs++)
        {

            if (this->shutdown) break;

            rowNumber++;
            dp.Reset();

            for (int colIndex =1; colIndex <=columnCount;colIndex++) 
                dp.SetEntry(rowNumber, colIndex, rs.Get<ostring>(colIndex));

            if(rowNumber==loadsize) {

                dp.Convert();
                dp.Load();
               // totalLoadRows = totalLoadRows + dp.GetCurrentRows() ;
               
                tm = Timestamp::SysTimestamp();
                stringstream ss;
                ss << get_objname(objname,tmpname) <<", "<< tm.ToString("YYYY-MM-DD HH24:MI:SS:FF3") << " => loaded rows : " << dp.GetRowCount() << std::endl;
                outfile << ss.rdbuf();
		        outfile.flush();
                //std::cout <<get_objname(objname,partitionname) <<", "<< tm.ToString("YYYY-MM-DD HH24:MI:SS:FF3") << " => loaded rows : " << dp.GetRowCount() << std::endl;

                rowNumber=0; //reset  rowNumber

            }  

            
        }

        if (rowNumber > 0 && rowNumber< loadsize) {
            dp.SetCurrentRows(rowNumber);
            dp.Convert();
            dp.Load();
            //totalLoadRows = totalLoadRows + dp.GetCurrentRows() ;
       
            tm = Timestamp::SysTimestamp();
            stringstream ss;
            ss << get_objname(objname,tmpname) <<", "<< tm.ToString("YYYY-MM-DD HH24:MI:SS:FF3") << " => loaded rows : " << dp.GetRowCount() << std::endl;
            outfile << ss.rdbuf();
            outfile.flush();
            rowNumber = 0;

        }

        dp.Finish();
        tm = Timestamp::SysTimestamp();
        stringstream ss;
        ss << get_objname(objname,tmpname) <<", "<< tm.ToString("YYYY-MM-DD HH24:MI:SS:FF3")  <<", Total loaded rows : " <<  dp.GetRowCount() << " (Total)" << std::endl;
        outfile << ss.rdbuf();
		outfile.flush();
        

    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    
}





int OracleReader::initOracleEnv() {
    
    try{      

        srcCon = Connection(dbtable->db_constr,dbtable->db_user,dbtable->db_password);
        srcCon.SetFormat(FormatDate,"YYYY-MM-DD HH24:MI:SS");	
        srcCon.SetFormat(FormatTimestamp,"YYYY-MM-DD HH24:MI:SS:FF3");



        targetCon = Connection(dbtable->db_constr2,dbtable->db_user2,dbtable->db_password2);
        targetCon.SetFormat(FormatDate,"YYYY-MM-DD HH24:MI:SS");	
        targetCon.SetFormat(FormatTimestamp,"YYYY-MM-DD HH24:MI:SS:FF3");	
        


    }
    catch (exception &ex)
    {
        cout << ex.what() << endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}



