
/*
Rem $Header: oracleMigrate.cpp  $
Rem
Rem
Rem oracleMigrate (c) 2020, 2020, yanshoupeng. All rights reserved.  
Rem
Rem    NAME
Rem      oracleMigrate.cpp - main program of oracleMigrate
Rem
Rem    DESCRIPTION
Rem      The program oracleMigrate used to
Rem      load RDBMS data to other oracle database
Rem
Rem    NOTES
Rem      
Rem    MODIFIED   (YYYY-MM-DD)
Rem    yanshoupeng 2019-12-10 created

*/

#include <iostream>
#include <iomanip>
#include <fstream>
#include <list>
#include <streambuf>
#include <mutex>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include "StreamConf.h"
#include "ocilib.hpp"
#include <boost/algorithm/string.hpp>
#include "OracleReader.h"
#include "DBTable.h"


using namespace std;
using namespace ocilib;
//using namespace rapidjson;
//using namespace fullMigrate;

mutex mainMtx;
condition_variable mainThread;

void stopMain() {
    unique_lock<mutex> lck(mainMtx);
    mainThread.notify_all();
}

void signalHandler(int s) {
    cout << "Caught signal " << s << ", exiting" << endl;
    stopMain();
}

int main(int argc, char** argv)
{


    signal(SIGINT, signalHandler);
    signal(SIGPIPE, signalHandler);
	  
    list<OracleReader *> readers;
    
	// Output version information
	cout << "OracleMigrate v1.0.0 (C) 2020-2021 by yanshoupeng, yanshoupeng@yunhuansoft.com" << endl;
    cout << endl;
    
       
    try {    

        // parameters
        StreamConf *streamConf = new StreamConf(argc,argv);

        string source        = streamConf->getString("source");
        string target        = streamConf->getString("target");
        string tabstr        = streamConf->getString("tables");
        string logfile        = streamConf->getString("logfile","OracleMigrate");
        uint32_t threads     = streamConf->getInt("threads",1); 
        uint32_t bindsize    = streamConf->getInt("bindsize",1000); 
        uint32_t buffersize  = streamConf->getInt("buffersize",1048576); 
        bool merge           = streamConf->getBool("merge",false); 
        

        Environment::Initialize(Environment::Threaded);

        DBTable *dbtable = new DBTable(source,target,tabstr);
    
        
        for (int i=1;   i<=threads;  i++) {

            OracleReader  *oraReader =  new OracleReader(dbtable,i,bindsize,buffersize,merge,logfile);

            if (oraReader != nullptr) 
                pthread_create(&oraReader->pthread, nullptr, &OracleReader::runStatic, (void*)oraReader);
            readers.push_back(oraReader);
            oraReader = nullptr;
        }



        //sleep until killed
        {
            unique_lock<mutex> lck(mainMtx);
            mainThread.wait(lck);
        }

        for (OracleReader * reader : readers) {

            reader->stop();
        }
        

        for (OracleReader * reader : readers) {
            pthread_join(reader->pthread, nullptr);
            delete reader;
        }


        if (dbtable != nullptr) {
                delete dbtable;
                dbtable = nullptr;
        }         
       
    }
    catch (exception &ex)
    {
        cout << ex.what() << endl;
    }

   Environment::Cleanup();
   return EXIT_SUCCESS;
}


