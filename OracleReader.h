
#include <condition_variable>
#include <mutex>
#include <map>
#include <string>
#include <pthread.h>
#include "DBThread.h"
#include "DBTable.h"
#include "ocilib.hpp"


#ifndef ORACLEREADER_H_
#define ORACLEREADER_H_

using namespace std;
using namespace ocilib;


    class OracleReader : public DBThread {

    private:
    	Connection srcCon;
        Connection targetCon;
    	//Statement st;

        uint64_t totalRows;
        ofstream outfile;

    public:
        string tableName;
        DBTable *dbtable;
        uint32_t threadID;
        uint32_t loadsize;
        uint32_t buffersize;
        bool merge;
        string logfile;
        OracleReader(DBTable *dbtable, const uint32_t threadID,const uint32_t loadsize,const uint32_t buffersize,const bool merge,const string logfile);
        ~OracleReader();
        void *run(void);
        int initOracleEnv();
        void dataMIgrate(Connection srcCon,Connection tarCon,string objname,string partitionname,string tmpname);
    };

#endif
