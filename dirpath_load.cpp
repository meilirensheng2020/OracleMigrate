#include <iostream>
#include <string>
#include "ocilib.hpp"
/* requires script demo/products.sql */
using namespace ocilib;
using namespace std;

const int DirPathBufferSize = 104857600;
const int DirPathLoadSize = 10000;
//const int DirPathLoadCount = 10;
const int DirPathColumnCount = 5;
const int CommitNumber = DirPathLoadSize*10;


void dataMIgrate(Connection srcCon,Connection tarCon,string objname,string partitionname){
    try
    {
        
        uint64_t totalLoadRows = 0;
        uint64_t rowNumber = 0;

        TypeInfo table(tarCon, objname, TypeInfo::Table);
        uint16_t columnCount = table.GetColumnCount();// column counts
        DirectPath dp(table, columnCount, DirPathLoadSize,partitionname);

        /* optional attributes to set */
        dp.SetBufferSize(DirPathBufferSize);
        dp.SetNoLog(true);
        dp.SetParallel(false);
        dp.SetCacheSize(DirPathBufferSize);

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
        st.Execute("select * from " + objname);
        Resultset rs = st.GetResultset();

        /* prepare the load */
        dp.Prepare();
        dp.Reset();

        while (rs++)
        {


            rowNumber++;

            for (int colIndex =1; colIndex <=columnCount;colIndex++) 
                dp.SetEntry(rowNumber, colIndex, rs.Get<ostring>(colIndex));

            if(rowNumber==DirPathLoadSize) {

                dp.Convert();
                dp.Load();
               // totalLoadRows = totalLoadRows + dp.GetCurrentRows() ;
                Timestamp tm(Timestamp::NoTimeZone);
                tm = Timestamp::SysTimestamp();
                std::cout << tm.ToString("YYYY-MM-DD HH24:MI:SS:FF3") << " => loaded rows : " << dp.GetRowCount() << std::endl;

                dp.Reset();
                rowNumber=0; //reset  rowNumber

            }  

            
        }

        if (rowNumber > 0 && rowNumber< DirPathLoadSize) {
            dp.SetCurrentRows(rowNumber);
            dp.Convert();
            dp.Load();
            //totalLoadRows = totalLoadRows + dp.GetCurrentRows() ;
            Timestamp tm(Timestamp::NoTimeZone);
            tm = Timestamp::SysTimestamp();
            std::cout << tm.ToString("YYYY-MM-DD HH24:MI:SS:FF3") << " => loaded rows : " << dp.GetRowCount() << std::endl;

            dp.Reset();
            rowNumber = 0;

        }

        dp.Finish();
        std::cout << "- INFO: Total loaded rows : " <<  dp.GetRowCount() << " (Total)" << std::endl;
        

    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    
}



int main(void)
{
    try
    {
        Environment::Initialize();

        

        Connection con("127.0.0.1/yunhuandb", "dbmgr", "yanshoupeng");
        con.SetFormat(FormatDate,"YYYY-MM-DD HH24:MI:SS");	
        con.SetFormat(FormatTimestamp,"YYYY-MM-DD HH24:MI:SS:FF3");	
        
        Connection targetCon("127.0.0.1/yunhuandb", "test", "test");
        targetCon.SetFormat(FormatDate,"YYYY-MM-DD HH24:MI:SS");	
        targetCon.SetFormat(FormatTimestamp,"YYYY-MM-DD HH24:MI:SS:FF3");	


        dataMIgrate(con,targetCon,"YANSP_TMP","");


        

    }
    catch (std::exception &ex)
    {
        std::cout << ex.what() << std::endl;
    }

    Environment::Cleanup();

    return EXIT_SUCCESS;
}


