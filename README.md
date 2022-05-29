# OracleMigrate
一款基于OCI的Oracle在线迁移工具，程序采用DIRECT PATH WRITE方式。

程序依赖于C++  boost库和Oracle OCI 库

[yansp@localserver OracleMigrate]$ ./OracleMigrate 
OracleMigrate v1.0.0 (C) 2020-2021 by yanshoupeng

Supported Options:
  -h [ --help ]         show help message
  
  --source arg           source database information
  
  --target arg          target database information
  
  --tables arg          tables to migrate
  
  --logfile arg         logfile name
  
  --threads arg         number of threads(for pattition table)
  
  --bindsize arg        number of row  for every load
  
  --merge arg           merge partition table into non partition table ,default
                        false
                        
  --buffersize arg      buffer size 
  


