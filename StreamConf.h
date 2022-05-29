
#include <string>
#include <boost/program_options.hpp>
#include <fstream>
#include <sstream>
#include <list>
#include <chrono>

	
  namespace po = boost::program_options;
  class StreamConf {
   public:
    StreamConf(int ac, char** av);
    int getInt(const char*, int default_value = -1);
    std::string getString(const char*, const char* default_value = "");
    bool getBool(const char*, bool default_value = false);
    uint32_t getUint32(const char* para);

   private:
    void add_options();
    void validParams();
   // void parseConfigFile(std::ifstream& inf, std::stringstream& ss);

   private:
    po::options_description desc;
    po::variables_map vm;
  };
  


