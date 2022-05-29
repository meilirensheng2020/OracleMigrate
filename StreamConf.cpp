#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include <string>
#include <algorithm>
#include <memory>
#include <iostream>
#include <unistd.h>
#include "StreamConf.h"

using namespace std;
 namespace po = boost::program_options;

  static void normalFile(std::ifstream& inf, std::stringstream& ss) {
    std::string s;
    const int max_line_size = 1024;
    char line[1024];
    while (inf) {
      inf.getline(line, max_line_size);
      s = line;
      boost::trim(s);
      if (boost::starts_with(s, "#")) continue;
      if (!s.empty()) ss << s << std::endl;
    }
  }

  StreamConf::StreamConf(int argc, char** argv) : desc("Supported Options") {
    try
    {
      add_options();
      po::store(po::parse_command_line(argc, argv, desc), vm);

      if(vm.empty() || vm.count("help") ){

          validParams();
          exit(1);
      }

      if (vm.count("confFile")) {
        std::ifstream inf(vm["confFile"].as<std::string>().c_str());
        //util::dassert("Can't open read from config file", inf.is_open());
        std::stringstream ss;
        normalFile(inf, ss);
        po::store(po::parse_config_file(ss, desc), vm);
      }

      po::notify(vm);
      
    }
    catch(const std::exception& e)
    {
      std::cerr << "error:" << e.what()  << endl;
      std::cout << endl;
    }
    
  }

  void StreamConf::add_options() {
    desc.add_options()("help,h", "show help message")(
        "source", po::value<std::string>()," source database information")(
        "target", po::value<std::string>(), "target database information")(
        "tables", po::value<std::string>(), "tables to migrate")(
        "logfile", po::value<std::string>(), "logfile name")(
        "threads", po::value<int>(), "number of threads(for pattition table)")(
        "bindsize", po::value<int>(), "number of row  for every load")(
        "merge", po::value<bool>(), "merge partition table into non partition table ,default false")(
        "buffersize", po::value<int>(),
        "buffer size ");
  }

  void StreamConf::validParams() {
      std::cout << desc << std::endl;
    /*util::dassert("srcConn/tarConn/tableConf are must be set.",
                  vm.count("srcConn") && vm.count("tarConn") &&
                      vm.count("tableConf") && vm.count("instId"));*/
  }

  int StreamConf::getInt(const char* para, int default_value) {
    if (vm.count(para)) return vm[para].as<int>();
    return default_value;
  }

  uint32_t StreamConf::getUint32(const char* para) {
    return vm[para].as<uint32_t>();
  }

  std::string StreamConf::getString(const char* para,
                                    const char* default_value) {
    if (vm.count(para)) return vm[para].as<std::string>();
    return default_value;
  }

  bool StreamConf::getBool(const char* para, bool default_value) {
    if (vm.count(para)) return vm[para].as<bool>();
    return default_value;
  }


/*
  static std::list<TabConf> initCaptualTable(const char* filename) {
    std::list<TabConf> cap_tables;
    std::stringstream ss;
    std::ifstream inf(filename);
    util::dassert("Can't open read from config file", inf.is_open());
    normalFile(inf, ss);
    std::string line;
    std::vector<std::string> v(2);
    while (ss) {
      std::getline(ss, line);
      boost::trim(line);
      if (boost::starts_with(line, "#")) {
        continue;
      }
      boost::split(v, line, boost::is_any_of("#"));
      switch (v.size()) {
        case 1:
          cap_tables.push_back(TabConf(boost::trim_copy(v[0])));
          break;
        case 2:
          cap_tables.push_back(
              TabConf(boost::trim_copy(v[0]), boost::trim_copy(v[1])));
          break;
        default:
          LOG(ERROR) << "bad format of tabconf " << line;
      }
    }
    return cap_tables;
  }


  void initStream(int ac, char** av) {
    streamconf = new StreamConf(ac, av);
    std::stringstream ss;
    ss << "logging_" << streamconf->getUint32("instId") << ".conf";
    el::Configurations conf(ss.str().c_str());
    el::Loggers::reconfigureLogger("default", conf);
    auto captual_tables =
        initCaptualTable(streamconf->getString("tableConf").c_str());
    for (auto table_conf : captual_tables) {
      auto first = table_conf.tab_name.find_first_of('.');
      auto last = table_conf.tab_name.find_last_of('.');
      if (first == last && first != table_conf.tab_name.npos) {
        std::string owner = table_conf.tab_name.substr(0, first);
        std::string tablename = table_conf.tab_name.substr(
            first + 1, table_conf.tab_name.npos - first - 1);
        LOG(INFO) << "Init table " << table_conf.tab_name;
        auto tab_def =
            getMetadata().initTabDefFromName(owner.c_str(), tablename.c_str());
        if (tab_def != NULL) {
          LOG(DEBUG) << " init tab def " << tab_def->toString();
          SimpleApplier::getApplier(streamconf->getString("tarConn").c_str(),
                                    streamconf->getString("srcConn").c_str())
              .addTable(tab_def, table_conf);
        }
        // if (tabdef) tabdef->dump();
      } else {
        if (!table_conf.tab_name.empty())
          LOG(WARNING) << "Invalid Table format " << table_conf.tab_name
                       << ", The correct format is owner.table_name";
      }
    }
  }
  */
