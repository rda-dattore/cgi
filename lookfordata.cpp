#include <iostream>
#include <string>
#include <set>
#include <map>
#include <sstream>
#include <regex>
#include <web/web.hpp>
#include <MySQL.hpp>
#include <metadata.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <gridutils.hpp>
#include <search.hpp>
#include <bsort.hpp>

using std::cout;
using std::endl;
using std::find_if;
using std::make_tuple;
using std::map;
using std::regex;
using std::regex_search;
using std::shared_ptr;
using std::stoi;
using std::string;
using std::stringstream;
using std::set;
using std::tuple;
using std::vector;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
string myerror = "";
string mywarning = "";

struct LocalArgs {
  LocalArgs() : url_input(), lkey(), new_browse(false), display_cache(false) { }

  struct UrlInput {
    UrlInput() : refine_by(), browse_by(), browse_value(), browse_by_list(),
        browse_value_list(), compare_list(), origin(), from_home_page(),
        refine_color() { }

    string refine_by, browse_by, browse_value;
    std::list<string> browse_by_list, browse_value_list, compare_list;
    string origin, from_home_page, refine_color;
  };

  UrlInput url_input;
  string lkey;
  bool new_browse, display_cache;
} local_args;

struct TimeResolution {
  TimeResolution() : key(), types(nullptr) { }

  string key;
  shared_ptr<string> types;
};

struct ComparisonEntry {
  ComparisonEntry() : key(), title(), summary(), type(), start(), end(),
      order(0), time_resolution_table(), data_types(), formats(),
      grid_resolutions(), projects(), supported_projects(), platforms() { }

  string key;
  string title, summary, type;
  string start, end;
  size_t order;
  my::map<TimeResolution> time_resolution_table;
  std::list<string> data_types, formats, grid_resolutions, projects,
      supported_projects, platforms;
};

struct ProdComp {
  bool operator()(const string& left, const string& right) const {
    auto l = left;
    auto r = right;
    if (l.find("-hour") == string::npos) {
      l = "0-hour" + l;
    }
    auto n = 3 - l.find("-hour");
    if (n > 0) {
      l.insert(0, n, '0');
    }
    if (r.find("-hour") == string::npos) {
      r = "0-hour" + r;
    }
    n = 3 - r.find("-hour");
    if (n > 0) {
      r.insert(0, n, '0');
    }
    if (l <= r) {
      return true;
    }
    return false;
  }
};

struct GridProducts {
  GridProducts() : tables(), table(), found_analyses(false), tid(0) { }

  struct Tables {
    Tables() : forecast(), average(), accumulation(), weekly_mean(),
        monthly_mean(), monthly_var_covar(), mean(), var_covar() {}

    set<string, ProdComp> forecast, average, accumulation, weekly_mean,
        monthly_mean, monthly_var_covar, mean, var_covar;
  } tables;
  string table;
  bool found_analyses;
  pthread_t tid;
};

struct GridCoverages {
  GridCoverages() : table(), dsnum(), coverages(), tid(0) { }

  string table, dsnum;
  std::list<string> coverages;
  pthread_t tid;
};

const string SERVER_ROOT = "/" + strutils::token(unixutils::host_name(),
    ".", 0);
const string INDEXABLE_DATASET_CONDITIONS = "(d.type = 'P' or d.type = "
    "'H') and d.dsid < '999.0'";
const size_t EXPANDABLE_SUMMARY_LENGTH = 30;
set<string> prev_results_table;
vector<tuple<string, string>> breadcrumbs_table;
string http_host;
std::ofstream cache;

bool compare_strings(string& left,string& right) {
  return (left < right);
}

struct find_breadcrumb {
  find_breadcrumb(string key) : key(key) { }
  bool operator()(const tuple<string, string>& t) {
    return std::get<0>(t) == key;
  }

  string key;
};

extern "C" void *thread_summarize_grid_products(void *gpstruct) {
  MySQL::Query query;
  MySQL::Row row;
  GridProducts *g=(GridProducts *)gpstruct;
  string sdum;
  size_t fidx,aidx,cidx,zidx;

  MySQL::Server tserver(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  g->found_analyses=false;
  query.set("select distinct t.timeRange from (select distinct timeRange_code from "+g->table+") as g left join GrML.timeRanges as t on t.code = g.timeRange_code");
  if (query.submit(tserver) == 0) {
    while (query.fetch_row(row)) {
      sdum=row[0];
      fidx=sdum.find("-hour Forecast");
      aidx=sdum.find("Average");
      cidx=sdum.find("-hour Accumulation");
      if (sdum == "Analysis") {
        g->found_analyses=true;
      } else if (fidx != string::npos && fidx < 4) {

        // forecasts
        auto f = sdum.substr(0, sdum.find(" "));
        if (g->tables.forecast.find(f) == g->tables.forecast.end()) {
          g->tables.forecast.emplace(f);
        }
      } else if (aidx != string::npos) {

        // averages
        auto a = sdum.substr(0, aidx);
        strutils::trim(a);
        if (g->tables.average.find(a) == g->tables.average.end()) {
          g->tables.average.emplace(a);
        }
      } else if (cidx != string::npos && cidx < 4) {

        // accumulations
        auto a = sdum.substr(0, sdum.find(" "));
        if (g->tables.accumulation.find(a) == g->tables.accumulation.end()) {
          g->tables.accumulation.emplace(a);
        }
      } else if (regex_search(sdum,regex("^Weekly Mean"))) {
        auto m = sdum.substr(sdum.find("of") + 3);
        if (g->tables.weekly_mean.find(m) == g->tables.weekly_mean.end()) {
          g->tables.weekly_mean.emplace(m);
        }
      } else if (regex_search(sdum,regex("^Monthly Mean"))) {
        zidx=sdum.find("of");
        if (zidx != string::npos) {
          auto m = sdum.substr(zidx + 3);
          strutils::trim(m);
          if (g->tables.monthly_mean.find(m) == g->tables.monthly_mean.end()) {
            g->tables.monthly_mean.emplace(m);
          }
        }
      } else if (regex_search(sdum,regex("Mean"))) {
        std::string m = "x";
        zidx = sdum.find("of");
        if (zidx != string::npos) {
          m = sdum.substr(zidx + 3);
          zidx = m.find("at");
          if (zidx != string::npos) {
            m = m.substr(0, zidx);
          }
        } else {
          zidx=sdum.find("Mean");
          if (zidx != string::npos) {
            m = sdum.substr(0, zidx);
          }
        }
        strutils::trim(m);
        if (g->tables.mean.find(m) == g->tables.mean.end()) {
          g->tables.mean.emplace(m);
        }
      } else if (regex_search(sdum,regex("^Variance/Covariance"))) {
        auto v = sdum.substr(sdum.find("of") + 3);
        v = v.substr(v.find(" ") + 1);
        v = v.substr(0, v.find("at") - 1);
        if (g->tables.var_covar.find(v) == g->tables.var_covar.end()) {
          g->tables.var_covar.emplace(v);
        }
      }
    }
  }
  tserver.disconnect();
/*
  g->tables.forecast.keysort(sort_nhour_keys);
  g->tables.average.keysort(sort_nhour_keys);
  g->tables.accumulation.keysort(sort_nhour_keys);
  g->tables.weekly_mean.keysort(sort_nhour_keys);
  g->tables.monthly_mean.keysort(sort_nhour_keys);
  g->tables.monthly_var_covar.keysort(sort_nhour_keys);
  g->tables.mean.keysort(sort_nhour_keys);
  g->tables.var_covar.keysort(sort_nhour_keys);
*/
  return NULL;
}

extern "C" void *thread_summarize_grid_coverages(void *gcstruct) {
  GridCoverages *g=(GridCoverages *)gcstruct;
  MySQL::Server tserver(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  MySQL::Query query("select distinct d.definition,d.defParams from "+g->table+" as s left join GrML.gridDefinitions as d on d.code = s.gridDefinition_code where s.dsid = '"+g->dsnum+"'");
  if (query.submit(tserver) == 0) {
    for (const auto& row : query) {
      g->coverages.emplace_back(row[0]+"<!>"+row[1]);
    }
  }
  tserver.disconnect();
  return NULL;
}

string category(string short_name) {
  string long_name;

  if (short_name == "var") {
    long_name="Variable / Parameter";
  } else if (short_name == "tres") {
    long_name="Time Resolution";
  } else if (short_name == "plat") {
    long_name="Platform";
  } else if (short_name == "sres") {
    long_name="Spatial Resolution";
  } else if (short_name == "topic") {
    long_name="Topic / Subtopic";
  } else if (short_name == "proj") {
    long_name="Project / Experiment";
  } else if (short_name == "type") {
    long_name="Type of Data";
  } else if (short_name == "supp") {
    long_name="Supports Project";
  } else if (short_name == "fmt") {
    long_name="Data Format";
  } else if (short_name == "instr") {
    long_name="Instrument";
  } else if (short_name == "loc") {
    long_name="Location";
  } else if (short_name == "prog") {
    long_name="Progress";
  } else if (short_name == "ftext") {
    long_name="Free Text";
  } else if (short_name == "recent") {
    long_name="Recently Added / Updated";
  } else if (short_name == "doi") {
    long_name="Datasets with DOIs";
  } else if (short_name == "all") {
    long_name="All RDA Datasets";
  }
  if (!long_name.empty()) {
    return long_name;
  }
  return short_name;
}

void read_cache() {
  std::ifstream ifs;
  std::ofstream ofs;
  char line[256];
  string rmatch,bmatch;
  int nmatch=0,n=0;
  int num_lines=0;
  TempFile *tfile=NULL;
  std::deque<string> sp;

  ifs.open((SERVER_ROOT+"/tmp/browse."+local_args.lkey).c_str());
  if (ifs.is_open()) {
    if (regex_search(local_args.url_input.refine_by,regex("^@"))) {
      sp=strutils::split(local_args.url_input.refine_by,"-");
      rmatch=sp[0];
      if (sp.size() > 1) {
        nmatch=stoi(sp[1]);
      } else {
        nmatch=1;
      }
      local_args.url_input.refine_by=sp[0].substr(1);
    } else if (local_args.url_input.browse_by.size() > 0) {
      bmatch="@"+local_args.url_input.browse_by+"<!>"+local_args.url_input.browse_value;
    }
    ifs.getline(line,256);
    while (!ifs.eof()) {
      ++num_lines;
      std::string s = line;
      if (line[0] == '@') {
        sp=strutils::split(s,"<!>");
        if (!rmatch.empty()) {
          if (sp[0] == rmatch) {
            ++n;
            if (n == nmatch) {
              break;
            }
          }
        } else if (!bmatch.empty()) {
          if (regex_search(s,regex("^"+bmatch))) {
            break;
          }
        }
        auto key = sp[0].substr(1) + "<!>" + sp[1];
        if (find_if(breadcrumbs_table.begin(), breadcrumbs_table.end(),
            find_breadcrumb(key)) == breadcrumbs_table.end()) {
          breadcrumbs_table.emplace_back(make_tuple(key, sp[2]));
        }
        prev_results_table.clear();
      } else if (prev_results_table.find(s) == prev_results_table.end()) {
        prev_results_table.insert(s);
      }
      ifs.getline(line,256);
    }
    if (!ifs.eof()) {
      ifs.seekg(0,std::ios::beg);
      tfile=new TempFile;
      ofs.open(tfile->name().c_str());
      num_lines--;
      for (n=0; n < num_lines; n++) {
        ifs.getline(line,256);
        ofs << line << endl;
      }
      ofs.close();
    }
    ifs.close();
    if (tfile != NULL) {
      stringstream oss,ess;
      unixutils::mysystem2("/bin/mv "+tfile->name()+" "+SERVER_ROOT+"/tmp/browse."+local_args.lkey,oss,ess);
    }
  }
}

void redirect_to_error() {
  cout << "Location: /index.html?hash=error&code=404" << endl << endl;
  exit(1);
}

void parse_query() {
  QueryString query_string(QueryString::GET);
  if (!query_string) {
    redirect_to_error();
  }
  auto nb=query_string.value("nb");
  if (nb == "y") {
    local_args.new_browse=true;
  }
  auto dc=query_string.value("dc");
  if (dc == "y") {
    local_args.display_cache=true;
  }
  local_args.url_input.refine_by=query_string.value("r");
  local_args.url_input.browse_by_list=query_string.values("b");
  if (local_args.url_input.browse_by_list.size() < 2) {
    local_args.url_input.browse_by=query_string.value("b");
  }
  local_args.url_input.browse_value_list=query_string.values("v");
  if (local_args.url_input.browse_value_list.size() < 2) {
    local_args.url_input.browse_value=query_string.value("v");
  }
  local_args.url_input.origin=query_string.value("o");
  if (local_args.url_input.browse_by_list.size() != local_args.url_input.browse_value_list.size()) {
    redirect_to_error();
  }
  if (local_args.url_input.browse_by_list.size() > 1) {
    local_args.new_browse=true;
  }
  local_args.url_input.from_home_page=query_string.value("hp");
  local_args.url_input.refine_color=query_string.value("rc");
  if (local_args.url_input.refine_color.empty()) {
    local_args.url_input.refine_color="#eafaff";
  }
  if (local_args.url_input.browse_by == "type") {
    local_args.url_input.browse_value=strutils::substitute(strutils::to_lower(local_args.url_input.browse_value)," ","_");
  }
  local_args.lkey=value_from_cookie("lkey");
  local_args.url_input.compare_list=query_string.values("cmp");
  if (!local_args.new_browse) {
    read_cache();
  } else {
    if (!local_args.lkey.empty()) {
      system(("rm -f "+SERVER_ROOT+"/tmp/browse."+local_args.lkey).c_str());
    }
    local_args.lkey=strutils::strand(30);
    cout << "Set-Cookie: lkey=" << local_args.lkey << "; domain=" << http_host << "; path=/;" << endl;
  }
}

void parse_refine_query(MySQL::Query& query) {
  metautils::read_config("lookfordata","","");
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  vector<std::tuple<string, int>> kw_cnts;
  if (query.submit(server) == 0) {
    MySQL::Row row;
    while (query.fetch_row(row)) {
      bool add_to_list;
      if (prev_results_table.size() > 0) {
        if (prev_results_table.find(row[1]) != prev_results_table.end()) {
          add_to_list=true;
        } else {
          add_to_list=false;
        }
      } else {
        add_to_list=true;
      }
      if (add_to_list) {
        std::string key;
        if (row[0].empty()) {
          key = "Not specified";
        } else if (local_args.url_input.refine_by == "loc") {
          key = strutils::substitute(row[0], "United States Of America", "USA");
        } else if (local_args.url_input.refine_by == "prog") {
          if (row[0] == "Y") {
            key = "Continually Updated";
          } else if (row[0] == "N") {
            key = "Complete";
          }
        } else {
          key = row[0];
        }
        strutils::trim(key);
        if (breadcrumbs_table.size() == 0 || find_if(breadcrumbs_table.begin(),
            breadcrumbs_table.end(), find_breadcrumb(local_args.url_input
            .refine_by + "<!>" + key)) == breadcrumbs_table.end()) {
          if (local_args.url_input.refine_by == "type") {
            key = strutils::capitalize(key);
          }
          int n;
          if (prev_results_table.size() > 0) {
            n = 1;
          } else {
            n = stoi(row[1]);
          }
          auto i = std::find_if(kw_cnts.begin(), kw_cnts.end(),
              [&key](const std::tuple<string, int>& t) -> bool {
                if (std::get<0>(t) == key) {
                  return true;
                }
                return false;
              });
          if (i == kw_cnts.end()) {
            kw_cnts.emplace_back(std::make_tuple(key, n));
          }
          else {
            std::get<1>(*i) += n;
          }
        }
      }
    }
  }
  cout << "Content-type: text/html" << endl << endl;
  cout << "<div style=\"background-color: #27aae0\">";
  if (local_args.url_input.from_home_page != "yes") {
    cout << "<div style=\"font-size: 16px; font-weight: bold; padding: 2px; text-align: center; width: auto; height: 20px; line-height: 20px\">" << category(local_args.url_input.refine_by) << "</div>";
  }
  if (kw_cnts.size() > 0) {
    if (local_args.url_input.refine_by.substr(1) != "res") {
      std::sort(kw_cnts.begin(), kw_cnts.end(),
          [](const std::tuple<string, int>& left, const std::tuple<string, int>&
              right) -> bool {
            if (std::get<0>(left) == "Not specified") {
              return true;
            } else if (std::get<0>(right) == "Not specified") {
              return false;
            } else {
              auto l = strutils::substitute(strutils::to_lower(std::get<0>(
                  left)), "proprietary_", "");
              auto r = strutils::substitute(strutils::to_lower(std::get<0>(
                  right)), "proprietary_", "");
              return l <= r;
            }
          });
    }
    for (const auto& e : kw_cnts) {
      cout << "<div style=\"background-color: " << local_args.url_input.refine_color << "; margin-bottom: 1px\"><table style=\"font-size: 13px\"><tr valign=\"top\"><td>&nbsp;&bull;</td><td>";
      if (local_args.url_input.from_home_page == "yes") {
        cout << "<a href=\"/index.html#!lfd?b=" << local_args.url_input.refine_by << "&v=" << std::get<0>(e) << "\" onClick=\"javascript:slideUp('refine-slider')\">";
      } else {
        cout << "<a href=\"javascript:void(0)\" onClick=\"javascript:slideIn('refine-slider',function(){ document.getElementById(lastoutid).style.fontWeight='normal'; });getContent('lfd-content','/cgi-bin/lookfordata?b=" << local_args.url_input.refine_by << "&v=" << std::get<0>(e) << "')\">";
      }
      if (local_args.url_input.refine_by == "proj" || local_args.url_input.refine_by == "supp") {
        size_t idx;
        if ( (idx=std::get<0>(e).find(">")) != string::npos) {
          cout << "<b>" << std::get<0>(e).substr(0,idx) << "</b>" << std::get<0>(e).substr(idx);
        } else {
          cout << std::get<0>(e);
        }
      } else if (local_args.url_input.refine_by == "fmt") {
        auto fmt=strutils::substitute(std::get<0>(e),"proprietary_","");
        cout << strutils::to_capital(fmt);
      } else {
        if (std::get<0>(e) == strutils::to_upper(std::get<0>(e))) {
          cout << strutils::capitalize(std::get<0>(e));
        } else {
          cout << std::get<0>(e);
        }
      }
      cout << "</a> <small class=\"mediumGrayText\">(" << std::get<1>(e) << ")</small>" << "</td></tr></table></div>" << endl;
    }
    cout << "<div style=\"background-color: " << local_args.url_input.refine_color << "; line-height: 1px\">&nbsp;</div>";
    cout << endl;
  } else {
    cout << "<div style=\"background-color: " << local_args.url_input.refine_color << "; margin-top: 1px\"><table style=\"font-size: 13px\"><tr><td>&nbsp;&nbsp;</td><td>No additional options are available</td></tr></table></div>" << endl;
  }
  cout << "</div>" << endl;
  server.disconnect();
}

void show_refine_results() {
  MySQL::Query query;
  if (local_args.url_input.refine_by == "var") {
    if (prev_results_table.size() > 0) {
      query.set("select distinct substring_index(g.path,' > ',-1),v.dsid from search.variables_new as v left join search.GCMD_sciencekeywords as g on g.uuid = v.keyword where v.vocabulary = 'GCMD'");
    } else {
      query.set("select s.keyword,count(distinct s.dsid) from (select distinct substring_index(g.path,' > ',-1) as keyword,v.dsid from search.variables_new as v left join search.GCMD_sciencekeywords as g on g.uuid = v.keyword left join search.datasets as d on d.dsid = v.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and v.vocabulary = 'GCMD') as s group by s.keyword");
    }
  } else if (local_args.url_input.refine_by == "tres") {
    if (prev_results_table.size() > 0) {
      query.set("select distinct replace(substring(t.keyword,5),' - ',' to '),d.dsid,s.idx from search.datasets as d left join search.time_resolutions as t on t.dsid = d.dsid left join search.time_resolution_sort as s on s.keyword = t.keyword where "+INDEXABLE_DATASET_CONDITIONS+" order by s.idx");
    } else {
      query.set("select replace(substring(t.keyword,5),' - ',' to '),count(distinct d.dsid),any_value(s.idx) as idx from search.datasets as d left join search.time_resolutions as t on t.dsid = d.dsid left join search.time_resolution_sort as s on s.keyword = t.keyword where "+INDEXABLE_DATASET_CONDITIONS+" and !isnull(idx) group by t.keyword order by idx");
    }
  } else if (local_args.url_input.refine_by == "plat") {
    if (prev_results_table.size() > 0) {
      query.set("select distinct g.last_in_path,d.dsid from search.datasets as d left join search.platforms_new as p on p.dsid = d.dsid left join search.GCMD_platforms as g on g.uuid = p.keyword where "+INDEXABLE_DATASET_CONDITIONS);
    } else {
      query.set("select distinct g.last_in_path,count(distinct d.dsid) from search.datasets as d left join search.platforms_new as p on p.dsid = d.dsid left join search.GCMD_platforms as g on g.uuid = p.keyword where "+INDEXABLE_DATASET_CONDITIONS+" group by g.last_in_path");
    }
  } else if (local_args.url_input.refine_by == "sres") {
    if (prev_results_table.size() > 0) {
      query.set("select distinct replace(substring(r.keyword,5),' - ',' to '),d.dsid,s.idx from search.datasets as d left join search.grid_resolutions as r on r.dsid = d.dsid left join search.grid_resolution_sort as s on s.keyword = r.keyword where "+INDEXABLE_DATASET_CONDITIONS+" order by s.idx");
    } else {
      query.set("select replace(substring(r.keyword,5),' - ',' to '),count(distinct d.dsid),any_value(s.idx) as idx from search.datasets as d left join search.grid_resolutions as r on r.dsid = d.dsid left join search.grid_resolution_sort as s on s.keyword = r.keyword where "+INDEXABLE_DATASET_CONDITIONS+" and !isnull(idx) group by r.keyword order by idx");
    }
  } else if (local_args.url_input.refine_by == "topic") {
    if (prev_results_table.size() > 0) {
      query.set("select distinct substring_index(substring_index(g.path,' > ',-3),' > ',2),v.dsid from search.variables_new as v left join search.GCMD_sciencekeywords as g on g.uuid = v.keyword where v.vocabulary = 'GCMD'");
    } else {
      query.set("select s.keyword,count(distinct s.dsid) from (select distinct substring_index(substring_index(g.path,' > ',-3),' > ',2) as keyword,v.dsid from search.variables_new as v left join search.GCMD_sciencekeywords as g on g.uuid = v.keyword left join search.datasets as d on d.dsid = v.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and v.vocabulary = 'GCMD') as s group by s.keyword");
    }
  } else if (local_args.url_input.refine_by == "proj") {
    if (prev_results_table.size() > 0) {
      query.set("select distinct g.last_in_path,d.dsid from search.datasets as d left join search.projects_new as p on p.dsid = d.dsid left join search.GCMD_projects as g on g.uuid = p.keyword where "+INDEXABLE_DATASET_CONDITIONS);
    } else {
      query.set("select distinct g.last_in_path,count(distinct d.dsid) from search.datasets as d left join search.projects_new as p on p.dsid = d.dsid left join search.GCMD_projects as g on g.uuid = p.keyword where "+INDEXABLE_DATASET_CONDITIONS+" group by g.last_in_path");
    }
  } else if (local_args.url_input.refine_by == "type") {
    if (prev_results_table.size() > 0) {
      query.set("select distinct t.keyword,d.dsid from search.datasets as d left join search.data_types as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS);
    } else {
      query.set("select distinct t.keyword,count(distinct d.dsid) from search.datasets as d left join search.data_types as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" group by t.keyword");
    }
  } else if (local_args.url_input.refine_by == "supp") {
    if (prev_results_table.size() > 0) {
      query.set("select distinct g.last_in_path,d.dsid from search.datasets as d left join search.supportedProjects_new as p on p.dsid = d.dsid left join search.GCMD_projects as g on g.uuid = p.keyword where "+INDEXABLE_DATASET_CONDITIONS);
    } else {
      query.set("select g.last_in_path,count(distinct d.dsid) from search.datasets as d left join search.supportedProjects_new as p on p.dsid = d.dsid left join search.GCMD_projects as g on g.uuid = p.keyword where "+INDEXABLE_DATASET_CONDITIONS+" group by g.last_in_path");
    }
  } else if (local_args.url_input.refine_by == "fmt") {
    if (prev_results_table.size() > 0) {
      query.set("select distinct f.keyword,d.dsid from search.datasets as d left join search.formats as f on f.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS);
    } else {
      query.set("select distinct f.keyword,count(distinct d.dsid) from search.datasets as d left join search.formats as f on f.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" group by f.keyword");
    }
  } else if (local_args.url_input.refine_by == "instr") {
    if (prev_results_table.size() > 0) {
      query.set("select distinct g.last_in_path,d.dsid from search.datasets as d left join search.instruments_new as i on i.dsid = d.dsid left join search.GCMD_instruments as g on g.uuid = i.keyword where "+INDEXABLE_DATASET_CONDITIONS);
    } else {
      query.set("select distinct g.last_in_path,count(distinct d.dsid) from search.datasets as d left join search.instruments_new as i on i.dsid = d.dsid left join search.GCMD_instruments as g on g.uuid = i.keyword where "+INDEXABLE_DATASET_CONDITIONS+" group by g.last_in_path");
    }
  } else if (local_args.url_input.refine_by == "loc") {
    if (prev_results_table.size() > 0) {
      query.set("select distinct if(instr(l.keyword,'United States') > 0,substring_index(l.keyword,' > ',-2),substring_index(l.keyword,' > ',-1)),d.dsid from search.datasets as d left join search.locations as l on l.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and (isnull(l.include) or l.include != 'N')");
    } else {
      query.set("select distinct if(instr(l.keyword,'United States') > 0,substring_index(l.keyword,' > ',-2),substring_index(l.keyword,' > ',-1)),count(distinct d.dsid) from search.datasets as d left join search.locations as l on l.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and (isnull(l.include) or l.include != 'N') group by l.keyword");
    }
  } else if (local_args.url_input.refine_by == "prog") {
    if (prev_results_table.size() > 0) {
      query.set("select continuing_update,dsid from search.datasets as d where "+INDEXABLE_DATASET_CONDITIONS);
    } else {
      query.set("select continuing_update,count(distinct dsid) from search.datasets as d where "+INDEXABLE_DATASET_CONDITIONS+" group by continuing_update");
    }
  } else if (local_args.url_input.refine_by == "ftext") {
    cout << "Content-type: text/html" << endl << endl;
    cout << "<div style=\"padding: 5px 0px 0px 5px\"><form name=\"fts\" action=\"javascript:void(0)\" onSubmit=\"javascript:if (v.value.length == 0) { alert('Please enter one or more free text keywords to search on'); return false; } else { slideIn('refine-slider',function(){ document.getElementById(lastoutid).style.fontWeight='normal'; }); getContent('lfd-content','/cgi-bin/lookfordata?b='+document.fts.b.value+'&v='+document.fts.v.value); return true; }\"><span class=\"fs14px\">Filter the dataset list by entering one or more keywords.  If you preceed a keyword with a minus sign (e.g. <nobr><span class=\"fixedWidth14\">-temperature</span></nobr>), then datasets containing that keyword will be excluded.  Otherwise, datasets containing your keyword(s) will be included in the filtered list.</span><br /><input class=\"fixedWidth12\" type=\"text\" name=\"v\" value=\"keyword(s)\" size=\"25\" onClick=\"javascript:this.value=''\" /><input type=\"hidden\" name=\"b\" value=\"ftext\" /><br /><input type=\"submit\" /></form></div>" << endl;
    exit(0);
  }
//std::cerr << query.show() << endl;
  if (query) {
    parse_refine_query(query);
  } else {
    redirect_to_error();
  }
}

void open_cache_for_writing() {
  if (local_args.new_browse) {
    cache.open((SERVER_ROOT+"/tmp/browse."+local_args.lkey).c_str());
  } else {
    cache.open((SERVER_ROOT+"/tmp/browse."+local_args.lkey).c_str(),std::fstream::app);
  }
}

void add_breadcrumbs(size_t num_results) {
  string breadcrumbs="Showing datasets with these attributes: ";
  cout << "<div id=\"breadcrumbs\" style=\"background-color: #9cc991; padding: 5px; margin-top: 3px; margin-bottom: 10px; font-size: 13px\">" << breadcrumbs;
  map<string, int> count_table;
  auto n=0;
  for (const auto& e : breadcrumbs_table) {
    if (n > 0) {
      cout << " <b>&gt;</b> ";
    }
    auto kparts=strutils::split(std::get<0>(e), "<!>");
    auto bval=kparts[1];
    if (kparts[0] == "var" || kparts[0] == "type") {
      bval=strutils::capitalize(bval);
    } else if (kparts[0] == "ftext") {
      bval="'"+bval+"'";
    }
    cout << "<a style=\"font-weight: bold; padding-left: 5px\" href=\"javascript:void(0)\" onClick=\"javascript:document.getElementById('breadcrumbs').innerHTML='"+breadcrumbs+"';slideOutFrom('" << kparts[0];
    if (n > 0) {
      breadcrumbs+=" <b>&gt;</b> ";
    }
    breadcrumbs+="<a style=&quot;font-weight: bold; padding-left: 5px&quot; href=&quot;javascript:void(0)&quot; onClick=&quot;javascript:slideOutFrom(\\'"+kparts[0];
    if (count_table.find(kparts[0]) == count_table.end()) {
      count_table.emplace(kparts[0], 1);
    } else {
      ++count_table[kparts[0]];
    }
    cout << "-" << count_table[kparts[0]] << "','')\"><nobr>" << category(kparts[0]) << "</nobr></a> : " << bval << " <span class=\"mediumGrayText\">(" << std::get<1>(e) << ")</span>";
    breadcrumbs+="-"+strutils::itos(count_table[kparts[0]])+"\\',\\'\\')&quot;><nobr>"+category(kparts[0])+"</nobr></a> : "+bval+" <span class=&quot;mediumGrayText&quot;>("+std::get<1>(e)+")</span>";
    ++n;
  }
  if (n > 0) {
    cout << " <b>&gt;</b> ";
  }
  auto& k = local_args.url_input.browse_by;
  cout << "<a style=\"font-weight: bold; padding-left: 5px\" href=\"javascript:void(0)\" onClick=\"javascript:document.getElementById('breadcrumbs').innerHTML='"+breadcrumbs+"';slideOutFrom('" << k;
  if (count_table.find(k) == count_table.end()) {
    count_table.emplace(k, 1);
  } else {
    ++count_table[k];
  }
  cout << "-" << count_table[k] << "','')\"><nobr>" << category(k) << "</nobr></a> : ";
  if (k == "var" || k == "type") {
    cout << strutils::capitalize(local_args.url_input.browse_value);
  } else if (k == "ftext") {
    cout << "'" << local_args.url_input.browse_value << "'";
  } else {
    cout << local_args.url_input.browse_value;
  }
  cout << " <span class=\"mediumGrayText\">(" << num_results << ")</span>";
  cout << "</div>" << endl;
  if (num_results > 1) {
    cout << "<script id=\"dscompare\" language=\"javascript\">" << endl;
    cout << "function submitCompare() {" << endl;
    cout << "  var parameters='';" << endl;
    cout << "  var num_checked=0;" << endl;
    cout << "  for (n=0; n < document.compare.elements.length; n++) {" << endl;
    cout << "    if (document.compare.elements[n].checked) {" << endl;
    cout << "      parameters+='&cmp='+document.compare.elements[n].value;" << endl;
    cout << "      num_checked++;" << endl;
    cout << "    }" << endl;
    cout << "  }" << endl;
    cout << "  if (num_checked > 2) {" << endl;
    cout << "    alert(\"You can only compare two datasets.  Please uncheck all but two checkboxes.\");" << endl;
    cout << "    return;" << endl;
    cout << "  } else if (num_checked < 2) {" << endl;
    cout << "    alert(\"Please check the boxes beside two datasets that you would like to compare.\");" << endl;
    cout << "    return;" << endl;
    cout << "  }" << endl;
    cout << "  location='/index.html#!lfd?'+parameters.substr(1);" << endl;
    cout << "}" << endl;
    cout << "</script>" << endl;
    cout << "<div style=\"overflow: hidden; background-color: #eff5df; padding: 5px; margin-bottom: 10px\"><div style=\"display: inline; float: left; font-size: 14px\">Select two datasets and <input type=\"button\" value=\"Compare\" onClick=\"javascript:submitCompare()\"> them.</div><div style=\"display: inline; float: right; margin-right: 10px; font-size: 14px\"><input type=\"reset\" value=\"Reset\"> checkboxes</div></div>" << endl;
  }
}

void show_datasets_after_processing(MySQL::PreparedStatement& pstmt,int num_entries,bool display_results) {
  MySQL::Row row;
  string sdum;
  size_t num_results=0,iterator;
  int n=0;

  map<string, int> multi_table;
  while (pstmt.fetch_row(row)) {
    if (prev_results_table.find(row[0]) != prev_results_table.end()) {
      if (num_entries < 2) {
        ++num_results;
      } else {
        if (multi_table.find(row[0]) == multi_table.end()) {
          multi_table.emplace(row[0], 0);
        }
        ++multi_table[row[0]];
        if (multi_table[row[0]] == num_entries) {
          ++num_results;
        }
      }
    }
  }
  open_cache_for_writing();
  cache << "@" << local_args.url_input.browse_by << "<!>" << local_args.url_input.browse_value << "<!>" << num_results << endl;
  if (display_results) {
    cout << "Content-type: text/html" << endl << endl;
    cout << "<span class=\"fs24px bold\">Browse the RDA</span><br>" << endl;
    cout << "<form name=\"compare\" action=\"javascript:void(0)\" method=\"get\">" << endl;
    add_breadcrumbs(num_results);
  }
  pstmt.rewind();
  while (pstmt.fetch_row(row)) {
    if (prev_results_table.find(row[0]) != prev_results_table.end() && (num_entries < 2 || (multi_table.find(row[0]) != multi_table.end() && multi_table[row[0]] == num_entries))) {
      multi_table.erase(row[0]);
      cache << row[0] << endl;
      sdum=strutils::itos(n+1);
      if (display_results) {
        cout << "<div style=\"padding: 5px\" onMouseOver=\"javascript:this.style.backgroundColor='#eafaff'\" onMouseOut=\"javascript:this.style.backgroundColor='transparent'\">";
        if (row[3] == "H") {
          cout << "<img src=\"/images/alert.gif\">&nbsp;<span style=\"color: red\">For ancillary use only - not recommended as a primary research dataset.  It has likely been superseded by newer and better datasets.</span><br>";
        }
        if (num_results > 1) {
          cout << "<input type=\"checkbox\" name=\"cmp\" value=\"" << row[0] << "\">&nbsp;";
        }
        cout << sdum << ". <a class=\"underline\" href=\"/datasets/ds" << row[0] << "/\" target=\"_blank\"><b>" << row[1] << "</b></a> <span class=\"mediumGrayText\">(ds" << row[0] << ")</span><br>" << endl;
        cout << "<div class=\"browseSummary\">" << searchutils::convert_to_expandable_summary(row[2],EXPANDABLE_SUMMARY_LENGTH,iterator) << "</div>" << endl;
        cout << "</div><br>" << endl;
        ++n;
      }
    }
  }
  cache.close();
  if (display_results) {
    cout << "</form>" << endl;
    cout << "</body></html>" << endl;
  }
}

void show_datasets_from_query(MySQL::PreparedStatement& pstmt,bool display_results) {
  MySQL::Row row;
  string sdum;
  int n=0;
  size_t iterator;

  open_cache_for_writing();
  cache << "@" << local_args.url_input.browse_by << "<!>" << local_args.url_input.browse_value << "<!>" << pstmt.num_rows() << endl;
  if (display_results) {
    cout << "Content-type: text/html" << endl << endl;
    cout << "<span class=\"fs24px bold\">Browse the RDA</span><br>" << endl;
    cout << "<form name=\"compare\" action=\"javascript:void(0)\">" << endl;
    add_breadcrumbs(pstmt.num_rows());
  }
  while (pstmt.fetch_row(row)) {
    cache << row[0] << endl;
    if (display_results) {
      sdum=strutils::itos(n+1);
      cout << "<div style=\"padding: 5px\" onMouseOver=\"javascript:this.style.backgroundColor='#eafaff'\" onMouseOut=\"javascript:this.style.backgroundColor='transparent'\" itemscope itemtype=\"http://schema.org/Dataset\">";
      if (row[3] == "H") {
        cout << "<img src=\"/images/alert.gif\">&nbsp;<span style=\"color: red\">For ancillary use only - not recommended as a primary research dataset.  It has likely been superseded by newer and better datasets.</span><br>";
      }
      if (pstmt.num_rows() > 1) {
        cout << "<input type=\"checkbox\" name=\"cmp\" value=\""+row[0]+"\">&nbsp;";
      }
      cout << sdum+". <a class=\"underline\" href=\"/datasets/ds"+row[0]+"/\" target=\"_blank\" itemprop=\"url\"><b itemprop=\"name\">"+row[1]+"</b></a> <span class=\"mediumGrayText\">(ds"+row[0]+")</span><br>" << endl;
      cout << "<div class=\"browseSummary\" itemprop=\"description\">"+searchutils::convert_to_expandable_summary(row[2],EXPANDABLE_SUMMARY_LENGTH,iterator)+"</div>" << endl;
      cout << "</div><br>" << endl;
      ++n;
    }
  }
  if (display_results) {
    cout << "</form>" << endl;
  }
  cache.close();
}

void browse(bool display_results = true) {
  metautils::read_config("lookfordata","","");
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  int num_entries=0;
  MySQL::PreparedStatement pstmt;
  string pstmt_error;
  bool pstmt_good=false;
  if (local_args.url_input.browse_by == "var") {
    pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.variables_new as v left join search.GCMD_sciencekeywords as g on g.uuid = v.keyword left join search.datasets as d on d.dsid = v.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and g.path like concat('% > ',?) group by d.dsid order by d.type,trank",vector<enum_field_types>{MYSQL_TYPE_STRING},vector<string>{local_args.url_input.browse_value},pstmt,pstmt_error);
  } else if (local_args.url_input.browse_by == "tres") {
    if (local_args.url_input.browse_value == "Not specified") {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.time_resolutions as r on r.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(r.keyword) group by d.dsid order by d.type,trank",vector<enum_field_types>{},vector<string>{},pstmt,pstmt_error);
    } else {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.time_resolutions as r left join search.datasets as d on d.dsid = r.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and r.keyword = concat('T : ',?) group by d.dsid order by d.type,trank",vector<enum_field_types>{MYSQL_TYPE_STRING},vector<string>{strutils::substitute(local_args.url_input.browse_value," to "," - ")},pstmt,pstmt_error);
    }
  } else if (local_args.url_input.browse_by == "plat") {
    if (local_args.url_input.browse_value == "Not specified") {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.platforms_new as p on p.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(p.keyword) group by d.dsid order by d.type,trank",vector<enum_field_types>{},vector<string>{},pstmt,pstmt_error);
    } else {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.platforms_new as p left join search.GCMD_platforms as g on g.uuid = p.keyword left join search.datasets as d on d.dsid = p.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and (g.last_in_path = ? or g.path like concat('% > ',?)) group by d.dsid order by d.type,trank",vector<enum_field_types>{MYSQL_TYPE_STRING,MYSQL_TYPE_STRING},vector<string>{local_args.url_input.browse_value,local_args.url_input.browse_value},pstmt,pstmt_error);
    }
  } else if (local_args.url_input.browse_by == "sres") {
    if (local_args.url_input.browse_value == "Not specified") {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.grid_resolutions as g on g.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(g.keyword) group by d.dsid order by d.type,trank",vector<enum_field_types>{},vector<string>{},pstmt,pstmt_error);
    } else {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.grid_resolutions as g left join search.datasets as d on d.dsid = g.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and g.keyword = concat('H : ',?) group by d.dsid order by d.type,trank",vector<enum_field_types>{MYSQL_TYPE_STRING},vector<string>{strutils::substitute(local_args.url_input.browse_value," to "," - ")},pstmt,pstmt_error);
    }
  } else if (local_args.url_input.browse_by == "topic") {
    auto parts=strutils::split(local_args.url_input.browse_value," > ");
    auto topic_condition="v.topic = '"+parts[0]+"'";
    if (parts.size() > 1) {
      topic_condition+=" and v.term = '"+parts[1]+"'";
    }
    pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.GCMD_variables as v left join search.datasets as d on d.dsid = v.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and "+topic_condition+" group by d.dsid order by d.type,trank",vector<enum_field_types>{},vector<string>{},pstmt,pstmt_error);
  } else if (local_args.url_input.browse_by == "proj") {
    if (local_args.url_input.browse_value == "Not specified") {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.projects_new as p on p.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(p.keyword) group by d.dsid order by d.type,trank",vector<enum_field_types>{},vector<string>{},pstmt,pstmt_error);
    } else {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.projects_new as p left join search.GCMD_projects as g on g.uuid = p.keyword left join search.datasets as d on d.dsid = p.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and (g.last_in_path = ? or g.path like concat('% > ',?)) group by d.dsid order by d.type,trank",vector<enum_field_types>{MYSQL_TYPE_STRING,MYSQL_TYPE_STRING},vector<string>{local_args.url_input.browse_value,local_args.url_input.browse_value},pstmt,pstmt_error);
    }
  } else if (local_args.url_input.browse_by == "type") {
    if (local_args.url_input.browse_value == "Not specified") {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.data_types as y on y.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(y.keyword) group by d.dsid order by d.type,trank",vector<enum_field_types>{},vector<string>{},pstmt,pstmt_error);
    } else {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.data_types as y on y.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and y.keyword = ? group by d.dsid order by d.type,trank",vector<enum_field_types>{MYSQL_TYPE_STRING},vector<string>{local_args.url_input.browse_value},pstmt,pstmt_error);
    }
  } else if (local_args.url_input.browse_by == "supp") {
    if (local_args.url_input.browse_value == "Not specified") {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.supportedProjects_new as s on s.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(s.keyword) group by d.dsid order by d.type,trank",vector<enum_field_types>{},vector<string>{},pstmt,pstmt_error);
    } else {
      vector<enum_field_types> parameter_types;
      vector<string> parameters;
      string qspec="select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.supportedProjects_new as p left join search.GCMD_projects as g on g.uuid = p.keyword left join search.datasets as d on d.dsid = p.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and (g.last_in_path = '"+local_args.url_input.browse_value+"' or g.path like '% > "+local_args.url_input.browse_value+"')";
      string pstmt_spec="select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.supportedProjects_new as p left join search.GCMD_projects as g on g.uuid = p.keyword left join search.datasets as d on d.dsid = p.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and (g.last_in_path = ? or g.path like concat('% > ',?))";
      parameter_types.emplace_back(MYSQL_TYPE_STRING);
      parameters.emplace_back(local_args.url_input.browse_value);
      parameter_types.emplace_back(MYSQL_TYPE_STRING);
      parameters.emplace_back(local_args.url_input.browse_value);
      if (!local_args.url_input.origin.empty()) {
        pstmt_spec+=" and p.origin = ?";
        parameter_types.emplace_back(MYSQL_TYPE_STRING);
        parameters.emplace_back(local_args.url_input.origin);
      }
      pstmt_spec+=" group by d.dsid order by d.type,trank";
      pstmt_good=run_prepared_statement(server,pstmt_spec,parameter_types,parameters,pstmt,pstmt_error);
    }
  } else if (local_args.url_input.browse_by == "fmt") {
    if (local_args.url_input.browse_value == "Not specified") {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.formats as f on f.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(f.keyword) group by d.dsid order by d.type,trank",vector<enum_field_types>{},vector<string>{},pstmt,pstmt_error);
    } else {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.formats as f left join search.datasets as d on d.dsid = f.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and f.keyword = ? group by d.dsid order by d.type,trank",vector<enum_field_types>{MYSQL_TYPE_STRING},vector<string>{local_args.url_input.browse_value},pstmt,pstmt_error);
    }
  } else if (local_args.url_input.browse_by == "instr") {
    if (local_args.url_input.browse_value == "Not specified") {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.instruments_new as i on i.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(i.keyword) group by d.dsid order by d.type,trank",vector<enum_field_types>{},vector<string>{},pstmt,pstmt_error);
    } else {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.instruments_new as i left join search.GCMD_instruments as g on g.uuid = i.keyword left join search.datasets as d on d.dsid = i.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and g.last_in_path = ? group by d.dsid order by d.type,trank",vector<enum_field_types>{MYSQL_TYPE_STRING},vector<string>{local_args.url_input.browse_value},pstmt,pstmt_error);
    }
  } else if (local_args.url_input.browse_by == "loc") {
    if (local_args.url_input.browse_value == "Not specified") {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.locations as l on l.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(l.keyword) group by d.dsid order by d.type,trank",vector<enum_field_types>{},vector<string>{},pstmt,pstmt_error);
    } else {
      auto keyword=strutils::substitute(local_args.url_input.browse_value,"USA","United States Of America");
      strutils::replace_all(keyword,"'","\\'");
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.locations as l left join search.datasets as d on d.dsid = l.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and l.keyword like concat('% > ',?) group by d.dsid order by d.type,trank",vector<enum_field_types>{MYSQL_TYPE_STRING},vector<string>{keyword},pstmt,pstmt_error);
    }
  } else if (local_args.url_input.browse_by == "prog") {
    if (local_args.url_input.browse_value == "Complete") {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and d.continuing_update = 'N' group by d.dsid order by d.type,trank",vector<enum_field_types>{},vector<string>{},pstmt,pstmt_error);
    } else if (local_args.url_input.browse_value == "Continually Updated") {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and d.continuing_update = 'Y' group by d.dsid order by d.type,trank",vector<enum_field_types>{},vector<string>{},pstmt,pstmt_error);
    }
  } else if (local_args.url_input.browse_by == "ftext") {
    auto parts=strutils::split(local_args.url_input.browse_value);
    string include_words,exclude_words;
    vector<enum_field_types> parameter_types;
    vector<string> parameters;
    for (size_t n=0; n < parts.size(); ++n) {
      if (parts[n].front() == '-') {
        if (!exclude_words.empty()) {
          exclude_words+=" or ";
        }
        exclude_words+="word = ?";
        parameter_types.emplace_back(MYSQL_TYPE_STRING);
        parameters.emplace_back(parts[n].substr(1));
      } else {
        auto word=parts[n];
        bool ignore;
        auto sword=searchutils::cleaned_search_word(word,ignore);
        if (!include_words.empty()) {
          include_words+=" or ";
        }
        include_words+="word = ? or (word like ? and sword = ?)";
        for (size_t m=0; m < 2; ++m) {
          parameter_types.emplace_back(MYSQL_TYPE_STRING);
          parameters.emplace_back(word);
          parameter_types.emplace_back(MYSQL_TYPE_STRING);
          parameters.emplace_back(sword);
          parameter_types.emplace_back(MYSQL_TYPE_STRING);
          parameters.emplace_back(strutils::soundex(sword));
        }
        num_entries++;
      }
    }
    if (!include_words.empty() && !exclude_words.empty()) {
    } else if (!include_words.empty()) {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank,word from (select dsid,word from search.title_wordlist where "+include_words+" union select dsid,word from search.summary_wordlist where "+include_words+") as u left join search.datasets as d on d.dsid = u.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" group by d.dsid,word order by d.type,trank",parameter_types,parameters,pstmt,pstmt_error);
    } else {
      pstmt_good=run_prepared_statement(server,"select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join (select dsid from search.title_wordlist where "+exclude_words+" union select dsid from search.summary_wordlist where "+exclude_words+") as u on u.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(u.dsid) group by d.dsid order by d.type,trank",parameter_types,parameters,pstmt,pstmt_error);
    }
  } else if (local_args.url_input.browse_by == "recent") {
    if (prev_results_table.size() > 0) {
      redirect_to_error();
    } else {
      pstmt_good=run_prepared_statement(server,"select d.dsid,d.title,d.summary,d.type,max(mssdate) as dm from dssdb.dataset as m left join search.datasets as d on concat('ds',d.dsid) = m.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and mssdate >= '"+dateutils::current_date_time().days_subtracted(60).to_string("%Y-%m-%d")+"' group by d.dsid order by d.type,dm desc",vector<enum_field_types>{},vector<string>{},pstmt,pstmt_error);
    }
  } else if (local_args.url_input.browse_by == "doi") {
    if (prev_results_table.size() > 0) {
      redirect_to_error();
    } else {
      pstmt_good=run_prepared_statement(server,"select d.dsid,d.title,d.summary,d.type from dssdb.dsvrsn as v left join search.datasets as d on concat('ds',d.dsid) = v.dsid where v.status = 'A' and (d.type = 'P' or d.type = 'H') order by d.type,d.dsid",vector<enum_field_types>{},vector<string>{},pstmt,pstmt_error);
    }
  } else if (local_args.url_input.browse_by == "all") {
    if (prev_results_table.size() > 0) {
      redirect_to_error();
    } else {
      pstmt_good=run_prepared_statement(server,"select dsid,title,summary,type from search.datasets as d where "+INDEXABLE_DATASET_CONDITIONS+" order by type,dsid",vector<enum_field_types>{},vector<string>{},pstmt,pstmt_error);
    }
  }
  if (pstmt_good) {
    server.disconnect();
    if (prev_results_table.size() > 0) {
      show_datasets_after_processing(pstmt,num_entries,display_results);
    } else {
      show_datasets_from_query(pstmt,display_results);
    }
  } else {
std::cerr << "LOOKFORDATA query failed with error " << pstmt_error << ": '" << pstmt.show() << "'" << endl;
    redirect_to_error();
  }
}

void display_cache() {
  breadcrumbs_table.clear();
  read_cache();
  cout << "Content-type: text/html" << endl << endl;
  cout << "<span class=\"fs24px bold\">Browse the RDA</span><br>" << endl;
  cout << "<form name=\"compare\" action=\"javascript:void(0)\" method=\"get\">" << endl;
  auto bparts = strutils::split(std::get<0>(breadcrumbs_table.back()), "<!>");
  local_args.url_input.browse_by=bparts[0];
  local_args.url_input.browse_value=bparts[1];
  breadcrumbs_table.pop_back();
  auto num_results=prev_results_table.size();
  add_breadcrumbs(num_results);
  string qstring;
  for (const auto& e : prev_results_table) {
    if (!qstring.empty()) {
      qstring+=",";
    }
    qstring += "'" + e + "'";
  }
  metautils::read_config("lookfordata","","");
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  MySQL::LocalQuery query("select dsid,title,summary,type from search.datasets where dsid in ("+qstring+") order by field(dsid,"+qstring+")");
  if (query.submit(server) == 0) {
    MySQL::Row row;
    auto n=0;
    size_t iterator;
    while (query.fetch_row(row)) {
      auto snum=strutils::itos(n+1);
      cout << "<div style=\"padding: 5px\" onMouseOver=\"javascript:this.style.backgroundColor='#eafaff'\" onMouseOut=\"javascript:this.style.backgroundColor='transparent'\">";
      if (row[3] == "H") {
        cout << "<img src=\"/images/alert.gif\">&nbsp;<span style=\"color: red\">For ancillary use only - not recommended as a primary research dataset.  It has likely been superseded by newer and better datasets.</span><br>";
      }
      if (num_results > 1) {
        cout << "<input type=\"checkbox\" name=\"cmp\" value=\"" << row[0] << "\">&nbsp;";
      }
      cout << snum << ". <a class=\"underline\" href=\"/datasets/ds" << row[0] << "/\" target=\"_blank\"><b>" << row[1] << "</b></a> <span class=\"mediumGrayText\">(ds" << row[0] << ")</span><br>" << endl;
      cout << "<div class=\"browseSummary\">" << searchutils::convert_to_expandable_summary(row[2],EXPANDABLE_SUMMARY_LENGTH,iterator) << "</div>" << endl;
      cout << "</div><br>" << endl;
      ++n;
    }
  }
  cout << "</form>" << endl;
  server.disconnect();
}

void show_start() {
  MySQL::Query query;
  MySQL::Row row;
  string num_ds;

  metautils::read_config("lookfordata","","");
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  query.set("select count(distinct dsid) from search.datasets as d where "+INDEXABLE_DATASET_CONDITIONS);
  if (query.submit(server) == 0) {
    if (query.fetch_row(row)) {
      num_ds=row[0];
    }
  }
  query.set("select 'var',count(distinct s1.dsid) from (select v.dsid from search.GCMD_variables as v left join search.datasets as d on d.dsid = v.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s1 union select 'tres',count(distinct s2.dsid) from (select t.dsid from search.time_resolutions as t left join search.datasets as d on d.dsid = t.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s2 union select 'plat',count(distinct s3.dsid) from (select p.dsid from search.platforms as p left join search.datasets as d on d.dsid = p.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s3 union select 'sres',count(distinct s4.dsid) from (select g.dsid from search.grid_resolutions as g left join search.datasets as d on d.dsid = g.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s4 union select 'topic',count(distinct s5.dsid) from (select t.dsid from search.GCMD_topics as t left join search.datasets as d on d.dsid = t.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s5 union select 'proj',count(distinct s6.dsid) from (select p.dsid from search.projects as p left join search.datasets as d on d.dsid = p.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s6 union select 'type',count(distinct s7.dsid) from (select t.dsid from search.data_types as t left join search.datasets as d on d.dsid = t.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s7 union select 'supp',count(distinct s8.dsid) from (select s.dsid from search.supportedProjects_new as s left join search.datasets as d on d.dsid = s.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s8 union select 'fmt',count(distinct s9.dsid) from (select f.dsid from search.formats as f left join search.datasets as d on d.dsid = f.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s9 union select 'loc',count(distinct s10.dsid) from (select l.dsid from search.locations as l left join search.datasets as d on d.dsid = l.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s10");
  map<string, string> cnts;
  if (query.submit(server) == 0) {
    while (query.fetch_row(row)) {
      cnts.emplace(row[0], row[1]);
    }
  }
  server.disconnect();
  cout << "Content-type: text/html" << endl << endl;
  cout << "<h1>Browse the RDA</h1>" << endl;
  cout << "<p>There are " << num_ds << " public datasets in the CISL RDA.  You can begin browsing the datasets by choosing one of the facets in the menu to the left.  Facet descriptions are given below, along with the number (in parentheses) of datasets in each.</p>" << endl;
  cout << "<table cellspacing=\"0\" cellpadding=\"5\" border=\"0\">" << endl;
  cout << "<tr valign=\"top\">" << endl;
  cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Variable / Parameter";
  auto i = cnts.find("var");
  if (i != cnts.end()) {
    cout << " <small class=\"mediumGrayText\">(" << i->second << ")</small>";
  }
  cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">A variable or parameter is the quantity that is measured, derived, or computed - e.g. the data value.</div></td>" << endl;
  cout << "<td>&nbsp;&nbsp;&nbsp;&nbsp;</td>" << endl;
  cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Type of Data";
  i = cnts.find("type");
  if (i != cnts.end()) {
    cout << " <small class=\"mediumGrayText\">(" << i->second << ")</small>";
  }
  cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">This refers to the type of data values - e.g. grid (interpolated or computed gridpoint data), platform observation (in-situ and remotely sensed measurements), etc.</div></td>" << endl;
  cout << "</tr>" << endl;
  cout << "<tr valign=\"top\">" << endl;
  cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Time Resolution";
  i = cnts.find("tres");
  if (i != cnts.end()) {
    cout << " <small class=\"mediumGrayText\">(" << i->second << ")</small>";
  }
  cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">This refers to the distance in time between discrete observation measurements, model product valid times, etc.</div></td>" << endl;
  cout << "<td>&nbsp;&nbsp;&nbsp;&nbsp;</td>" << endl;
  cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Platform";
  i = cnts.find("plat");
  if (i != cnts.end()) {
    cout << " <small class=\"mediumGrayText\">(" << i->second << ")</small>";
  }
  cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">The platform is the entity or type of entity that acquired or computed the data (e.g. aircraft, land station, reanalysis model).</div></td>" << endl;
  cout << "</tr>" << endl;
  cout << "<tr valign=\"top\">" << endl;
  cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Spatial Resolution";
  i = cnts.find("sres");
  if (i != cnts.end()) {
    cout << " <small class=\"mediumGrayText\">(" << i->second << ")</small>";
  }
  cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">This refers to the horizontal distance between discrete gridpoints of a model product, reporting stations in a network, measurements of a moving platform, etc.</div></td>" << endl;
  cout << "<td>&nbsp;&nbsp;&nbsp;&nbsp;</td>" << endl;
  cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Topic / Subtopic";
  i = cnts.find("topic");
  if (i != cnts.end()) {
    cout << " <small class=\"mediumGrayText\">(" << i->second << ")</small>";
  }
  cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">Topic and subtopic are high-level groupings of parameters - e.g. Atmosphere (topic), Clouds (subtopic of Atmosphere).</div></td>" << endl;
  cout << "</tr>" << endl;
  cout << "<tr valign=\"top\">";
  cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Project / Experiment";
  i = cnts.find("proj");
  if (i != cnts.end()) {
    cout << " <small class=\"mediumGrayText\">(" << i->second << ")</small>";
  }
  cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">This is the scientific project, field campaign, or experiment that acquired the data.</div></td>" << endl;
  cout << "<td>&nbsp;&nbsp;&nbsp;&nbsp;</td>" << endl;
  cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Supports Project";
  i = cnts.find("supp");
  if (i != cnts.end()) {
    cout << " <small class=\"mediumGrayText\">(" << i->second << ")</small>";
  }
  cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">This refers to data that were acquired to support a scientific project or experiment (e.g. GATE) or that can be used as ingest for a project (e.g. WRF).</div></td>" << endl;
  cout << "</tr>" << endl;
  cout << "<tr valign=\"top\">" << endl;
  cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Data Format";
  i = cnts.find("fmt");
  if (i != cnts.end()) {
    cout << " <small class=\"mediumGrayText\">(" << i->second << ")</small>";
  }
  cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">This refers to the structure of the bitstream used to encapsulate the data values in a record or file - e.g ASCII, netCDF, etc.</div></td>" << endl;
  cout << "<td>&nbsp;&nbsp;&nbsp;&nbsp;</td>" << endl;
  cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Location";
  i = cnts.find("loc");
  if (i != cnts.end()) {
    cout << " <small class=\"mediumGrayText\">(" << i->second << ")</small>";
  }
  cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">This the name of the (usually geographic) location or region for which the data are valid.</div></td>" << endl;
  cout << "</tr>" << endl;
  cout << "</table>" << endl;
  server.disconnect();
}

string set_date_time(string datetime,string flag,string time_zone) {
  string dt;
  size_t n;
  
  n=stoi(flag);
  dt=datetime;
  switch (n) {
    case 1:
      dt=dt.substr(0,4);
      break;
    case 2:
      dt=dt.substr(0,7);
      break;
    case 3:
      dt=dt.substr(0,10);
      break;
    case 4:
      dt=dt.substr(0,13);
      dt+=" "+time_zone;
      break;
    case 5:
      dt=dt.substr(0,16);
      dt+=" "+time_zone;
      break;
    case 6:
      dt+=" "+time_zone;
      break;
  }
  return dt;
}

void fill_comparison_dataset(ComparisonEntry& de_ref) {
  metautils::read_config("lookfordata","","");
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  MySQL::Query query("title,type","search.datasets","dsid = '"+de_ref.key+"'");
  if (query.submit(server) < 0) {
    web_error("fill_comparison_dataset():\n"+query.error()+"\n"+query.show());
  }
  MySQL::Row row;
  query.fetch_row(row);
  de_ref.title=row[0];
  de_ref.type=row[1];
  query.set("select min(concat(date_start,' ',time_start)),min(start_flag),max(concat(date_end,' ',time_end)),min(end_flag),any_value(time_zone) from dssdb.dsperiod where dsid = 'ds"+de_ref.key+"' group by dsid");
  if (query.submit(server) < 0) {
    web_error("fill_comparison_dataset():\n"+query.error()+"\n"+query.show());
  }
  query.fetch_row(row);
  if (!row.empty()) {
    de_ref.start=set_date_time(row[0],row[1],row[4]);
    de_ref.end=set_date_time(row[2],row[3],row[4]);
  } else {
    de_ref.start=set_date_time("9998","1","");
  }
  de_ref.data_types.clear();
  query.set("select distinct keyword from search.data_types where dsid = '"+de_ref.key+"' order by keyword");
  if (query.submit(server) < 0) {
    web_error("fill_comparison_dataset():\n"+query.error()+"\n"+query.show());
  }
  while (query.fetch_row(row)) {
    de_ref.data_types.emplace_back(row[0]);
  }
  de_ref.formats.clear();
  query.set("select distinct keyword from search.formats where dsid = '"+de_ref.key+"' order by keyword");
  if (query.submit(server) < 0) {
    web_error("fill_comparison_dataset():\n"+query.error()+"\n"+query.show());
  }
  while (query.fetch_row(row)) {
    de_ref.formats.emplace_back(row[0]);
  }
  de_ref.time_resolution_table.clear();
  query.set("select distinct t.keyword,t.origin,ts.idx from search.time_resolutions as t left join search.time_resolution_sort as ts on t.keyword = ts.keyword where dsid = '"+de_ref.key+"' order by ts.idx");
  if (query.submit(server) < 0) {
    web_error("fill_comparison_dataset():\n"+query.error()+"\n"+query.show());
  }
  while (query.fetch_row(row)) {
    TimeResolution tr;
    if (!de_ref.time_resolution_table.found(row[0],tr)) {
      tr.key=row[0];
      tr.types.reset(new string);
      de_ref.time_resolution_table.insert(tr);
    }
    if (tr.types->length() > 0) {
      *(tr.types)+=", ";
    }
    if (row[1] == "GrML") {
      *(tr.types)+="Grids";
    } else if (row[1] == "ObML") {
      *(tr.types)+="Platform Observations";
    }
  }
  de_ref.grid_resolutions.clear();
  query.set("select distinct t.keyword,ts.idx from search.grid_resolutions as t left join search.grid_resolution_sort as ts on t.keyword = ts.keyword where dsid = '"+de_ref.key+"' order by ts.idx");
  if (query.submit(server) < 0) {
    web_error("fill_comparison_dataset():\n"+query.error()+"\n"+query.show());
  }
  while (query.fetch_row(row)) {
    de_ref.grid_resolutions.emplace_back(row[0]);
  }
  de_ref.projects.clear();
  query.set("select distinct g.last_in_path from search.projects as p left join search.GCMD_projects as g on g.uuid = p.keyword where p.dsid = '"+de_ref.key+"' order by g.last_in_path");
  if (query.submit(server) < 0) {
    web_error("fill_comparison_dataset():\n"+query.error()+"\n"+query.show());
  }
  while (query.fetch_row(row)) {
    de_ref.projects.emplace_back(row[0]);
  }
  de_ref.supported_projects.clear();
  query.set("select distinct g.last_in_path from search.supportedProjects_new as p left join search.GCMD_projects as g on g.uuid = p.keyword where p.dsid = '"+de_ref.key+"' order by g.last_in_path");
  if (query.submit(server) < 0) {
    web_error("fill_comparison_dataset():\n"+query.error()+"\n"+query.show());
  }
  while (query.fetch_row(row)) {
    de_ref.supported_projects.emplace_back(row[0]);
  }
  de_ref.platforms.clear();
  query.set("select distinct g.last_in_path from search.platforms_new as p left join search.GCMD_platforms as g on g.uuid = p.keyword  where p.dsid = '"+de_ref.key+"' order by g.last_in_path");
  if (query.submit(server) < 0) {
    web_error("fill_comparison_dataset():\n"+query.error()+"\n"+query.show());
  }
  while (query.fetch_row(row)) {
    de_ref.platforms.emplace_back(row[0]);
  }
}

void write_entries(const set<string, ProdComp>& s) {
  auto started = false;
  for (const auto& e : s) {
    if (started) {
      cout << ", ";
    }
    cout << e;
    started = true;
  }
}

void write_gridded_products(GridProducts& gp) {
  size_t num=0;

  if (gp.found_analyses) {
    ++num;
  }
  if (gp.tables.forecast.size() > 0) {
    ++num;
  }
  if (gp.tables.average.size() > 0) {
    ++num;
  }
  if (gp.tables.accumulation.size() > 0) {
    ++num;
  }
  if (gp.tables.weekly_mean.size() > 0) {
    ++num;
  }
  if (gp.tables.monthly_mean.size() > 0) {
    ++num;
  }
  if (gp.tables.monthly_var_covar.size() > 0) {
    ++num;
  }
  if (gp.tables.mean.size() > 0) {
    ++num;
  }
  if (gp.tables.var_covar.size() > 0) {
    ++num;
  }
  if (gp.found_analyses) {
    if (num > 1) {
      cout << "&bull;&nbsp;";
    }
    cout << "Analyses<br>";
  }
  if (gp.tables.forecast.size() > 0) {
    if (num > 1) {
      cout << "&bull;&nbsp;";
    }
    cout << "Forecasts <small class=\"mediumGrayText\">(";
    write_entries(gp.tables.forecast);
    cout << ")</small><br>";
  }
  if (gp.tables.average.size() > 0) {
    if (num > 1) {
      cout << "&bull;&nbsp;";
    }
    cout << "Averages <small class=\"mediumGrayText\">(";
    write_entries(gp.tables.average);
    cout << ")</small><br>";
  }
  if (gp.tables.accumulation.size() > 0) {
    if (num > 1) {
      cout << "&bull;&nbsp;";
    }
    cout << "Accumulations <small class=\"mediumGrayText\">(";
    write_entries(gp.tables.accumulation);
    cout << ")</small><br>";
  }
  if (gp.tables.weekly_mean.size() > 0) {
    if (num > 1) {
      cout << "&bull;&nbsp;";
    }
    cout << "Weekly Means <small class=\"mediumGrayText\">(";
    write_entries(gp.tables.weekly_mean);
    cout << ")</small><br>";
  }
  if (gp.tables.monthly_mean.size() > 0) {
    if (num > 1) {
      cout << "&bull;&nbsp;";
    }
    cout << "Monthly Means <small class=\"mediumGrayText\">";
    cout << "(";
    write_entries(gp.tables.monthly_mean);
    cout << ")</small><br>";
  }
  if (gp.tables.monthly_var_covar.size() > 0) {
    if (num > 1) {
      cout << "&bull;&nbsp;";
    }
    cout << "Monthly Variances/Covariances <small class=\"mediumGrayText\">(";
    write_entries(gp.tables.monthly_var_covar);
    cout << ")</small><br>";
  }
  if (gp.tables.mean.size() > 0) {
    if (num > 1) {
      cout << "&bull;&nbsp;";
    }
    cout << "Means <small class=\"mediumGrayText\">(";
    write_entries(gp.tables.mean);
    cout << ")</small><br>";
  }
  if (gp.tables.var_covar.size() > 0) {
    if (num > 1) {
      cout << "&bull;&nbsp;";
    }
    cout << "Variances/Covariances <small class=\"mediumGrayText\">(";
    write_entries(gp.tables.var_covar);
    cout << ")</small><br>";
  }
}

void compare() {
  XMLDocument xdoc;
  std::list<XMLElement> elist;
  ComparisonEntry ce1,ce2;
  string sdum;
  vector<string> array;
  TimeResolution tr;
  string dsnum1,dsnum2,table1,table2;
  GridProducts gp1,gp2;
  GridCoverages gc1,gc2;
  size_t n;
  std::list<string>::iterator it;

  metautils::read_config("lookfordata","","");
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (local_args.url_input.compare_list.size() < 2) {
    web_error2("bad query","400 Bad Request");
  }
  it=local_args.url_input.compare_list.begin();
  ce1.key=*it;
  fill_comparison_dataset(ce1);
  ++it;
  ce2.key=*it;
  fill_comparison_dataset(ce2);
  cout << "Content-type: text/html" << endl << endl;
  cout << "<style id=\"compare\">" << endl;
  cout << "table.compare th," << endl;
  cout << "table.compare td {" << endl;
  cout << "  border: 1px solid #96afbf;" << endl;
  cout << "}" << endl;
  cout << ".left {" << endl;
  cout << "  background-color: #b8edab;" << endl;
  cout << "}" << endl;
  cout << "</style>" << endl;
  cout << "<span class=\"fs24px bold\">Comparing Datasets</span><br /><br />" << endl;
  cout << "<table class=\"compare\" style=\"border-collapse: collapse\" width=\"100%\" cellspacing=\"0\" cellpadding=\"5\">" << endl;
  cout << "<tr><th class=\"left\" width=\"1\" align=\"left\"><nobr>Dataset ID</nobr></th><th width=\"50%\" align=\"left\"><a href=\"/datasets/ds"+ce1.key+"/\">ds"+ce1.key+"</a></th><th width=\"50%\" align=\"left\"><a href=\"/datasets/ds"+ce2.key+"/\">ds"+ce2.key+"</a></th></tr>" << endl;
  cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\">Title</th><td align=\"left\">"+ce1.title+"</td><td align=\"left\">"+ce2.title+"</td></tr>" << endl;
  cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\"><nobr>Data Types</nobr></th><td align=\"left\">";
  for (const auto& type : ce1.data_types) {
    cout << "&bull;&nbsp;"+strutils::capitalize(type)+"<br>";
  }
  cout << "</td><td align=\"left\">";
  for (const auto& type : ce2.data_types) {
    cout << "&bull;&nbsp;"+strutils::capitalize(type)+"<br>";
  }
  cout << "</td></tr>" << endl;
  cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\"><nobr>Data Formats</nobr></th><td align=\"left\">";
  for (auto format : ce1.formats) {
    if (regex_search(format,regex("^proprietary_"))) {
      strutils::replace_all(format,"proprietary_","");
      format+=" (see dataset documentation)";
    }
    if (ce1.formats.size() > 1) {
      cout << "&bull;&nbsp;";
    }
    cout << strutils::to_capital(format)+"<br>";
  }
  cout << "</td><td align=\"left\">";
  for (auto format : ce2.formats) {
    if (regex_search(format,regex("^proprietary_"))) {
      strutils::replace_all(format,"proprietary_","");
      format+=" (see dataset documentation)";
    }
    if (ce2.formats.size() > 1)
      cout << "&bull;&nbsp;";
    cout << strutils::to_capital(format)+"<br>";
  }
  cout << "</td></tr>" << endl;
  cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\">Platforms</th><td align=\"left\">";
  for (const auto& platform : ce1.platforms) {
    if (ce1.platforms.size() > 1)
      cout << "&bull;&nbsp;";
    cout << platform+"<br>";
  }
  cout << "</td><td align=\"left\">";
  for (const auto& platform : ce2.platforms) {
    if (ce2.platforms.size() > 1)
      cout << "&bull;&nbsp;";
    cout << platform+"</br>";
  }
  cout << "</td></tr>" << endl;
  cout << "<tr valign=\"bottom\"><th class=\"left\" align=\"left\">Temporal Range</th><td align=\"left\">";
  if (!regex_search(ce1.start,regex("^9998"))) {
    cout << "<nobr>" << ce1.start+"</nobr> to <nobr>"+ce1.end+"</nobr>";
  }
  cout << "</td><td align=\"left\">";
  if (!regex_search(ce2.start,regex("^9998"))) {
    cout << "<nobr>"+ce2.start+"</nobr> to <nobr>"+ce2.end+"</nobr>";
  }
  cout << "</td></tr>" << endl;
  if (ce1.time_resolution_table.size() > 0 || ce2.time_resolution_table.size() > 0) {
    cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\">Temporal Resolutions</th><td align=\"left\">";
    for (const auto& key : ce1.time_resolution_table.keys()) {
      sdum=key;
      strutils::replace_all(sdum,"T : ","");
      strutils::replace_all(sdum,"-","to");
      if (ce1.time_resolution_table.size() > 1) {
        cout << "&bull;&nbsp;";
      }
      if (ce1.data_types.size() > 1) {
        ce1.time_resolution_table.found(key,tr);
        cout << sdum+" <small class=\"mediumGrayText\">("+*(tr.types)+")</small><br>";
      } else {
        cout << sdum+"<br>";
      }
    }
    cout << "</td><td align=\"left\">";
    for (const auto& key : ce2.time_resolution_table.keys()) {
      sdum=key;
      strutils::replace_all(sdum,"T : ","");
      strutils::replace_all(sdum,"-","to");
      if (ce2.time_resolution_table.size() > 1) {
        cout << "&bull;&nbsp;";
      }
      if (ce2.data_types.size() > 1) {
        ce2.time_resolution_table.found(key,tr);
        cout << sdum+" <small class=\"mediumGrayText\">("+*(tr.types)+")</small><br>";
      } else {
        cout << sdum+"<br>";
      }
    }
    cout << "</td></tr>" << endl;
  }
  dsnum1=strutils::substitute(ce1.key,".","");
  dsnum2=strutils::substitute(ce2.key,".","");
  table1="GrML.ds"+dsnum1+"_grids";
  table2="GrML.ds"+dsnum2+"_grids";
  if (table_exists(server,table1) || table_exists(server,table2)) {
    gp1.table=table1;
    pthread_create(&gp1.tid,NULL,thread_summarize_grid_products,reinterpret_cast<void *>(&gp1));
    gp2.table=table2;
    pthread_create(&gp2.tid,NULL,thread_summarize_grid_products,reinterpret_cast<void *>(&gp2));
    gc1.table="GrML.summary";
    gc1.dsnum=ce1.key;
    pthread_create(&gc1.tid,NULL,thread_summarize_grid_coverages,reinterpret_cast<void *>(&gc1));
    gc2.table="GrML.summary";
    gc2.dsnum=ce2.key;
    pthread_create(&gc2.tid,NULL,thread_summarize_grid_coverages,reinterpret_cast<void *>(&gc2));
    pthread_join(gp1.tid,NULL);
    pthread_join(gp2.tid,NULL);
    pthread_join(gc1.tid,NULL);
    pthread_join(gc2.tid,NULL);
    cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\">Gridded Products</th><td class=\"bg0\" align=\"left\">";
    if (table_exists(server,table1)) {
      write_gridded_products(gp1);
    }
    cout << "</td><td class=\"bg0\" align=\"left\">";
    if (table_exists(server,table2)) {
      write_gridded_products(gp2);
    }
    cout << "</td></tr>" << endl;
    cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\">Grid Resolution and Coverage</th><td class=\"bg0\" align=\"left\">";
    if (table_exists(server,table1)) {
      for (const auto& coverage : gc1.coverages) {
        if (gc1.coverages.size() > 1) {
          cout << "&bull;&nbsp;";
        }
        cout << gridutils::convert_grid_definition(coverage)+"<br>";
      }
    }
    cout << "</td><td class=\"bg0\" align=\"left\">";
    if (table_exists(server,table2)) {
      for (const auto& coverage : gc2.coverages) {
        if (gc2.coverages.size() > 1) {
          cout << "&bull;&nbsp;";
        }
        cout << gridutils::convert_grid_definition(coverage)+"<br>";
      }
    }
    cout << "</td></tr>" << endl;
    cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\">GCMD Parameters</th><td class=\"bg0\" align=\"left\"><div style=\"height: 150px; overflow: auto\">";
    xdoc.open(SERVER_ROOT+"/web/datasets/ds"+ce1.key+"/metadata/dsOverview.xml");
    elist=xdoc.element_list("dsOverview/variable@vocabulary=GCMD");
    array.clear();
    array.reserve(elist.size());
    for (auto e : elist) {
      sdum=e.content();
      strutils::replace_all(sdum,"EARTH SCIENCE > ","");
      array.emplace_back(sdum);
    }
    binary_sort(array,compare_strings);
    for (n=0; n < elist.size(); n++)
      cout << "&bull;&nbsp;"+array[n]+"<br>";
    xdoc.close();
    cout << "</div></td><td class=\"bg0\" align=\"left\"><div style=\"height: 150px; overflow: auto\">";
    xdoc.open(SERVER_ROOT+"/web/datasets/ds"+ce2.key+"/metadata/dsOverview.xml");
    elist=xdoc.element_list("dsOverview/variable@vocabulary=GCMD");
    array.clear();
    array.reserve(elist.size());
    for (auto e : elist) {
      sdum=e.content();
      strutils::replace_all(sdum,"EARTH SCIENCE > ","");
      array.emplace_back(sdum);
    }
    binary_sort(array,compare_strings);
    for (n=0; n < elist.size(); n++)
      cout << "&bull;&nbsp;"+array[n]+"<br>";
    xdoc.close();
    cout << "</div></td></tr>" << endl;
  } else if (ce1.grid_resolutions.size() > 0 || ce2.grid_resolutions.size() > 0) {
    cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\">Grid Resolutions</th><td class=\"bg0\" align=\"left\">";
    for (auto res : ce1.grid_resolutions) {
      strutils::replace_all(res,"H : ","");
      strutils::replace_all(res,"-","to");
      if (ce1.grid_resolutions.size() > 1)
        cout << "&bull;&nbsp;";
      cout << res+"<br>";
    }
    cout << "</td><td class=\"bg0\" align=\"left\">";
    for (auto res : ce2.grid_resolutions) {
      strutils::replace_all(res,"H : ","");
      strutils::replace_all(res,"-","to");
      if (ce2.grid_resolutions.size() > 1)
        cout << "&bull;&nbsp;";
      cout << res+"<br>";
    }
    cout << "</td></tr>" << endl;
  }
  table1="ObML.ds"+strutils::substitute(ce1.key,".","")+"_primaries2";
  table2="ObML.ds"+strutils::substitute(ce2.key,".","")+"_primaries2";
  if (table_exists(server,table1) || table_exists(server,table2)) {
  }
  server.disconnect();
}

int main(int argc, char **argv) {
  char *env = getenv("HTTP_HOST");
  if (env != nullptr) {
    http_host = env;
    if (http_host.empty()) {
      web_error2("empty HTTP_HOST", "400 Bad Request");
    }
  } else {
    web_error2("missing HTTP_HOST", "400 Bad Request");
  }
  parse_query();
  if (local_args.display_cache) {
    display_cache();
  } else if (local_args.url_input.compare_list.size() > 0) {
    compare();
  } else if (!local_args.url_input.refine_by.empty()) {
    show_refine_results();
  } else if (local_args.url_input.browse_by_list.size() > 0) {
    if (local_args.url_input.browse_by_list.size() != local_args.url_input
        .browse_value_list.size()) {
      redirect_to_error();
    } else {
      auto n = 0;
      auto bb = local_args.url_input.browse_by_list.begin();
      auto bb_end = local_args.url_input.browse_by_list.end();
      auto bv = local_args.url_input.browse_value_list.begin();
      for (; bb != bb_end; ++bb, ++bv) {
        local_args.url_input.browse_by = *bb;
        local_args.url_input.browse_value = *bv;
        if (local_args.url_input.browse_by == "type") {
          local_args.url_input.browse_value = strutils::substitute(strutils::
              to_lower(local_args.url_input.browse_value), " ", "_");
        }
        if (n > 0 && !local_args.new_browse) {
          read_cache();
        }
        browse(*bb == local_args.url_input.browse_by_list.back());
        ++n;
        local_args.new_browse = false;
      }
    }
  } else if (!local_args.url_input.browse_by.empty()) {
    if (local_args.url_input.browse_by == "type") {
      local_args.url_input.browse_value = strutils::substitute(strutils::
          to_lower(local_args.url_input.browse_value), " ", "_");
    }
    browse();
  } else if (local_args.new_browse) {
    show_start();
  } else {
    redirect_to_error();
  }
}
