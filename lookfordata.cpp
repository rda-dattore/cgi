#include <iostream>
#include <string>
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

metautils::Directives metautils::directives;
metautils::Args metautils::args;
std::string myerror="";
std::string mywarning="";

struct LocalArgs {
  LocalArgs() : refine_by(),browse_by(),browse_value(),origin(),browse_by_list(),browse_value_list(),lkey(),from_home_page(),refine_color(),compare_list(),new_browse(false),display_cache(false) {}

  std::string refine_by,browse_by,browse_value,origin;
  std::list<std::string> browse_by_list,browse_value_list;
  std::string lkey,from_home_page,refine_color;
  std::list<std::string> compare_list;
  bool new_browse,display_cache;
} local_args;
struct DsEntry {
  DsEntry() : key(),summary() {}

  std::string key;
  std::string summary;
};
my::map<DsEntry> prev_results_table(999);
const size_t EXPANDABLE_SUMMARY_LENGTH=30;
std::string bgcolors[2];
std::string http_host=getenv("HTTP_HOST");
std::string server_root="/"+strutils::token(unixutils::host_name(),".",0);
std::ofstream cache;
struct BreadCrumbsEntry {
  BreadCrumbsEntry() : key(),count(nullptr) {}

  std::string key;
  std::shared_ptr<std::string> count;
};
my::map<BreadCrumbsEntry> breadcrumbs_table;
struct CountEntry {
  CountEntry() : key(),count(nullptr) {}

  std::string key;
  std::shared_ptr<int> count;
};
struct TimeResolution {
  TimeResolution() : key(),types(nullptr) {}

  std::string key;
  std::shared_ptr<std::string> types;
};
struct ComparisonEntry {
  ComparisonEntry() : key(),title(),summary(),type(),start(),end(),order(0),time_resolution_table(),data_types(),formats(),grid_resolutions(),projects(),supported_projects(),platforms() {}

  std::string key;
  std::string title,summary,type;
  std::string start,end;
  size_t order;
  my::map<TimeResolution> time_resolution_table;
  std::list<std::string> data_types,formats,grid_resolutions,projects,supported_projects,platforms;
};
struct StringEntry {
  StringEntry() : key() {}

  std::string key;
};
struct GridProducts {
  GridProducts() : table(),found_analyses(false),tables(),tid(0) {}

  std::string table;
  bool found_analyses;
  struct Tables {
    Tables() : forecast(99999),average(99999),accumulation(99999),weekly_mean(99999),monthly_mean(99999),monthly_var_covar(99999),mean(99999),var_covar(99999) {}

    my::map<StringEntry> forecast,average,accumulation,weekly_mean,monthly_mean,monthly_var_covar,mean,var_covar;
  } tables;
  pthread_t tid;
};
struct GridCoverages {
  GridCoverages() : table(),dsnum(),coverages(),tid(0) {}

  std::string table,dsnum;
  std::list<std::string> coverages;
  pthread_t tid;
};
const std::string INDEXABLE_DATASET_CONDITIONS="(d.type = 'P' or d.type = 'H') and d.dsid != '999.8' and d.dsid != '999.9'";

bool compare_strings(std::string& left,std::string& right)
{
  return (left < right);
}

bool sort_nhour_keys(const std::string& left,const std::string& right)
{
  std::string l=left;
  std::string r=right;
  int n;

  if (l.find("-hour") == std::string::npos) {
    l="0-hour"+l;
  }
  if ( (n=3-l.find("-hour")) > 0) {
    l.insert(0,n,'0');
  }
  if (r.find("-hour") == std::string::npos) {
    r="0-hour"+r;
  }
  if ( (n=3-r.find("-hour")) > 0) {
    r.insert(0,n,'0');
  }
  if (l <= r) {
    return true;
  }
  else {
    return false;
  }
}

extern "C" void *thread_summarize_grid_products(void *gpstruct)
{
  MySQL::Query query;
  MySQL::Row row;
  GridProducts *g=(GridProducts *)gpstruct;
  std::string sdum;
  size_t fidx,aidx,cidx,zidx;
  StringEntry se;

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
	}
// forecasts
	else if (fidx != std::string::npos && fidx < 4) {
	  se.key=sdum.substr(0,sdum.find(" "));
	  if (!g->tables.forecast.found(se.key,se)) {
	    g->tables.forecast.insert(se);
	  }
	}
// averages
	else if (aidx != std::string::npos) {
	  se.key=sdum.substr(0,aidx);
	  strutils::trim(se.key);
	  if (!g->tables.average.found(se.key,se)) {
	    g->tables.average.insert(se);
	  }
	}
// accumulations
	else if (cidx != std::string::npos && cidx < 4) {
	  se.key=sdum.substr(0,sdum.find(" "));
	  if (!g->tables.accumulation.found(se.key,se)) {
	    g->tables.accumulation.insert(se);
	  }
	}
	else if (std::regex_search(sdum,std::regex("^Weekly Mean"))) {
	  se.key=sdum.substr(sdum.find("of")+3);
	  if (!g->tables.weekly_mean.found(se.key,se)) {
	    g->tables.weekly_mean.insert(se);
	  }
	}
	else if (std::regex_search(sdum,std::regex("^Monthly Mean"))) {
	  if ( (zidx=sdum.find("of")) != std::string::npos) {
	    se.key=sdum.substr(zidx+3);
	    strutils::trim(se.key);
	    if (!g->tables.monthly_mean.found(se.key,se)) {
		g->tables.monthly_mean.insert(se);
	    }
	  }
	}
	else if (std::regex_search(sdum,std::regex("Mean"))) {
	  se.key="x";
	  if ( (zidx=sdum.find("of")) != std::string::npos) {
	    se.key=sdum.substr(zidx+3);
	    if ( (zidx=se.key.find("at")) != std::string::npos) {
		se.key=se.key.substr(0,zidx);
	    }
	  }
	  else {
	    if ( (zidx=sdum.find("Mean")) != std::string::npos) {
		se.key=sdum.substr(0,zidx);
	    }
	  }
	  strutils::trim(se.key);
	  if (!g->tables.mean.found(se.key,se)) {
	    g->tables.mean.insert(se);
	  }
	}
	else if (std::regex_search(sdum,std::regex("^Variance/Covariance"))) {
	  se.key=sdum.substr(sdum.find("of")+3);
	  se.key=se.key.substr(se.key.find(" ")+1);
	  se.key=se.key.substr(0,se.key.find("at")-1);
	  if (!g->tables.var_covar.found(se.key,se)) {
	    g->tables.var_covar.insert(se);
	  }
	}
    }
  }
  tserver.disconnect();
  g->tables.forecast.keysort(sort_nhour_keys);
  g->tables.average.keysort(sort_nhour_keys);
  g->tables.accumulation.keysort(sort_nhour_keys);
  g->tables.weekly_mean.keysort(sort_nhour_keys);
  g->tables.monthly_mean.keysort(sort_nhour_keys);
  g->tables.monthly_var_covar.keysort(sort_nhour_keys);
  g->tables.mean.keysort(sort_nhour_keys);
  g->tables.var_covar.keysort(sort_nhour_keys);
  return NULL;
}

extern "C" void *thread_summarize_grid_coverages(void *gcstruct)
{
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

std::string category(std::string short_name)
{
  std::string long_name;

  if (short_name == "var") {
    long_name="Variable / Parameter";
  }
  else if (short_name == "tres") {
    long_name="Time Resolution";
  }
  else if (short_name == "plat") {
    long_name="Platform";
  }
  else if (short_name == "sres") {
    long_name="Spatial Resolution";
  }
  else if (short_name == "topic") {
    long_name="Topic / Subtopic";
  }
  else if (short_name == "proj") {
    long_name="Project / Experiment";
  }
  else if (short_name == "type") {
    long_name="Type of Data";
  }
  else if (short_name == "supp") {
    long_name="Supports Project";
  }
  else if (short_name == "fmt") {
    long_name="Data Format";
  }
  else if (short_name == "instr") {
    long_name="Instrument";
  }
  else if (short_name == "loc") {
    long_name="Location";
  }
  else if (short_name == "prog") {
    long_name="Progress";
  }
  else if (short_name == "ftext") {
    long_name="Free Text";
  }
  else if (short_name == "recent") {
    long_name="Recently Added / Updated";
  }
  else if (short_name == "doi") {
    long_name="Datasets with DOIs";
  }
  else if (short_name == "all") {
    long_name="All RDA Datasets";
  }
  if (!long_name.empty()) {
    return long_name;
  }
  else {
    return short_name;
  }
}

void read_cache()
{
  std::ifstream ifs;
  std::ofstream ofs;
  char line[256];
  DsEntry dse;
  BreadCrumbsEntry bce;
  std::string rmatch,bmatch;
  int nmatch=0,n=0;
  int num_lines=0;
  TempFile *tfile=NULL;
  std::deque<std::string> sp;

  ifs.open((server_root+"/tmp/browse."+local_args.lkey).c_str());
  if (ifs.is_open()) {
    if (std::regex_search(local_args.refine_by,std::regex("^@"))) {
	sp=strutils::split(local_args.refine_by,"-");
	rmatch=sp[0];
	if (sp.size() > 1) {
	  nmatch=std::stoi(sp[1]);
	}
	else {
	  nmatch=1;
	}
	local_args.refine_by=sp[0].substr(1);
    }
    else if (local_args.browse_by.size() > 0) {
	bmatch="@"+local_args.browse_by+"<!>"+local_args.browse_value;
    }
    ifs.getline(line,256);
    while (!ifs.eof()) {
	++num_lines;
	if (line[0] == '@') {
	  sp=strutils::split(line,"<!>");
	  if (!rmatch.empty()) {
	    if (sp[0] == rmatch) {
		++n;
		if (n == nmatch) {
		  break;
		}
	    }
	  }
	  else if (!bmatch.empty()) {
	    if (std::regex_search(line,std::regex("^"+bmatch))) {
		break;
	    }
	  }
	  bce.key=sp[0].substr(1)+"<!>"+sp[1];
	  if (!breadcrumbs_table.found(bce.key,bce)) {
	    bce.count.reset(new std::string);
	    *bce.count=sp[2];
	    breadcrumbs_table.insert(bce);
	  }
	  prev_results_table.clear();
	}
	else {
	  if (!prev_results_table.found(line,dse)) {
	    dse.key=line;
	    prev_results_table.insert(dse);
	  }
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
	  ofs << line << std::endl;
	}
	ofs.close();
    }
    ifs.close();
    if (tfile != NULL) {
	std::stringstream oss,ess;
	unixutils::mysystem2("/bin/mv "+tfile->name()+" "+server_root+"/tmp/browse."+local_args.lkey,oss,ess);
    }
  }
}

void parse_query()
{
  QueryString query_string(QueryString::GET);
  std::string sdum;

  if (!query_string) {
    std::cout << "Location: /cgi-bin/error?code=404" << std::endl << std::endl;
  }
  sdum=query_string.value("nb");
  if (sdum == "y") {
    local_args.new_browse=true;
  }
  sdum=query_string.value("dc");
  if (sdum == "y") {
    local_args.display_cache=true;
  }
  local_args.refine_by=query_string.value("r");
  local_args.browse_by_list=query_string.values("b");
  if (local_args.browse_by_list.size() < 2) {
    local_args.browse_by=query_string.value("b");
  }
  local_args.browse_value_list=query_string.values("v");
  if (local_args.browse_value_list.size() < 2) {
    local_args.browse_value=query_string.value("v");
  }
  local_args.origin=query_string.value("o");
  if (local_args.browse_by_list.size() != local_args.browse_value_list.size()) {
    std::cout << "Location: /cgi-bin/error?code=404" << std::endl << std::endl;
  }
  if (local_args.browse_by_list.size() > 1) {
    local_args.new_browse=true;
  }
  local_args.from_home_page=query_string.value("hp");
  local_args.refine_color=query_string.value("rc");
  if (local_args.refine_color.empty()) {
    local_args.refine_color="#eafaff";
  }
  if (local_args.browse_by == "type") {
    local_args.browse_value=strutils::substitute(strutils::to_lower(local_args.browse_value)," ","_");
  }
  local_args.lkey=value_from_cookie("lkey");
  local_args.compare_list=query_string.values("cmp");
  if (!local_args.new_browse) {
    read_cache();
  }
  else {
    if (!local_args.lkey.empty()) {
	system(("rm -f "+server_root+"/tmp/browse."+local_args.lkey).c_str());
    }
    local_args.lkey=strutils::strand(30);
    std::cout << "Set-Cookie: lkey=" << local_args.lkey << "; domain=" << http_host << "; path=/;" << std::endl;
  }
}

void parse_refine_query(MySQL::Query& query)
{
  metautils::read_config("lookfordata","","");
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  my::map<CountEntry> keyword_count_table;
  if (query.submit(server) == 0) {
    MySQL::Row row;
    while (query.fetch_row(row)) {
	bool add_to_list;
	if (prev_results_table.size() > 0) {
	  DsEntry dse;
	  if (prev_results_table.found(row[1],dse)) {
	    add_to_list=true;
	  }
	  else {
	    add_to_list=false;
	  }
	}
	else {
	  add_to_list=true;
	}
	if (add_to_list) {
	  CountEntry ce;
	  if (row[0].empty()) {
	    ce.key="Not specified";
	  }
	  else if (local_args.refine_by == "loc") {
	    ce.key=strutils::substitute(row[0],"United States Of America","USA");
	  }
	  else if (local_args.refine_by == "prog") {
	    if (row[0] == "Y") {
		ce.key="Continually Updated";
	    }
	    else if (row[0] == "N") {
		ce.key="Complete";
	    }
	  }
	  else {
	    ce.key=row[0];
	  }
	  strutils::trim(ce.key);
	  BreadCrumbsEntry bce;
	  if (breadcrumbs_table.size() == 0 || !breadcrumbs_table.found(local_args.refine_by+"<!>"+ce.key,bce)) {
	    if (local_args.refine_by == "type") {
		ce.key=strutils::capitalize(ce.key);
	    }
	    if (!keyword_count_table.found(ce.key,ce)) {
		ce.count.reset(new int);
		*ce.count=0;
		keyword_count_table.insert(ce);
	    }
	    if (prev_results_table.size() > 0) {
		++(*ce.count);
	    }
	    else {
		(*ce.count)+=std::stoi(row[1]);
	    }
	  }
	}
    }
  }
  std::cout << "Content-type: text/html" << std::endl << std::endl;
  std::cout << "<div style=\"background-color: #27aae0\">";
  if (local_args.from_home_page != "yes") {
    std::cout << "<div style=\"font-size: 16px; font-weight: bold; padding: 2px; text-align: center; width: auto; height: 20px; line-height: 20px\">" << category(local_args.refine_by) << "</div>";
  }
  if (keyword_count_table.size() > 0) {
    if (local_args.refine_by.substr(1) != "res") {
	keyword_count_table.keysort(
	[](std::string& left,std::string& right) -> bool
	{
	  std::string l,r;

	  if (left == "Not specified") {
	    return true;
	  }
	  else if (right == "Not specified") {
	    return true;
	  }
	  else {
	    l=strutils::substitute(strutils::to_lower(left),"proprietary_","");
	    r=strutils::substitute(strutils::to_lower(right),"proprietary_","");
	    if (l <= r) {
		return true;
	    }
	    else {
		return false;
	    }
	  }
	});
    }
    for (const auto& key : keyword_count_table.keys()) {
	CountEntry ce;
	keyword_count_table.found(key,ce);
	std::cout << "<div style=\"background-color: " << local_args.refine_color << "; margin-bottom: 1px\"><table style=\"font-size: 13px\"><tr valign=\"top\"><td>&nbsp;&bull;</td><td>";
	if (local_args.from_home_page == "yes") {
	  std::cout << "<a href=\"/index.html#!lfd?b=" << local_args.refine_by << "&v=" << key << "\" onClick=\"javascript:slideUp('refine-slider')\">";
	}
	else {
	  std::cout << "<a href=\"javascript:void(0)\" onClick=\"javascript:slideIn('refine-slider',function(){ document.getElementById(lastoutid).style.fontWeight='normal'; });getContent('lfd-content','/cgi-bin/lookfordata?b=" << local_args.refine_by << "&v=" << key << "')\">";
	}
	if (local_args.refine_by == "proj" || local_args.refine_by == "supp") {
	  size_t idx;
	  if ( (idx=key.find(">")) != std::string::npos) {
	    std::cout << "<b>" << key.substr(0,idx) << "</b>" << key.substr(idx);
	  }
	  else {
//	    if (key == "Not specified") {
		std::cout << key;
/*
	    }
	    else {
		std::cout << "<b>" << key << "</b>";
	    }
*/
	  }
	}
	else if (local_args.refine_by == "fmt") {
	  auto fmt=strutils::substitute(key,"proprietary_","");
	  std::cout << strutils::to_capital(fmt);
	}
	else {
	  if (key == strutils::to_upper(key)) {
	    std::cout << strutils::capitalize(key);
	  }
	  else {
	    std::cout << key;
	  }
	}
	std::cout << "</a> <small class=\"mediumGrayText\">(" << *(ce.count) << ")</small>" << "</td></tr></table></div>" << std::endl;
    }
    std::cout << "<div style=\"background-color: " << local_args.refine_color << "; line-height: 1px\">&nbsp;</div>";
    std::cout << std::endl;
  }
  else {
    std::cout << "<div style=\"background-color: " << local_args.refine_color << "; margin-top: 1px\"><table style=\"font-size: 13px\"><tr><td>&nbsp;&nbsp;</td><td>No additional options are available</td></tr></table></div>" << std::endl;
  }
  std::cout << "</div>" << std::endl;
  server.disconnect();
}

void show_refine_results()
{
  MySQL::Query query;
  if (local_args.refine_by == "var") {
    if (prev_results_table.size() > 0) {
	query.set("select distinct substring_index(g.path,' > ',-1),v.dsid from search.variables_new as v left join search.GCMD_sciencekeywords as g on g.uuid = v.keyword where v.vocabulary = 'GCMD'");
    }
    else {
	query.set("select s.keyword,count(distinct s.dsid) from (select distinct substring_index(g.path,' > ',-1) as keyword,v.dsid from search.variables_new as v left join search.GCMD_sciencekeywords as g on g.uuid = v.keyword left join search.datasets as d on d.dsid = v.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and v.vocabulary = 'GCMD') as s group by s.keyword");
    }
  }
  else if (local_args.refine_by == "tres") {
    if (prev_results_table.size() > 0) {
	query.set("select distinct replace(substring(t.keyword,5),' - ',' to '),d.dsid,s.idx from search.datasets as d left join search.time_resolutions as t on t.dsid = d.dsid left join search.time_resolution_sort as s on s.keyword = t.keyword where "+INDEXABLE_DATASET_CONDITIONS+" order by s.idx");
    }
    else {
	query.set("select replace(substring(t.keyword,5),' - ',' to '),count(distinct d.dsid),any_value(s.idx) as idx from search.datasets as d left join search.time_resolutions as t on t.dsid = d.dsid left join search.time_resolution_sort as s on s.keyword = t.keyword where "+INDEXABLE_DATASET_CONDITIONS+" and !isnull(idx) group by t.keyword order by idx");
    }
  }
  else if (local_args.refine_by == "plat") {
    if (prev_results_table.size() > 0) {
	query.set("select distinct g.last_in_path,d.dsid from search.datasets as d left join search.platforms_new as p on p.dsid = d.dsid left join search.GCMD_platforms as g on g.uuid = p.keyword where "+INDEXABLE_DATASET_CONDITIONS);
    }
    else {
	query.set("select distinct g.last_in_path,count(distinct d.dsid) from search.datasets as d left join search.platforms_new as p on p.dsid = d.dsid left join search.GCMD_platforms as g on g.uuid = p.keyword where "+INDEXABLE_DATASET_CONDITIONS+" group by g.last_in_path");
    }
  }
  else if (local_args.refine_by == "sres") {
    if (prev_results_table.size() > 0) {
	query.set("select distinct replace(substring(r.keyword,5),' - ',' to '),d.dsid,s.idx from search.datasets as d left join search.grid_resolutions as r on r.dsid = d.dsid left join search.grid_resolution_sort as s on s.keyword = r.keyword where "+INDEXABLE_DATASET_CONDITIONS+" order by s.idx");
    }
    else {
	query.set("select replace(substring(r.keyword,5),' - ',' to '),count(distinct d.dsid),any_value(s.idx) as idx from search.datasets as d left join search.grid_resolutions as r on r.dsid = d.dsid left join search.grid_resolution_sort as s on s.keyword = r.keyword where "+INDEXABLE_DATASET_CONDITIONS+" and !isnull(idx) group by r.keyword order by idx");
    }
  }
  else if (local_args.refine_by == "topic") {
    if (prev_results_table.size() > 0) {
	query.set("select distinct substring_index(substring_index(g.path,' > ',-3),' > ',2),v.dsid from search.variables_new as v left join search.GCMD_sciencekeywords as g on g.uuid = v.keyword where v.vocabulary = 'GCMD'");
    }
    else {
	query.set("select s.keyword,count(distinct s.dsid) from (select distinct substring_index(substring_index(g.path,' > ',-3),' > ',2) as keyword,v.dsid from search.variables_new as v left join search.GCMD_sciencekeywords as g on g.uuid = v.keyword left join search.datasets as d on d.dsid = v.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and v.vocabulary = 'GCMD') as s group by s.keyword");
    }
  }
  else if (local_args.refine_by == "proj") {
    if (prev_results_table.size() > 0) {
	query.set("select distinct g.last_in_path,d.dsid from search.datasets as d left join search.projects_new as p on p.dsid = d.dsid left join search.GCMD_projects as g on g.uuid = p.keyword where "+INDEXABLE_DATASET_CONDITIONS);
    }
    else {
	query.set("select distinct g.last_in_path,count(distinct d.dsid) from search.datasets as d left join search.projects_new as p on p.dsid = d.dsid left join search.GCMD_projects as g on g.uuid = p.keyword where "+INDEXABLE_DATASET_CONDITIONS+" group by g.last_in_path");
    }
  }
  else if (local_args.refine_by == "type") {
    if (prev_results_table.size() > 0) {
	query.set("select distinct t.keyword,d.dsid from search.datasets as d left join search.data_types as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS);
    }
    else {
	query.set("select distinct t.keyword,count(distinct d.dsid) from search.datasets as d left join search.data_types as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" group by t.keyword");
    }
  }
  else if (local_args.refine_by == "supp") {
    if (prev_results_table.size() > 0) {
	query.set("select distinct g.last_in_path,d.dsid from search.datasets as d left join search.supportedProjects_new as p on p.dsid = d.dsid left join search.GCMD_projects as g on g.uuid = p.keyword where "+INDEXABLE_DATASET_CONDITIONS);
    }
    else {
	query.set("select g.last_in_path,count(distinct d.dsid) from search.datasets as d left join search.supportedProjects_new as p on p.dsid = d.dsid left join search.GCMD_projects as g on g.uuid = p.keyword where "+INDEXABLE_DATASET_CONDITIONS+" group by g.last_in_path");
    }
  }
  else if (local_args.refine_by == "fmt") {
    if (prev_results_table.size() > 0) {
	query.set("select distinct f.keyword,d.dsid from search.datasets as d left join search.formats as f on f.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS);
    }
    else {
	query.set("select distinct f.keyword,count(distinct d.dsid) from search.datasets as d left join search.formats as f on f.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" group by f.keyword");
    }
  }
  else if (local_args.refine_by == "instr") {
    if (prev_results_table.size() > 0) {
	query.set("select distinct g.last_in_path,d.dsid from search.datasets as d left join search.instruments_new as i on i.dsid = d.dsid left join search.GCMD_instruments as g on g.uuid = i.keyword where "+INDEXABLE_DATASET_CONDITIONS);
    }
    else {
	query.set("select distinct g.last_in_path,count(distinct d.dsid) from search.datasets as d left join search.instruments_new as i on i.dsid = d.dsid left join search.GCMD_instruments as g on g.uuid = i.keyword where "+INDEXABLE_DATASET_CONDITIONS+" group by g.last_in_path");
    }
  }
  else if (local_args.refine_by == "loc") {
    if (prev_results_table.size() > 0) {
	query.set("select distinct if(instr(l.keyword,'United States') > 0,substring_index(l.keyword,' > ',-2),substring_index(l.keyword,' > ',-1)),d.dsid from search.datasets as d left join search.locations as l on l.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and (isnull(l.include) or l.include != 'N')");
    }
    else {
	query.set("select distinct if(instr(l.keyword,'United States') > 0,substring_index(l.keyword,' > ',-2),substring_index(l.keyword,' > ',-1)),count(distinct d.dsid) from search.datasets as d left join search.locations as l on l.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and (isnull(l.include) or l.include != 'N') group by l.keyword");
    }
  }
  else if (local_args.refine_by == "prog") {
    if (prev_results_table.size() > 0) {
	query.set("select continuing_update,dsid from search.datasets as d where "+INDEXABLE_DATASET_CONDITIONS);
    }
    else {
	query.set("select continuing_update,count(distinct dsid) from search.datasets as d where "+INDEXABLE_DATASET_CONDITIONS+" group by continuing_update");
    }
  }
  else if (local_args.refine_by == "ftext") {
    std::cout << "Content-type: text/html" << std::endl << std::endl;
    std::cout << "<div style=\"padding: 5px 0px 0px 5px\"><form name=\"fts\" action=\"javascript:void(0)\" onSubmit=\"javascript:if (v.value.length == 0) { alert('Please enter one or more free text keywords to search on'); return false; } else { slideIn('refine-slider',function(){ document.getElementById(lastoutid).style.fontWeight='normal'; }); getContent('lfd-content','/cgi-bin/lookfordata?b='+document.fts.b.value+'&v='+document.fts.v.value); return true; }\"><span class=\"fs14px\">Filter the dataset list by entering one or more keywords.  If you preceed a keyword with a minus sign (e.g. <nobr><span class=\"fixedWidth14\">-temperature</span></nobr>), then datasets containing that keyword will be excluded.  Otherwise, datasets containing your keyword(s) will be included in the filtered list.</span><br /><input class=\"fixedWidth12\" type=\"text\" name=\"v\" value=\"keyword(s)\" size=\"25\" onClick=\"javascript:this.value=''\" /><input type=\"hidden\" name=\"b\" value=\"ftext\" /><br /><input type=\"submit\" /></form></div>" << std::endl;
    exit(0);
  }
//std::cerr << query.show() << std::endl;
  if (query) {
    parse_refine_query(query);
  }
  else {
    std::cout << "Location: /cgi-bin/error?code=404" << std::endl << std::endl;
  }
}

void open_cache_for_writing()
{
  if (local_args.new_browse) {
    cache.open((server_root+"/tmp/browse."+local_args.lkey).c_str());
  }
  else {
    cache.open((server_root+"/tmp/browse."+local_args.lkey).c_str(),std::fstream::app);
  }
}

void close_cache()
{
  cache.close();
}

void add_breadcrumbs(size_t num_results)
{
  my::map<CountEntry> count_table;
  CountEntry ce;
  std::string breadcrumbs="Showing datasets with these attributes: ";
  std::cout << "<div id=\"breadcrumbs\" style=\"background-color: #9cc991; padding: 5px; margin-top: 3px; margin-bottom: 10px; font-size: 13px\">" << breadcrumbs;
  auto n=0;
  for (const auto& key : breadcrumbs_table.keys()) {
    if (n > 0) {
	std::cout << " <b>&gt;</b> ";
    }
    BreadCrumbsEntry bce;
    breadcrumbs_table.found(key,bce);
    auto kparts=strutils::split(key,"<!>");
    auto bval=kparts[1];
    if (kparts[0] == "var" || kparts[0] == "type") {
	bval=strutils::capitalize(bval);
    }
    else if (kparts[0] == "ftext") {
	bval="'"+bval+"'";
    }
    std::cout << "<a style=\"font-weight: bold; padding-left: 5px\" href=\"javascript:void(0)\" onClick=\"javascript:document.getElementById('breadcrumbs').innerHTML='"+breadcrumbs+"';slideOutFrom('" << kparts[0];
    if (n > 0) {
	breadcrumbs+=" <b>&gt;</b> ";
    }
    breadcrumbs+="<a style=&quot;font-weight: bold; padding-left: 5px&quot; href=&quot;javascript:void(0)&quot; onClick=&quot;javascript:slideOutFrom(\\'"+kparts[0];
    if (!count_table.found(kparts[0],ce)) {
	ce.key=kparts[0];
	ce.count.reset(new int);
	*ce.count=1;
	count_table.insert(ce);
    }
    else {
	++(*ce.count);
    }
    std::cout << "-" << *ce.count << "','')\"><nobr>" << category(kparts[0]) << "</nobr></a> : " << bval << " <span class=\"mediumGrayText\">(" << *bce.count << ")</span>";
    breadcrumbs+="-"+strutils::itos(*ce.count)+"\\',\\'\\')&quot;><nobr>"+category(kparts[0])+"</nobr></a> : "+bval+" <span class=&quot;mediumGrayText&quot;>("+*bce.count+")</span>";
    ++n;
  }
  if (n > 0) {
    std::cout << " <b>&gt;</b> ";
  }
  std::cout << "<a style=\"font-weight: bold; padding-left: 5px\" href=\"javascript:void(0)\" onClick=\"javascript:document.getElementById('breadcrumbs').innerHTML='"+breadcrumbs+"';slideOutFrom('" << local_args.browse_by;
  if (!count_table.found(local_args.browse_by,ce)) {
    ce.key=local_args.browse_by;
    ce.count.reset(new int);
    *ce.count=1;
    count_table.insert(ce);
  }
  else {
    ++(*ce.count);
  }
  std::cout << "-" << *ce.count << "','')\"><nobr>" << category(local_args.browse_by) << "</nobr></a> : ";
  if (local_args.browse_by == "var" || local_args.browse_by == "type") {
    std::cout << strutils::capitalize(local_args.browse_value);
  }
  else if (local_args.browse_by == "ftext") {
    std::cout << "'" << local_args.browse_value << "'";
  }
  else {
    std::cout << local_args.browse_value;
  }
  std::cout << " <span class=\"mediumGrayText\">(" << num_results << ")</span>";
  std::cout << "</div>" << std::endl;
  if (num_results > 1) {
    std::cout << "<script id=\"dscompare\" language=\"javascript\">" << std::endl;
    std::cout << "function submitCompare() {" << std::endl;
    std::cout << "  var parameters='';" << std::endl;
    std::cout << "  var num_checked=0;" << std::endl;
    std::cout << "  for (n=0; n < document.compare.elements.length; n++) {" << std::endl;
    std::cout << "    if (document.compare.elements[n].checked) {" << std::endl;
    std::cout << "      parameters+='&cmp='+document.compare.elements[n].value;" << std::endl;
    std::cout << "      num_checked++;" << std::endl;
    std::cout << "    }" << std::endl;
    std::cout << "  }" << std::endl;
    std::cout << "  if (num_checked > 2) {" << std::endl;
    std::cout << "    alert(\"You can only compare two datasets.  Please uncheck all but two checkboxes.\");" << std::endl;
    std::cout << "    return;" << std::endl;
    std::cout << "  }" << std::endl;
    std::cout << "  else if (num_checked < 2) {" << std::endl;
    std::cout << "    alert(\"Please check the boxes beside two datasets that you would like to compare.\");" << std::endl;
    std::cout << "    return;" << std::endl;
    std::cout << "  }" << std::endl;
    std::cout << "  location='/index.html#!lfd?'+parameters.substr(1);" << std::endl;
    std::cout << "}" << std::endl;
    std::cout << "</script>" << std::endl;
    std::cout << "<div style=\"overflow: hidden; background-color: #eff5df; padding: 5px; margin-bottom: 10px\"><div style=\"display: inline; float: left; font-size: 14px\">Select two datasets and <input type=\"button\" value=\"Compare\" onClick=\"javascript:submitCompare()\"> them.</div><div style=\"display: inline; float: right; margin-right: 10px; font-size: 14px\"><input type=\"reset\" value=\"Reset\"> checkboxes</div></div>" << std::endl;
  }
}

void show_datasets_after_processing(MySQL::LocalQuery& query,int num_entries,bool display_results)
{
  MySQL::Row row;
  std::string sdum;
  size_t num_results=0,iterator;
  DsEntry dse;
  int n=0;
  my::map<CountEntry> multi_table;
  CountEntry ce;

  while (query.fetch_row(row)) {
    if (prev_results_table.found(row[0],dse)) {
	if (num_entries < 2) {
	  ++num_results;
	}
	else {
	  if (!multi_table.found(row[0],ce)) {
	    ce.key=row[0];
	    ce.count.reset(new int);
	    (*ce.count)=0;
	    multi_table.insert(ce);
	  }
	  ++(*ce.count);
	  if ((*ce.count) == num_entries) {
	    ++num_results;
	  }
	}
    }
  }
  open_cache_for_writing();
  cache << "@" << local_args.browse_by << "<!>" << local_args.browse_value << "<!>" << num_results << std::endl;
  if (display_results) {
    std::cout << "Content-type: text/html" << std::endl << std::endl;
    std::cout << "<span class=\"fs24px bold\">Browse the RDA</span><br>" << std::endl;
    std::cout << "<form name=\"compare\" action=\"javascript:void(0)\" method=\"get\">" << std::endl;
    add_breadcrumbs(num_results);
  }
  query.rewind();
  while (query.fetch_row(row)) {
    ce.key="";
    if (prev_results_table.found(row[0],dse) && (num_entries < 2 || (multi_table.found(row[0],ce) && (*ce.count) == static_cast<int>(num_entries)))) {
	if (!ce.key.empty()) {
	  (*ce.count)=0;
	}
	cache << row[0] << std::endl;
	sdum=strutils::itos(n+1);
	if (display_results) {
	  std::cout << "<div style=\"padding: 5px\" onMouseOver=\"javascript:this.style.backgroundColor='#eafaff'\" onMouseOut=\"javascript:this.style.backgroundColor='transparent'\">";
	  if (row[3] == "H") {
	    std::cout << "<img src=\"/images/alert.gif\">&nbsp;<span style=\"color: red\">For ancillary use only - not recommended as a primary research dataset.  It has likely been superseded by newer and better datasets.</span><br>";
	  }
	  if (num_results > 1) {
	    std::cout << "<input type=\"checkbox\" name=\"cmp\" value=\"" << row[0] << "\">&nbsp;";
	  }
	  std::cout << sdum << ". <a class=\"underline\" href=\"/datasets/ds" << row[0] << "/\" target=\"_blank\"><b>" << row[1] << "</b></a> <span class=\"mediumGrayText\">(ds" << row[0] << ")</span><br>" << std::endl;
	  std::cout << "<div class=\"browseSummary\">" << searchutils::convert_to_expandable_summary(row[2],EXPANDABLE_SUMMARY_LENGTH,iterator) << "</div>" << std::endl;
	  std::cout << "</div><br>" << std::endl;
	  ++n;
	}
    }
  }
  close_cache();
  if (display_results) {
    std::cout << "</form>" << std::endl;
    std::cout << "</body></html>" << std::endl;
  }
}

void show_datasets_from_query(MySQL::LocalQuery& query,bool display_results)
{
  MySQL::Row row;
  std::string sdum;
  int n=0;
  size_t iterator;

  open_cache_for_writing();
  cache << "@" << local_args.browse_by << "<!>" << local_args.browse_value << "<!>" << query.num_rows() << std::endl;
  if (display_results) {
    std::cout << "Content-type: text/html" << std::endl << std::endl;
    std::cout << "<span class=\"fs24px bold\">Browse the RDA</span><br>" << std::endl;
    std::cout << "<form name=\"compare\" action=\"javascript:void(0)\">" << std::endl;
    add_breadcrumbs(query.num_rows());
  }
  while (query.fetch_row(row)) {
    cache << row[0] << std::endl;
    if (display_results) {
	sdum=strutils::itos(n+1);
	std::cout << "<div style=\"padding: 5px\" onMouseOver=\"javascript:this.style.backgroundColor='#eafaff'\" onMouseOut=\"javascript:this.style.backgroundColor='transparent'\" itemscope itemtype=\"http://schema.org/Dataset\">";
	if (row[3] == "H") {
	  std::cout << "<img src=\"/images/alert.gif\">&nbsp;<span style=\"color: red\">For ancillary use only - not recommended as a primary research dataset.  It has likely been superseded by newer and better datasets.</span><br>";
	}
	if (query.num_rows() > 1) {
	  std::cout << "<input type=\"checkbox\" name=\"cmp\" value=\""+row[0]+"\">&nbsp;";
	}
	std::cout << sdum+". <a class=\"underline\" href=\"/datasets/ds"+row[0]+"/\" target=\"_blank\" itemprop=\"url\"><b itemprop=\"name\">"+row[1]+"</b></a> <span class=\"mediumGrayText\">(ds"+row[0]+")</span><br>" << std::endl;
	std::cout << "<div class=\"browseSummary\" itemprop=\"description\">"+searchutils::convert_to_expandable_summary(row[2],EXPANDABLE_SUMMARY_LENGTH,iterator)+"</div>" << std::endl;
	std::cout << "</div><br>" << std::endl;
	++n;
    }
  }
  if (display_results) {
    std::cout << "</form>" << std::endl;
  }
  close_cache();
}

void parse_browse_query(MySQL::LocalQuery& query,int num_entries,bool display_results)
{
  bgcolors[0]="#ffffff";
  bgcolors[1]="#f8fcff";
  metautils::read_config("lookfordata","","");
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (query.submit(server) == 0) {
    if (prev_results_table.size() > 0) {
	show_datasets_after_processing(query,num_entries,display_results);
    }
    else {
	show_datasets_from_query(query,display_results);
    }
  }
  else {
    std::cerr << "LOOKFORDATA query failed with error " << query.error() << ": '" << query.show() << "'" << std::endl;
  }
  server.disconnect();
}

void browse(bool display_results = true)
{
  MySQL::LocalQuery query;
  std::string sdum,sword;
  std::deque<std::string> sp;
  size_t n;
  std::string include_words,exclude_words;
  int num_entries=0;
  bool ignore;

  if (local_args.browse_by == "var") {
    query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.variables_new as v left join search.GCMD_sciencekeywords as g on g.uuid = v.keyword left join search.datasets as d on d.dsid = v.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and g.path like '% > "+local_args.browse_value+"' group by d.dsid order by d.type,trank");
  }
  else if (local_args.browse_by == "tres") {
    if (local_args.browse_value == "Not specified") {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.time_resolutions as r on r.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(r.keyword) group by d.dsid order by d.type,trank");
    }
    else {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.time_resolutions as r left join search.datasets as d on d.dsid = r.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and r.keyword = 'T : "+strutils::substitute(local_args.browse_value," to "," - ")+"' group by d.dsid order by d.type,trank");
    }
  }
  else if (local_args.browse_by == "plat") {
    if (local_args.browse_value == "Not specified") {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.platforms_new as p on p.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(p.keyword) group by d.dsid order by d.type,trank");
    }
    else {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.platforms_new as p left join search.GCMD_platforms as g on g.uuid = p.keyword left join search.datasets as d on d.dsid = p.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and (g.last_in_path = '"+local_args.browse_value+"' or g.path like '% > "+local_args.browse_value+"') group by d.dsid order by d.type,trank");
    }
  }
  else if (local_args.browse_by == "sres") {
    if (local_args.browse_value == "Not specified") {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.grid_resolutions as g on g.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(g.keyword) group by d.dsid order by d.type,trank");
    }
    else {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.grid_resolutions as g left join search.datasets as d on d.dsid = g.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and g.keyword = 'H : "+strutils::substitute(local_args.browse_value," to "," - ")+"' group by d.dsid order by d.type,trank");
    }
  }
  else if (local_args.browse_by == "topic") {
    sp=strutils::split(local_args.browse_value," > ");
    sdum="v.topic = '"+sp[0]+"'";
    if (sp.size() > 1) {
	sdum+=" and v.term = '"+sp[1]+"'";
    }
    query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.GCMD_variables as v left join search.datasets as d on d.dsid = v.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and "+sdum+" group by d.dsid order by d.type,trank");
  }
  else if (local_args.browse_by == "proj") {
    if (local_args.browse_value == "Not specified") {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.projects_new as p on p.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(p.keyword) group by d.dsid order by d.type,trank");
    }
    else {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.projects_new as p left join search.GCMD_projects as g on g.uuid = p.keyword left join search.datasets as d on d.dsid = p.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and (g.last_in_path = '"+local_args.browse_value+"' or g.path like '% > "+local_args.browse_value+"') group by d.dsid order by d.type,trank");
    }
  }
  else if (local_args.browse_by == "type") {
    if (local_args.browse_value == "Not specified") {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.data_types as y on y.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(y.keyword) group by d.dsid order by d.type,trank");
    }
    else {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.data_types as y on y.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and y.keyword = '"+local_args.browse_value+"' group by d.dsid order by d.type,trank");
    }
  }
  else if (local_args.browse_by == "supp") {
    if (local_args.browse_value == "Not specified") {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.supportedProjects_new as s on s.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(s.keyword) group by d.dsid order by d.type,trank");
    }
    else {
	std::string qspec="select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.supportedProjects_new as p left join search.GCMD_projects as g on g.uuid = p.keyword left join search.datasets as d on d.dsid = p.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and (g.last_in_path = '"+local_args.browse_value+"' or g.path like '% > "+local_args.browse_value+"')";
	if (!local_args.origin.empty()) {
	  qspec+=" and p.origin = '"+local_args.origin+"'";
	}
	qspec+=" group by d.dsid order by d.type,trank";
	query.set(qspec);
    }
  }
  else if (local_args.browse_by == "fmt") {
    if (local_args.browse_value == "Not specified") {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.formats as f on f.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(f.keyword) group by d.dsid order by d.type,trank");
    }
    else {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.formats as f left join search.datasets as d on d.dsid = f.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and f.keyword = '"+local_args.browse_value+"' group by d.dsid order by d.type,trank");
    }
  }
  else if (local_args.browse_by == "instr") {
    if (local_args.browse_value == "Not specified") {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.instruments_new as i on i.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(i.keyword) group by d.dsid order by d.type,trank");
    }
    else {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.instruments_new as i left join search.GCMD_instruments as g on g.uuid = i.keyword left join search.datasets as d on d.dsid = i.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and g.last_in_path = '"+local_args.browse_value+"' group by d.dsid order by d.type,trank");
    }
  }
  else if (local_args.browse_by == "loc") {
    if (local_args.browse_value == "Not specified") {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.locations as l on l.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(l.keyword) group by d.dsid order by d.type,trank");
    }
    else {
	sdum=strutils::substitute(local_args.browse_value,"USA","United States Of America");
	strutils::replace_all(sdum,"'","\\'");
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.locations as l left join search.datasets as d on d.dsid = l.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and l.keyword like '% > "+sdum+"' group by d.dsid order by d.type,trank");
    }
  }
  else if (local_args.browse_by == "prog") {
    if (local_args.browse_value == "Complete") {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and d.continuing_update = 'N' group by d.dsid order by d.type,trank");
    }
    else if (local_args.browse_value == "Continually Updated") {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and d.continuing_update = 'Y' group by d.dsid order by d.type,trank");
    }
  }
  else if (local_args.browse_by == "ftext") {
    sp=strutils::split(local_args.browse_value);
    for (n=0; n < sp.size(); ++n) {
	if (sp[n].front() == '-') {
	  if (!exclude_words.empty()) {
	    exclude_words+=" or ";
	  }
	  exclude_words+="word = '"+sp[n].substr(1)+"'";
	}
	else {
	  sdum=sp[n];
	  sword=searchutils::cleaned_search_word(sdum,ignore);
	  if (!include_words.empty()) {
	    include_words+=" or ";
	  }
	  include_words+="word = '"+sdum+"' or (word like '"+sword+"' and sword = '"+strutils::soundex(sword)+"')";
	  num_entries++;
	}
    }
    if (!include_words.empty() && !exclude_words.empty()) {
    }
    else if (!include_words.empty()) {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank,word from (select dsid,word from search.title_wordlist where "+include_words+" union select dsid,word from search.summary_wordlist where "+include_words+") as u left join search.datasets as d on d.dsid = u.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" group by d.dsid,word order by d.type,trank");
    }
    else {
	query.set("select distinct d.dsid,d.title,d.summary,d.type,max(t.rank) as trank from search.datasets as d left join (select dsid from search.title_wordlist where "+exclude_words+" union select dsid from search.summary_wordlist where "+exclude_words+") as u on u.dsid = d.dsid left join search.GCMD_topics as t on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and isnull(u.dsid) group by d.dsid order by d.type,trank");
    }
  }
  else if (local_args.browse_by == "recent") {
    if (prev_results_table.size() > 0) {
	std::cout << "Location: /cgi-bin/error?code=404" << std::endl << std::endl;
    }
    else {
	query.set("select d.dsid,d.title,d.summary,d.type,max(mssdate) as dm from dssdb.dataset as m left join search.datasets as d on concat('ds',d.dsid) = m.dsid where "+INDEXABLE_DATASET_CONDITIONS+" and mssdate >= '"+dateutils::current_date_time().days_subtracted(60).to_string("%Y-%m-%d")+"' group by d.dsid order by d.type,dm desc");
    }
  }
  else if (local_args.browse_by == "doi") {
    if (prev_results_table.size() > 0) {
	std::cout << "Location: /cgi-bin/error?code=404" << std::endl << std::endl;
    }
    else {
	query.set("select d.dsid,d.title,d.summary,d.type from dssdb.dsvrsn as v left join search.datasets as d on concat('ds',d.dsid) = v.dsid where v.status = 'A' and (d.type = 'P' or d.type = 'H') order by d.type,d.dsid");
    }
  }
  else if (local_args.browse_by == "all") {
    if (prev_results_table.size() > 0) {
	std::cout << "Location: /cgi-bin/error?code=404" << std::endl << std::endl;
    }
    else {
	query.set("select dsid,title,summary,type from search.datasets as d where "+INDEXABLE_DATASET_CONDITIONS+" order by type,dsid");
    }
  }
  if (query) {
    parse_browse_query(query,num_entries,display_results);
  }
  else {
    std::cout << "Location: /cgi-bin/error?code=404" << std::endl << std::endl;
  }
}

struct CatEntry {
  CatEntry() : key(),count() {}

  std::string key;
  std::string count;
};

void display_cache()
{
  breadcrumbs_table.clear();
  read_cache();
  std::cout << "Content-type: text/html" << std::endl << std::endl;
  std::cout << "<span class=\"fs24px bold\">Browse the RDA</span><br>" << std::endl;
  std::cout << "<form name=\"compare\" action=\"javascript:void(0)\" method=\"get\">" << std::endl;
  auto bparts=strutils::split(breadcrumbs_table.keys().back(),"<!>");
  local_args.browse_by=bparts[0];
  local_args.browse_value=bparts[1];
  breadcrumbs_table.remove(breadcrumbs_table.keys().back());
  auto num_results=prev_results_table.size();
  add_breadcrumbs(num_results);
  std::string qstring;
  for (const auto& key : prev_results_table.keys()) {
    if (!qstring.empty()) {
	qstring+=",";
    }
    qstring+="'"+key+"'";
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
	std::cout << "<div style=\"padding: 5px\" onMouseOver=\"javascript:this.style.backgroundColor='#eafaff'\" onMouseOut=\"javascript:this.style.backgroundColor='transparent'\">";
	if (row[3] == "H") {
	  std::cout << "<img src=\"/images/alert.gif\">&nbsp;<span style=\"color: red\">For ancillary use only - not recommended as a primary research dataset.  It has likely been superseded by newer and better datasets.</span><br>";
	}
	if (num_results > 1) {
	  std::cout << "<input type=\"checkbox\" name=\"cmp\" value=\"" << row[0] << "\">&nbsp;";
	}
	std::cout << snum << ". <a class=\"underline\" href=\"/datasets/ds" << row[0] << "/\" target=\"_blank\"><b>" << row[1] << "</b></a> <span class=\"mediumGrayText\">(ds" << row[0] << ")</span><br>" << std::endl;
	std::cout << "<div class=\"browseSummary\">" << searchutils::convert_to_expandable_summary(row[2],EXPANDABLE_SUMMARY_LENGTH,iterator) << "</div>" << std::endl;
	std::cout << "</div><br>" << std::endl;
	++n;
    }
  }
  std::cout << "</form>" << std::endl;
  server.disconnect();
}

void show_start()
{
  MySQL::Query query;
  MySQL::Row row;
  my::map<CatEntry> cat_table;
  CatEntry ce;
  std::string num_ds;

  metautils::read_config("lookfordata","","");
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  query.set("select count(distinct dsid) from search.datasets as d where "+INDEXABLE_DATASET_CONDITIONS);
  if (query.submit(server) == 0) {
    if (query.fetch_row(row)) {
	num_ds=row[0];
    }
  }
  query.set("select 'var',count(distinct s1.dsid) from (select v.dsid from search.GCMD_variables as v left join search.datasets as d on d.dsid = v.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s1 union select 'tres',count(distinct s2.dsid) from (select t.dsid from search.time_resolutions as t left join search.datasets as d on d.dsid = t.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s2 union select 'plat',count(distinct s3.dsid) from (select p.dsid from search.platforms as p left join search.datasets as d on d.dsid = p.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s3 union select 'sres',count(distinct s4.dsid) from (select g.dsid from search.grid_resolutions as g left join search.datasets as d on d.dsid = g.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s4 union select 'topic',count(distinct s5.dsid) from (select t.dsid from search.GCMD_topics as t left join search.datasets as d on d.dsid = t.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s5 union select 'proj',count(distinct s6.dsid) from (select p.dsid from search.projects as p left join search.datasets as d on d.dsid = p.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s6 union select 'type',count(distinct s7.dsid) from (select t.dsid from search.data_types as t left join search.datasets as d on d.dsid = t.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s7 union select 'supp',count(distinct s8.dsid) from (select s.dsid from search.supportedProjects_new as s left join search.datasets as d on d.dsid = s.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s8 union select 'fmt',count(distinct s9.dsid) from (select f.dsid from search.formats as f left join search.datasets as d on d.dsid = f.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s9 union select 'loc',count(distinct s10.dsid) from (select l.dsid from search.locations as l left join search.datasets as d on d.dsid = l.dsid where "+INDEXABLE_DATASET_CONDITIONS+") as s10");
  if (query.submit(server) == 0) {
    while (query.fetch_row(row)) {
	ce.key=row[0];
	ce.count=row[1];
	cat_table.insert(ce);
    }
  }
  server.disconnect();
  std::cout << "Content-type: text/html" << std::endl << std::endl;
  std::cout << "<h1>Browse the RDA</h1>" << std::endl;
  std::cout << "<p>There are " << num_ds << " public datasets in the CISL RDA.  You can begin browsing the datasets by choosing one of the facets in the menu to the left.  Facet descriptions are given below, along with the number (in parentheses) of datasets in each.</p>" << std::endl;
  std::cout << "<table cellspacing=\"0\" cellpadding=\"5\" border=\"0\">" << std::endl;
  std::cout << "<tr valign=\"top\">" << std::endl;
  std::cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Variable / Parameter";
  if (cat_table.found("var",ce))
    std::cout << " <small class=\"mediumGrayText\">(" << ce.count << ")</small>";
  std::cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">A variable or parameter is the quantity that is measured, derived, or computed - e.g. the data value.</div></td>" << std::endl;
  std::cout << "<td>&nbsp;&nbsp;&nbsp;&nbsp;</td>" << std::endl;
  std::cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Type of Data";
  if (cat_table.found("type",ce))
    std::cout << " <small class=\"mediumGrayText\">(" << ce.count << ")</small>";
  std::cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">This refers to the type of data values - e.g. grid (interpolated or computed gridpoint data), platform observation (in-situ and remotely sensed measurements), etc.</div></td>" << std::endl;
  std::cout << "</tr>" << std::endl;
  std::cout << "<tr valign=\"top\">" << std::endl;
  std::cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Time Resolution";
  if (cat_table.found("tres",ce))
    std::cout << " <small class=\"mediumGrayText\">(" << ce.count << ")</small>";
  std::cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">This refers to the distance in time between discrete observation measurements, model product valid times, etc.</div></td>" << std::endl;
  std::cout << "<td>&nbsp;&nbsp;&nbsp;&nbsp;</td>" << std::endl;
  std::cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Platform";
  if (cat_table.found("plat",ce))
    std::cout << " <small class=\"mediumGrayText\">(" << ce.count << ")</small>";
  std::cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">The platform is the entity or type of entity that acquired or computed the data (e.g. aircraft, land station, reanalysis model).</div></td>" << std::endl;
  std::cout << "</tr>" << std::endl;
  std::cout << "<tr valign=\"top\">" << std::endl;
  std::cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Spatial Resolution";
  if (cat_table.found("sres",ce))
    std::cout << " <small class=\"mediumGrayText\">(" << ce.count << ")</small>";
  std::cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">This refers to the horizontal distance between discrete gridpoints of a model product, reporting stations in a network, measurements of a moving platform, etc.</div></td>" << std::endl;
  std::cout << "<td>&nbsp;&nbsp;&nbsp;&nbsp;</td>" << std::endl;
  std::cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Topic / Subtopic";
  if (cat_table.found("topic",ce))
    std::cout << " <small class=\"mediumGrayText\">(" << ce.count << ")</small>";
  std::cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">Topic and subtopic are high-level groupings of parameters - e.g. Atmosphere (topic), Clouds (subtopic of Atmosphere).</div></td>" << std::endl;
  std::cout << "</tr>" << std::endl;
  std::cout << "<tr valign=\"top\">";
  std::cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Project / Experiment";
  if (cat_table.found("proj",ce))
    std::cout << " <small class=\"mediumGrayText\">(" << ce.count << ")</small>";
  std::cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">This is the scientific project, field campaign, or experiment that acquired the data.</div></td>" << std::endl;
  std::cout << "<td>&nbsp;&nbsp;&nbsp;&nbsp;</td>" << std::endl;
  std::cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Supports Project";
  if (cat_table.found("supp",ce))
    std::cout << " <small class=\"mediumGrayText\">(" << ce.count << ")</small>";
  std::cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">This refers to data that were acquired to support a scientific project or experiment (e.g. GATE) or that can be used as ingest for a project (e.g. WRF).</div></td>" << std::endl;
  std::cout << "</tr>" << std::endl;
  std::cout << "<tr valign=\"top\">" << std::endl;
  std::cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Data Format";
  if (cat_table.found("fmt",ce))
    std::cout << " <small class=\"mediumGrayText\">(" << ce.count << ")</small>";
  std::cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">This refers to the structure of the bitstream used to encapsulate the data values in a record or file - e.g ASCII, netCDF, etc.</div></td>" << std::endl;
  std::cout << "<td>&nbsp;&nbsp;&nbsp;&nbsp;</td>" << std::endl;
  std::cout << "<td width=\"50%\"><div style=\"font-size: 16px; font-weight: bold\">Location";
  if (cat_table.found("loc",ce))
    std::cout << " <small class=\"mediumGrayText\">(" << ce.count << ")</small>";
  std::cout << "</div><div style=\"margin-bottom: 15px; margin-left: 10px\">This the name of the (usually geographic) location or region for which the data are valid.</div></td>" << std::endl;
  std::cout << "</tr>" << std::endl;
  std::cout << "</table>" << std::endl;
  server.disconnect();
}

std::string set_date_time(std::string datetime,std::string flag,std::string time_zone)
{
  std::string dt;
  size_t n;
  
  n=std::stoi(flag);
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

void fill_comparison_dataset(ComparisonEntry& de_ref)
{
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
  }
  else {
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
	tr.types.reset(new std::string);
	de_ref.time_resolution_table.insert(tr);
    }
    if (tr.types->length() > 0) {
	*(tr.types)+=", ";
    }
    if (row[1] == "GrML") {
	*(tr.types)+="Grids";
    }
    else if (row[1] == "ObML") {
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

void write_keys(const std::list<std::string>& keys)
{
  bool started;

  started=false;
  for (const auto& key : keys) {
    if (started) {
	std::cout << ", ";
    }
    std::cout << key;
    started=true;
  }
}

void write_gridded_products(GridProducts& gp)
{
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
	std::cout << "&bull;&nbsp;";
    }
    std::cout << "Analyses<br>";
  }
  if (gp.tables.forecast.size() > 0) {
    if (num > 1) {
	std::cout << "&bull;&nbsp;";
    }
    std::cout << "Forecasts <small class=\"mediumGrayText\">(";
    write_keys(gp.tables.forecast.keys());
    std::cout << ")</small><br>";
  }
  if (gp.tables.average.size() > 0) {
    if (num > 1) {
	std::cout << "&bull;&nbsp;";
    }
    std::cout << "Averages <small class=\"mediumGrayText\">(";
    write_keys(gp.tables.average.keys());
    std::cout << ")</small><br>";
  }
  if (gp.tables.accumulation.size() > 0) {
    if (num > 1) {
	std::cout << "&bull;&nbsp;";
    }
    std::cout << "Accumulations <small class=\"mediumGrayText\">(";
    write_keys(gp.tables.accumulation.keys());
    std::cout << ")</small><br>";
  }
  if (gp.tables.weekly_mean.size() > 0) {
    if (num > 1) {
	std::cout << "&bull;&nbsp;";
    }
    std::cout << "Weekly Means <small class=\"mediumGrayText\">(";
    write_keys(gp.tables.weekly_mean.keys());
    std::cout << ")</small><br>";
  }
  if (gp.tables.monthly_mean.size() > 0) {
    if (num > 1) {
	std::cout << "&bull;&nbsp;";
    }
    std::cout << "Monthly Means <small class=\"mediumGrayText\">";
    std::cout << "(";
    write_keys(gp.tables.monthly_mean.keys());
    std::cout << ")</small><br>";
  }
  if (gp.tables.monthly_var_covar.size() > 0) {
    if (num > 1) {
	std::cout << "&bull;&nbsp;";
    }
    std::cout << "Monthly Variances/Covariances <small class=\"mediumGrayText\">(";
    write_keys(gp.tables.monthly_var_covar.keys());
    std::cout << ")</small><br>";
  }
  if (gp.tables.mean.size() > 0) {
    if (num > 1) {
	std::cout << "&bull;&nbsp;";
    }
    std::cout << "Means <small class=\"mediumGrayText\">(";
    write_keys(gp.tables.mean.keys());
    std::cout << ")</small><br>";
  }
  if (gp.tables.var_covar.size() > 0) {
    if (num > 1) {
	std::cout << "&bull;&nbsp;";
    }
    std::cout << "Variances/Covariances <small class=\"mediumGrayText\">(";
    write_keys(gp.tables.var_covar.keys());
    std::cout << ")</small><br>";
  }
}

void compare()
{
  XMLDocument xdoc;
  std::list<XMLElement> elist;
  ComparisonEntry ce1,ce2;
  std::string sdum;
  std::vector<std::string> array;
  TimeResolution tr;
  std::string dsnum1,dsnum2,table1,table2;
  GridProducts gp1,gp2;
  GridCoverages gc1,gc2;
  size_t n;
  std::list<std::string>::iterator it;

  metautils::read_config("lookfordata","","");
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (local_args.compare_list.size() < 2) {
    web_error("bad query");
  }
  it=local_args.compare_list.begin();
  ce1.key=*it;
  fill_comparison_dataset(ce1);
  ++it;
  ce2.key=*it;
  fill_comparison_dataset(ce2);
  std::cout << "Content-type: text/html" << std::endl << std::endl;
  std::cout << "<style id=\"compare\">" << std::endl;
  std::cout << "table.compare th," << std::endl;
  std::cout << "table.compare td {" << std::endl;
  std::cout << "  border: 1px solid #96afbf;" << std::endl;
  std::cout << "}" << std::endl;
  std::cout << ".left {" << std::endl;
  std::cout << "  background-color: #b8edab;" << std::endl;
  std::cout << "}" << std::endl;
  std::cout << "</style>" << std::endl;
  std::cout << "<span class=\"fs24px bold\">Comparing Datasets</span><br /><br />" << std::endl;
  std::cout << "<table class=\"compare\" style=\"border-collapse: collapse\" width=\"100%\" cellspacing=\"0\" cellpadding=\"5\">" << std::endl;
  std::cout << "<tr><th class=\"left\" width=\"1\" align=\"left\"><nobr>Dataset ID</nobr></th><th width=\"50%\" align=\"left\"><a href=\"/datasets/ds"+ce1.key+"/\">ds"+ce1.key+"</a></th><th width=\"50%\" align=\"left\"><a href=\"/datasets/ds"+ce2.key+"/\">ds"+ce2.key+"</a></th></tr>" << std::endl;
  std::cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\">Title</th><td align=\"left\">"+ce1.title+"</td><td align=\"left\">"+ce2.title+"</td></tr>" << std::endl;
  std::cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\"><nobr>Data Types</nobr></th><td align=\"left\">";
  for (const auto& type : ce1.data_types) {
    std::cout << "&bull;&nbsp;"+strutils::capitalize(type)+"<br>";
  }
  std::cout << "</td><td align=\"left\">";
  for (const auto& type : ce2.data_types) {
    std::cout << "&bull;&nbsp;"+strutils::capitalize(type)+"<br>";
  }
  std::cout << "</td></tr>" << std::endl;
  std::cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\"><nobr>Data Formats</nobr></th><td align=\"left\">";
  for (auto format : ce1.formats) {
    if (std::regex_search(format,std::regex("^proprietary_"))) {
	strutils::replace_all(format,"proprietary_","");
	format+=" (see dataset documentation)";
    }
    if (ce1.formats.size() > 1) {
	std::cout << "&bull;&nbsp;";
    }
    std::cout << strutils::to_capital(format)+"<br>";
  }
  std::cout << "</td><td align=\"left\">";
  for (auto format : ce2.formats) {
    if (std::regex_search(format,std::regex("^proprietary_"))) {
	strutils::replace_all(format,"proprietary_","");
	format+=" (see dataset documentation)";
    }
    if (ce2.formats.size() > 1)
	std::cout << "&bull;&nbsp;";
    std::cout << strutils::to_capital(format)+"<br>";
  }
  std::cout << "</td></tr>" << std::endl;
  std::cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\">Platforms</th><td align=\"left\">";
  for (const auto& platform : ce1.platforms) {
    if (ce1.platforms.size() > 1)
	std::cout << "&bull;&nbsp;";
    std::cout << platform+"<br>";
  }
  std::cout << "</td><td align=\"left\">";
  for (const auto& platform : ce2.platforms) {
    if (ce2.platforms.size() > 1)
	std::cout << "&bull;&nbsp;";
    std::cout << platform+"</br>";
  }
  std::cout << "</td></tr>" << std::endl;
  std::cout << "<tr valign=\"bottom\"><th class=\"left\" align=\"left\">Temporal Range</th><td align=\"left\">";
  if (!std::regex_search(ce1.start,std::regex("^9998"))) {
    std::cout << "<nobr>" << ce1.start+"</nobr> to <nobr>"+ce1.end+"</nobr>";
  }
  std::cout << "</td><td align=\"left\">";
  if (!std::regex_search(ce2.start,std::regex("^9998"))) {
    std::cout << "<nobr>"+ce2.start+"</nobr> to <nobr>"+ce2.end+"</nobr>";
  }
  std::cout << "</td></tr>" << std::endl;
  if (ce1.time_resolution_table.size() > 0 || ce2.time_resolution_table.size() > 0) {
    std::cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\">Temporal Resolutions</th><td align=\"left\">";
    for (const auto& key : ce1.time_resolution_table.keys()) {
	sdum=key;
	strutils::replace_all(sdum,"T : ","");
	strutils::replace_all(sdum,"-","to");
	if (ce1.time_resolution_table.size() > 1) {
	  std::cout << "&bull;&nbsp;";
	}
	if (ce1.data_types.size() > 1) {
	  ce1.time_resolution_table.found(key,tr);
	  std::cout << sdum+" <small class=\"mediumGrayText\">("+*(tr.types)+")</small><br>";
	}
	else {
	  std::cout << sdum+"<br>";
	}
    }
    std::cout << "</td><td align=\"left\">";
    for (const auto& key : ce2.time_resolution_table.keys()) {
	sdum=key;
	strutils::replace_all(sdum,"T : ","");
	strutils::replace_all(sdum,"-","to");
	if (ce2.time_resolution_table.size() > 1) {
	  std::cout << "&bull;&nbsp;";
	}
	if (ce2.data_types.size() > 1) {
	  ce2.time_resolution_table.found(key,tr);
	  std::cout << sdum+" <small class=\"mediumGrayText\">("+*(tr.types)+")</small><br>";
	}
	else {
	  std::cout << sdum+"<br>";
	}
    }
    std::cout << "</td></tr>" << std::endl;
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
    std::cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\">Gridded Products</th><td class=\"bg0\" align=\"left\">";
    if (table_exists(server,table1)) {
      write_gridded_products(gp1);
    }
    std::cout << "</td><td class=\"bg0\" align=\"left\">";
    if (table_exists(server,table2)) {
      write_gridded_products(gp2);
    }
    std::cout << "</td></tr>" << std::endl;
    std::cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\">Grid Resolution and Coverage</th><td class=\"bg0\" align=\"left\">";
    if (table_exists(server,table1)) {
	for (const auto& coverage : gc1.coverages) {
	  if (gc1.coverages.size() > 1) {
	    std::cout << "&bull;&nbsp;";
	  }
	  std::cout << gridutils::convert_grid_definition(coverage)+"<br>";
	}
    }
    std::cout << "</td><td class=\"bg0\" align=\"left\">";
    if (table_exists(server,table2)) {
	for (const auto& coverage : gc2.coverages) {
	  if (gc2.coverages.size() > 1) {
	    std::cout << "&bull;&nbsp;";
	  }
	  std::cout << gridutils::convert_grid_definition(coverage)+"<br>";
	}
    }
    std::cout << "</td></tr>" << std::endl;
    std::cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\">GCMD Parameters</th><td class=\"bg0\" align=\"left\"><div style=\"height: 150px; overflow: auto\">";
    xdoc.open(server_root+"/web/datasets/ds"+ce1.key+"/metadata/dsOverview.xml");
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
	std::cout << "&bull;&nbsp;"+array[n]+"<br>";
    xdoc.close();
    std::cout << "</div></td><td class=\"bg0\" align=\"left\"><div style=\"height: 150px; overflow: auto\">";
    xdoc.open(server_root+"/web/datasets/ds"+ce2.key+"/metadata/dsOverview.xml");
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
	std::cout << "&bull;&nbsp;"+array[n]+"<br>";
    xdoc.close();
    std::cout << "</div></td></tr>" << std::endl;
  }
  else if (ce1.grid_resolutions.size() > 0 || ce2.grid_resolutions.size() > 0) {
    std::cout << "<tr valign=\"top\"><th class=\"left\" align=\"left\">Grid Resolutions</th><td class=\"bg0\" align=\"left\">";
    for (auto res : ce1.grid_resolutions) {
	strutils::replace_all(res,"H : ","");
	strutils::replace_all(res,"-","to");
	if (ce1.grid_resolutions.size() > 1)
	  std::cout << "&bull;&nbsp;";
	std::cout << res+"<br>";
    }
    std::cout << "</td><td class=\"bg0\" align=\"left\">";
    for (auto res : ce2.grid_resolutions) {
	strutils::replace_all(res,"H : ","");
	strutils::replace_all(res,"-","to");
	if (ce2.grid_resolutions.size() > 1)
	  std::cout << "&bull;&nbsp;";
	std::cout << res+"<br>";
    }
    std::cout << "</td></tr>" << std::endl;
  }
  table1="ObML.ds"+strutils::substitute(ce1.key,".","")+"_primaries";
  table2="ObML.ds"+strutils::substitute(ce2.key,".","")+"_primaries";
  if (table_exists(server,table1) || table_exists(server,table2)) {
  }
  server.disconnect();
}

int main(int argc,char **argv)
{
  parse_query();
  if (local_args.display_cache) {
    display_cache();
  }
  else if (local_args.compare_list.size() > 0) {
    compare();
  }
  else if (!local_args.refine_by.empty()) {
    show_refine_results();
  }
  else if (local_args.browse_by_list.size() > 0) {
    if (local_args.browse_by_list.size() != local_args.browse_value_list.size()) {
	std::cout << "Location: /index.html?hash=error&code=404" << std::endl << std::endl;
    }
    else {
	auto n=0;
	auto bb=local_args.browse_by_list.begin();
	auto bb_end=local_args.browse_by_list.end();
	auto bv=local_args.browse_value_list.begin();
	for (; bb != bb_end; ++bb,++bv) {
	  local_args.browse_by=*bb;
	  local_args.browse_value=*bv;
	  if (local_args.browse_by == "type") {
	    local_args.browse_value=strutils::substitute(strutils::to_lower(local_args.browse_value)," ","_");
	  }
	  if (n > 0 && !local_args.new_browse) {
	    read_cache();
	  }
	  browse(*bb == local_args.browse_by_list.back());
	  ++n;
	  local_args.new_browse=false;
	}
    }
  }
  else if (!local_args.browse_by.empty()) {
    if (local_args.browse_by == "type") {
	local_args.browse_value=strutils::substitute(strutils::to_lower(local_args.browse_value)," ","_");
    }
    browse();
  }
  else if (local_args.new_browse) {
    show_start();
  }
  else {
    std::cout << "Location: /index.html?hash=error&code=404" << std::endl << std::endl;
  }
}
