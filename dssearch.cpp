#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <map>
#include <MySQL.hpp>
#include <web/web.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <search.hpp>
#include <xml.hpp>
#include <metadata.hpp>
#include <bsort.hpp>
#include <myerror.hpp>

metautils::Directives metautils::directives;
metautils::Args metautils::args;
std::string myerror="";
std::string mywarning="";

struct LocalArgs {
  LocalArgs() : words(),exclude_words(),compound_words(),word_list(),request_uri(),is_single_special_word(false),is_file_name(false),show_advanced(false),advanced(),action() {}

  std::list<std::string> words,exclude_words,compound_words;
  std::string word_list,request_uri;
  bool is_single_special_word,is_file_name,show_advanced;
  struct Advanced {
    struct Show {
	Show() : startd(),endd(),tres(),format(),type(),gres() {}

	std::string startd,endd,tres,format,type,gres;
    };
    Advanced() : show(),sf() {}

    Show show;
    std::string sf;
  } advanced;
  std::string action;
} local_args;
struct DsEntry {
  DsEntry() : dsid(),rating(0.),type(),locations(),matched_words() {}

  std::string dsid;
  float rating;
  std::string type;
  std::vector<int> locations;
  std::unordered_set<std::string> matched_words;
};
struct WordListEntry {
  WordListEntry() : wordList(),weight(0.) {}

  std::string wordList;
  double weight;
};
std::unordered_map<std::string,DsEntry> result_table;
std::unordered_set<std::string> unmatched_term_table,exclude_set;
struct DatasetEntry {
  DatasetEntry() : type(),title(),summary() {}

  std::string type,title,summary;
};
std::unordered_map<std::string,DatasetEntry> dataset_map;
std::list<WordListEntry> list_of_word_lists;
std::unordered_map<std::string,int> word_hash_map;
const size_t EXPANDABLE_SUMMARY_LENGTH=50;
static const char *t[]={"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
std::string ignored_word_list;
QueryString query_string;
MySQL::Server server;
const std::string SERVER_ROOT="/"+strutils::token(unixutils::host_name(),".",0);
const std::string INDEXABLE_DATASET_CONDITIONS="(d.type = 'P' or d.type = 'H') and d.dsid not like '999._'";

void parseQuery(int argc,char **argv)
{
  local_args.request_uri=getenv("REQUEST_URI");
  query_string.fill(QueryString::GET);
  if (!query_string) {
    query_string.fill(QueryString::POST);
  }
  std::string words;
  if (query_string) {
    words=query_string.value("words");
    local_args.advanced.show.format=query_string.value("format");
    local_args.advanced.show.type=query_string.value("type");
    local_args.advanced.show.gres=query_string.value("gres");
    local_args.advanced.show.startd=query_string.value("startd");
    local_args.advanced.show.endd=query_string.value("endd");
    local_args.advanced.show.tres=query_string.value("tres");
    local_args.advanced.sf=query_string.value("sf");
    local_args.action=query_string.value("action");
  }
  else if (local_args.request_uri.length() == 0) {
    words=unixutils::unix_args_string(argc,argv,' ');
  }
  if (words.size() > 0) {
    strutils::replace_all(words,"\"","");
    auto wlist=strutils::split(words);
    for (auto word : wlist) {
// clean-up of words
	strutils::replace_all(word,"\"","");
	if (strutils::has_ending(word,",")) {
	  strutils::chop(word);
	}
// only match valid words
	if (!strutils::has_no_letters(word) || strutils::is_numeric(word)) {
	  if (local_args.word_list.length() > 0) {
	    local_args.word_list+=" ";
	  }
	  local_args.word_list+=word;
// words to exclude from search
	  if (strutils::has_beginning(word,"-")) {
	    local_args.exclude_words.emplace_back(word.substr(1));
	  }
// words to include in search
	  else {
// if the word is a compound word, it does not need to be matched exactly, but
//   its parts do
	    std::string separator;
	    if (searchutils::is_compound_term(word,separator)) {
		auto wparts=strutils::split(word,separator);
		for (const auto& w : wparts) {
		  local_args.words.emplace_back(w);
		}
		local_args.compound_words.emplace_back(word);
// if there is only one word and it is a compound word, add it to the list of
//   words that need to be matched
		if (wlist.size() == 1) {
		  local_args.words.emplace_back(word);
		  unmatched_term_table.emplace(word);
		}
	    }
// if the word is not a compound word, it must be matched
	    else {
// only match valid words
		local_args.words.emplace_back(word);
		unmatched_term_table.emplace(word);
	    }
	  }
	}
	else {
	  if (ignored_word_list.length() > 0) {
	    ignored_word_list+=", ";
	  }
	  ignored_word_list+="'"+word+"'";
	}
    }
  }
  for (const auto& word : local_args.words) {
    word_hash_map[word]=0;
  }
}

void start(std::string date_error = "")
{
  std::string sdum;
  std::ifstream ifs;
  char line[256];
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::unordered_set<std::string> gres_set,tres_set;
  std::string startd=local_args.advanced.show.startd,endd=local_args.advanced.show.endd;

  std::cout << "Content-type: text/html" << std::endl << std::endl;
  if (startd.length() == 0) {
    startd="YYYY-MM";
  }
  if (endd.length() == 0) {
    endd="YYYY-MM";
  }
  if (local_args.words.size() == 0 || date_error.length() == 0) {
    std::cout << "<img src=\"/images/transpace.gif\" width=\"1\" height=\"1\" onLoad=\"javascript:document.search.query.focus()\" />" << std::endl;
    std::cout << "<p>Enter one or more keywords into the text box above.</p>" << std::endl;
  }
  else {
// get the possible time and spatial resolutions
    query.set("select distinct keyword from search.grid_resolutions as r left join search.datasets as d on r.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS);
    if (query.submit(server) == 0) {
	while (query.fetch_row(row)) {
	  gres_set.emplace(row[0]);
	}
    }
    query.set("select distinct keyword from search.time_resolutions as r left join search.datasets as d on r.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS);
    if (query.submit(server) == 0) {
	while (query.fetch_row(row)) {
	  tres_set.emplace(row[0]);
	}
    }
    ifs.open((SERVER_ROOT+"/web/metadata/gcmd_resolution").c_str());
    ifs.getline(line,256);
    while (!ifs.eof()) {
	sdum=line;
	strutils::replace_all(sdum," - "," to ");
	strutils::replace_all(sdum,"<","&lt;");
	strutils::replace_all(sdum,">","&gt;");
	ifs.getline(line,256);
    }
    ifs.close();
    std::cout << "<link rel=\"stylesheet\" type=\"text/css\" href=\"/css/transform.css\">" << std::endl;
    std::cout << "<script id=\"dssearch_script\" language=\"javascript\">" << std::endl;
    std::cout << "function clearDate(t) {" << std::endl;
    std::cout << "  if (t.value == \"YYYY-MM\")" << std::endl;
    std::cout << "    t.value='';" << std::endl;
    std::cout << "}" << std::endl;
    std::cout << "function fillDate(t) {" << std::endl;
    std::cout << "  if (t.value == \"\")" << std::endl;
    std::cout << "    t.value='YYYY-MM';" << std::endl;
    std::cout << "}" << std::endl;
    std::cout << "function submitType() {" << std::endl;
    std::cout << "  document.search.adv.value=\"true\";" << std::endl;
    std::cout << "  document.search.submit();" << std::endl;
    std::cout << "}" << std::endl;
    std::cout << "</script>" << std::endl;
    std::cout << "<p><center><form name=\"search\" action=\"/cgi-bin/dssearch\" method=\"get\"><table class=\"paneltext\" cellspacing=\"0\" cellpadding=\"5\" border=\"0\">" << std::endl;
    std::cout << "<tr><td colspan=\"2\">Find datasets with words:&nbsp;<input type=\"text\" name=\"words\" size=\"75\" value=\"" << local_args.word_list << "\"></td></tr>" << std::endl;
    std::cout << "<tr><td colspan=\"2\">Show results with data:<table class=\"paneltext\" cellspacing=\"5\" cellpadding=\"0\" border=\"0\">" << std::endl;
    if (date_error.length() > 0) {
	std::cout << "<tr><td></td><td width=\"1\"><img src=\"/images/alert.gif\" width=\"16\" height=\"16\">&nbsp;" << date_error << "</td></tr>";
    }
    std::cout << "<tr><td>between:</td><td><input type=\"text\" name=\"startd\" class=\"fixedWidth14\" size=\"7\" value=\"" << startd << "\" onFocus=\"javascript:clearDate(this)\" onBlur=\"javascript:fillDate(this)\">&nbsp;and&nbsp;<input type=\"text\" name=\"endd\" class=\"fixedWidth14\" size=\"7\" value=\"" << endd << "\" onFocus=\"javascript:clearDate(this)\" onBlur=\"javascript:fillDate(this)\">&nbsp;<input type=\"button\" value=\"Reset\" onClick=\"javascript:document.search.startd.value=document.search.endd.value='YYYY-MM';\"></td></tr>" << std::endl;
    if (tres_set.size() > 0) {
	std::cout << "<tr><td>having temporal resolution:</td><td><select name=\"tres\"><option value=\"\">any</option>";
	for (const auto& ele : tres_set) {
	  sdum=ele;
	  strutils::replace_all(sdum," - "," to ");
	  strutils::replace_all(sdum,"<","&lt;");
	  strutils::replace_all(sdum,">","&gt;");
	  std::cout << "<option value=\"T : " << ele << "\"";
	  if (ele == local_args.advanced.show.tres) {
	    std::cout << " selected";
	  }
	  std::cout << ">" << sdum << "</option>";
	}
	std::cout << "</select></td></tr>" << std::endl;
    }
    std::cout << "<tr><td>in format:</td><td><select name=\"format\"><option value=\"\">any</option>" << std::endl;
    query.set("select distinct keyword from search.formats as f left join search.datasets as d on f.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" order by keyword");
    if (query.submit(server) == 0) {
	while (query.fetch_row(row)) {
	  sdum=row[0];
	  if (!strutils::contains(sdum,"proprietary")) {
	    std::cout << "<option value=\"" << sdum << "\"";
	    if (sdum == local_args.advanced.show.format) {
		std::cout << " selected";
	    }
	    std::cout << ">" << strutils::capitalize(sdum) << "</option>";
	  }
	}
    }
    std::cout << "</select></td></tr>" << std::endl;
    std::cout << "<tr><td>of type:</td><td><select name=\"type\" onChange=\"javascript:submitType()\"><option value=\"\">any</option>";
    query.set("select distinct keyword from search.data_types as t left join search.datasets as d on t.dsid = d.dsid where "+INDEXABLE_DATASET_CONDITIONS+" order by keyword");
    if (query.submit(server) == 0) {
	while (query.fetch_row(row)) {
	  sdum=row[0];
	  std::cout << "<option value=\"" << sdum << "\"";
	  if (sdum == local_args.advanced.show.type) {
	    std::cout << " selected";
	  }
	  std::cout << ">" << strutils::capitalize(sdum) << "</option>";
	}
    }
    std::cout << "</select></td></tr>" << std::endl;
    if (local_args.advanced.show.type == "grid" && gres_set.size() > 0) {
	std::cout << "<tr><td>having spatial resolution:</td><td><select name=\"gres\"><option value=\"\">any</option>";
	for (const auto& ele : gres_set) {
	  sdum=ele;
	  strutils::replace_all(sdum," - "," to ");
	  strutils::replace_all(sdum,"<","&lt;");
	  strutils::replace_all(sdum,">","&gt;");
	  std::cout << "<option value=\"H : " << ele << "\"";
	  if (ele == local_args.advanced.show.gres) {
	    std::cout << " selected";
	  }
	  std::cout << ">" << sdum << "</option>";
	}
	std::cout << "</select></td></tr>" << std::endl;
    }
    std::cout << "</table></td></tr>" << std::endl;
    std::cout << "</table><br><input type=\"hidden\" name=\"adv\" value=\"false\"><input type=\"submit\" value=\"Search\"></form></center></p>" << std::endl;
  }
}

void addToAdvancedCriteria(std::string table,std::string keyword,std::string& advSpec,std::string& advWhere,size_t n)
{
  if (advSpec.length() > 0) {
    advSpec+=" left join "+table+" as "+t[n]+" on "+t[n]+".dsid = "+t[n-1]+".dsid";
    if (advWhere.length() > 0) {
	advWhere+=" and ";
    }
    advWhere+=std::string(t[n])+".keyword = '"+keyword+"'";
  }
  else {
    advSpec=table+" as "+t[n];
    advWhere=std::string(t[n])+".keyword = '"+keyword+"'";
  }
}

void insertSearchTips()
{
  std::cout << "<li>Make sure you spelled all words correctly.</li>" << std::endl;
  std::cout << "<li>If an acronym could not be matched, try spelling it out.</li>" << std::endl;
  std::cout << "<li>Try using a common acronym if one exists (e.g. ERA40, FNL).</li>" << std::endl;
  std::cout << "<li>If you are not looking for data, try our <a href=\"http://www.googlesyndicatedsearch.com/u/ncar?hq=site%3Arda.ucar.edu&q="+local_args.word_list+"\">full site search</a>.</li>" << std::endl;
  std::cout << "<li>Contact us at <a href=\"mailto:rdahelp@ucar.edu\">rdahelp@ucar.edu</a>.  If we don't have the data you want, we may know where you can obtain them.</li>" << std::endl;
}

void showResults()
{
  std::ofstream log;
  std::vector<DsEntry> array;
  size_t n,iterator,num_check,num_chars;
  DatasetEntry dentry;
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::string unmatchedTermList,newTermList,selSpec,advSpec,advWhere,querySpecification;
  std::unordered_set<std::string> adv_table,alternate_table;
  std::string qstartd,qendd,sdum,soundex;
  std::vector<std::string> sarray;
  size_t qstartflag,qendflag;
  std::list<std::string> remove_list;
  bool addToTable,addToLog=false,startedHistorical=false;
  bool excluded,startedSingle=false;

  addToLog=true;
  std::cout << "Content-type: text/html" << std::endl << std::endl;
  std::cout << "<h1>RDA Dataset Search</h1>" << std::endl;
  if (!local_args.is_single_special_word) {
    n=0;
    if (local_args.advanced.show.format.length() > 0) {
	addToAdvancedCriteria("search.formats",local_args.advanced.show.format,advSpec,advWhere,n++);
    }
    if (local_args.advanced.show.type.length() > 0) {
	addToAdvancedCriteria("search.data_types",local_args.advanced.show.type,advSpec,advWhere,n++);
    }
    if (local_args.advanced.show.gres.length() > 0) {
	addToAdvancedCriteria("search.grid_resolutions",local_args.advanced.show.gres,advSpec,advWhere,n++);
    }
    if (local_args.advanced.show.tres.length() > 0) {
	addToAdvancedCriteria("search.time_resolutions",local_args.advanced.show.tres,advSpec,advWhere,n++);
    }
    if (n > 0) {
	selSpec="distinct a.dsid";
	if (local_args.advanced.show.startd.length() > 0 && local_args.advanced.show.startd != "YYYY-MM" && local_args.advanced.show.endd != "YYYY-MM") {
	  selSpec+=","+std::string(t[n])+".date_start,"+t[n]+".time_start,"+t[n]+".start_flag,"+t[n]+".date_end,"+t[n]+".time_end,"+t[n]+".end_flag";
	  advSpec+=" left join dssdb.dsperiod as "+std::string(t[n])+" on substr("+t[n]+".dsid,3) = "+t[n-1]+".dsid";
	}
	querySpecification="select "+selSpec+" from "+advSpec+" where "+advWhere;
    }
    else if (local_args.advanced.show.startd.length() > 0 && local_args.advanced.show.startd != "YYYY-MM" && local_args.advanced.show.endd != "YYYY-MM") {
	querySpecification="select substr(dsid,3),date_start,time_start,start_flag,date_end,time_end,end_flag from dssdb.dsperiod";
    }
    if (querySpecification.length() > 0) {
	query.set(querySpecification);
	if (query.submit(server) < 0) {
	  web_error(query.error());
	}
	else {
	  while (query.fetch_row(row)) {
	    if (adv_table.find(row[0]) == adv_table.end()) {
		addToTable=true;
		if (query.length() > 1) {
		  qstartd=row[1];
		  if (qstartd.length() == 0) {
		    addToTable=false;
		  }
		  else {
		    qstartflag=std::stoi(row[3]);
		    if (qstartflag < 2) {
			qstartd=qstartd.substr(0,5)+"01";
		    }
		  }
		  qendd=row[4];
		  if (qendd.length() == 0) {
		    addToTable=false;
		  }
		  else {
		    qendflag=std::stoi(row[6]);
		    if (qendflag < 2) {
			qendd=qendd.substr(0,5)+"12";
		    }
		  }
		  if (addToTable && (local_args.advanced.show.startd > qendd || local_args.advanced.show.endd < qstartd)) {
		    addToTable=false;
		  }
		}
		if (addToTable) {
		  adv_table.emplace(row[0]);
		}
	    }
	  }
	}
    }
  }
  else {
    for (const auto& word : local_args.words) {
	result_table[word].rating=1;
	for (n=0; n < local_args.words.size(); ++n) {
	  result_table[word].matched_words.emplace(word+strutils::itos(n));
	}
    }
    unmatched_term_table.clear();
  }
  array.clear();
  array.reserve(result_table.size());
  for (const auto& kv : result_table) {
    if (!(excluded=exclude_set.find(kv.first) != exclude_set.end()) && (adv_table.size() == 0 || adv_table.find(kv.first) != adv_table.end()) && kv.second.matched_words.size() >= (local_args.words.size()-local_args.compound_words.size())) {
	if (dataset_map.find(kv.first) != dataset_map.end()) {
	  array.emplace_back(kv.second);
	  array.back().dsid=kv.first;
	  array.back().type=dataset_map[kv.first].type;
	}
	else {
// remove internal datasets from the result list
	  remove_list.emplace_back(kv.first);
	}
    }
    else {
	if (!excluded && kv.second.matched_words.size() > 0) {
	  for (const auto& ele : kv.second.matched_words) {
	    ++word_hash_map[ele];
	  }
	}
	remove_list.emplace_back(kv.first);
    }
  }
  for (const auto& key : remove_list) {
    result_table.erase(key);
  }
  if (query_string) {
    binary_sort(array,
    [](const DsEntry& left,const DsEntry& right) -> bool
    {
	if (left.type != right.type) {
	  if (left.type == "P") {
	    return true;
	  }
	  else {
	    return false;
	  }
	}
	else {
	  return (left.rating > right.rating);
	}
    });
    std::stringstream oss,ess;
    unixutils::mysystem2("/usr/bin/php "+SERVER_ROOT+"/web/php/inc/main/dssearch.inc",oss,ess);
    std::cout << "<center>" << oss.str() << "</center>" << std::endl;
    if (addToLog) {
	log.open((SERVER_ROOT+"/logs/dssearch_log").c_str(),std::fstream::app);
	log << dateutils::current_date_time().to_string() << "<!>" << local_args.word_list << "<!>";
    }
    if (ignored_word_list.length() > 0) {
	std::cout << "The following keywords were ignored:  <span class=\"mediumGrayText\"><b>" << ignored_word_list << "</b></span>.<br>" << std::endl;
    }
    if (local_args.words.size() > 1 && unmatched_term_table.size() > 0) {
	local_args.word_list="";
	for (const auto& word : local_args.words) {
	  if (unmatched_term_table.find(word) != unmatched_term_table.end()) {
	    if (unmatchedTermList.length() > 0) {
		unmatchedTermList+=", ";
	    }
	    unmatchedTermList+="'"+word+"'";
	  }
	  else {
	    if (newTermList.length() > 0) {
		newTermList+=" ";
	    }
	    newTermList+=word;
	  }
	}
	std::cout << "The following keywords could not be matched:  <font color=\"red\"><b>" << unmatchedTermList << "</b></font>.";
	if (newTermList.length() > 0) {
	  std::cout << "&nbsp;&nbsp;Retry your search with '<a href=\"/index.html#!cgi-bin/dssearch?words=" << strutils::substitute(newTermList," ","+") << "\"><b>" << newTermList << "</b></a>'?<p>Otherwise:<ul>" << std::endl;
	  insertSearchTips();
          std::cout << "</ul></p>" << std::endl;
	}
	if (addToLog)
	  log << "<!>UNMATCHED:" << unmatchedTermList << std::endl;
    }
    else {
	if (result_table.size() > 0) {
	  if (local_args.is_file_name)
	    std::cout << "<p>It was matched as a data file name in the following dataset(s):</p>" << std::endl;
	  else {
	    if (result_table.size() == 1) {
		std::cout << "<p>1 dataset was identified:</p>" << std::endl;
	    }
	    else {
		std::cout << "<p>" << result_table.size() << " datasets (sorted by relevance) were identified:</p>" << std::endl;
	    }
	  }
	  iterator=0;
	  for (n=0; n < array.size(); ++n) {
	    if (array[n].type == "H" && !startedHistorical) {
		std::cout << "<div style=\"border: 1px solid black; padding: 5px\"><img src=\"/images/alert.gif\" width=\"16\" height=\"16\">&nbsp;<span style=\"color: red\">The following datasets are recommended for ancillary use only and not as primary research datasets.  They have likely been superseded by newer and better datasets.</span><br><br>" << std::endl;
		startedHistorical=true;
	    }
	    std::cout << "<div style=\"padding: 10px 10px 10px 5px; margin: 0px 10px 15px 0px\" onMouseOver=\"javascript:this.style.backgroundColor='#eafaff'\" onMouseOut=\"javascript:this.style.backgroundColor='transparent'\">";
	    if (startedHistorical) {
		std::cout << "<img src=\"/images/alert.gif\" width=\"16\" height-\"16\" />&nbsp;";
	    }
	    std::cout << n+1 << ".&nbsp;<a href=\"/datasets/ds" << array[n].dsid << "/\"><b>" << dataset_map[array[n].dsid].title << "</b></a> <span class=\"mediumGrayText\">(ds" << array[n].dsid << ")</span><br />" << searchutils::convert_to_expandable_summary(dataset_map[array[n].dsid].summary,EXPANDABLE_SUMMARY_LENGTH,iterator) << "</div>" << std::endl;
	  }
	  if (startedHistorical) {
	    std::cout << "</div>" << std::endl;
	  }
	  if (addToLog) {
	    log << "<!>FOUND:" << result_table.size() << std::endl;
	  }
	}
	else {
	  if (unmatched_term_table.size() == 1) {
// one keyword that couldn't be matched; check for an alternate word
	    soundex=strutils::soundex(searchutils::root_of_word(local_args.word_list));
	    if (soundex.length() > 0 && !strutils::has_ending(soundex,"00")) {
		for (const auto& item : list_of_word_lists) {
		  query.set("select distinct s.word from "+item.wordList+" as s left join search.datasets as d on d.dsid = s.dsid where sword = '"+soundex+"' and "+INDEXABLE_DATASET_CONDITIONS);
		  if (query.submit(server) == 0) {
		    while (query.fetch_row(row)) {
			alternate_table.insert(row[0]);
		    }
		  }
		}
	    }
	  }
	  std::cout << "<p>No datasets were identified.";
	  if (alternate_table.size() == 1) {
	    auto ele=alternate_table.begin();
	    std::cout << "  Did you mean to search for '<a class=\"underline\" href=\"/index.html#cgi-bin/dssearch?words=" << *ele << "\"><b>" << *ele << "</b></a>'?  If not,";
	  }
	  else if (alternate_table.size() > 1) {
	    sarray.clear();
	    sarray.reserve(alternate_table.size());
	    for (const auto& ele : alternate_table) {
		num_check= (local_args.word_list.length()) < ele.length() ? local_args.word_list.length() : ele.length();
		num_chars=0;
		for (n=0; n < num_check; ++n) {
		  if (ele[n] == local_args.word_list[n]) {
		    num_chars++;
		  }
		  else {
		    n=num_check;
		  }
		}
		sarray.emplace_back(strutils::to_upper(ele,0,num_chars));
	    }
	    binary_sort(sarray,
	    [](const std::string& left,const std::string& right) -> bool
	    {
		if (left < right) {
		  return true;
	 	}
		else {
		  return false;
		}
	    });
	    sarray[0]=strutils::to_lower(sarray[0]);
	    std::cout << "  Did you mean to search for '<a class=\"underline\" href=\"/index.html#cgi-bin/dssearch?words="+sarray[0]+"\"><b>"+sarray[0]+"</b></a>'?  If not,";
	  }
	  else {
	    for (const auto& word : local_args.words) {
		if (word_hash_map[word] > 0) {
		  if (!startedSingle) {
		    std::cout << "  However, the following words individually match RDA datasets:<ul>";
		    startedSingle=true;
		  }
		  std::cout << "<a href=\"javascript:void(0)\" onClick=\"javascript:location='http://" << getenv("SERVER_NAME") << "/index.html#cgi-bin/dssearch?words=" << word << "'\">" << word << "</a> <span style=\"font-size: 13px; color: #6a6a6a\">(" << word_hash_map[word] << ")</span><br />" << std::endl;
		}
	    }
	    if (startedSingle)
		std::cout << "</ul>Otherwise:" << std::endl;
	  }
	  std::cout << "<ul>" << std::endl;
	  insertSearchTips();
	  std::cout << "</ul></p><p><a href=\"" << getenv("HTTP_REFERER") << "\">Return to the Search page</a></p>" << std::endl;
	  if (addToLog)
	    log << "<!>NONE_FOUND" << std::endl;
	}
    }
    if (addToLog)
	log.close();
  }
  else {
    for (n=0; n < array.size(); ++n) {
	std::cout << array[n].dsid << std::endl;
    }
  }
}

void useWordlistToModifyResults(std::list<std::string>& list,std::string table,double weight)
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::string separator,rword,sword,whereConditions,sdum;
  size_t m,l;
  int diff;
  bool rateByLocation;

  for (const auto& item : list) {
    rateByLocation=true;
    whereConditions="word = '"+item+"'";
    if (weight > 0.) {
	rword=searchutils::root_of_word(item);
	if (rword.length() > 0) {
	  sword=strutils::soundex(rword);
	  whereConditions+=" or (word like '"+rword+"%' and sword = '"+sword+"')";
	}
    }
    query.set("dsid,location,word",table,whereConditions);
    if (query.submit(server) < 0) {
	sdum=query.error();
	if (strutils::contains(sdum,"Unknown column 'location'")) {
	  query.set("select dsid,count(keyword),'x' from "+table+" where keyword like '%"+item+"%' group by dsid");
	  if (query.submit(server) < 0) {
	    web_error(query.error());
	  }
	  else {
	    rateByLocation=false;
	  }
	}
    }
    while (query.fetch_row(row)) {
	m=std::stoi(row[1]);
	if (weight > 0.) {
// determine the rating for the word - 1 x weight if the word exactly matches
//   and 0.5 x weight if the root/soundex matches
	  if (rateByLocation) {
// rating by location stores the location of the word within the metadata field
//   for later comparison to other search words in the field
	    if (item == row[2]) {
		result_table[row[0]].rating+=weight;
	    }
	    else {
		result_table[row[0]].rating+=weight*0.5;
	    }
	    result_table[row[0]].locations.emplace_back(m);
	  }
	  else {
// not rating by location weights the search word by the number of times it
//   appears in the metadata field
	    if (item == row[2]) {
		result_table[row[0]].rating+=(m*weight);
	    }
	    else {
		result_table[row[0]].rating+=(m*weight*0.5);
	    }
	  }
	  if (result_table[row[0]].matched_words.find(item) == result_table[row[0]].matched_words.end()) {
	    if (!searchutils::is_compound_term(item,separator)) {
		result_table[row[0]].matched_words.emplace(item);
	    }
	  }
	}
	else {
	  exclude_set.insert(row[0]);
	}
    }
    if (query.num_rows() > 0) {
	unmatched_term_table.erase(item);
    }
  }
  for (auto& pair : result_table) {
    for (m=0; m < pair.second.locations.size(); ++m) {
	for (l=m+1; l < pair.second.locations.size(); ++l) {
	  diff=pair.second.locations[l]-pair.second.locations[m];
	  if (diff < 0) {
	    diff=100;
	  }
	  else {
	    --diff;
	  }
	  pair.second.rating+=(1./pow(2.,diff)*weight);
	}
    }
    pair.second.locations.clear();
  }
}

void useKeywordListToModifyResults(std::list<std::string>& list,std::string table,double weight)
{
  for (const auto& item : list ) {
    std::string whereConditions="keyword rlike '(^|.{1,}[ -/])"+item+"([ -/].{1,}|$)'";
    MySQL::LocalQuery query("dsid",table,whereConditions);
    if (query.submit(server) < 0) {
	web_error(query.error());
    }
    MySQL::Row row;
    while (query.fetch_row(row)) {
	result_table[row[0]].rating+=weight;
	if (result_table[row[0]].matched_words.find(item) == result_table[row[0]].matched_words.end()) {
	  std::string separator;
	  if (!searchutils::is_compound_term(item,separator)) {
	    result_table[row[0]].matched_words.emplace(item);
	  }
	}
    }
    if (query.num_rows() > 0 && unmatched_term_table.find(item) != unmatched_term_table.end()) {
	unmatched_term_table.erase(item);
    }
  }
}

void search()
{
  WordListEntry wle;
  wle.wordList="search.summary_wordlist";
  wle.weight=1.;
  list_of_word_lists.emplace_back(wle);
  wle.wordList="search.title_wordlist";
  wle.weight=2.1;
  list_of_word_lists.emplace_back(wle);
  wle.wordList="search.references_wordlist";
  wle.weight=0.85;
  list_of_word_lists.emplace_back(wle);
  wle.wordList="search.variables_wordlist";
  wle.weight=2.;
  list_of_word_lists.emplace_back(wle);
  wle.wordList="search.locations_wordlist";
  wle.weight=2.;
  list_of_word_lists.emplace_back(wle);
//  wle.wordList="search.projects";
wle.wordList="(select p.dsid as dsid, g.path as keyword from search.projects_new as p left join search.gcmd_projects as g on g.uuid = p.keyword) as x";
  wle.weight=5.;
  list_of_word_lists.emplace_back(wle);
  wle.wordList="search.supported_projects";
  wle.weight=5.;
  list_of_word_lists.emplace_back(wle);
  wle.wordList="search.formats";
  wle.weight=0.5;
  list_of_word_lists.emplace_back(wle);
  std::string date_error;
  if ((local_args.advanced.show.startd.length() > 0 && local_args.advanced.show.startd != "YYYY-MM") || (local_args.advanced.show.endd.length() > 0 && local_args.advanced.show.endd != "YYYY-MM")) {
    if (local_args.advanced.show.startd == "YYYY-MM" || local_args.advanced.show.endd == "YYYY-MM") {
	date_error="Both dates must be present to narrow your search by date range";
    }
    else if (local_args.advanced.show.startd.length() != 7 || local_args.advanced.show.endd.length() != 7 || local_args.advanced.show.startd[4] != '-' || local_args.advanced.show.endd[4] != '-') {
	date_error="Start date and end date must be entered as \"YYYY-MM\" where YYYY is the 4-digit year and MM is the two-digit month";
    }
    else if (local_args.advanced.show.endd < local_args.advanced.show.startd) {
	date_error="The start date cannot be later than the end date";
    }
  }
  if (date_error.length() > 0) {
    start(date_error);
  }
  else {
    MySQL::LocalQuery query("select d.dsid,d.type,d.title,d.summary from search.datasets as d where "+INDEXABLE_DATASET_CONDITIONS);
    if (query.submit(server) < 0) {
	std::cout << "Location: /cgi-bin/error?code=5500&directory=/." << std::endl << std::endl;
	exit(1);
    }
    MySQL::Row row;
    while (query.fetch_row(row)) {
	dataset_map[row[0]].type=row[1];
	dataset_map[row[0]].title=row[2];
	dataset_map[row[0]].summary=row[3];
    }
// find results containing required words
    for (const auto& item : list_of_word_lists) {
	useWordlistToModifyResults(local_args.words,item.wordList,item.weight);
    }
// modify results for excluded words
    for (const auto& item : list_of_word_lists) {
	useWordlistToModifyResults(local_args.exclude_words,item.wordList,-1.);
    }
    std::string separator;
// special treatment for only one search word
    if (result_table.size() == 0 && local_args.words.size() == 1) {
	auto single_word=local_args.words.front();
// check to see if it is a dataset number or partial dataset number
	if (single_word.length() >= 3 && single_word.length() <= 7 && !searchutils::is_compound_term(single_word,separator)) {
	  single_word=strutils::to_lower(single_word);
	  if (strutils::has_beginning(single_word,"ds")) {
	    single_word=single_word.substr(2);
	    single_word=strutils::to_lower(single_word);
	  }
	  if (single_word.length() == 5 && dataset_map.find(single_word) != dataset_map.end()) {
	    local_args.is_single_special_word=true;
	    local_args.words.clear();
	    local_args.words.emplace_back(single_word);
	  }
	  else if (single_word.length() < 5) {
	    auto word=single_word;
	    if (word.length() == 4) {
		word.insert(3,".");
		if (dataset_map.find(word) != dataset_map.end()) {
		  local_args.is_single_special_word=true;
		  local_args.words.clear();
		  local_args.words.emplace_back(word);
		}
	    }
	    else {
		word+=".";
		for (size_t n=0; n < 10; ++n) {
		  auto wn=word+strutils::itos(n);
		  if (dataset_map.find(wn) != dataset_map.end()) {
		    if (!local_args.is_single_special_word) {
			local_args.words.clear();
		    }
		    local_args.is_single_special_word=true;
		    local_args.words.emplace_back(wn);
		  }
		}
	    }
	  }
	}
    }
    if (local_args.advanced.sf == "on") {
// check to see if keyword(s) match data file names
	for (const auto& word : local_args.words) {
	  query.set("select distinct dsid from dssdb.mssfile where lower(mssfile) like '%"+word+"%' and property = 'P' and retention_days > 0 union select distinct dsid from dssdb.wfile where lower(wfile) like '%"+word+"%' and property = 'A' and (type = 'D' or type = 'U')");
	  if (query.submit(server) < 0) {
	    web_error(query.error());
	  }
	  while (query.fetch_row(row)) {
	    auto key=row[0].substr(2);
	    result_table[key].rating+=1.;
	    if (result_table[key].matched_words.find(word) == result_table[key].matched_words.end()) {
		if (!searchutils::is_compound_term(word,separator)) {
		  result_table[key].matched_words.emplace(word);
		}
	    }
	  }
	  if (query.num_rows() > 0) {
	    unmatched_term_table.erase(word);
	  }
	}
    }
    showResults();
  }
}

int main(int argc,char **argv)
{
  metautils::read_config("dssearch","",false);
  parseQuery(argc,argv);
  server.connect(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (!server) {
    web_error("unable to connect to database");
  }
  if (local_args.words.size() == 0) {
    std::cout << "Location: http://rda.ucar.edu/find_data.html" << std::endl << std::endl;
    exit(1);
  }
  else {
    search();
  }
}
