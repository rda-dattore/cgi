#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <map>
#include <PostgreSQL.hpp>
#include <web/web.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <search_pg.hpp>
#include <xml.hpp>
#include <metadata.hpp>
#include <bsort.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using std::cerr;
using std::cout;
using std::endl;
using std::list;
using std::stoi;
using std::string;
using std::stringstream;
using std::to_string;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::chop;
using strutils::has_ending;
using strutils::replace_all;
using strutils::substitute;
using strutils::split;
using strutils::to_lower;
using unixutils::mysystem2;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
string myerror = "";
string mywarning = "";

struct LocalArgs {
  LocalArgs() : words(), exclude_words(), compound_words(), word_list(),
      request_uri(), is_single_special_word(false), is_file_name(false),
      show_advanced(false), advanced() { }

  list<string> words, exclude_words, compound_words;
  string word_list, request_uri;
  bool is_single_special_word, is_file_name, show_advanced;

  struct Advanced {
    struct Show {
      Show() : startd(), endd(), tres(), format(), type(), gres() { }

      string startd, endd, tres, format, type, gres;
    };
    Advanced() : show( ),sf( ) { }

    Show show;
    string sf;
  } advanced;

} local_args;

struct DsEntry {
  DsEntry() : dsid(), rating(0.), type(), locations(), matched_words() { }

  string dsid;
  float rating;
  string type;
  vector<int> locations;
  unordered_set<string> matched_words;
};

struct WordListEntry {
  WordListEntry() : wordList(), weight(0.) { }

  string wordList;
  double weight;
};

unordered_map<string, DsEntry> result_table;
unordered_set<string> unmatched_term_table, exclude_set;

struct DatasetEntry {
  DatasetEntry() : type(), title(), summary() { }

  string type, title, summary;
};

unordered_map<string, DatasetEntry> dataset_map;
vector<WordListEntry> list_of_word_lists;
unordered_map<string, int> word_hash_map;
const size_t EXPANDABLE_SUMMARY_LENGTH = 50;

string ignored_word_list;
QueryString query_string;
Server server;
const string SERVER_ROOT = "/" + strutils::token(unixutils::host_name(), ".",
    0);
const string INDEXABLE_DATASET_CONDITIONS = "(d.type = 'P' or d.type = 'H') "
    "and d.dsid not like '999._'";

void parse_query(int argc, char **argv) {
  local_args.request_uri = getenv("REQUEST_URI");
  query_string.fill(QueryString::GET);
  if (!query_string) {
    query_string.fill(QueryString::POST);
  }
  string words;
  if (query_string) {
    words = query_string.value("words");
    local_args.advanced.show.format = query_string.value("format");
    if (!local_args.advanced.show.format.empty()) {
      local_args.show_advanced = true;
    }
    local_args.advanced.show.type = query_string.value("type");
    if (!local_args.advanced.show.type.empty()) {
      local_args.show_advanced = true;
    }
    local_args.advanced.show.gres = query_string.value("gres");
    local_args.advanced.show.startd = query_string.value("startd");
    if (!local_args.advanced.show.startd.empty()) {
      local_args.show_advanced = true;
    }
    local_args.advanced.show.endd = query_string.value("endd");
    if (!local_args.advanced.show.endd.empty()) {
      local_args.show_advanced = true;
    }
    local_args.advanced.show.tres = query_string.value("tres");
    if (!local_args.advanced.show.tres.empty()) {
      local_args.show_advanced = true;
    }
    local_args.advanced.sf = query_string.value("sf");
    if (!local_args.advanced.sf.empty()) {
      local_args.show_advanced = true;
    }
  } else if (local_args.request_uri.empty()) {
    words = unixutils::unix_args_string(argc, argv, ' ');
  }
  if (!words.empty()) {
    replace_all(words, "\"", "");
    auto wlist = split(words);
    for (auto word : wlist) {

      // clean-up of words
      replace_all(word, "\"", "");
      if (has_ending(word, ",")) {
        chop(word);
      }

      // only match valid words
      if (!strutils::has_no_letters(word) || strutils::is_numeric(word)) {
        if (!local_args.word_list.empty()) {
          local_args.word_list += " ";
        }
        local_args.word_list += word;

        // words to exclude from search
        if (word.find("-") == 0) {
          local_args.exclude_words.emplace_back(word.substr(1));
        } else {

          // words to include in search
          // if the word is a compound word, it does not need to be matched
          //   exactly, but its parts do
          string separator;
          if (searchutils::is_compound_term(word, separator)) {
            auto wparts = split(word, separator);
            for (const auto& w : wparts) {
              local_args.words.emplace_back(w);
            }
            local_args.compound_words.emplace_back(word);

            // if there is only one word and it is a compound word, add it to
            //   the list of words that need to be matched
            if (wlist.size() == 1) {
              local_args.words.emplace_back(word);
              unmatched_term_table.emplace(word);
            }
          } else {

            // if the word is not a compound word, it must be matched
            // only match valid words
            local_args.words.emplace_back(word);
            unmatched_term_table.emplace(word);
          }
        }
      } else {
        if (!ignored_word_list.empty()) {
          ignored_word_list += ", ";
        }
        ignored_word_list += "'" + word + "'";
      }
    }
  }
  for (const auto& word : local_args.words) {
    word_hash_map[word] = 0;
  }
}

void start(string date_error = "") {
  cout << "Content-type: text/html" << endl << endl;
  auto startd = local_args.advanced.show.startd;
  auto endd = local_args.advanced.show.endd;
  if (startd.empty()) {
    startd = "YYYY-MM";
  }
  if (endd.empty()) {
    endd = "YYYY-MM";
  }
  if (local_args.words.empty() || date_error.empty()) {
    cout << "<img src=\"/images/transpace.gif\" width=\"1\" height=\"1\" "
        "onload=\"document.search.query.focus()\" />" << endl;
    cout << "<p>Enter one or more keywords into the text box above.</p>" <<
        endl;
  } else {

    // get the possible time and spatial resolutions
    LocalQuery query("select distinct keyword from search.grid_resolutions as "
        "r left join search.datasets as d on r.dsid = d.dsid where " +
        INDEXABLE_DATASET_CONDITIONS);
    unordered_set<string> gres_set;
    if (query.submit(server) == 0) {
      for (const auto& row : query) {
        gres_set.emplace(row[0]);
      }
    }
    query.set("select distinct keyword from search.time_resolutions as r left "
        "join search.datasets as d on r.dsid = d.dsid where " +
        INDEXABLE_DATASET_CONDITIONS);
    unordered_set<string> tres_set;
    if (query.submit(server) == 0) {
      for (const auto& row : query) {
        tres_set.emplace(row[0]);
      }
    }
    std::ifstream ifs((SERVER_ROOT+"/web/metadata/gcmd_resolution").c_str());
    char line[256];
    ifs.getline(line, 256);
    while (!ifs.eof()) {
      string sline = line;
      replace_all(sline, " - ", " to ");
      replace_all(sline, "<", "&lt;");
      replace_all(sline, ">", "&gt;");
      ifs.getline(line, 256);
    }
    ifs.close();
    cout << "<link rel=\"stylesheet\" type=\"text/css\" href=\"/css/"
        "transform.css\">" << endl;
    cout << "<script id=\"dssearch_script\" language=\"javascript\">" << endl;
    cout << "function clearDate(t) {" << endl;
    cout << "  if (t.value == \"YYYY-MM\")" << endl;
    cout << "    t.value='';" << endl;
    cout << "}" << endl;
    cout << "function fillDate(t) {" << endl;
    cout << "  if (t.value == \"\")" << endl;
    cout << "    t.value='YYYY-MM';" << endl;
    cout << "}" << endl;
    cout << "function submitType() {" << endl;
    cout << "  document.search.adv.value=\"true\";" << endl;
    cout << "  document.search.submit();" << endl;
    cout << "}" << endl;
    cout << "</script>" << endl;
    cout << "<p><center><form name=\"search\" action=\"/cgi-bin/dssearch\" "
        "method=\"get\"><table class=\"paneltext\" cellspacing=\"0\" "
        "cellpadding=\"5\" border=\"0\">" << endl;
    cout << "<tr><td colspan=\"2\">Find datasets with words:&nbsp;<input type="
        "\"text\" name=\"words\" size=\"75\" value=\"" << local_args.word_list
        << "\"></td></tr>" << endl;
    cout << "<tr><td colspan=\"2\">Show results with data:<table class=\""
        "paneltext\" cellspacing=\"5\" cellpadding=\"0\" border=\"0\">" << endl;
    if (!date_error.empty()) {
      cout << "<tr><td></td><td width=\"1\"><img src=\"/images/alert.gif\" "
          "width=\"16\" height=\"16\">&nbsp;" << date_error << "</td></tr>";
    }
    cout << "<tr><td>between:</td><td><input type=\"text\" name=\"startd\" "
        "class=\"fixedWidth14\" size=\"7\" value=\"" << startd << "\" onfocus="
        "\"clearDate(this)\" onblur=\"fillDate(this)\">&nbsp;and&nbsp;<input "
        "type=\"text\" name=\"endd\" class=\"fixedWidth14\" size=\"7\" value=\""
        << endd << "\" onfocus=\"clearDate(this)\" onblur=\"fillDate(this)\">"
        "&nbsp;<input type=\"button\" value=\"Reset\" onclick=\"document."
        "search.startd.value=document.search.endd.value='YYYY-MM';\"></td></tr>"
        << endl;
    if (!tres_set.empty()) {
      cout << "<tr><td>having temporal resolution:</td><td><select name=\""
          "tres\"><option value=\"\">any</option>";
      for (const auto& ele : tres_set) {
        auto s = ele;
        replace_all(s, " - ", " to ");
        replace_all(s, "<", "&lt;");
        replace_all(s, ">", "&gt;");
        cout << "<option value=\"T : " << ele << "\"";
        if (ele == local_args.advanced.show.tres) {
          cout << " selected";
        }
        cout << ">" << s << "</option>";
      }
      cout << "</select></td></tr>" << endl;
    }
    cout << "<tr><td>in format:</td><td><select name=\"format\"><option value="
        "\"\">any</option>" << endl;
    query.set("select distinct keyword from search.formats as f left join "
        "search.datasets as d on f.dsid = d.dsid where " +
        INDEXABLE_DATASET_CONDITIONS + " order by keyword");
    if (query.submit(server) == 0) {
      for (const auto& row : query) {
        auto s = row[0];
        if (s.find("proprietary") == string::npos) {
          cout << "<option value=\"" << s << "\"";
          if (s == local_args.advanced.show.format) {
            cout << " selected";
          }
          cout << ">" << strutils::capitalize(s) << "</option>";
        }
      }
    }
    cout << "</select></td></tr>" << endl;
    cout << "<tr><td>of type:</td><td><select name=\"type\" onchange=\""
        "submitType()\"><option value=\"\">any</option>";
    query.set("select distinct keyword from search.data_types as t left join "
        "search.datasets as d on t.dsid = d.dsid where " +
        INDEXABLE_DATASET_CONDITIONS + " order by keyword");
    if (query.submit(server) == 0) {
      for (const auto& row : query) {
        auto s = row[0];
        cout << "<option value=\"" << s << "\"";
        if (s == local_args.advanced.show.type) {
          cout << " selected";
        }
        cout << ">" << strutils::capitalize(s) << "</option>";
      }
    }
    cout << "</select></td></tr>" << endl;
    if (local_args.advanced.show.type == "grid" && !gres_set.empty()) {
      cout << "<tr><td>having spatial resolution:</td><td><select name=\""
          "gres\"><option value=\"\">any</option>";
      for (const auto& ele : gres_set) {
        auto s = ele;
        replace_all(s, " - ", " to ");
        replace_all(s, "<", "&lt;");
        replace_all(s, ">", "&gt;");
        cout << "<option value=\"H : " << ele << "\"";
        if (ele == local_args.advanced.show.gres) {
          cout << " selected";
        }
        cout << ">" << s << "</option>";
      }
      cout << "</select></td></tr>" << endl;
    }
    cout << "</table></td></tr>" << endl;
    cout << "</table><br><input type=\"hidden\" name=\"adv\" value=\"false\">"
        "<input type=\"submit\" value=\"Search\"></form></center></p>" << endl;
  }
}

void add_to_advanced_criteria(string table, string keyword, string& adv_spec,
    string& adv_where, size_t n) {
  if (!adv_spec.empty()) {
    adv_spec += " left join " + table + " as t" + to_string(n) + " on t" +
        to_string(n) + ".dsid = t" + to_string(n-1) + ".dsid";
    if (!adv_where.empty()) {
      adv_where += " and ";
    }
    adv_where += "t" + to_string(n) + ".keyword = '" + keyword + "'";
  } else {
    adv_spec = table + " as t" + to_string(n);
    adv_where = "t" + to_string(n) + ".keyword = '" + keyword + "'";
  }
}

void insert_search_tips() {
  cout << "<li>Make sure you spelled all words correctly.</li>" << endl;
  cout << "<li>If an acronym could not be matched, try spelling it out.</li>" <<
      endl;
  cout << "<li>Try using a common acronym if one exists (e.g. ERA40, FNL).</li>"
      << endl;
  cout << "<li>If you are not looking for data, try our <a href=\"http://www."
      "googlesyndicatedsearch.com/u/ncar?hq=site%3Arda.ucar.edu&q=" +
      local_args.word_list + "\">full site search</a>.</li>" << endl;
  cout << "<li>Contact us at <a href=\"mailto:rdahelp@ucar.edu\">rdahelp@ucar."
      "edu</a>.  If we don't have the data you want, we may know where you can "
      "obtain them.</li>" << endl;
}

void show_results() {
  string adv_spec, adv_where;
  unordered_set<string> adv_table, alternate_table;
  auto add_to_log = true;
  if (!local_args.is_single_special_word) {
    auto n = 0;
    if (!local_args.advanced.show.format.empty()) {
      add_to_advanced_criteria("search.formats", local_args.advanced.show.
          format, adv_spec, adv_where, n++);
    }
    if (!local_args.advanced.show.type.empty()) {
      add_to_advanced_criteria("search.data_types", local_args.advanced.show.
          type, adv_spec, adv_where, n++);
    }
    if (!local_args.advanced.show.gres.empty()) {
      add_to_advanced_criteria("search.grid_resolutions", local_args.advanced.
          show.gres, adv_spec, adv_where, n++);
    }
    if (!local_args.advanced.show.tres.empty()) {
      add_to_advanced_criteria("search.time_resolutions", local_args.advanced.
          show.tres, adv_spec, adv_where, n++);
    }
    string query_spec;
    if (n > 0) {
      string sel_spec = "distinct t0.dsid";
      if (!local_args.advanced.show.startd.empty() && local_args.advanced.show.
          startd != "YYYY-MM" && local_args.advanced.show.endd != "YYYY-MM") {
        sel_spec += ", t" + to_string(n) + ".date_start, t" + to_string(n) +
            ".time_start, t" + to_string(n) + ".start_flag, t" + to_string(n) +
            ".date_end, t" + to_string(n) + ".time_end, t" + to_string(n) +
            ".end_flag";
        adv_spec += " left join dssdb.dsperiod as t" + to_string(n) + " on "
            "substr(t" + to_string(n) + ".dsid,3) = t" + to_string(n-1) +
            ".dsid";
      }
      query_spec = "select " + sel_spec + " from " + adv_spec + " where " +
          adv_where;
    } else if (!local_args.advanced.show.startd.empty() && local_args.advanced.
        show.startd != "YYYY-MM" && local_args.advanced.show.endd !=
        "YYYY-MM") {
      query_spec = "select substr(dsid, 3), date_start, time_start, "
          "start_flag, date_end, time_end, end_flag from dssdb.dsperiod";
    }
    if (!query_spec.empty()) {
      LocalQuery query(query_spec);
      if (query.submit(server) < 0) {
        cerr << "DSSEARCH ERROR: query: '" << query.show() << "', error: '" <<
            query.error() << "'" << endl;
        web_error("Database error");
      } else {
        for (const auto& row : query) {
          if (adv_table.find(row[0]) == adv_table.end()) {
            auto addToTable = true;
            if (query.num_columns() > 1) {
              auto qstartd = row[1];
              if (qstartd.empty()) {
                addToTable = false;
              } else {
                auto qstartflag = stoi(row[3]);
                if (qstartflag < 2) {
                  qstartd = qstartd.substr(0, 5) + "01";
                }
              }
              auto qendd = row[4];
              if (qendd.empty()) {
                addToTable = false;
              } else {
                auto qendflag = stoi(row[6]);
                if (qendflag < 2) {
                  qendd = qendd.substr(0, 5) + "12";
                }
              }
              if (addToTable && (local_args.advanced.show.startd > qendd ||
                  local_args.advanced.show.endd < qstartd)) {
                addToTable = false;
              }
            }
            if (addToTable) {
              adv_table.emplace(row[0]);
            }
          }
        }
        if (adv_table.empty()) {
          adv_table.emplace("1000.0");
        }
      }
    }
  } else {
    for (const auto& word : local_args.words) {
      result_table[word].rating=1;
      for (size_t n = 0; n < local_args.words.size(); ++n) {
        result_table[word].matched_words.emplace(word + to_string(n));
      }
    }
    unmatched_term_table.clear();
  }
  vector<DsEntry> array(result_table.size());
  list<string> remove_list;
  for (const auto& kv : result_table) {
    auto excluded = exclude_set.find(kv.first);
    if (excluded == exclude_set.end() && (adv_table.empty() || adv_table.find(
        kv.first) != adv_table.end()) && kv.second.matched_words.size() >=
        (local_args.words.size() - local_args.compound_words.size())) {
      if (dataset_map.find(kv.first) != dataset_map.end()) {
        array.emplace_back(kv.second);
        array.back().dsid = kv.first;
        array.back().type = dataset_map[kv.first].type;
      } else {

        // remove internal datasets from the result list
        remove_list.emplace_back(kv.first);
      }
    } else {
      if (excluded == exclude_set.end() && !kv.second.matched_words.empty()) {
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
  cout << "Content-type: text/html" << endl << endl;
  if (query_string) {
    binary_sort(array,
    [](const DsEntry& left, const DsEntry& right) -> bool {
      if (left.type != right.type) {
        if (left.type == "P") {
          return true;
        }
        return false;
      }
      if (left.rating == right.rating) {
        return (left.dsid < right.dsid);
      }
      return (left.rating > right.rating);
    });
    std::ofstream log;
    if (add_to_log) {
      log.open((SERVER_ROOT + "/logs/dssearch_log").c_str(), std::fstream::app);
      log << dateutils::current_date_time().to_string() << "<!>" << local_args.
          word_list << "<!>";
    }
    if (!ignored_word_list.empty()) {
      cout << "The following keywords were ignored:  <span class=\""
          "mediumGrayText\"><b>" << ignored_word_list << "</b></span>.<br>" <<
          endl;
    }
    if (local_args.words.size() > 1 && !unmatched_term_table.empty()) {
      local_args.word_list = "";
      string new_term_list, unmatched_term_list;
      for (const auto& word : local_args.words) {
        if (unmatched_term_table.find(word) != unmatched_term_table.end()) {
          if (!unmatched_term_list.empty()) {
            unmatched_term_list += ", ";
          }
          unmatched_term_list += "'" + word + "'";
        } else {
          if (!new_term_list.empty()) {
            new_term_list += " ";
          }
          new_term_list += word;
        }
      }
      cout << "The following keywords could not be matched:  <font color=\""
          "red\"><b>" << unmatched_term_list << "</b></font>.";
      if (!new_term_list.empty()) {
        cout << "&nbsp;&nbsp;Retry your search with '<a href=\"/dssearch?words="
            << substitute(new_term_list, " ", "+") << "\"><b>" << new_term_list
            << "</b></a>'?<p>Otherwise:<ul>" << endl;
        insert_search_tips();
        cout << "</ul></p>" << endl;
      }
      if (add_to_log)
        log << "<!>UNMATCHED:" << unmatched_term_list << endl;
    } else {
      if (!result_table.empty()) {
        if (local_args.is_file_name) {
          cout << "<p>It was matched as a data file name in the following "
              "dataset(s):</p>" << endl;
        } else {
          if (result_table.size() == 1) {
            cout << "<p>1 dataset was identified:</p>" << endl;
          } else {
            cout << "<p>" << result_table.size() << " datasets (sorted by "
                "relevance) were identified:</p>" << endl;
          }
        }
        size_t iterator = 0;
        auto started_historical = false;
        for (size_t n = 0; n < result_table.size(); ++n) {
          if (array[n].type == "H" && !started_historical) {
            cout << "<div style=\"border: 1px solid black; padding: 5px\"><img "
                "src=\"/images/alert.gif\" width=\"16\" height=\"16\">&nbsp;"
                "<span style=\"color: red\">The following datasets are "
                "recommended for ancillary use only and not as primary "
                "research datasets.  They have likely been superseded by newer "
                "and better datasets.</span><br><br>" << endl;
            started_historical = true;
          }
          cout << "<div style=\"padding: 10px 10px 10px 5px; margin: 0px 10px "
              "15px 0px\" onmouseover=\"this.style.backgroundColor='#eafaff'\" "
              "onmouseout=\"this.style.backgroundColor='transparent'\">";
          if (started_historical) {
            cout << "<img src=\"/images/alert.gif\" width=\"16\" height-\"16\">"
               "&nbsp;";
          }
          cout << n+1 << ".&nbsp;<a href=\"/datasets/ds" << array[n].dsid <<
              "/\"><b>" << dataset_map[array[n].dsid].title << "</b></a> <span "
              "class=\"mediumGrayText\">(ds" << array[n].dsid << ")</span><br>"
              << searchutils::convert_to_expandable_summary(dataset_map[array[
              n].dsid].summary, EXPANDABLE_SUMMARY_LENGTH, iterator) << "</div>"
              << endl;
        }
        if (started_historical) {
          cout << "</div>" << endl;
        }
        if (add_to_log) {
          log << "<!>FOUND:" << result_table.size() << endl;
        }
      } else {
        if (unmatched_term_table.size() == 1) {

          // one keyword that couldn't be matched; check for an alternate word
          auto soundex = strutils::soundex(searchutils::root_of_word(local_args.
              word_list));
          if (!soundex.empty() && !has_ending(soundex, "00")) {
            for (const auto& item : list_of_word_lists) {
              LocalQuery query("select distinct s.word from " + item.wordList +
                  " as s left join search.datasets as d on d.dsid = s.dsid "
                  "where sword = '" + soundex + "' and " +
                  INDEXABLE_DATASET_CONDITIONS);
              if (query.submit(server) == 0) {
                for (const auto& row : query) {
                  alternate_table.insert(row[0]);
                }
              }
            }
          }
        }
        cout << "<p>No datasets were identified.";
        if (alternate_table.size() == 1) {
          auto ele = alternate_table.begin();
          cout << "  Did you mean to search for '<a class=\"underline\" href=\""
              "/dssearch?words=" << *ele << "\"><b>" << *ele << "</b></a>'? If "
              "not,";
        } else if (alternate_table.size() > 1) {
          vector<string> sarray(alternate_table.size());
          for (const auto& ele : alternate_table) {
            auto num_check = (local_args.word_list.length()) < ele.length() ?
                local_args.word_list.length() : ele.length();
            auto num_chars = 0;
            for (size_t n = 0; n < num_check; ++n) {
              if (ele[n] == local_args.word_list[n]) {
                ++num_chars;
              } else {
                n = num_check;
              }
            }
            sarray.emplace_back(strutils::to_upper(ele, 0, num_chars));
          }
          binary_sort(sarray,
          [](const string& left,const string& right) -> bool {
            if (left < right) {
              return true;
            }
            return false;
          });
          sarray[0] = to_lower(sarray[0]);
          cout << "  Did you mean to search for '<a class=\"underline\" href=\""
              "/dssearch?words=" + sarray[0] + "\"><b>" + sarray[0] + "</b>"
              "</a>'? If not,";
        } else {
          auto started_single = false;
          for (const auto& word : local_args.words) {
            if (word_hash_map[word] > 0) {
              if (!started_single) {
                cout << "  However, the following words individually match RDA "
                    "datasets:<ul>";
                started_single = true;
              }
              cout << "<a href=\"javascript:void(0)\" onclick=\"location='"
                  "http://" << getenv("SERVER_NAME") << "/dssearch?words=" <<
                  word << "'\">" << word << "</a> <span style=\"font-size: "
                  "13px; color: #6a6a6a\">(" << word_hash_map[word] << ")"
                  "</span><br />" << endl;
            }
          }
          if (started_single) {
            cout << "</ul>Otherwise:" << endl;
          }
        }
        cout << "<ul>" << endl;
        insert_search_tips();
        cout << "</ul></p><p><a href=\"" << getenv("HTTP_REFERER") <<
            "\">Return to the Search page</a></p>" << endl;
        if (add_to_log)
          log << "<!>NONE_FOUND" << endl;
      }
    }
    if (add_to_log) {
      log.close();
    }
  } else {
    for (size_t n = 0; n < array.size(); ++n) {
      cout << array[n].dsid << endl;
    }
  }
}

void use_wordlist_to_modify_results(list<string>& list, string table, double
    weight) {
  auto rate_by_location = false;
  for (const auto& item : list) {
    rate_by_location = true;
    auto wc = "word ilike '" + item + "'";
    if (weight > 0.) {
      auto rword = searchutils::root_of_word(item);
      if (!rword.empty()) {
        auto sword = strutils::soundex(rword);
        wc += " or (word ilike '" + rword + "%' and sword = '" + sword + "')";
      }
    }
    LocalQuery query("select dsid, location, word from " + table + " where " +
        wc);
    if (query.submit(server) != 0) {
      auto e = query.error();
      if (e.find("Unknown column 'location'") != string::npos) {
        query.set("select dsid, count(keyword), 'x' from " + table + " where "
            "keyword ilike '%" + item + "%' group by dsid");
        if (query.submit(server) != 0) {
          cerr << "DSSEARCH ERROR: query: '" << query.show() << "', error: '" <<
              query.error() << "'" << endl;
          web_error("Database error");
        } else {
          rate_by_location = false;
        }
      }
    }
    for (const auto& row : query) {
      if (dataset_map.find(row[0]) == dataset_map.end()) {

        // ignore non-indexable datasets
        continue;
      }
      auto m = stoi(row[1]);
      if (weight > 0.) {

        // determine the rating for the word - 1 x weight if the word exactly
        //   matches and 0.5 x weight if the root/soundex matches
        if (rate_by_location) {

          // rating by location stores the location of the word within the
          //   metadata field for later comparison to other search words in
          // the field
          if (item == row[2]) {
            result_table[row[0]].rating += weight;
          } else {
            result_table[row[0]].rating += weight*0.5;
          }
          result_table[row[0]].locations.emplace_back(m);
        } else {

          // not rating by location weights the search word by the number of
          //   times it  appears in the metadata field
          if (item == row[2]) {
            result_table[row[0]].rating += m * weight;
          } else {
            result_table[row[0]].rating += m * weight * 0.5;
          }
        }
        if (result_table[row[0]].matched_words.find(item) == result_table[row[
            0]].matched_words.end()) {
          string separator;
          if (!searchutils::is_compound_term(item, separator)) {
            result_table[row[0]].matched_words.emplace(item);
          }
        }
      } else {
        exclude_set.insert(row[0]);
      }
    }
    if (query.num_rows() > 0) {
      unmatched_term_table.erase(item);
    }
  }
  for (auto& pair : result_table) {
    for (size_t m = 0; m < pair.second.locations.size(); ++m) {
      for (size_t l = m+1; l < pair.second.locations.size(); ++l) {
        auto diff = pair.second.locations[l] - pair.second.locations[m];
        if (diff < 0) {
          diff=100;
        } else {
          --diff;
        }
        pair.second.rating += 1. / pow(2., diff) * weight;
      }
    }
    pair.second.locations.clear();
  }
}

void search() {
  WordListEntry wle;
  wle.wordList = "search.summary_wordlist";
  wle.weight = 1.;
  list_of_word_lists.emplace_back(wle);
  wle.wordList = "search.title_wordlist";
  wle.weight = 2.1;
  list_of_word_lists.emplace_back(wle);
  wle.wordList = "search.references_wordlist";
  wle.weight = 0.85;
  list_of_word_lists.emplace_back(wle);
  wle.wordList = "search.variables_wordlist";
  wle.weight = 2.;
  list_of_word_lists.emplace_back(wle);
  wle.wordList = "search.locations_wordlist";
  wle.weight = 2.;
  list_of_word_lists.emplace_back(wle);
  wle.wordList = "(select p.dsid as dsid, g.path as keyword from search."
      "projects_new as p left join search.gcmd_projects as g on g.uuid = p."
      "keyword) as x";
  wle.weight = 5.;
  list_of_word_lists.emplace_back(wle);
  wle.wordList = "search.supported_projects";
  wle.weight = 5.;
  list_of_word_lists.emplace_back(wle);
  wle.wordList = "search.formats";
  wle.weight = 0.5;
  list_of_word_lists.emplace_back(wle);
  string date_error;
  if ((!local_args.advanced.show.startd.empty() && local_args.advanced.show.
      startd != "YYYY-MM") || (!local_args.advanced.show.endd.empty() &&
      local_args.advanced.show.endd != "YYYY-MM")) {
    if (local_args.advanced.show.startd == "YYYY-MM" || local_args.advanced.
        show.endd == "YYYY-MM") {
      date_error = "Both dates must be present to narrow your search by date "
          "range";
    } else if (local_args.advanced.show.startd.length() != 7 || local_args.
        advanced.show.endd.length() != 7 || local_args.advanced.show.startd[4]
        != '-' || local_args.advanced.show.endd[4] != '-') {
      date_error = "Start date and end date must be entered as \"YYYY-MM\" "
          "where YYYY is the 4-digit year and MM is the two-digit month";
    } else if (local_args.advanced.show.endd < local_args.advanced.show.
        startd) {
      date_error = "The start date cannot be later than the end date";
    }
  }
  if (!date_error.empty()) {
    start(date_error);
  } else {
    LocalQuery query("select d.dsid,d.type,d.title,d.summary from search."
        "datasets as d where " + INDEXABLE_DATASET_CONDITIONS);
    if (query.submit(server) != 0) {
      cerr << "DSSEARCH ERROR: query: '" << query.show() << "', error: '" <<
          query.error() << "'" << endl;
      web_error("Database error");
    }
    for (const auto& row : query) {
      dataset_map[row[0]].type = row[1];
      dataset_map[row[0]].title = row[2];
      dataset_map[row[0]].summary = row[3];
    }

    // find results containing required words
    for (const auto& item : list_of_word_lists) {
      use_wordlist_to_modify_results(local_args.words, item.wordList, item.
          weight);
    }

    // modify results for excluded words
    for (const auto& item : list_of_word_lists) {
      use_wordlist_to_modify_results(local_args.exclude_words, item.wordList,
          -1.);
    }
    string separator;

    // special treatment for only one search word
    if (result_table.empty() && local_args.words.size() == 1) {
      auto single_word = local_args.words.front();

      // check to see if it is a dataset number or partial dataset number
      if (single_word.length() >= 3 && single_word.length() <= 7 &&
          !searchutils::is_compound_term(single_word, separator)) {
        single_word = to_lower(single_word);
        if (single_word.find("ds") == 0) {
          single_word = single_word.substr(2);
          single_word = to_lower(single_word);
        }
        if (single_word.length() == 5 && dataset_map.find(single_word) !=
            dataset_map.end()) {
          local_args.is_single_special_word = true;
          local_args.words.clear();
          local_args.words.emplace_back(single_word);
        } else if (single_word.length() < 5) {
          auto word = single_word;
          if (word.length() == 4) {
            word.insert(3, ".");
            if (dataset_map.find(word) != dataset_map.end()) {
              local_args.is_single_special_word = true;
              local_args.words.clear();
              local_args.words.emplace_back(word);
            }
          } else {
            word += ".";
            for (size_t n = 0; n < 10; ++n) {
              auto wn = word + to_string(n);
              if (dataset_map.find(wn) != dataset_map.end()) {
                if (!local_args.is_single_special_word) {
                  local_args.words.clear();
                }
                local_args.is_single_special_word = true;
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
        query.set("select distinct dsid from dssdb.wfile where wfile ilike "
            "'%" + word + "%' and status = 'P' and type in ('D', 'U')");
        if (query.submit(server) != 0) {
            cerr << "DSSEARCH ERROR: query: '" << query.show() << "', error: '"
                << query.error() << "'" << endl;
          web_error("Database error");
        }
        for (const auto& row : query) {
          auto key = row[0].substr(2);
          result_table[key].rating += 1.;
          if (result_table[key].matched_words.find(word) == result_table[key].
              matched_words.end()) {
            if (!searchutils::is_compound_term(word, separator)) {
              result_table[key].matched_words.emplace(word);
            }
          }
        }
        if (query.num_rows() > 0) {
          unmatched_term_table.erase(word);
        }
      }
    }
    show_results();
  }
}

int main(int argc, char **argv) {
  metautils::read_config("dssearch", "", false);
  parse_query(argc, argv);
  server.connect(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  if (!server) {
    web_error("unable to connect to database");
  }
  if (local_args.words.empty()) {
    cout << "Location: http://rda.ucar.edu/find_data.html" << endl << endl;
    exit(1);
  } else {
    search();
  }
}
