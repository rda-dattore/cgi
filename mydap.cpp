#include <iostream>
#include <sstream>
#include <MySQL.hpp>
#include <strutils.hpp>
#include <web/web.hpp>
#include <datetime.hpp>
#include <gridutils.hpp>

using std::cout;
using std::endl;
using std::stoll;
using std::string;
using std::stringstream;
using std::vector;
using strutils::append;
using strutils::replace_all;
using strutils::split;

const string SERVER_NAME = strutils::get_env("SERVER_NAME");
const string HTTP_REFERER = strutils::get_env("HTTP_REFERER");
const string DUSER = duser();
bool g_from_dashboard;

void showAggregations(MySQL::Server& server) {
  MySQL::LocalQuery query("select d.id, d.rinfo, d.date, s.title from metautil."
      "custom_dap as d left join search.datasets as s on s.dsid = substr(d."
      "rinfo, locate('dsnum=', d.rinfo)+6, 5) where duser = '" + DUSER +
      "' order by d.date");
  if (query.submit(server) < 0) {
    web_error("database query error");
  }
  cout << "Content-type: text/html" << endl << endl;
  cout << "<script id=\"row_color_script\" language=\"javascript\">" << endl;
  auto nrows = query.num_rows();
  cout << "var nrows=" << nrows-- << ";" << endl;
  cout << "function deleteAggregation(a,n) {" << endl;
  cout << "  document.getElementById('modal-window-content').innerHTML='"
      "<center><p></p>Deleting... please wait...</center>';" << endl;
  cout << "  var r=getContentFromSynchronousPost(null, '/cgi-bin/mydap', "
      "'action=delete&id='+a);" << endl;
  cout << "  if (r == 'Success\\n') {" << endl;
  cout << "    document.getElementById('r'+n).style.display='none';" << endl;
  if (HTTP_REFERER.find("/sindex.html") != string::npos) {
    cout << "    getContent('mydap_msg','/cgi-bin/dashboard?action=getmydap');"
        << endl;
  }
  cout << "    hideModalWindow();" << endl;
  cout << "  }" << endl;
  cout << "  else {" << endl;
  cout << "    document.getElementById('modal-window-content').innerHTML="
      "'<center><p></p>There was an error while trying to complete your "
      "request.</center>';" << endl;
  cout << "  }" << endl;
  cout << "}" << endl;
  cout << "function hideRow(n,a) {" << endl;
  cout << "  popModalWindowWithHTML('<center><p></p>Are you sure you want to "
      "delete this dataset?<p></p><table cellspacing=\"0\" cellpadding=\"0\" "
      "border=\"0\"><tr valign=\"middle\"><td style=\"width: 40px; height: "
      "28px; border: 1px solid #2a70ae; background-color: #d6e4ff; "
      "border-radius: 8px 8px 8px 8px; text-align: center; color: #2a70ae; "
      "cursor: pointer\" onmouseover=\"this.style.backgroundColor=&apos;"
      "#fffff5&apos;\" onmouseout=\"this.style.backgroundColor=&apos;#d6e4ff"
      "&apos;\" onclick=\"deleteAggregation(&apos;'+a+'&apos;,&apos;'+n+"
      "'&apos;)\">Yes</td><td>&nbsp;&nbsp;</td><td style=\"width: 40px; "
      "height: 28px; border: 1px solid #2a70ae; background-color: #d6e4ff; "
      "border-radius: 8px 8px 8px 8px; text-align: center; color: #2a70ae; "
      "cursor: pointer\" onmouseover=\"this.style.backgroundColor=&apos;#fffff5"
      "&apos;\" onmouseout=\"this.style.backgroundColor=&apos;#d6e4ff&apos;\" "
      "onclick=\"hideModalWindow()\">No</td></tr></table></center>',400,120);"
      << endl;
  cout << "  --nrows;" << endl;
  cout << "  if (nrows == 0) {" << endl;
  cout <<"     document.getElementById(\"noagg\").style.display=\"block\";" <<
      endl;
  cout << "  }" << endl;
  cout << "}" << endl;
  cout << "function pad2(x) {" << endl;
  cout << "  x=''+x;" << endl;
  cout << "  if (x.length < 2) {" << endl;
  cout << "    x='0'+x;" << endl;
  cout << "  }" << endl;
  cout << "  return x;" << endl;
  cout << "}" << endl;
  cout << "</script>" << endl;
  cout << "<style type=\"text/css\">" << endl;
  cout << ".dap-border {" << endl;
  cout << "  background-color: #b5ceff;" << endl;
  cout << "}" << endl;
  cout << ".dap-pad {" << endl;
  cout << "  padding: 5px 8px 5px 8px;" << endl;
  cout << "}" << endl;
  cout << "</style>" << endl;
  cout << "<table width=\"99%\" cellspacing=\"0\">" << endl;
  cout << "<tr class=\"dap-border\"><td class=\"border-top";
  size_t n = 0;
  if (n != query.num_rows()) {
    cout << " border-bottom";
  }
  cout << " dap-pad\" style=\"border-radius: 10px 0px 0px 0px\" align=\""
      "center\"><strong>OPeNDAP URL</strong></td><td class=\"border-top "
      "border-bottom dap-pad\" align=\"center\"><strong>RDA Dataset ID</strong>"
      "</td><td class=\"border-top border-botto dap-padm\" align=\"center\">"
      "<strong>Date Range</strong></td><td class=\"border-top border-botto "
      "dap-padm\" align=\"center\"><strong>Expiration<font color=\"red\">*"
      "</font></strong></td><td class=\"border-top border-botto dap-padm\" "
      "style=\"border-radius: 0px 10px 0px 0px\" align=\"center\"><strong>"
      "Manage</strong></td></tr>" << endl;
  vector<string> titles, aggs;
  auto curr_dt = dateutils::current_date_time();
  for (const auto& row: query) {
    titles.emplace_back(row[3]);
    gridutils::gridSubset::Args subset_args;
    gridutils::gridSubset::decode_grid_subset_string(row[1], subset_args);
    auto ll_date_s = row[2];
    replace_all(ll_date_s, "-", "");
    if (subset_args.subset_bounds.nlat < 99.) {
      stringstream ss;
      ss << row[0] << "<!>" << subset_args.grid_definition_code << "<!>" <<
          fabs(subset_args.subset_bounds.nlat);
      if (subset_args.subset_bounds.nlat < 0) {
        ss << "S";
      } else {
        ss << "N";
      }
      ss << "<!>" << fabs(subset_args.subset_bounds.slat);
      if (subset_args.subset_bounds.slat < 0) {
        ss << "S";
      } else {
        ss << "N";
      }
      ss << "<!>" << fabs(subset_args.subset_bounds.wlon);
      if (subset_args.subset_bounds.wlon < 0) {
        ss << "W";
      } else {
        ss << "E";
      }
      ss << "<!>" << fabs(subset_args.subset_bounds.elon);
      if (subset_args.subset_bounds.elon < 0) {
        ss << "W";
      } else {
        ss << "E";
      }
      aggs.emplace_back(ss.str());
    } else {
      aggs.emplace_back(row[0] + "<!>" + subset_args.grid_definition_code);
    }
    cout << "<tr id=\"r" << n << "\" style=\"background-color: #eff2ff\" "
        "onmouseover=\"highlightRow(this)\" onmouseout=\"timeout=setTimeout("
        "'unHighlightRow()',100)\"><td class=\"border-top";
    if (n != nrows) {
      cout << " border-bottom";
    }
    cout << " dap-pad\" align=\"center\"><input type=\"text\" class=\""
        "fixedWidth14\" size=\"43\" value=\"https://" << SERVER_NAME <<
        "/opendap/" << row[0] << "\" /></td><td class=\"border-top";
    if (n != nrows) {
      cout << " border-bottom";
    }
    cout << " dap-pad\" align=\"center\"><span onmouseover=\"popInfo(this, 'it"
        << n << "', null, 'left-10', 'bottom+10')\" onmouseout=\"hideInfo('it"
        << n << "')\">" << subset_args.dsnum << "</span></td><td class=\""
        "border-top";
    if (n != nrows) {
      cout << " border-bottom";
    }
    cout << " dap-pad\" style=\"font-size: 14px\" align=\"center\">" <<
        subset_args.startdate << "<br />to<br />" << subset_args.enddate <<
        "</td><td class=\"border-top";
    if (n != nrows) {
      cout << " border-bottom";
    }
    auto exp_dt = DateTime(stoll(ll_date_s) * 1000000).days_added(28);
    cout << " dap-pad\" align=\"center\">" << exp_dt.to_string("%Y-%m-%d") <<
        "</td><td class=\"border-top";
    if (n != nrows) {
      cout << " border-bottom";
    }
    cout << " dap-pad\" align=\"center\"><a href=\"javascript:void(0)\" "
        "onclick=\"clearTimeout(timeout);ignore=true;";
    cout << "popModalWindowWithURL('http";
    if (g_from_dashboard) {
      cout << "s";
    }
    cout << "://" << SERVER_NAME << "/opendap/" << row[0] << ".info',950,600)";
    cout << "\"><img src=\"/images/view.gif\" width=\"18\" height=\"18\" "
        "border=\"0\" title=\"Click to view full details\" onmouseover=\""
        "popInfo(this, 'ir" << n << "', null, 'rcenter+30', 'bottom+10')\" "
        "onmouseout=\"hideInfo('ir" << n << "')\" /></a>&nbsp;<a href=\""
        "javascript:void(0)\" onclick=\"hideRow(" << n << ",'" << row[0] <<
        "')\"><img src=\"/images/delete.png\" width=\"18\" height=\"18\" "
        "border=\"0\" title=\"Click to delete\" /></a>";
    if (exp_dt.days_since(curr_dt) < 8) {
      cout << "&nbsp;<a href=\"javascript:void(0)\" onclick=\"d=new Date("
          "new Date(" << exp_dt.year() << "," << (exp_dt.month() - 1) << ","
          << exp_dt.day() << ").getTime()+1209600000);popConfirm('The "
          "expiration date on this aggregation will be extended to '+d."
          "getFullYear()+'-'+pad2(d.getMonth()+1)+'-'+pad2(d.getDate())+'. Do "
          "you wish to continue?','getAjaxContent(\\'POST\\',\\'id=" << row[0]
          << "&exp=" << exp_dt.to_string("%Y%m%d") << "&action=refresh\\',\\'"
          "/cgi-bin/mydap\\',\\'modal-window-content\\',null,function(){ "
          "getContent(\\'mydap\\',\\'/cgi-bin/mydap\\'); })',500,200)\"><img "
          "src=\"/images/refresh.png\" width=\"18\" height=\"18\" border=\"0\" "
          "title=\"Click to extend the expiration date\" /></a>";
    }
    cout << "</td></tr>" << endl;
    ++n;
  }
  cout << "<tr><td align=\"center\" colspan=\"5\"><div id=\"noagg\" style=\""
      "display: ";
  if (n == 0) {
    cout << "block";
  } else {
    cout << "none";
  }
  cout << "; height: 56px\"><br />You have no aggregations to display</div>"
      "</td></tr>" << endl;
  cout << "<tr class=\"dap-border\"><td class=\"border-bottom dap-pad\" "
     "style=\"border-radius: 0px 0px 10px 10px; font-size: 14px\" align=\""
     "center\" colspan=\"5\">Information valid as of " << curr_dt.to_string(
     "%dd %h %Y %H:%MM:%SS ");
  if (curr_dt.utc_offset() == -600) {
    cout << "MDT";
  } else if (curr_dt.utc_offset() == -700) {
    cout << "MST";
  } else {
    cout << "???";
  }
  cout << "<br /><div id=\"dap_updlink\" style=\"display: inline\"><a style=\""
      "font-weight: bold; text-decoration: underline\" href=\"javascript:void("
      "0)\" onclick=\"document.getElementById('dap_updlink').innerHTML="
      "'Updating...';getContent('mydap','/cgi-bin/mydap')\">Update</a></div>"
      "</td></tr>" << endl;
  cout << "</table>" << endl;
  cout << "<font color=\"red\">*</font>When the current date is within 7 days "
      "of the expiration date, you will have the opportunity to extend your "
      "aggregation before it expires. You will see a refresh button as one of "
      "the \"Manage\" options for the aggregation." << endl;
  n = 0;
  for (const auto& agg : aggs) {
    auto sp = split(agg, "<!>");
    string s = "";
    query.set("select definition, def_params from WGrML.grid_definitions where "
        "code = " + sp[1]);
    MySQL::Row row;
    if (query.submit(server) == 0 && query.fetch_row(row)) {
      s += "<table border=\"0\"><tr valign=\"top\"><td align=\"right\">Grid:"
          "</td><td align=\"left\">" + gridutils::convert_grid_definition(row[
          0] + "<!>" + row[1]);
      if (sp.size() > 2) {
        s += "<br /><div style=\"margin-left: 10px\">Subset: " + sp[2] + " to "
            + sp[3] + " and " + sp[4] + " to " + sp[5] + "</div>";
      }
      s += "</td></tr></table>";
    }
    query.set("select distinct param from metautil.custom_dap_grid_index where "
        "id = '" + sp[0] + "' and time_slice_index = 0");
    if (query.submit(server) == 0) {
      s += "<table border=\"0\"><tr valign=\"top\"><td align=\"right\">"
          "Variable(s):</td><td align=\"left\">";
      for (const auto& row : query) {
        append(s, row[0], ", ");
      }
      s += "</td></tr></table>";
    }
    cout << "<div id=\"ir" << n << "\" class=\"bubble-top-right-arrow\" "
        "style=\"width: 500px; top: 0px; left: 0px; visibility: hidden; "
        "font-size: 14px\">" << s << "</div>" << endl;
    ++n;
  }
  n = 0;
  for (const auto& title : titles) {
    cout << "<div id=\"it" << n << "\" class=\"bubble-top-left-arrow\" "
        "style=\"width: 300px; top: 0px; left: 0px; visibility: hidden\">" <<
        title << "</div>" << endl;
    ++n;
  }
}

int main(int argc, char **argv) {
  if (DUSER.empty()) {
    web_error("not signed in");
  }
  MySQL::Server server("rda-db.ucar.edu", "metadata", "metadata", "");
  if (!server) {
    web_error("database connection error");
  }
  if (HTTP_REFERER.find("/sindex.html") != string::npos) {
    g_from_dashboard = true;
  } else {
    g_from_dashboard = false;
  }
  QueryString queryString(QueryString::POST);
  if (queryString) {
    auto action = queryString.value("action");
    auto id = queryString.value("id");
    cout << "Content-type: text/html" << endl << endl;
    if (action == "delete") {
      if (server._delete("metautil.custom_dap_times", "id = '" + id + "'") <
          0) {
        cout << "Error" << endl;
        exit(0);
      }
      server._delete("metautil.custom_dap_time_index", "id = '" + id + "'");
      if (server._delete("metautil.custom_dap_levels", "id = '" + id + "'") <
          0) {
        cout << "Error" << endl;
        exit(0);
      }
      if (server._delete("metautil.custom_dap_level_index", "id = '" + id + "'")
          < 0) {
        cout << "Error" << endl;
        exit(0);
      }
      if (server._delete("metautil.custom_dap_grid_index", "id = '" + id + "'")
          < 0) {
        cout << "Error" << endl;
        exit(0);
      }
      if (server._delete("metautil.custom_dap", "id = '" + id + "'") < 0) {
        cout << "Error" << endl;
        exit(0);
      }
      cout << "Success" << endl;
    } else if (action == "refresh") {
      auto exp = queryString.value("exp");
      MySQL::Server server("rda-db.ucar.edu", "metadata", "metadata", "");
      if (server && server.update("metautil.custom_dap", "date = '" + DateTime(
          stoll(exp) * 1000000).days_subtracted(14).to_string("%Y-%m-%d") + "'",
          "id = '" + id + "'") == 0) {
        cout << "<h1>Success</h1><p>The expiration date for your aggregation "
            "has been successfully extended.</p>" << endl;
      } else {
        cout << "<h1>Error</h1><p>There was an error processing your request. "
            "Please try again later.</p>" << endl;
      }
    } else {
      cout << "Error" << endl;
    }
  } else {
    showAggregations(server);
  }
}
