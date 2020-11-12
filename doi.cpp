/*
** DOI resolver for DOIs that have been assigned to RDA datasets.
*/

#include <iostream>
#include <stdlib.h>
#include <sys/stat.h>
#include <regex>
#include <web/web.hpp>
#include <citation.hpp>
#include <MySQL.hpp>
#include <xml.hpp>
#include <utils.hpp>
#include <myerror.hpp>

std::string myerror="";
std::string mywarning="";

void parse_query_string(std::string& doi)
{
  QueryString queryString(QueryString::GET);

  doi=queryString.value("doi");
  if (doi.length() == 0) {
    web_error2("missing DOI","400 Bad Request");
  }
  if (!std::regex_search(doi,std::regex("^10.5065/"))) {
    web_error2("invalid DOI","400 Bad Request");
  }
}

void print_web_page_title()
{
  std::cout << "Content-type: text/html" << std::endl << std::endl;
  std::cout << "<span class=\"fs24px bold\">RDA DOI Resolver</span>" << std::endl;
}

int main(int argc,char **argv)
{
// read the cgi-bin configuration
  webutils::cgi::Directives directives;
  if (!webutils::cgi::read_config(directives)) {
    web_error2("unable to read configuration","500 Internal Server Error");
  }

// parse the query string
  std::string doi;
  parse_query_string(doi);

// connect to the database
  MySQL::Server server(directives.database_server,directives.rdadb_username,directives.rdadb_password,"dssdb");
  if (!server) {
    web_error2("unable to connect to the database","500 Internal Server Error");
  }

// query the version table for the dataset
  MySQL::LocalQuery version_query("select v.dsid,v.status,g.logname,g.fstname,g.lstname from dsvrsn as v left join dsowner as o on o.dsid = v.dsid left join dssgrp as g on g.logname = o.specialist where v.doi = '"+doi+"' order by find_in_set(status,'A,H')");
  if (version_query.submit(server) < 0) {
    web_error2("database query error","500 Internal Server Error");
  }
  if (version_query.num_rows() == 0) {
    web_error2("DOI not found","400 Bad Request");
  }
  else {
    MySQL::Row version_query_row;
    if (!version_query.fetch_row(version_query_row) != 0) {
	web_error2("unable to connect to the database","500 Internal Server Error");
    }
    auto dsnum=version_query_row["v.dsid"];
    auto ds_status=version_query_row["v.status"];
    if (ds_status == "A") {
	MySQL::LocalQuery dataset_query("title,pub_date","search.datasets","dsid = '"+dsnum.substr(2)+"'");
	if (dataset_query.submit(server) < 0) {
	  web_error2("database query error","500 Internal Server Error");
	}
	MySQL::Row dataset_query_row;
	dataset_query.fetch_row(dataset_query_row);
	print_web_page_title();
	std::cout << "<script id=\"citation_script\" language=\"javascript\">" << std::endl;
	std::cout << "function changeCitation() {" << std::endl;
	std::cout << "  if (document.citation)" << std::endl;
	std::cout << "    var style=document.citation.style[document.citation.style.selectedIndex].value;" << std::endl;
	std::cout << "  else" << std::endl;
	std::cout << "    var style='esip';" << std::endl;
	std::cout << "  document.getElementById(\"citation\").innerHTML=\"Loading the citation...<br /><br />\";" << std::endl;
	std::cout << "  getContent('citation','/cgi-bin/datasets/citation?dsnum=" << dsnum << "&style='+style);" << std::endl;
	std::cout << "}" << std::endl;
	std::cout << "registerAjaxCallback('changeCitation');" << std::endl;
	std::cout << "</script>" << std::endl;
	std::cout << "<ul>" << std::endl;
	std::cout << "<p><b>DOI:</b>&nbsp;&nbsp;" << doi << "</p>" << std::endl;
	std::cout << "<p><b>RDA Dataset ID:</b>&nbsp;&nbsp;" << dsnum << "</p>" << std::endl;
	std::cout << "<p><b>RDA Dataset Title:</b>&nbsp;&nbsp;<span class=\"underline\">" << dataset_query_row[0] << "</span></p>" << std::endl;
	std::cout << "<p><b>URL:</b>&nbsp;&nbsp;<a target=\"_blank_doi\" href=\"/datasets/" << dsnum << "/\">http://rda.ucar.edu/datasets/" << dsnum << "/</a>&nbsp;<img src=\"/images/newwin.gif\" title=\"opens in a new window or tab\" /></p>" << std::endl;
	XMLDocument ds_overview;
	if (ds_overview.open("/usr/local/www/server_root/web/datasets/"+dsnum+"/metadata/dsOverview.xml")) { {
	  std::cout << "<b>How to Cite This Dataset:</b><table cellspacing=\"5\" cellpadding=\"3\" border=\"0\"><tr valign=\"top\"><td><div style=\"background-color: #2a70ae; color: white; width: 40px; padding: 1px; margin-top: 3px; font-size: 16px; font-weight: bold; border-radius: 5px 5px 5px 5px; text-align: center; cursor: pointer\" onClick=\"javascript:location='/cgi-bin/datasets/citation?dsnum=" << dsnum << "&style=ris'\" title=\"download citation in RIS format\">RIS</div></td><td><div id=\"citation\"></div></td></tr></table>" << std::endl;
	  }
	  ds_overview.close();
	}
	std::cout << "</ul>" << std::endl;
    }
    else if (ds_status == "H") {
	MySQL::LocalQuery doi_query("doi","dsvrsn","dsid = '"+dsnum+"' and status = 'A'");
	if (doi_query.submit(server) < 0) {
	  web_error2("database query error","500 Internal Server Error");
	}
	if (doi_query.num_rows() == 0) {
// terminated DOI
	  print_web_page_title();
	}
	else if (doi_query.num_rows() == 1) {
// superseded DOI
	  print_web_page_title();
	  std::cout << "<p><img src=\"/images/alert.gif\" />&nbsp;The DOI that you provided: " << doi << " has been superseded by a new version of the data.  Your options are:<ul><li>Go to the <a href=\"/datasets/"+dsnum+"/\">new version</a> of the data</li><li>Contact <a href=\"mailto:"+version_query_row["g.logname"]+"@ucar.edu?subject=DOI "+doi+"\">"+version_query_row["g.fstname"]+" "+version_query_row["g.lstname"]+"</a> to find out how you can get access to the superseded data";
	  if (version_query.num_rows() > 1) {
	    std::cout << "<ul>Note:  Multiple versions of superseded data exist.  If you know the date that the data were accessed, it will help us get you the exact files associated with this DOI.</ul>";
	  }
	  std::cout << "</li></p>" << std::endl;
	}
	else {
	  web_error2("database error","500 Internal Server Error");
	}
    }
    else {
	web_error2("database error","500 Internal Server Error");
    }
  }
}
