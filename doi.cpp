#include <iostream>
#include <stdlib.h>
#include <sys/stat.h>
#include <regex>
#include <web/web.h>
#include <citation.h>
#include <MySQL.h>
#include <xml.h>
#include <utils.h>

// testing
/*
const std::string SHOULDER="10.5072/FK2";
*/
// operational
const std::string SHOULDER="10.5065/D6";

std::string doi,type,dsnum;

void parseQueryString()
{
  QueryString queryString(QueryString::GET);

  doi=queryString.getValue("doi");
  if (doi.length() == 0) {
    webError("missing DOI");
  }
  if (!std::regex_search(doi,std::regex("^"+SHOULDER))) {
    webError("invalid DOI");
  }
}

void printTitle()
{
  std::cout << "Content-type: text/html" << std::endl << std::endl;
  std::cout << "<span class=\"fs24px bold\">RDA DOI Resolver</span>" << std::endl;
}

int main(int argc,char **argv)
{
  MySQLLocalQuery query,query2;
  MySQLRow row,row2;
  XMLDocument xdoc;

  parseQueryString();
  MySQLServer server(<host>,<username>,<password>,<database>);
  if (!server.isConnected()) {
    webError("unable to connect to the database");
  }
  query.set("select d.dsid,d.status,g.logname,g.fstname,g.lstname from dsvrsn as d left join dsowner as o on o.dsid = d.dsid left join dssgrp as g on g.logname = o.specialist where d.doi = '"+doi+"' order by find_in_set(status,'A,H')");
  if (query.submit(server) < 0) {
    webError(query.error());
  }
  if (query.getResultLength() == 0) {
    webError("DOI not found");
  }
  else {
    if (query.getCurrentResult(row) != 0) {
	webError("unable to connect to the database");
    }
    if (row[1] == "A") {
	query2.set("title,pub_date","search.datasets","dsid = '"+row[0].substr(2)+"'");
	if (query2.submit(server) < 0) {
	  webError(query2.error());
	}
	query2.getCurrentResult(row2);
	printTitle();
	std::cout << "<script id=\"citation_script\" language=\"javascript\">" << std::endl;
	std::cout << "function changeCitation() {" << std::endl;
	std::cout << "  if (document.citation)" << std::endl;
	std::cout << "    var style=document.citation.style[document.citation.style.selectedIndex].value;" << std::endl;
	std::cout << "  else" << std::endl;
	std::cout << "    var style='esip';" << std::endl;
	std::cout << "  document.getElementById(\"citation\").innerHTML=\"Loading the citation...<br /><br />\";" << std::endl;
	std::cout << "  getContent('citation','/cgi-bin/datasets/citation?dsnum=" << row[0] << "&style='+style);" << std::endl;
	std::cout << "}" << std::endl;
	std::cout << "registerAjaxCallback('changeCitation');" << std::endl;
	std::cout << "</script>" << std::endl;
	std::cout << "<ul>" << std::endl;
	std::cout << "<p><b>DOI:</b>&nbsp;&nbsp;" << doi << "</p>" << std::endl;
	std::cout << "<p><b>RDA Dataset ID:</b>&nbsp;&nbsp;" << row[0] << "</p>" << std::endl;
	std::cout << "<p><b>RDA Dataset Title:</b>&nbsp;&nbsp;<span class=\"underline\">" << row2[0] << "</span></p>" << std::endl;
	std::cout << "<p><b>URL:</b>&nbsp;&nbsp;<a target=\"_blank_doi\" href=\"/datasets/" << row[0] << "/\">http://rda.ucar.edu/datasets/" << row[0] << "/</a>&nbsp;<img src=\"/images/newwin.gif\" title=\"opens in a new window or tab\" /></p>" << std::endl;
	if (xdoc.open("/usr/local/www/server_root/web/datasets/"+row[0]+"/metadata/dsOverview.xml")) { {
	  std::cout << "<b>How to Cite This Dataset:</b><table cellspacing=\"5\" cellpadding=\"3\" border=\"0\"><tr valign=\"top\"><td><div style=\"background-color: #2a70ae; color: white; width: 40px; padding: 1px; margin-top: 3px; font-size: 16px; font-weight: bold; border-radius: 5px 5px 5px 5px; text-align: center; cursor: pointer\" onClick=\"javascript:location='/cgi-bin/datasets/citation?dsnum=" << row[0] << "&style=ris'\" title=\"download citation in RIS format\">RIS</div></td><td><div id=\"citation\"></div></td></tr></table>" << std::endl;
	  }
	  xdoc.close();
	}
	std::cout << "</ul>" << std::endl;
    }
    else if (row[1] == "H") {
	query2.set("doi","dsvrsn","dsid = '"+row[0]+"' and status = 'A'");
	if (query2.submit(server) < 0) {
	  webError(query2.error());
	}
	if (query2.getResultLength() == 0) {
// terminated DOI
	  printTitle();
	}
	else if (query2.getResultLength() == 1) {
// superseded DOI
	  printTitle();
	  std::cout << "<p><img src=\"/images/alert.gif\" />&nbsp;The DOI that you provided: " << doi << " has been superseded by a new version of the data.  Your options are:<ul><li>Go to the <a href=\"/datasets/"+row[0]+"/\">new version</a> of the data</li><li>Contact <a href=\"mailto:"+row[2]+"@ucar.edu?subject=DOI "+doi+"\">"+row[3]+" "+row[4]+"</a> to find out how you can get access to the superseded data";
	  if (query.getResultLength() > 1) {
	    std::cout << "<ul>Note:  Multiple versions of superseded data exist.  If you know the date that the data were accessed, it will help us get you the exact files associated with this DOI.</ul>";
	  }
	  std::cout << "</li></p>" << std::endl;
	}
	else
// SEND EMAIL
	  webError("database error");
    }
    else
// SEND EMAIL
	webError("database error");
  }
}
