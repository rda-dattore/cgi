#include <iostream>
#include <string>
#include <citation_pg.hpp>
#include <web/web.hpp>
#include <PostgreSQL.hpp>
#include <strutils.hpp>

using namespace PostgreSQL;
using std::cout;
using std::endl;
using std::string;

string myerror = "";
string mywarning = "";

void print_citation_script(string dsnum) {
  cout << "<script id=\"citation_script\" language=\"javascript\">" << endl;
  cout << "function changeCitation() {" << endl;
  cout << "  if (document.citation) {" << endl;
  cout << "    var style=document.citation.style[document.citation.style.selectedIndex].value;" << endl;
  cout << "  } else {" << endl;
  cout << "    var style='esip';" << endl;
  cout << "  }" << endl;
  cout << "  if (document.doi_select) {" << endl;
  cout << "    var doi=document.doi_select.doi[document.doi_select.doi.selectedIndex].value;" << endl;
  cout << "  } else {" << endl;
  cout << "    var doi='';" << endl;
  cout << "  }" << endl;
  cout << "  document.getElementById(\"citation\").innerHTML=\"Loading the citation...<br /><br />\";" << endl;
  cout << "  getAjaxContent('GET',null,'/cgi-bin/datasets/citation?dsnum=" << dsnum << "&style='+style+'&doi='+doi,'citation');" << endl;
  cout << "}" << endl;
  cout << "</script>" << endl;
}

int main(int argc, char **argv) {
  Server server("rda-db.ucar.edu", "metadata", "metadata", "rdadb");
  if (!server) {
    web_error("unable to connect to database");
  } else {
    QueryString query_string(QueryString::GET);
    auto dsnum = query_string.value("dsnum");
    auto style = query_string.value("style");
    if (style.empty()) {
      style = "esip";
    }
    auto date = query_string.value("date");
    if (style == "ris") {
      cout << "Content-type: application/x-research-info-systems" << endl;
      cout << "Content-disposition: inline; filename=citation-rda-ds" << dsnum
          << ".ris" << endl;
      cout << endl;
      citation::export_to_ris(cout, dsnum, "", server);
    } else if (style == "bibtex") {
      cout << "Content-type: application/x-bibtex" << endl;
      cout << "Content-disposition: inline; filename=citation-rda-ds" << dsnum
          << ".bib" << endl;
      cout << endl;
      citation::export_to_bibtex(cout, dsnum, "", server);
    } else if (query_string.value("temp") == "yes") {
      cout << "Content-type: text/html" << endl << endl;
      print_citation_script(dsnum);
      cout << "<div id=\"citation\">" << endl;
      cout << citation::temporary_citation(dsnum, style, date, server, true);
      cout << "</div>" << endl;
    } else {
      cout << "Content-type: text/html" << endl << endl;
      print_citation_script(dsnum);
      auto doi = query_string.value("doi");
      if (!doi.empty()) {
        dsnum += "<!>" + doi;
      }
      cout << citation::citation(dsnum, style, date, server, true);
    }
    server.disconnect();
  }
}
