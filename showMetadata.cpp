#include <iostream>
#include <sstream>
#include <strutils.hpp>
#include <metadata_export_pg.hpp>
#include <metadata.hpp>
#include <web/web.hpp>
#include <tokendoc.hpp>
#include <PostgreSQL.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using std::cout;
using std::endl;
using std::string;
using std::stringstream;
using std::unique_ptr;
using strutils::ds_aliases;
using strutils::ng_gdex_id;
using strutils::replace_all;
using strutils::to_sql_tuple_string;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
string myerror = "";
string mywarning = "";

struct LocalArgs {
  LocalArgs() : action("ListFormats"), format() { }

  string action, format;
} local_args;

void parseQuery() {
  QueryString query_string(QueryString::GET);
  auto v = query_string.value("action");
  if (!v.empty()) {
    local_args.action = v;
  }
  metautils::args.dsid = query_string.value("dsnum");
  local_args.format = query_string.value("format");
}

void list_formats() {
  cout << "Content-type: text/html" << endl << endl;
  TokenDocument tdoc("/usr/local/www/server_root/web/html/oai/showMetadata-"
      "format-menu.html");
  tdoc.add_replacement("__DSNUM__", metautils::args.dsid);
  Server server(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  LocalQuery q("doi", "dssdb.dsvrsn", "dsid in " + to_sql_tuple_string(
      ds_aliases(ng_gdex_id(metautils::args.dsid))) + " and status = 'A'");
  Row row;
  if (q.submit(server) == 0 && q.fetch_row(row)) {
    tdoc.add_repeat("__FORMAT_OPTIONS__", TokenDocument::REPEAT_PAIRS{
        { "IDENTIFIER", "datacite4" },
        { "DESCRIPTION", "DataCite v4" }
    });
    tdoc.add_repeat("__FORMAT_OPTIONS__", TokenDocument::REPEAT_PAIRS{
        { "IDENTIFIER", "datacite" },
        { "DESCRIPTION", "DataCite v3" }
    });
  }
  server.disconnect();
  tdoc.add_repeat("__FORMAT_OPTIONS__", TokenDocument::REPEAT_PAIRS{
      { "IDENTIFIER", "dif" },
      { "DESCRIPTION", "GCMD Directory Interchange Format (DIF)" }
  });
  tdoc.add_repeat("__FORMAT_OPTIONS__", TokenDocument::REPEAT_PAIRS{
      { "IDENTIFIER", "oai_dc" },
      { "DESCRIPTION", "Dublin Core (DC)" }
  });
  tdoc.add_repeat("__FORMAT_OPTIONS__", TokenDocument::REPEAT_PAIRS{
      { "IDENTIFIER", "fgdc" },
      { "DESCRIPTION", "Federal Geographic Data Committee (FGDC)" }
  });
  tdoc.add_repeat("__FORMAT_OPTIONS__", TokenDocument::REPEAT_PAIRS{
      { "IDENTIFIER", "iso19139" },
      { "DESCRIPTION", "International Organization for Standardization (ISO) 19139" }
  });
  tdoc.add_repeat("__FORMAT_OPTIONS__", TokenDocument::REPEAT_PAIRS{
      { "IDENTIFIER", "iso19115-3" },
      { "DESCRIPTION", "International Organization for Standardization (ISO) 19115-3" }
  });
  tdoc.add_repeat("__FORMAT_OPTIONS__", TokenDocument::REPEAT_PAIRS{
      { "IDENTIFIER", "json-ld" },
      { "DESCRIPTION", "JSON-LD Structured Data" }
  });
  cout << tdoc << endl;
}

void get_content() {
  stringstream xmlss;
  unique_ptr<TokenDocument> token_doc;
  metadataExport::export_metadata(local_args.format, token_doc, xmlss,
      metautils::args.dsid);
  auto xs = xmlss.str();
  replace_all(xs, "<", "&lt;");
  replace_all(xs, ">", "&gt;");
  cout << "Content-type: text/plain" << endl << endl;
  cout << xs << endl;
}

int main(int argc, char **argv) {
  metautils::read_config("showMetadata", "", false);
  parseQuery();
  if (local_args.action == "ListFormats") {
    list_formats();
  } else if (local_args.action == "GetContent") {
    get_content();
  }
}
