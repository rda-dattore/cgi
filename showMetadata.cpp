#include <iostream>
#include <sstream>
#include <strutils.hpp>
#include <metadata_export.hpp>
#include <metadata.hpp>
#include <web/web.hpp>
#include <hereDoc.hpp>
#include <tokendoc.hpp>
#include <myerror.hpp>

metautils::Directives metautils::directives;
metautils::Args metautils::args;
std::string myerror="";
std::string mywarning="";

struct ME_Args {
  ME_Args() : action("ListFormats"),format() {}

  std::string action,format;
} me_args;

void parseQuery()
{
  QueryString query_string(QueryString::GET);
  auto sdum=query_string.value("action");
  if (!sdum.empty()) {
    me_args.action=sdum;
  }
  metautils::args.dsnum=query_string.value("dsnum");
  me_args.format=query_string.value("format");
}

void list_formats()
{
  std::cout << "Content-type: text/html" << std::endl << std::endl;
  hereDoc::Tokens tokens;
  tokens.replaces.emplace_back("__DSNUM__<!>"+metautils::args.dsnum);
  hereDoc::print("/usr/local/www/server_root/web/html/oai/showMetadata-format-menu.html",&tokens);
}

void get_content()
{
  metautils::read_config("showMetadata","",false);
  std::stringstream xmlss;
  std::unique_ptr<TokenDocument> token_doc;
  metadataExport::export_metadata(me_args.format,token_doc,xmlss,metautils::args.dsnum);
  auto xs=xmlss.str();
  strutils::replace_all(xs,"<","&lt;");
  strutils::replace_all(xs,">","&gt;");
  std::cout << "Content-type: text/plain" << std::endl << std::endl;
  std::cout << xs << std::endl;
}

int main(int argc,char **argv)
{
  parseQuery();
  if (me_args.action == "ListFormats") {
    list_formats();
  }
  else if (me_args.action == "GetContent") {
    get_content();
  }
}
