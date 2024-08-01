#include <iostream>
#include <string>
#include <regex>
#include <web/web.hpp>
#include <strutils.hpp>
#include <myerror.hpp>

using std::cout;
using std::endl;
using std::string;
using std::vector;

string myerror = "";
string mywarning = "";

struct Args {
  Args() : server(), directory(), type(), dsnum(), script(), parameters(),
      level(), product(), start_date(), end_date(), files() { }

  string server, directory, type, dsnum, script, parameters, level, product;
  string start_date, end_date;
  vector<string> files;
} args;

void parseQuery() {
  QueryString query_string(QueryString::POST);
  args.server = query_string.value("server");
  args.directory = query_string.value("directory");
  if (!args.directory.empty() && args.directory[0] != '/') {
    args.directory = "/" + args.directory;
  }
  args.type = query_string.value("stype_top");
  if (args.type.empty()) {
    args.type = "wget";
  }

// patch until QueryString uses vector instead of list
auto list = query_string.values_that_begin_with("sfile");
for (const auto& e : list) {
args.files.emplace_back(e);
}
//  args.files = query_string.values_that_begin_with("sfile");
  args.dsnum = query_string.value("dsnum");
  args.script = query_string.value("script_top");
  if (args.script.empty()) {
    args.script = "csh";
  }
  args.parameters = query_string.value("parameters");
  args.level = query_string.value("level");
  args.product = query_string.value("product");
  args.start_date = query_string.value("sd");
  args.end_date = query_string.value("ed");
}

int main(int argc, char **argv) {
  parseQuery();
  cout << "Content-type: text/plain" << endl << endl;
  if (args.type == "wget") {
    create_wget_script(args.files, args.server, args.directory, args.script);
  } else if (args.type == "curl") {
    create_curl_script(args.files, args.server, args.directory, args.dsnum,
        args.script, args.parameters, args.level, args.product, args.start_date,
        args.end_date);
  } else if (args.type == "python") {
    create_python_script(args.files, args.server, args.directory, args.script);
  }
}
