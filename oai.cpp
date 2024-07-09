#include <iostream>
#include <string>
#include <list>
#include <web/web.hpp>
#include <datetime.hpp>
#include <PostgreSQL.hpp>
#include <xml.hpp>
#include <metadata.hpp>
#include <metadata_export_pg.hpp>
#include <strutils.hpp>
#include <tokendoc.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using std::cout;
using std::endl;
using std::string;
using std::unique_ptr;
using strutils::chop;
using strutils::replace_all;
using strutils::substitute;
using strutils::to_lower;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
string myerror = "";
string mywarning = "";

QueryString query_string;
string verb;
std::list<string> oai_args,bad_args;
const size_t NUM_FORMATS=8;
const string metadata_formats[NUM_FORMATS] = {
    "oai_dc",
    "datacite",
    "dif",
    "fgdc",
    "iso19139",
    "iso19115-3",
    "native",
    "thredds"
};
const string REPOSITORY_IDENTIFIER = "edu.ucar.rda";

void parse_query() {
  QueryString query_string_get(QueryString::GET);
  QueryString query_string_post(QueryString::POST);

  if (query_string_get) {
    query_string = query_string_get;
  } else if (query_string_post) {
    query_string = query_string_post;
  } else {
    cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << endl;
    cout << "  <error code=\"badVerb\">'verb' is missing</error>" << endl;
    cout << "</OAI-PMH>" << endl;
    exit(1);
  }
  verb = query_string.value("verb");
  oai_args = query_string.raw_names();
}

void check_formats(string metadata_prefix) {
  size_t n = 0;
  for (; n < NUM_FORMATS; ++n) {
    if (metadata_prefix == metadata_formats[n]) {
      break;
    }
  }
  if (n == NUM_FORMATS) {
    cout << "  <error code=\"cannotDisseminateFormat\" />" << endl;
    cout << "</OAI-PMH>" << endl;
    exit(1);
  }
}

void print_bad_arguments_list() {
  cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << endl;
  for (auto& a : bad_args) {
    cout << "  <error code=\"badArgument\">'" << a << "' is not allowed here"
        "</error>" << endl;
  }
}

void print_request_element() {
  cout << "  <request";
  for (auto& a : oai_args) {
    cout << " " << a << "=\"" << query_string.value(a) << "\"";
  }
  cout << ">https://rda.ucar.edu/cgi-bin/oai</request>" << endl;
}

void identify() {
  auto found_error = false;
  cout << "  <request verb=\"Identify\">https://rda.ucar.edu/cgi-bin/oai"
      "</request>" << endl;
  for (const auto& a : oai_args) {
    if (a != "verb") {
      cout << "  <error code=\"badArgument\">'" << a << "' is not allowed "
            "here</error>" << endl;
      found_error = true;
    }
  }
  if (found_error) {
    return;
  }
  cout << "  <Identify>" << endl;
  cout << "    <repositoryName>CISL Research Data Archive</repositoryName>" <<
      endl;
  cout << "    <baseURL>https://rda.ucar.edu/cgi-bin/oai</baseURL>" << endl;
  cout << "    <protocolVersion>2.0</protocolVersion>" << endl;
  cout << "    <adminEmail>rdahelp@ucar.edu</adminEmail>" << endl;
  cout << "    <earliestDatestamp>2006-10-15T00:00:00Z</earliestDatestamp>" <<
      endl;
  cout << "    <deletedRecord>no</deletedRecord>" << endl;
  cout << "    <granularity>YYYY-MM-DDThh:mm:ssZ</granularity>" << endl;
  cout << "    <description>" << endl;
  cout << "      <oai-identifier xmlns=\"http://www.openarchives.org/OAI/2.0/"
      "oai-identifier\"" << endl;
  cout << "                      xmlns:xsi=\"http://www.w3.org/2001/"
      "XMLSchema-instance\"" << endl;
  cout << "                      xsi:schemaLocation=\"http://"
      "www.openarchives.org/OAI/2.0/oai-identifier" << endl;
  cout << "                                          http://"
      "www.openarchives.org/OAI/2.0/oai-identifier.xsd\">" << endl;
  cout << "        <scheme>oai</scheme>" << endl;
  cout << "        <repositoryIdentifier>" << REPOSITORY_IDENTIFIER <<
      "</repositoryIdentifier>" << endl;
  cout << "        <delimiter>:</delimiter>" << endl;
  cout << "        <sampleIdentifier>oai:" << REPOSITORY_IDENTIFIER <<
      ":ds010.0</sampleIdentifier>" << endl;
  cout << "      </oai-identifier>" << endl;
  cout << "    </description>" << endl;
  cout << "  </Identify>" << endl;
}

void list_metadata_formats() {
  Server server(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  string dsid;
  for (auto& a : oai_args) {
    if (a == "identifier") {
      dsid = query_string.value("identifier");
    } else if (a != "verb") {
      bad_args.emplace_back(a);
    }
  }
  if (bad_args.size() > 0) {
    print_bad_arguments_list();
    return;
  }
  print_request_element();
  LocalQuery query;
  if (!dsid.empty()) {
    replace_all(dsid, "oai:" + REPOSITORY_IDENTIFIER + ":ds", "");
    query.set("type", "search.datasets", "dsid = '" + dsid + "'");
    if (query.submit(server) < 0) {
      cout << "Content-type: text/plain" << endl << endl;
      cout << "Database error: " << query.error() << endl;
      exit(1);
    }
    if (query.num_rows() == 0) {
      cout << "  <error code=\"idDoesNotExist\" />" << endl;
      return;
    }
    Row row;
    query.fetch_row(row);
    if (row[0] == "I") {
      cout << "  <error code=\"noMetadataFormats\" />" << endl;
      return;
    }
  }
  cout << "  <ListMetadataFormats>" << endl;
  cout << "    <metadataFormat>" << endl;
  cout << "      <metadataPrefix>oai_dc</metadataPrefix>" << endl;
  cout << "      <schema>http://www.openarchives.org/OAI/2.0/oai_dc.xsd"
      "</schema>" << endl;
  cout << "      <metadataNamespace>http://www.openarchives.org/OAI/2.0/oai_dc/"
      "</metadataNamespace>" << endl;
  cout << "    </metadataFormat>" << endl;
  cout << "    <metadataFormat>" << endl;
  cout << "      <metadataPrefix>dif</metadataPrefix>" << endl;
  cout << "      <schema>http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/dif_v9.7.xsd"
      "</schema>" << endl;
  cout << "      <metadataNamespace>http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif"
      "</metadataNamespace>" << endl;
  cout << "    </metadataFormat>" << endl;
  cout << "    <metadataFormat>" << endl;
  cout << "      <metadataPrefix>datacite</metadataPrefix>" << endl;
  cout << "      <schema>http://schema.datacite.org/meta/kernel-2.2/metadata."
      "xsd</schema>" << endl;
  cout << "      <metadataNamespace>http://schema.datacite.org/meta/kernel-2.2"
      "</metadataNamespace>" << endl;
  cout << "    </metadataFormat>" << endl;
  cout << "    <metadataFormat>" << endl;
  cout << "      <metadataPrefix>fgdc</metadataPrefix>" << endl;
  cout << "      <schema>http://www.fgdc.gov/schemas/metadata/"
      "fgdc-std-001-1998.xsd</schema>" << endl;
  cout << "      <metadataNamespace>http://www.fgdc.gov/schemas/metadata"
      "</metadataNamespace>" << endl;
  cout << "    </metadataFormat>" << endl;
  cout << "    <metadataFormat>" << endl;
  cout << "      <metadataPrefix>iso19115-3</metadataPrefix>" << endl;
  cout << "      <schema>http://standards.iso.org/iso/19115/-3/mds/1.0/mds.xsd"
      "</schema>" << endl;
  cout << "      <metadataNamespace>http://standards.iso.org/iso/19115/-3/mds/"
      "1.0</metadataNamespace>" << endl;
  cout << "    </metadataFormat>" << endl;
  cout << "    <metadataFormat>" << endl;
  cout << "      <metadataPrefix>iso19139</metadataPrefix>" << endl;
  cout << "      <schema>http://www.isotc211.org/2005/gmd/gmd.xsd</schema>" <<
      endl;
  cout << "      <metadataNamespace>http://www.isotc211.org/2005/gmd"
      "</metadataNamespace>" << endl;
  cout << "    </metadataFormat>" << endl;
  cout << "    <metadataFormat>" << endl;
  cout << "      <metadataPrefix>native</metadataPrefix>" << endl;
  cout << "      <schema>https://rda.ucar.edu/schemas/dsOverview.xsd</schema>"
      << endl;
  cout << "      <metadataNamespace>https://rda.ucar.edu/schemas"
      "</metadataNamespace>" << endl;
  cout << "    </metadataFormat>" << endl;
  cout << "    <metadataFormat>" << endl;
  cout << "      <metadataPrefix>thredds</metadataPrefix>" << endl;
  cout << "      <schema>http://www.unidata.ucar.edu/schemas/thredds/"
      "InvCatalog.1.0.xsd</schema>" << endl;
  cout << "      <metadataNamespace>http://www.unidata.ucar.edu/namespaces/"
      "thredds/InvCatalog/v1.0</metadataNamespace>" << endl;
  cout << "    </metadataFormat>" << endl;
  cout << "  </ListMetadataFormats>" << endl;
}

void list_sets() {
  for (auto& a : oai_args) {
    if (a != "verb") {
      bad_args.emplace_back(a);
    }
  }
  if (bad_args.size() > 0) {
    print_bad_arguments_list();
    return;
  }
  print_request_element();
  cout << "  <error code=\"noSetHierarchy\">Sets not supported</error>" << endl;
}

void build_list(LocalQuery& query, string& metadata_prefix, bool&
    found_errors) {
  Server server(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  found_errors = false;
  string from,until,set,resumption_token;
  bool other_arg = false;
  for (auto& a : oai_args) {
    if (a == "from") {
      from = query_string.value("from");
      other_arg = true;
    } else if (a == "until") {
      until = query_string.value("until");
      other_arg = true;
    } else if (a == "metadataPrefix") {
      metadata_prefix = to_lower(query_string.value("metadataPrefix"));
      other_arg = true;
    } else if (a == "set") {
      set = query_string.value("set");
      other_arg = true;
    } else if (a == "resumptionToken") {
      resumption_token = query_string.value("resumptionToken");
    } else if (a != "verb") {
      bad_args.emplace_back(a);
    }
  }

  // resumptionToken is an exclusive argument
  if (!resumption_token.empty() && other_arg) {
    bad_args.emplace_back("resumptionToken");
  }
  if (!bad_args.empty()) {
    print_bad_arguments_list();
    found_errors = true;
    return;
  }
  if (!from.empty() && from.length() != 10 && from.length() != 20) {
    cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << endl;
    cout << "  <error code=\"badArgument\">'from' is not a UTCdatetime value"
        "</error>" << endl;
    found_errors = true;
    return;
  }
  if (!until.empty() && until.length() != 10 && until.length() != 20) {
    cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << endl;
    cout << "  <error code=\"badArgument\">'until' is not a UTCdatetime value"
        "</error>" << endl;
    found_errors = true;
    return;
  }
  if (!from.empty() && !until.empty()) {
    if (from.length() != until.length()) {
      cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << endl;
      cout << "  <error code=\"badArgument\">'from' and 'until' are of "
          "different granularities</error>" << endl;
      found_errors = true;
      return;
    } else if (until < from) {
      cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << endl;
      cout << "  <error code=\"badArgument\">'until' must not precede 'from'"
          "</error>" << endl;
      found_errors=true;
      return;
    }
  }
  if (metadata_prefix.empty() && resumption_token.empty()) {
    cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << endl;
    cout << "  <error code=\"badArgument\">'metadataPrefix' is missing</error>"
        << endl;
    found_errors = true;
    return;
  }
  print_request_element();
if (!set.empty()) {
cout << "  <error code=\"noSetHierarchy\">Sets not supported</error>" << endl;
found_errors=true;
return;
}
if (!resumption_token.empty()) {
cout << "  <error code=\"badResumptionToken\" />" << endl;
found_errors=true;
return;
}
  check_formats(metadata_prefix);
  string wc = "type != 'I' and type != 'W' and type != 'D' and dsid != '999.9'";
  if (!from.empty()) {
    if (from.find("T") != string::npos) {
      replace_all(from, "T", " ");
      chop(from);
    } else
      from += " 00:00:00";
    wc += " and timestamp_utc >= '" + from + "'";
  }
  if (!until.empty()) {
    if (until.find("T") != string::npos) {
      replace_all(until, "T", " ");
      chop(until);
    } else
      until += " 23:59:59";
    wc += " and timestamp_utc <= '" + until + "'";
  }
  query.set("dsid, timestamp_utc", "search.datasets", wc);
  if (query.submit(server) < 0) {
    cout << "Content-type: text/plain" << endl << endl;
    cout << "Database error: " << query.error() << endl;
    exit(1);
  }
  if (query.num_rows() == 0) {
    cout << "  <error code=\"noRecordsMatch\" />" << endl;
    return;
  }
}

void list_identifiers() {
  LocalQuery query;
  string metadata_prefix;
  bool found_errors;
  build_list(query, metadata_prefix, found_errors);
  if (found_errors) {
    return;
  }
  cout << "  <ListIdentifiers>" << endl;
  for (const auto& row : query) {
    cout << "    <header>" << endl;
    cout << "      <identifier>oai:" << REPOSITORY_IDENTIFIER << ":ds" << row[0]
        << "</identifier>" << endl;
    auto s = row[1];
    replace_all(s, " ", "T");
    cout << "      <datestamp>" << s << "Z</datestamp>" << endl;
    cout << "    </header>" << endl;
  }
  cout << "  </ListIdentifiers>" << endl;
}

void list_records() {
  Server server(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  LocalQuery query;
  string metadata_prefix;
  bool found_errors;
  build_list(query, metadata_prefix, found_errors);
  if (found_errors) {
    return;
  }
  unique_ptr<TokenDocument> token_doc;
  if (metadata_prefix == "iso19115-3") {
    token_doc.reset(new TokenDocument("/usr/local/www/server_root/web/html/oai/"
        "iso19115-3.xml"));
  } else if (metadata_prefix == "iso19139") {
    token_doc.reset(new TokenDocument("/usr/local/www/server_root/web/html/oai/"
        "iso19139.xml"));
  }
  cout << "  <ListRecords>" << endl;
  for (const auto& row : query) {
    cout << "    <record>" << endl;
    cout << "      <header>" << endl;
    cout << "        <identifier>oai:" << REPOSITORY_IDENTIFIER << ":ds" << row[
        0] << "</identifier>" << endl;
    auto s = row[1];
    replace_all(s, " ", "T");
    cout << "        <datestamp>" << s << "Z</datestamp>" << endl;
    cout << "      </header>" << endl;
    cout << "      <metadata>" << endl;
    metadataExport::export_metadata(metadata_prefix, token_doc, cout, row[0],
        8);
    cout << "      </metadata>" << endl;
    cout << "    </record>" << endl;
    if (token_doc != nullptr) {
      token_doc->clear_all();
    }
  }
  cout << "  </ListRecords>" << endl;
}

void get_record() {

  Server server(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "rdadb");
  string identifier, metadata_prefix;
  for (auto& a : oai_args) {
    if (a == "identifier") {
      identifier = query_string.value("identifier");
    } else if (a == "metadataPrefix") {
      metadata_prefix = to_lower(query_string.value("metadataPrefix"));
    } else if (a != "verb") {
      bad_args.emplace_back(a);
    }
  }
  if (bad_args.size() > 0) {
    print_bad_arguments_list();
    return;
  }
  if (identifier.empty()) {
    cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << endl;
    cout << "  <error code=\"badArgument\">'identifier' is missing</error>" <<
        endl;
    return;
  }
  if (identifier.find("oai:" + REPOSITORY_IDENTIFIER + ":ds") != 0) {
    cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << endl;
    cout << "  <error code=\"badArgument\">'identifier' is not a valid "
        "oai-identifier</error>" << endl;
    return;
  }
  if (metadata_prefix.empty()) {
    cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << endl;
    cout << "  <error code=\"badArgument\">'metadataPrefix' is missing</error>"
        << endl;
    return;
  }
  print_request_element();
  check_formats(metadata_prefix);
  auto dsnum = substitute(identifier, "oai:" + REPOSITORY_IDENTIFIER + ":ds",
      "");
  LocalQuery query("type, timestamp_utc", "search.datasets", "dsid = '" + dsnum
      + "'");
  if (query.submit(server) < 0) {
    cout << "Content-type: text/plain" << endl << endl;
    cout << "Database error: " << query.error() << endl;
    exit(1);
  }
  if (query.num_rows() == 0) {
    cout << "  <error code=\"idDoesNotExist\" />" << endl;
    return;
  }
  Row row;
  if (!query.fetch_row(row)) {
    cout << "Content-type: text/plain" << endl << endl;
    cout << "Database error: " << query.error() << endl;
    exit(1);
  }
  if (row[0] != "P" && row[0] != "H") {
    cout << "  <error code=\"idDoesNotExist\" />" << endl;
    return;
  }
  cout << "  <GetRecord>" << endl;
  cout << "    <record>" << endl;
  cout << "      <header>" << endl;
  cout << "        <identifier>oai:" << REPOSITORY_IDENTIFIER << ":ds" << dsnum
      << "</identifier>" << endl;
  auto s = row[1];
  replace_all(s, " ", "T");
  cout << "        <datestamp>" << s << "Z</datestamp>" << endl;
  cout << "      </header>" << endl;
  cout << "      <metadata>" << endl;
  std::unique_ptr<TokenDocument> token_doc;
  metadataExport::export_metadata(metadata_prefix, token_doc, cout, dsnum, 8);
  cout << "      </metadata>" << endl;
  cout << "    </record>" << endl;
  cout << "  </GetRecord>" << endl;
}

int main(int argc, char **argv) {
  metautils::read_config("oai", "", false);
  cout << "Content-type: text/xml" << endl << endl;
  cout << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" << endl;
  cout << "<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"" << endl;
  cout << "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" <<
      endl;
  cout << "         xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/"
      << endl;
  cout << "                             http://www.openarchives.org/OAI/2.0/"
      "OAI-PMH.xsd\">" << endl;
  auto dt = dateutils::current_date_time();
  if (dt.utc_offset() == -700) {
    dt.add_hours(7);
  } else {
    dt.add_hours(6);
  }
  cout << "  <responseDate>" << dt.to_string("%Y-%m-%dT%H:%MM:%SSZ") <<
      "</responseDate>" << endl;
  parse_query();
  if (verb == "Identify") {
    identify();
  } else if (verb == "ListMetadataFormats") {
    list_metadata_formats();
  } else if (verb == "ListSets") {
    list_sets();
  } else if (verb == "ListIdentifiers") {
    list_identifiers();
  } else if (verb == "ListRecords") {
    list_records();
  } else if (verb == "GetRecord") {
    get_record();
  } else {
    cout << "  <error code=\"badVerb\" />" << endl;
  }
  cout << "</OAI-PMH>" << endl;
}
