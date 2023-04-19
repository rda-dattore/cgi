#include <iostream>
#include <string>
#include <list>
#include <web/web.hpp>
#include <datetime.hpp>
#include <MySQL.hpp>
#include <xml.hpp>
#include <metadata.hpp>
#include <metadata_export.hpp>
#include <strutils.hpp>
#include <tokendoc.hpp>
#include <myerror.hpp>

metautils::Directives metautils::directives;
metautils::Args metautils::args;
std::string myerror="";
std::string mywarning="";

QueryString query_string;
std::string verb;
std::list<std::string> oai_args,bad_args;
const size_t NUM_FORMATS=8;
const std::string metadata_formats[NUM_FORMATS]={"oai_dc","datacite","dif","fgdc","iso19139","iso19115-3","native","thredds"};
const std::string REPOSITORY_IDENTIFIER="edu.ucar.rda";

void parse_query()
{
  QueryString query_string_get(QueryString::GET);
  QueryString query_string_post(QueryString::POST);

  if (query_string_get) {
    query_string=query_string_get;
  }
  else if (query_string_post) {
    query_string=query_string_post;
  }
  else {
    std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
    std::cout << "  <error code=\"badVerb\">'verb' is missing</error>" << std::endl;
    std::cout << "</OAI-PMH>" << std::endl;
    exit(1);
  }
  verb=query_string.value("verb");
  oai_args=query_string.raw_names();
}

void check_formats(std::string metadata_prefix)
{
  size_t n;

  for (n=0; n < NUM_FORMATS; ++n) {
    if (metadata_prefix == metadata_formats[n]) {
	break;
    }
  }
  if (n == NUM_FORMATS) {
    std::cout << "  <error code=\"cannotDisseminateFormat\" />" << std::endl;
    std::cout << "</OAI-PMH>" << std::endl;
    exit(1);
  }
}

void print_bad_arguments_list()
{
  std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
  for (auto& a : bad_args) {
    std::cout << "  <error code=\"badArgument\">'" << a << "' is not allowed here</error>" << std::endl;
  }
}

void print_request_element()
{
  std::cout << "  <request";
  for (auto& a : oai_args) {
    std::cout << " " << a << "=\"" << query_string.value(a) << "\"";
  }
  std::cout << ">https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
}

void identify()
{
  auto found_error=false;
  std::cout << "  <request verb=\"Identify\">https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
  for (const auto& a : oai_args) {
    if (a != "verb") {
	std::cout << "  <error code=\"badArgument\">'" << a << "' is not allowed here</error>" << std::endl;
	found_error=true;
    }
  }
  if (found_error) {
    return;
  }
  std::cout << "  <Identify>" << std::endl;
  std::cout << "    <repositoryName>CISL Research Data Archive</repositoryName>" << std::endl;
  std::cout << "    <baseURL>https://rda.ucar.edu/cgi-bin/oai</baseURL>" << std::endl;
  std::cout << "    <protocolVersion>2.0</protocolVersion>" << std::endl;
  std::cout << "    <adminEmail>rdahelp@ucar.edu</adminEmail>" << std::endl;
  std::cout << "    <earliestDatestamp>2006-10-15T00:00:00Z</earliestDatestamp>" << std::endl;
  std::cout << "    <deletedRecord>no</deletedRecord>" << std::endl;
  std::cout << "    <granularity>YYYY-MM-DDThh:mm:ssZ</granularity>" << std::endl;
  std::cout << "    <description>" << std::endl;
  std::cout << "      <oai-identifier xmlns=\"http://www.openarchives.org/OAI/2.0/oai-identifier\"" << std::endl;
  std::cout << "                      xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" << std::endl;
  std::cout << "                      xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai-identifier" << std::endl;
  std::cout << "                                          http://www.openarchives.org/OAI/2.0/oai-identifier.xsd\">" << std::endl;
  std::cout << "        <scheme>oai</scheme>" << std::endl;
  std::cout << "        <repositoryIdentifier>" << REPOSITORY_IDENTIFIER << "</repositoryIdentifier>" << std::endl;
  std::cout << "        <delimiter>:</delimiter>" << std::endl;
  std::cout << "        <sampleIdentifier>oai:" << REPOSITORY_IDENTIFIER << ":ds010.0</sampleIdentifier>" << std::endl;
  std::cout << "      </oai-identifier>" << std::endl;
  std::cout << "    </description>" << std::endl;
  std::cout << "  </Identify>" << std::endl;
}

void list_metadata_formats()
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::string dsid;

  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  for (auto& a : oai_args) {
    if (a == "identifier") {
	dsid=query_string.value("identifier");
    }
    else if (a != "verb") {
	bad_args.emplace_back(a);
    }
  }
  if (bad_args.size() > 0) {
    print_bad_arguments_list();
    return;
  }
  print_request_element();
  if (!dsid.empty()) {
    strutils::replace_all(dsid,"oai:"+REPOSITORY_IDENTIFIER+":ds","");
    query.set("type","search.datasets","dsid = '"+dsid+"'");
    if (query.submit(server) < 0) {
	std::cout << "Content-type: text/plain" << std::endl << std::endl;
	std::cout << "Database error: " << query.error() << std::endl;
	exit(1);
    }
    if (query.num_rows() == 0) {
	std::cout << "  <error code=\"idDoesNotExist\" />" << std::endl;
	return;
    }
    query.fetch_row(row);
    if (row[0] == "I") {
	std::cout << "  <error code=\"noMetadataFormats\" />" << std::endl;
	return;
    }
  }
  std::cout << "  <ListMetadataFormats>" << std::endl;
  std::cout << "    <metadataFormat>" << std::endl;
  std::cout << "      <metadataPrefix>oai_dc</metadataPrefix>" << std::endl;
  std::cout << "      <schema>http://www.openarchives.org/OAI/2.0/oai_dc.xsd</schema>" << std::endl;
  std::cout << "      <metadataNamespace>http://www.openarchives.org/OAI/2.0/oai_dc/</metadataNamespace>" << std::endl;
  std::cout << "    </metadataFormat>" << std::endl;
  std::cout << "    <metadataFormat>" << std::endl;
  std::cout << "      <metadataPrefix>dif</metadataPrefix>" << std::endl;
  std::cout << "      <schema>http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/dif_v9.7.xsd</schema>" << std::endl;
  std::cout << "      <metadataNamespace>http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif</metadataNamespace>" << std::endl;
  std::cout << "    </metadataFormat>" << std::endl;
  std::cout << "    <metadataFormat>" << std::endl;
  std::cout << "      <metadataPrefix>datacite</metadataPrefix>" << std::endl;
  std::cout << "      <schema>http://schema.datacite.org/meta/kernel-2.2/metadata.xsd</schema>" << std::endl;
  std::cout << "      <metadataNamespace>http://schema.datacite.org/meta/kernel-2.2</metadataNamespace>" << std::endl;
  std::cout << "    </metadataFormat>" << std::endl;
  std::cout << "    <metadataFormat>" << std::endl;
  std::cout << "      <metadataPrefix>fgdc</metadataPrefix>" << std::endl;
  std::cout << "      <schema>http://www.fgdc.gov/schemas/metadata/fgdc-std-001-1998.xsd</schema>" << std::endl;
  std::cout << "      <metadataNamespace>http://www.fgdc.gov/schemas/metadata</metadataNamespace>" << std::endl;
  std::cout << "    </metadataFormat>" << std::endl;
  std::cout << "    <metadataFormat>" << std::endl;
  std::cout << "      <metadataPrefix>iso19115-3</metadataPrefix>" << std::endl;
  std::cout << "      <schema>http://standards.iso.org/iso/19115/-3/mds/1.0/mds.xsd</schema>" << std::endl;
  std::cout << "      <metadataNamespace>http://standards.iso.org/iso/19115/-3/mds/1.0</metadataNamespace>" << std::endl;
  std::cout << "    </metadataFormat>" << std::endl;
  std::cout << "    <metadataFormat>" << std::endl;
  std::cout << "      <metadataPrefix>iso19139</metadataPrefix>" << std::endl;
  std::cout << "      <schema>http://www.isotc211.org/2005/gmd/gmd.xsd</schema>" << std::endl;
  std::cout << "      <metadataNamespace>http://www.isotc211.org/2005/gmd</metadataNamespace>" << std::endl;
  std::cout << "    </metadataFormat>" << std::endl;
  std::cout << "    <metadataFormat>" << std::endl;
  std::cout << "      <metadataPrefix>native</metadataPrefix>" << std::endl;
  std::cout << "      <schema>https://rda.ucar.edu/schemas/dsOverview.xsd</schema>" << std::endl;
  std::cout << "      <metadataNamespace>https://rda.ucar.edu/schemas</metadataNamespace>" << std::endl;
  std::cout << "    </metadataFormat>" << std::endl;
  std::cout << "    <metadataFormat>" << std::endl;
  std::cout << "      <metadataPrefix>thredds</metadataPrefix>" << std::endl;
  std::cout << "      <schema>http://www.unidata.ucar.edu/schemas/thredds/InvCatalog.1.0.xsd</schema>" << std::endl;
  std::cout << "      <metadataNamespace>http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0</metadataNamespace>" << std::endl;
  std::cout << "    </metadataFormat>" << std::endl;
  std::cout << "  </ListMetadataFormats>" << std::endl;
}

void list_sets()
{
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
  std::cout << "  <error code=\"noSetHierarchy\">Sets not supported</error>" << std::endl;
}

void build_list(MySQL::LocalQuery& query,std::string& metadata_prefix,bool& found_errors)
{
  std::string wc;
  std::string from,until,set,resumption_token;
  bool other_arg=false;

  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  found_errors=false;
  for (auto& a : oai_args) {
    if (a == "from") {
	from=query_string.value("from");
	other_arg=true;
    }
    else if (a == "until") {
	until=query_string.value("until");
	other_arg=true;
    }
    else if (a == "metadataPrefix") {
	metadata_prefix=strutils::to_lower(query_string.value("metadataPrefix"));
	other_arg=true;
    }
    else if (a == "set") {
	set=query_string.value("set");
	other_arg=true;
    }
    else if (a == "resumptionToken") {
	resumption_token=query_string.value("resumptionToken");
    }
    else if (a != "verb") {
	bad_args.emplace_back(a);
    }
  }
// resumptionToken is an exclusive argument
  if (!resumption_token.empty() && other_arg) {
    bad_args.emplace_back("resumptionToken");
  }
  if (bad_args.size() > 0) {
    print_bad_arguments_list();
    found_errors=true;
    return;
  }
  if (!from.empty() && from.length() != 10 && from.length() != 20) {
    std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
    std::cout << "  <error code=\"badArgument\">'from' is not a UTCdatetime value</error>" << std::endl;
    found_errors=true;
    return;
  }
  if (!until.empty() && until.length() != 10 && until.length() != 20) {
    std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
    std::cout << "  <error code=\"badArgument\">'until' is not a UTCdatetime value</error>" << std::endl;
    found_errors=true;
    return;
  }
  if (!from.empty() && !until.empty()) {
    if (from.length() != until.length()) {
	std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
	std::cout << "  <error code=\"badArgument\">'from' and 'until' are of different granularities</error>" << std::endl;
	found_errors=true;
	return;
    }
    else if (until < from) {
	std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
	std::cout << "  <error code=\"badArgument\">'until' must not precede 'from'</error>" << std::endl;
	found_errors=true;
	return;
    }
  }
  if (metadata_prefix.empty() && resumption_token.empty()) {
    std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
    std::cout << "  <error code=\"badArgument\">'metadataPrefix' is missing</error>" << std::endl;
    found_errors=true;
    return;
  }
  print_request_element();
if (!set.empty()) {
std::cout << "  <error code=\"noSetHierarchy\">Sets not supported</error>" << std::endl;
found_errors=true;
return;
}
if (!resumption_token.empty()) {
std::cout << "  <error code=\"badResumptionToken\" />" << std::endl;
found_errors=true;
return;
}
  check_formats(metadata_prefix);
  wc="type != 'I' and type != 'W' and type != 'D' and dsid != '999.9'";
  if (!from.empty()) {
    if (strutils::contains(from,"T")) {
	strutils::replace_all(from,"T"," ");
	strutils::chop(from);
    }
    else
	from+=" 00:00:00";
    wc+=" and timestamp_Z >= '"+from+"'";
  }
  if (!until.empty()) {
    if (strutils::contains(until,"T")) {
	strutils::replace_all(until,"T"," ");
	strutils::chop(until);
    }
    else
	until+=" 23:59:59";
    wc+=" and timestamp_Z <= '"+until+"'";
  }
  query.set("dsid,timestamp_Z","search.datasets",wc);
  if (query.submit(server) < 0) {
    std::cout << "Content-type: text/plain" << std::endl << std::endl;
    std::cout << "Database error: " << query.error() << std::endl;
    exit(1);
  }
  if (query.num_rows() == 0) {
    std::cout << "  <error code=\"noRecordsMatch\" />" << std::endl;
    return;
  }
}

void list_identifiers()
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::string metadata_prefix,sdum;
  bool found_errors;

  build_list(query,metadata_prefix,found_errors);
  if (found_errors) {
    return;
  }
  std::cout << "  <ListIdentifiers>" << std::endl;
  while (query.fetch_row(row)) {
    std::cout << "    <header>" << std::endl;
    std::cout << "      <identifier>oai:" << REPOSITORY_IDENTIFIER << ":ds" << row[0] << "</identifier>" << std::endl;
    sdum=row[1];
    strutils::replace_all(sdum," ","T");
    std::cout << "      <datestamp>" << sdum << "Z</datestamp>" << std::endl;
    std::cout << "    </header>" << std::endl;
  }
  std::cout << "  </ListIdentifiers>" << std::endl;
}

void list_records()
{
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  MySQL::LocalQuery query;
  std::string metadata_prefix;
  bool found_errors;
  build_list(query,metadata_prefix,found_errors);
  if (found_errors) {
    return;
  }
  std::unique_ptr<TokenDocument> token_doc;
  if (metadata_prefix == "iso19115-3") {
    token_doc.reset(new TokenDocument("/usr/local/www/server_root/web/html/oai/iso19115-3.xml"));
  }
  else if (metadata_prefix == "iso19139") {
    token_doc.reset(new TokenDocument("/usr/local/www/server_root/web/html/oai/iso19139.xml"));
  }
  std::cout << "  <ListRecords>" << std::endl;
  MySQL::Row row;
  while (query.fetch_row(row)) {
    std::cout << "    <record>" << std::endl;
    std::cout << "      <header>" << std::endl;
    std::cout << "        <identifier>oai:" << REPOSITORY_IDENTIFIER << ":ds" << row[0] << "</identifier>" << std::endl;
    auto s=row[1];
    strutils::replace_all(s," ","T");
    std::cout << "        <datestamp>" << s << "Z</datestamp>" << std::endl;
    std::cout << "      </header>" << std::endl;
    std::cout << "      <metadata>" << std::endl;
    metadataExport::export_metadata(metadata_prefix,token_doc,std::cout,row[0],8);
    std::cout << "      </metadata>" << std::endl;
    std::cout << "    </record>" << std::endl;
    if (token_doc != nullptr) {
	token_doc->clear_all();
    }
  }
  std::cout << "  </ListRecords>" << std::endl;
}

void get_record()
{
  std::string dsnum,identifier,metadata_prefix,sdum;
  MySQL::LocalQuery query;
  MySQL::Row row;

  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  for (auto& a : oai_args) {
    if (a == "identifier") {
	identifier=query_string.value("identifier");
    }
    else if (a == "metadataPrefix") {
	metadata_prefix=strutils::to_lower(query_string.value("metadataPrefix"));
    }
    else if (a != "verb") {
	bad_args.emplace_back(a);
    }
  }
  if (bad_args.size() > 0) {
    print_bad_arguments_list();
    return;
  }
  if (identifier.empty()) {
    std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
    std::cout << "  <error code=\"badArgument\">'identifier' is missing</error>" << std::endl;
    return;
  }
  if (!strutils::has_beginning(identifier,"oai:"+REPOSITORY_IDENTIFIER+":ds")) {
    std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
    std::cout << "  <error code=\"badArgument\">'identifier' is not a valid oai-identifier</error>" << std::endl;
    return;
  }
  if (metadata_prefix.empty()) {
    std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
    std::cout << "  <error code=\"badArgument\">'metadataPrefix' is missing</error>" << std::endl;
    return;
  }
  print_request_element();
  check_formats(metadata_prefix);
  dsnum=strutils::substitute(identifier,"oai:"+REPOSITORY_IDENTIFIER+":ds","");
  query.set("type,timestamp_Z","search.datasets","dsid = '"+dsnum+"'");
  if (query.submit(server) < 0) {
    std::cout << "Content-type: text/plain" << std::endl << std::endl;
    std::cout << "Database error: " << query.error() << std::endl;
    exit(1);
  }
  if (query.num_rows() == 0) {
    std::cout << "  <error code=\"idDoesNotExist\" />" << std::endl;
    return;
  }
  if (!query.fetch_row(row)) {
    std::cout << "Content-type: text/plain" << std::endl << std::endl;
    std::cout << "Database error: " << query.error() << std::endl;
    exit(1);
  }
  if (row[0] != "P" && row[0] != "H") {
    std::cout << "  <error code=\"idDoesNotExist\" />" << std::endl;
    return;
  }
  std::cout << "  <GetRecord>" << std::endl;
  std::cout << "    <record>" << std::endl;
  std::cout << "      <header>" << std::endl;
  std::cout << "        <identifier>oai:" << REPOSITORY_IDENTIFIER << ":ds" << dsnum << "</identifier>" << std::endl;
  sdum=row[1];
  strutils::replace_all(sdum," ","T");
  std::cout << "        <datestamp>" << sdum << "Z</datestamp>" << std::endl;
  std::cout << "      </header>" << std::endl;
  std::cout << "      <metadata>" << std::endl;
  std::unique_ptr<TokenDocument> token_doc;
  metadataExport::export_metadata(metadata_prefix,token_doc,std::cout,dsnum,8);
  std::cout << "      </metadata>" << std::endl;
  std::cout << "    </record>" << std::endl;
  std::cout << "  </GetRecord>" << std::endl;
}

int main(int argc,char **argv)
{
  metautils::read_config("oai","",false);
  std::cout << "Content-type: text/xml" << std::endl << std::endl;
  std::cout << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" << std::endl;
  std::cout << "<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"" << std::endl;
  std::cout << "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" << std::endl;
  std::cout << "         xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/" << std::endl;
  std::cout << "                             http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd\">" << std::endl;
  auto dt=dateutils::current_date_time();
  if (dt.utc_offset() == -700) {
    dt.add_hours(7);
  }
  else {
    dt.add_hours(6);
  }
  std::cout << "  <responseDate>" << dt.to_string("%Y-%m-%dT%H:%MM:%SSZ") << "</responseDate>" << std::endl;
  parse_query();
  if (verb == "Identify") {
    identify();
  }
  else if (verb == "ListMetadataFormats") {
    list_metadata_formats();
  }
  else if (verb == "ListSets") {
    list_sets();
  }
  else if (verb == "ListIdentifiers") {
    list_identifiers();
  }
  else if (verb == "ListRecords") {
    list_records();
  }
  else if (verb == "GetRecord") {
    get_record();
  }
  else {
    std::cout << "  <error code=\"badVerb\" />" << std::endl;
  }
  std::cout << "</OAI-PMH>" << std::endl;
}
