#include <iostream>
#include <string>
#include <list>
#include <web/web.hpp>
#include <datetime.hpp>
#include <MySQL.hpp>
#include <xml.hpp>
#include <metadata.hpp>
#include <strutils.hpp>

metautils::Directives directives;
metautils::Args args;

QueryString queryString;
std::string verb;
std::list<std::string> oai_args,bad_args;
const size_t NUM_FORMATS=8;
const std::string metadata_formats[NUM_FORMATS]={"oai_dc","datacite","dif","fgdc","iso19139","iso19115-3","native","thredds"};
const std::string repositoryIdentifier("edu.ucar.rda");

void parseQuery()
{
  QueryString queryString_get(QueryString::GET);
  QueryString queryString_post(QueryString::POST);

  if (queryString_get) {
    queryString=queryString_get;
  }
  else if (queryString_post) {
    queryString=queryString_post;
  }
  else {
    std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
    std::cout << "  <error code=\"badVerb\">'verb' is missing</error>" << std::endl;
    std::cout << "</OAI-PMH>" << std::endl;
    exit(1);
  }
  verb=queryString.getValue("verb");
  oai_args=queryString.getRawNames();
}

void checkFormats(std::string metadataPrefix)
{
  size_t n;

  for (n=0; n < NUM_FORMATS; ++n) {
    if (metadataPrefix == metadata_formats[n]) {
	break;
    }
  }
  if (n == NUM_FORMATS) {
    std::cout << "  <error code=\"cannotDisseminateFormat\" />" << std::endl;
    std::cout << "</OAI-PMH>" << std::endl;
    exit(1);
  }
}

void printBadArgumentsList()
{
  std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
  for (auto& a : bad_args) {
    std::cout << "  <error code=\"badArgument\">'" << a << "' is not allowed here</error>" << std::endl;
  }
}

void printRequestElement()
{
  std::cout << "  <request";
  for (auto& a : oai_args) {
    std::cout << " " << a << "=\"" << queryString.getValue(a) << "\"";
  }
  std::cout << ">https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
}

void Identify()
{
  bool foundError=false;

  std::cout << "  <request verb=\"Identify\">https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
  for (auto& a : oai_args) {
    if (a != "verb") {
	std::cout << "  <error code=\"badArgument\">'" << a << "' is not allowed here</error>" << std::endl;
	foundError=true;
    }
  }
  if (foundError) {
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
  std::cout << "        <repositoryIdentifier>" << repositoryIdentifier << "</repositoryIdentifier>" << std::endl;
  std::cout << "        <delimiter>:</delimiter>" << std::endl;
  std::cout << "        <sampleIdentifier>oai:" << repositoryIdentifier << ":ds010.0</sampleIdentifier>" << std::endl;
  std::cout << "      </oai-identifier>" << std::endl;
  std::cout << "    </description>" << std::endl;
  std::cout << "  </Identify>" << std::endl;
}

void ListMetadataFormats()
{
  MySQL::Server server;
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::string dsid;

  metautils::connectToMetadataServer(server);
  for (auto& a : oai_args) {
    if (a == "identifier") {
	dsid=queryString.getValue("identifier");
    }
    else if (a != "verb") {
	bad_args.emplace_back(a);
    }
  }
  if (bad_args.size() > 0) {
    printBadArgumentsList();
    return;
  }
  printRequestElement();
  if (dsid.length() > 0) {
    strutils::replace_all(dsid,"oai:"+repositoryIdentifier+":ds","");
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

void ListSets()
{
  for (auto& a : oai_args) {
    if (a != "verb") {
	bad_args.emplace_back(a);
    }
  }
  if (bad_args.size() > 0) {
    printBadArgumentsList();
    return;
  }
  printRequestElement();
  std::cout << "  <error code=\"noSetHierarchy\">Sets not supported</error>" << std::endl;
}

void buildList(MySQL::LocalQuery& query,std::string& metadataPrefix,bool& foundErrors)
{
  std::string wc;
  std::string from,until,set,resumptionToken;
  MySQL::Server server;
  bool otherArg=false;

  metautils::connectToMetadataServer(server);
  foundErrors=false;
  for (auto& a : oai_args) {
    if (a == "from") {
	from=queryString.getValue("from");
	otherArg=true;
    }
    else if (a == "until") {
	until=queryString.getValue("until");
	otherArg=true;
    }
    else if (a == "metadataPrefix") {
	metadataPrefix=strutils::to_lower(queryString.getValue("metadataPrefix"));
	otherArg=true;
    }
    else if (a == "set") {
	set=queryString.getValue("set");
	otherArg=true;
    }
    else if (a == "resumptionToken") {
	resumptionToken=queryString.getValue("resumptionToken");
    }
    else if (a != "verb") {
	bad_args.emplace_back(a);
    }
  }
// resumptionToken is an exclusive argument
  if (resumptionToken.length() > 0 && otherArg) {
    bad_args.emplace_back("resumptionToken");
  }
  if (bad_args.size() > 0) {
    printBadArgumentsList();
    foundErrors=true;
    return;
  }
  if (from.length() > 0 && from.length() != 10 && from.length() != 20) {
    std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
    std::cout << "  <error code=\"badArgument\">'from' is not a UTCdatetime value</error>" << std::endl;
    foundErrors=true;
    return;
  }
  if (until.length() > 0 && until.length() != 10 && until.length() != 20) {
    std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
    std::cout << "  <error code=\"badArgument\">'until' is not a UTCdatetime value</error>" << std::endl;
    foundErrors=true;
    return;
  }
  if (from.length() > 0 && until.length() > 0) {
    if (from.length() != until.length()) {
	std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
	std::cout << "  <error code=\"badArgument\">'from' and 'until' are of different granularities</error>" << std::endl;
	foundErrors=true;
	return;
    }
    else if (until < from) {
	std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
	std::cout << "  <error code=\"badArgument\">'until' must not precede 'from'</error>" << std::endl;
	foundErrors=true;
	return;
    }
  }
  if (metadataPrefix.length() == 0 && resumptionToken.length() == 0) {
    std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
    std::cout << "  <error code=\"badArgument\">'metadataPrefix' is missing</error>" << std::endl;
    foundErrors=true;
    return;
  }
  printRequestElement();
if (set.length() > 0) {
std::cout << "  <error code=\"noSetHierarchy\">Sets not supported</error>" << std::endl;
foundErrors=true;
return;
}
if (resumptionToken.length() > 0) {
std::cout << "  <error code=\"badResumptionToken\" />" << std::endl;
foundErrors=true;
return;
}
  checkFormats(metadataPrefix);
  wc="type != 'I' and type != 'W' and type != 'D' and dsid != '999.9'";
  if (from.length() > 0) {
    if (strutils::contains(from,"T")) {
	strutils::replace_all(from,"T"," ");
	strutils::chop(from);
    }
    else
	from+=" 00:00:00";
    wc+=" and timestamp_Z >= '"+from+"'";
  }
  if (until.length() > 0) {
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

void ListIdentifiers()
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::string metadataPrefix,sdum;
  bool foundErrors;

  buildList(query,metadataPrefix,foundErrors);
  if (foundErrors)
    return;
  std::cout << "  <ListIdentifiers>" << std::endl;
  while (query.fetch_row(row)) {
    std::cout << "    <header>" << std::endl;
    std::cout << "      <identifier>oai:" << repositoryIdentifier << ":ds" << row[0] << "</identifier>" << std::endl;
    sdum=row[1];
    strutils::replace_all(sdum," ","T");
    std::cout << "      <datestamp>" << sdum << "Z</datestamp>" << std::endl;
    std::cout << "    </header>" << std::endl;
  }
  std::cout << "  </ListIdentifiers>" << std::endl;
}

void ListRecords()
{
  MySQL::Server server;
  metautils::connectToMetadataServer(server);
  MySQL::LocalQuery query;
  std::string metadataPrefix;
  bool foundErrors;
  buildList(query,metadataPrefix,foundErrors);
  if (foundErrors) {
    return;
  }
  std::unique_ptr<TokenDocument> token_doc;
  if (metadataPrefix == "iso19115-3") {
    token_doc.reset(new TokenDocument("/usr/local/www/server_root/web/html/oai/iso19115-3.xml"));
  }
  std::cout << "  <ListRecords>" << std::endl;
  MySQL::Row row;
  while (query.fetch_row(row)) {
    std::cout << "    <record>" << std::endl;
    std::cout << "      <header>" << std::endl;
    std::cout << "        <identifier>oai:" << repositoryIdentifier << ":ds" << row[0] << "</identifier>" << std::endl;
    auto s=row[1];
    strutils::replace_all(s," ","T");
    std::cout << "        <datestamp>" << s << "Z</datestamp>" << std::endl;
    std::cout << "      </header>" << std::endl;
    std::cout << "      <metadata>" << std::endl;
    metadataExport::exportMetadata(metadataPrefix,token_doc,std::cout,row[0],8);
    std::cout << "      </metadata>" << std::endl;
    std::cout << "    </record>" << std::endl;
    if (token_doc != nullptr) {
	token_doc->clear_all();
    }
  }
  std::cout << "  </ListRecords>" << std::endl;
}

void GetRecord()
{
  std::string dsnum,identifier,metadataPrefix,sdum;
  MySQL::Server server;
  MySQL::LocalQuery query;
  MySQL::Row row;

  metautils::connectToMetadataServer(server);
  for (auto& a : oai_args) {
    if (a == "identifier") {
	identifier=queryString.getValue("identifier");
    }
    else if (a == "metadataPrefix") {
	metadataPrefix=strutils::to_lower(queryString.getValue("metadataPrefix"));
    }
    else if (a != "verb") {
	bad_args.emplace_back(a);
    }
  }
  if (bad_args.size() > 0) {
    printBadArgumentsList();
    return;
  }
  if (identifier.length() == 0) {
    std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
    std::cout << "  <error code=\"badArgument\">'identifier' is missing</error>" << std::endl;
    return;
  }
  if (!strutils::has_beginning(identifier,"oai:"+repositoryIdentifier+":ds")) {
    std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
    std::cout << "  <error code=\"badArgument\">'identifier' is not a valid oai-identifier</error>" << std::endl;
    return;
  }
  if (metadataPrefix.length() == 0) {
    std::cout << "  <request>https://rda.ucar.edu/cgi-bin/oai</request>" << std::endl;
    std::cout << "  <error code=\"badArgument\">'metadataPrefix' is missing</error>" << std::endl;
    return;
  }
  printRequestElement();
  checkFormats(metadataPrefix);
  dsnum=strutils::substitute(identifier,"oai:"+repositoryIdentifier+":ds","");
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
  std::cout << "        <identifier>oai:" << repositoryIdentifier << ":ds" << dsnum << "</identifier>" << std::endl;
  sdum=row[1];
  strutils::replace_all(sdum," ","T");
  std::cout << "        <datestamp>" << sdum << "Z</datestamp>" << std::endl;
  std::cout << "      </header>" << std::endl;
  std::cout << "      <metadata>" << std::endl;
  std::unique_ptr<TokenDocument> token_doc;
  metadataExport::exportMetadata(metadataPrefix,token_doc,std::cout,dsnum,8);
  std::cout << "      </metadata>" << std::endl;
  std::cout << "    </record>" << std::endl;
  std::cout << "  </GetRecord>" << std::endl;
}

int main(int argc,char **argv)
{
  DateTime dt;

  metautils::readConfig("oai","","");
  std::cout << "Content-type: text/xml" << std::endl << std::endl;
  std::cout << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" << std::endl;
  std::cout << "<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"" << std::endl;
  std::cout << "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" << std::endl;
  std::cout << "         xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/" << std::endl;
  std::cout << "                             http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd\">" << std::endl;
  dt=getCurrentDateTime();
  if (dt.getUTCOffset() == -700) {
    dt.addHours(7);
  }
  else {
    dt.addHours(6);
  }
  std::cout << "  <responseDate>" << dt.toString("%Y-%m-%dT%H:%MM:%SSZ") << "</responseDate>" << std::endl;
  parseQuery();
  if (verb == "Identify") {
    Identify();
  }
  else if (verb == "ListMetadataFormats") {
    ListMetadataFormats();
  }
  else if (verb == "ListSets") {
    ListSets();
  }
  else if (verb == "ListIdentifiers") {
    ListIdentifiers();
  }
  else if (verb == "ListRecords") {
    ListRecords();
  }
  else if (verb == "GetRecord") {
    GetRecord();
  }
  else {
    std::cout << "  <error code=\"badVerb\" />" << std::endl;
  }
  std::cout << "</OAI-PMH>" << std::endl;
}
