#include <iostream>
#include <regex>
#include <sstream>
#include <unordered_map>
#include <MySQL.hpp>
#include <strutils.hpp>
#include <xmlutils.hpp>
#include <datetime.hpp>
#include <web/web.hpp>
#include <hereDoc.hpp>
#include <myerror.hpp>

/* mappings of queryables to RDA metadata database
**
** dc:title -> search.datasets.title
** dc:type -> always 'Dataset'
** dc:identifier -> dssdb.dsvrsn.doi,search.datasets.dsid
** dct:modified -> dssdb.dataset.date_change
** dct:abstract -> search.datasets.summary
** dc:subject -> search.variables.keyword@vocabulary=GCMD
** dc:format -> search.formats.keyword
**
** still need to map:
** dc:contributor
** dc:source
** dc:creator
** ows:BoundingBox
** dc:relation
** dc:date
** dc:publisher
** dc:rights
**
*/

std::string myerror="";
std::string mywarning="";

struct Constraint {
  Constraint() : predicate(),format_index(-1),subject_index(-1),contains_format(false),contains_subject(false) {}

  std::string predicate;
  int format_index,subject_index;
  bool contains_format,contains_subject;
} constraint;
std::string request;
QueryString query_string;

void print_exception_report(std::string exception_code,std::string locator,std::string exception_text = "")
{
  std::cout << "Content-type: application/xml" << std::endl << std::endl;
  std::cout << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" << std::endl;
  std::cout << "<ExceptionReport xmlns=\"http://www.opengis.net/ows/2.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.opengis.net/ows/2.0 http://schemas.opengis.net/ows/1.0.0/owsExceptionReport.xsd\" version=\"2.0.2\">" << std::endl;
  std::cout << "  <Exception exceptionCode=\"" << exception_code << "\"";
  if (!locator.empty()) {
    std::cout << " locator=\"" << locator << "\"";
  }
  std::cout << " />" << std::endl;
  if (!exception_text.empty()) {
    std::cout << "  <ExceptionText>" << exception_text << "</ExceptionText>" << std::endl;
  }
  std::cout << "</ExceptionReport>" << std::endl;
  exit(1);
}

std::string post_xml_to_query(std::string xml)
{
  std::unordered_map<std::string,std::vector<std::string>> kvp_list;
  size_t idx=0;
  if (std::regex_search(xml,std::regex("^<\\?xml "))) {
    idx=xml.find("?>");
  }
  XMLSnippet xmls(xml.substr(idx));
  if (!xmls) {
    print_exception_report("NoApplicableCode","","Malformed XML request");
  }
  auto root_element=xmls.root_element();
  request=strutils::to_lower(root_element.name());
  if ( (idx=request.find(":")) != std::string::npos) {
    request=request.substr(idx+1);
  }
  kvp_list.emplace("REQUEST",std::vector<std::string>{request});
  kvp_list.emplace("service",std::vector<std::string>{strutils::to_lower(root_element.attribute_value("service"))});
  if (request == "getcapabilities") {
    auto elist=xmls.element_list(root_element.name()+"/ows:AcceptVersions/ows:Version");
    if (elist.size() > 0) {
	kvp_list.emplace("version",std::vector<std::string>());
	for (const auto& e : elist) {
	  kvp_list["version"].emplace_back(e.content());
	}
    }
    elist=xmls.element_list(root_element.name()+"/ows:AcceptFormats/ows:OutputFormat");
    if (elist.size() > 0) {
	kvp_list.emplace("outputFormat",std::vector<std::string>());
	for (const auto& e : elist) {
	  kvp_list["outputFormat"].emplace_back(e.content());
	}
    }
  }
  else if (request == "getrecords") {
    kvp_list.emplace("version",std::vector<std::string>{root_element.attribute_value("version")});
    kvp_list.emplace("resultType",std::vector<std::string>{root_element.attribute_value("resultType")});
    auto e=root_element.element("Query");
    if (!e.name().empty()) {
	auto type_names=strutils::split(e.attribute_value("typeNames"));
	kvp_list.emplace("typeNames",std::vector<std::string>());
	for (const auto& type_name : type_names) {
	  if (!std::regex_search(type_name,std::regex(".+:.+"))) {
	    kvp_list["typeNames"].emplace_back("csw:"+type_name);
	  }
	  else {
	    kvp_list["typeNames"].emplace_back(type_name);
	  }
	}
	auto element_set_names=e.element_list("ElementSetName");
	kvp_list.emplace("elementSetName",std::vector<std::string>());
	for (const auto& e : element_set_names) {
	  kvp_list["elementSetName"].emplace_back(e.content());
	}
    }
  }
  std::string query;
  for (const auto& kvp : kvp_list) {
    if (!query.empty()) {
	query+="&";
    }
    query+=kvp.first+"=";
    for (size_t n=0; n < kvp.second.size(); ++n) {
	if (n > 0) {
	  query+=",";
	}
	query+=kvp.second[n];
    }
  }
std::cout << "Content-type: text/plain" << std::endl << std::endl;
std::cout << query << std::endl;
  return query;
}

void parse_query()
{
  query_string.fill(QueryString::GET);
  if (!query_string) {
    auto xml_request=webutils::cgi::post_data();
    strutils::trim(xml_request);
    if (!xml_request.empty()) {
	query_string.fill(post_xml_to_query(xml_request));
    }
  }
  if (query_string) {
    request=strutils::to_lower(query_string.value("REQUEST"));
    if (request.empty()) {
	print_exception_report("MissingParameterValue","REQUEST");
    }
    auto service=strutils::to_lower(query_string.value("service"));
    if (service.empty()) {
	print_exception_report("MissingParameterValue","service");
    }
    else if (service != "csw") {
	print_exception_report("InvalidParameterValue","service");
    }
  }
  else {
    print_exception_report("MissingParameterValue","REQUEST");
  }
}

void get_summary_records(MySQL::Server& server)
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::stringstream qspec;

  if (constraint.contains_format || constraint.contains_subject) {
    int next=6;
    qspec << "select x.dsid,x.`dc:identifier1`,x.`dc:title`,x.`dct:abstract`,x.`dc:identifier2`,x.`dct:modified`";
    if (constraint.contains_format) {
	qspec << ",group_concat(distinct x.`dc:format`)";
	constraint.format_index=next++;
    }
    if (constraint.contains_subject) {
	qspec << ",group_concat(distinct x.`dc:subject`)";
	constraint.subject_index=next++;
    }
    qspec << ",x.`dc:type` from (";
  }
  qspec << "select s.dsid as dsid,concat('csw:edu.ucar.rda:ds',s.dsid) as `dc:identifier1`,s.title as `dc:title`,s.summary as `dct:abstract`,concat('doi:',v.doi) as `dc:identifier2`,d.date_change as `dct:modified`";
  if (constraint.contains_format) {
    qspec << ",f.keyword as `dc:format`";
  }
  if (constraint.contains_subject) {
    qspec << ",var.keyword as `dc:subject`";
  }
  qspec << ",'Dataset' as `dc:type` from search.datasets as s left join dssdb.dsvrsn as v on v.dsid = concat('ds',s.dsid) and v.status = 'A' left join dssdb.dataset as d on d.dsid = concat('ds',s.dsid)";
  if (constraint.contains_format) {
    qspec << " left join search.formats as f on f.dsid = s.dsid";
  }
  if (constraint.contains_subject) {
    qspec << " left join search.variables as var on var.dsid = s.dsid and var.vocabulary = 'GCMD'";
  }
  qspec << " where (s.type = 'P' or s.type = 'H')";
  if (!constraint.predicate.empty()) {
    qspec << " having (" << constraint.predicate << ")";
  }
  qspec << " order by s.dsid";
  if (constraint.contains_format || constraint.contains_subject) {
    qspec << ") as x group by x.dsid";
  }
  query.set(qspec.str());
std::cerr << query.show() << std::endl;
  if (query.submit(server) < 0) {
    std::cerr << "CSW Server Error (summary) - " << query.show() << std::endl;
    print_exception_report("NoApplicableCode","","Database query failure");
  }
  std::cout << "Content-type: application/xml" << std::endl << std::endl;
  std::cout << "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" << std::endl;
  std::cout << "<csw:GetRecordsResponse xmlns:csw=\"http://www.opengis.net/cat/csw/2.0.2\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.opengis.net/cat/csw/2.0.2 http://schemas.opengis.net/csw/2.0.2/CSW-discovery.xsd\">" << std::endl;
  std::cout << "  <csw:SearchStatus timestamp=\"" << dateutils::current_date_time().to_string("%ISO8601") << "\" />" << std::endl;
  std::cout << "  <csw:SearchResults elementSet=\"summary\" numberOfRecordsMatched=\"" << query.num_rows() << "\" numberOfRecordsReturned=\"" << query.num_rows() << "\" nextRecord=\"0\">" << std::endl;
  while (query.fetch_row(row)) {
    std::cout << "    <csw:SummaryRecord>" << std::endl;
    std::cout << "      <dc:identifier>" << row[1] << "</dc:identifier>" << std::endl;
    if (!row[4].empty()) {
	std::cout << "      <dc:identifier>" << row[4] << "</dc:identifier>" << std::endl;
    }
    std::cout << "      <dc:title>" << row[2] << "</dc:title>" << std::endl;
    std::cout << "      <dc:type>Dataset</dc:type>" << std::endl;
    if (constraint.contains_subject) {
	auto sp=strutils::split(row[constraint.subject_index],",");
	for (auto& p : sp) {
	  std::cout << "      <dc:subject>" << p << "</dc:subject>" << std::endl;
	}
    }
    else {
	MySQL::LocalQuery query2;
	query2.set("keyword","search.variables","dsid = '"+row[0]+"' and vocabulary = 'GCMD'");
	if (query2.submit(server) == 0) {
	  MySQL::Row row2;
	  while (query2.fetch_row(row2)) {
	    std::cout << "      <dc:subject>" << row2[0] << "</dc:subject>" << std::endl;
	  }
	}
    }
    if (constraint.contains_format) {
	auto sp=strutils::split(row[constraint.format_index],",");
	for (auto& p : sp) {
	  std::cout << "      <dc:format>" << p << "</dc:format>" << std::endl;
	}
    }
    else {
	MySQL::LocalQuery query2;
	query2.set("keyword","search.formats","dsid = '"+row[0]+"'");
	if (query2.submit(server) == 0) {
	  MySQL::Row row2;
	  while (query2.fetch_row(row2)) {
	    std::cout << "      <dc:format>" << strutils::capitalize(strutils::substitute(row2[0],"proprietary_","")) << "</dc:format>" << std::endl;
	  }
	}
    }
    std::cout << "      <dct:modified>" << row[5] << "</dct:modified>" << std::endl;
    auto abstract=htmlutils::convert_html_summary_to_ascii("<summary>"+row[3]+"</summary>",74,6);
    strutils::trim(abstract);
    std::cout << "      <dct:abstract>" << abstract << "</dct:abstract>" << std::endl;
    std::cout << "    </csw:SummaryRecord>" << std::endl;
  }
  std::cout << "  </csw:SearchResults>" << std::endl;
  std::cout << "</csw:GetRecordsResponse>" << std::endl;
}

void get_brief_records(MySQL::Server& server)
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::stringstream qspec;

  qspec << "select concat('csw:edu.ucar.rda:ds',s.dsid) as `dc:identifier1`,s.title as `dc:title`,concat('doi:',v.doi) as `dc:identifier2`,d.date_change as `dct:modified` from search.datasets as s left join dssdb.dsvrsn as v on v.dsid = concat('ds',s.dsid) and v.status = 'A' left join dssdb.dataset as d on d.dsid = concat('ds',s.dsid) where (s.type = 'P' or s.type = 'H')";
  if (!constraint.predicate.empty()) {
    qspec << " having (" << constraint.predicate << ")";
  }
  qspec << " order by s.dsid";
  query.set(qspec.str());
  if (query.submit(server) < 0) {
    std::cerr << "CSW Server Error (brief) - " << query.show() << std::endl;
    print_exception_report("NoApplicableCode","","Database query failure");
  }
  std::cout << "Content-type: application/xml" << std::endl << std::endl;
  std::cout << "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" << std::endl;
  std::cout << "<csw:GetRecordsResponse xmlns:csw=\"http://www.opengis.net/cat/csw/2.0.2\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.opengis.net/cat/csw/2.0.2 http://schemas.opengis.net/csw/2.0.2/CSW-discovery.xsd\">" << std::endl;
  std::cout << "  <csw:SearchStatus timestamp=\"" << dateutils::current_date_time().to_string("%ISO8601") << "\" />" << std::endl;
  std::cout << "  <csw:SearchResults elementSet=\"brief\" numberOfRecordsMatched=\"" << query.num_rows() << "\" numberOfRecordsReturned=\"" << query.num_rows() << "\" nextRecord=\"0\">" << std::endl;
  while (query.fetch_row(row)) {
    std::cout << "    <csw:BriefRecord>" << std::endl;
    std::cout << "      <dc:identifier>" << row[0] << "</dc:identifier>" << std::endl;
    if (!row[2].empty()) {
	std::cout << "      <dc:identifier>" << row[2] << "</dc:identifier>" << std::endl;
    }
    std::cout << "      <dc:title>" << row[1] << "</dc:title>" << std::endl;
    std::cout << "      <dc:type>Dataset</dc:type>" << std::endl;
    std::cout << "    </csw:BriefRecord>" << std::endl;
  }
  std::cout << "  </csw:SearchResults>" << std::endl;
  std::cout << "</csw:GetRecordsResponse>" << std::endl;
}

void get_full_records(MySQL::Server& server)
{
  MySQL::LocalQuery query;
  std::stringstream qspec;

  qspec << "select concat('csw:edu.ucar.rda:ds',s.dsid) as `dc:identifier1`,s.title as `dc:title`,s.summary as `dct:abstract`,concat('doi:',v.doi) as `dc:identifier2`,d.date_change as `dct:modified` from search.datasets as s left join dssdb.dsvrsn as v on v.dsid = concat('ds',s.dsid) and v.status = 'A' left join dssdb.dataset as d on d.dsid = concat('ds',s.dsid) where (s.type = 'P' or s.type = 'H')";
  if (!constraint.predicate.empty()) {
    qspec << " having (" << constraint.predicate << ")";
  }
  qspec << " order by s.dsid";
  query.set(qspec.str());
  if (query.submit(server) < 0) {
    std::cerr << "CSW Server Error (full) - " << query.show() << std::endl;
    print_exception_report("NoApplicableCode","","Database query failure");
  }
  std::cout << "Content-type: application/xml" << std::endl << std::endl;
  std::cout << "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" << std::endl;
  std::cout << "<csw:GetRecordsResponse xmlns:csw=\"http://www.opengis.net/cat/csw/2.0.2\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.opengis.net/cat/csw/2.0.2 http://schemas.opengis.net/csw/2.0.2/CSW-discovery.xsd\">" << std::endl;
  std::cout << "  <csw:SearchStatus timestamp=\"" << dateutils::current_date_time().to_string("%ISO8601") << "\" />" << std::endl;
  std::cout << "  <csw:SearchResults elementSet=\"full\" numberOfRecordsMatched=\"" << query.num_rows() << "\" numberOfRecordsReturned=\"" << query.num_rows() << "\" nextRecord=\"0\">" << std::endl;
  MySQL::Row row;
  while (query.fetch_row(row)) {
    std::cout << "    <csw:Record>" << std::endl;
    std::cout << "      <dc:identifier>" << row[0] << "</dc:identifier>" << std::endl;
    if (!row[3].empty()) {
	std::cout << "      <dc:identifier>" << row[3] << "</dc:identifier>" << std::endl;
    }
    std::cout << "      <dc:title>" << row[1] << "</dc:title>" << std::endl;
    std::cout << "      <dc:type>Dataset</dc:type>" << std::endl;
    MySQL::LocalQuery query2;
    query2.set("keyword","search.formats","dsid = '"+row[0]+"'");
    if (query2.submit(server) == 0) {
	MySQL::Row row2;
	while (query2.fetch_row(row2)) {
	  std::cout << "      <dc:format>" << strutils::capitalize(strutils::substitute(row2[0],"proprietary_","")) << "</dc:format>" << std::endl;
	}
    }
    std::cout << "      <dct:modified>" << row[4] << "</dct:modified>" << std::endl;
    auto abstract=htmlutils::convert_html_summary_to_ascii("<summary>"+row[2]+"</summary>",74,6);
    strutils::trim(abstract);
    std::cout << "      <dct:abstract>" << abstract << "</dct:abstract>" << std::endl;
    std::cout << "    </csw:Record>" << std::endl;
  }
  std::cout << "  </csw:SearchResults>" << std::endl;
  std::cout << "</csw:GetRecordsResponse>" << std::endl;
}

void get_records()
{
  auto version=query_string.value("version");
  if (version.empty()) {
    print_exception_report("MissingParameterValue","version");
  }
  else if (version != "2.0.2") {
    print_exception_report("InvalidParameterValue","version");
  }
  auto element_set_name=query_string.value("ElementSetName");
  if (element_set_name.empty()) {
    element_set_name="summary";
  }
  else if (element_set_name != "brief" && element_set_name != "summary" && element_set_name != "full") {
    print_exception_report("InvalidParametervalue","ElementSetName");
  }
  auto result_type=query_string.value("resultType");
  if (result_type.empty()) {
    result_type="hits";
  }
  else if (result_type != "hits" && result_type != "results") {
    print_exception_report("InvalidParameterValue","resultType");
  }
  auto type_names=strutils::to_lower(query_string.value("typeNames"));
  if (type_names.empty()) {
    print_exception_report("MissingParameterValue","typeNames");
  }
  else if (type_names != strutils::to_lower("csw:Record")) {
    print_exception_report("InvalidParameterValue","typeNames");
  }
  auto constraint_language=strutils::to_lower(query_string.value("CONSTRAINTLANGUAGE"));
  if (!constraint_language.empty()) {
    if (constraint_language != strutils::to_lower("CQL_TEXT")) {
	print_exception_report("InvalidParameterValue","CONSTRAINTLANGUAGE");
    }
    constraint.predicate=strutils::to_lower(query_string.value("Constraint"));
    if (!constraint.predicate.empty()) {
	if (constraint.predicate.back() == '"') {
	  constraint.predicate.pop_back();
	}
	if (constraint.predicate.front() == '"') {
	  constraint.predicate.erase(0,1);
	}
	constraint.contains_format=std::regex_search(constraint.predicate,std::regex("dc:format"));
	constraint.contains_subject=std::regex_search(constraint.predicate,std::regex("dc:subject"));
	strutils::replace_all(constraint.predicate,"dc:title","`dc:title`");
	strutils::replace_all(constraint.predicate,"dct:abstract","`dct:abstract`");
	strutils::replace_all(constraint.predicate,"dct:modified","`dct:modified`");
	strutils::replace_all(constraint.predicate,"dc:type","`dc:type`");
	strutils::replace_all(constraint.predicate,"dc:format","`dc:format`");
	strutils::replace_all(constraint.predicate,"dc:subject","`dc:subject`");
	if (std::regex_search(constraint.predicate,std::regex("csw:anytext"))) {
	  std::stringstream p;
	  p << "(" << strutils::substitute(constraint.predicate,"csw:anytext","dc:identifier") << ") or (" << strutils::substitute(constraint.predicate,"csw:anytext","`dc:title`") << ") or (" << strutils::substitute(constraint.predicate,"csw:anytext","`dct:abstract`") << ") or (" << strutils::substitute(constraint.predicate,"csw:anytext","`dct:modified`") << ") or (" << strutils::substitute(constraint.predicate,"csw:anytext","`dc:type`") << ") or (" <<  strutils::substitute(constraint.predicate,"csw:anytext","`dc:format`") << ") or (" << strutils::substitute(constraint.predicate,"csw:anytext","`dc:subject`") << ")";
	  constraint.contains_format=true;
	  constraint.contains_subject=true;
	  constraint.predicate=p.str();
	}
	if (std::regex_search(constraint.predicate,std::regex("dc:identifier"))) {
	  auto p1=constraint.predicate;
	  strutils::replace_all(p1,"dc:identifier","`dc:identifier1`");
	  auto p2=constraint.predicate;
	  strutils::replace_all(p2,"dc:identifier","`dc:identifier2`");
	  constraint.predicate="("+p1+") or ("+p2+")";
	}
    }
  }
  MySQL::Server server("rda-db.ucar.edu","metadata","metadata","");
  if (!server) {
    print_exception_report("NoApplicableCode","","Database connection failure");
  }
  if (result_type == "hits") {
    MySQL::LocalQuery query;
    MySQL::Row row;
    std::stringstream qspec;
    qspec << "select count(x.`dc:identifier1`) from (select concat('csw:edu.ucar.rda:ds',s.dsid) as `dc:identifier1`,s.title as `dc:title`,s.summary as `dct:abstract`,concat('doi:',v.doi) as `dc:identifier2`,d.date_change as `dct:modified` from search.datasets as s left join dssdb.dsvrsn as v on v.dsid = concat('ds',s.dsid) and v.status = 'A' left join dssdb.dataset as d on d.dsid = concat('ds',s.dsid) where (s.type = 'P' or s.type = 'H')";
    if (!constraint.predicate.empty()) {
	qspec << " having (" << constraint.predicate << ")";
    }
    qspec << ") as x";
    query.set(qspec.str());
    if (query.submit(server) < 0 || !query.fetch_row(row)) {
	std::cerr << "CSW Server Error (hits) - " << query.show() << std::endl;
	print_exception_report("NoApplicableCode","","Database query failure");
    }
    std::cout << "Content-type: application/xml" << std::endl << std::endl;
    std::cout << "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" << std::endl;
    std::cout << "<csw:GetRecordsResponse xmlns:csw=\"http://www.opengis.net/cat/csw/2.0.2\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.opengis.net/cat/csw/2.0.2 http://schemas.opengis.net/csw/2.0.2/CSW-discovery.xsd\">" << std::endl;
    std::cout << "  <csw:SearchStatus timestamp=\"" << dateutils::current_date_time().to_string("%ISO8601") << "\" />" << std::endl;
    std::cout << "  <csw:SearchResults elementSet=\"" << element_set_name << "\" numberOfRecordsMatched=\"" << row[0] << "\" numberOfRecordsReturned=\"0\" nextRecord=\"1\" />" << std::endl;
    std::cout << "</csw:GetRecordsResponse>" << std::endl;
  }
  else {
    if (element_set_name == "summary") {
	get_summary_records(server);
    }
    else if (element_set_name == "brief") {
	get_brief_records(server);
    }
    else if (element_set_name == "full") {
	get_full_records(server);
    }
  }
  server.disconnect();
}

int main(int argc,char **argv)
{
  parse_query();
  if (request == strutils::to_lower("GetCapabilities")) {
    auto version_list=query_string.value("AcceptVersions");
    bool supported_version=true;
    auto versions=strutils::split(version_list,",");
    if (versions.size() > 0) {
	supported_version=false;
	for (const auto& v : versions) {
	  if (v == "2.0.2") {
	    supported_version=true;
	  }
	}
    }
    if (!supported_version) {
	print_exception_report("VersionNegotiationFailed","");
    }
    std::cout << "Content-type: application/xml" << std::endl << std::endl;
    hereDoc::Tokens tokens;
    hereDoc::IfEntry ife;
    auto section_list=query_string.value("Sections");
    if (!section_list.empty()) {
	auto sections=strutils::split(section_list,",");
	for (const auto& s : sections) {
	  auto l=strutils::to_lower(s);
	  if (l == "serviceidentification") {
	    ife.key="__PRINT_SERVICE_IDENTIFICATION__";
	    tokens.ifs.insert(ife);
	  }
	  else if (l == "operationsmetadata") {
	    ife.key="__PRINT_OPERATIONS_METADATA__";
	    tokens.ifs.insert(ife);
	  }
	}
    }
    else {
	ife.key="__PRINT_SERVICE_IDENTIFICATION__";
	tokens.ifs.insert(ife);
	ife.key="__PRINT_OPERATIONS_METADATA__";
	tokens.ifs.insert(ife);
    }
    hereDoc::print("/usr/local/www/server_root/web/html/csw/capabilities.xml",&tokens);
  }
  else if (request == strutils::to_lower("GetRecords")) {
    get_records();
  }
  else {
    print_exception_report("InvalidParameterValue","REQUEST");
  }
}
