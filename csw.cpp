#include <iostream>
#include <regex>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <PostgreSQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <xmlutils.hpp>
#include <datetime.hpp>
#include <web/web.hpp>
#include <tokendoc.hpp>
#include <metadata.hpp>
#include <metadata_export.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using std::cout;
using std::endl;
using std::string;
using std::stringstream;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
string myerror="";
string myoutput="";
string mywarning="";

struct Constraint {
  Constraint() : predicate(),format_index(-1),subject_index(-1),contains_format(false),contains_subject(false) {}

  string predicate;
  int format_index,subject_index;
  bool contains_format,contains_subject;
} constraint;
string request;
QueryString query_string;

const std::unordered_map<string,string> TYPE_NAMES{{"csw:Record","http://www.opengis.net/cat/csw/2.0.2"}};
const std::unordered_set<string> OUTPUT_FORMATS{"application/xml"};
const std::unordered_set<string> CONSTRAINTLANGUAGES{"CQL_TEXT"};
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
const std::unordered_set<string> DUBLIN_CORE_QUERYABLES{"dc:title","dc:type","dc:identifier","dct:modified","dct:abstract","dc:subject","dc:format"};

void print_exception_report(string exception_code,string locator,string exception_text = "")
{
  cout << "Content-type: application/xml" << endl << endl;
  cout << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" << endl;
  cout << "<ExceptionReport xmlns=\"http://www.opengis.net/ows/2.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.opengis.net/ows/2.0 http://schemas.opengis.net/ows/1.0.0/owsExceptionReport.xsd\" version=\"2.0.2\">" << endl;
  cout << "  <Exception exceptionCode=\"" << exception_code << "\"";
  if (!locator.empty()) {
    cout << " locator=\"" << locator << "\"";
  }
  cout << " />" << endl;
  if (!exception_text.empty()) {
    cout << "  <ExceptionText>" << exception_text << "</ExceptionText>" << endl;
  }
  cout << "</ExceptionReport>" << endl;
  exit(1);
}

void process_query_error(string query_error)
{
  if (std::regex_search(query_error,std::regex("syntax"))) {
    print_exception_report("NoApplicableCode","","Database query syntax error");
  } else {
    print_exception_report("NoApplicableCode","","Database query failure");
  }
}

string post_xml_to_query(string xml)
{
  std::unordered_map<string,std::vector<string>> kvp_list;
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
  if ( (idx=request.find(":")) != string::npos) {
    request=request.substr(idx+1);
  }
  kvp_list.emplace("REQUEST",std::vector<string>{request});
  kvp_list.emplace("service",std::vector<string>{strutils::to_lower(root_element.attribute_value("service"))});
  if (request == "getcapabilities") {
    auto elist=xmls.element_list(root_element.name()+"/ows:AcceptVersions/ows:Version");
    if (elist.size() > 0) {
      kvp_list.emplace("version",std::vector<string>());
      for (const auto& e : elist) {
        kvp_list["version"].emplace_back(e.content());
      }
    }
    elist=xmls.element_list(root_element.name()+"/ows:AcceptFormats/ows:OutputFormat");
    if (elist.size() > 0) {
      kvp_list.emplace("outputFormat",std::vector<string>());
      for (const auto& e : elist) {
        kvp_list["outputFormat"].emplace_back(e.content());
      }
    }
  } else if (request == "getrecords") {
    kvp_list.emplace("version",std::vector<string>{root_element.attribute_value("version")});
    kvp_list.emplace("resultType",std::vector<string>{root_element.attribute_value("resultType")});
    auto query_element=root_element.element("Query");
    if (!query_element.name().empty()) {
      auto type_names=strutils::split(query_element.attribute_value("typeNames"));
      kvp_list.emplace("typeNames",std::vector<string>());
      for (const auto& type_name : type_names) {
        if (!std::regex_search(type_name,std::regex(".+:.+"))) {
          kvp_list["typeNames"].emplace_back("csw:"+type_name);
        } else {
          kvp_list["typeNames"].emplace_back(type_name);
        }
      }
      auto element_set_names=query_element.element_list("ElementSetName");
      kvp_list.emplace("elementSetName",std::vector<string>());
      for (const auto& e : element_set_names) {
        kvp_list["elementSetName"].emplace_back(e.content());
      }
      auto constraint_element=query_element.element("Constraint");
      if (!constraint_element.name().empty()) {
        if (constraint_element.element_addresses().front().p->name() == "CqlText") {
          kvp_list.emplace("CONSTRAINTLANGUAGE",std::vector<string>{"CQL_TEXT"});
          kvp_list.emplace("Constraint",std::vector<string>{constraint_element.element_addresses().front().p->content()});
        } else if (constraint_element.element_addresses().front().p->name() == "ogc:Filter") {
          kvp_list.emplace("CONSTRAINTLANGUAGE",std::vector<string>{"CQL_TEXT"});
          std::unordered_map<string,string> comparison_operators{{"EqualTo"," = "},{"NotEqualTo"," != "},{"LessThan"," < "},{"GreaterThan"," > "},{"LessThanEqualTo"," <= "},{"GreaterThanEqualTo"," >= "},{"Like"," like "}};
          auto ogc_filter_element=constraint_element.element("ogc:Filter");
          if (std::regex_search(ogc_filter_element.element_addresses().front().p->name(),std::regex("^ogc:PropertyIs"))) {
            auto p=ogc_filter_element.element_addresses().front().p;
            auto property_name=p->element("ogc:PropertyName").content();
            auto literal=p->element("ogc:Literal").content();
            auto comparison_op=p->name().substr(14);
            if (comparison_op == "Like") {
              literal="%"+literal+"%";
            }
            kvp_list.emplace("Constraint",std::vector<string>{property_name+comparison_operators[comparison_op]+"'"+literal+"'"});
          }
        } else {
          print_exception_report("NoApplicableCode","Constraint","Invalid constraint XML");
        }
      }
    }
  } else if (request == "getrecordbyid") {
    kvp_list.emplace("version",std::vector<string>{root_element.attribute_value("version")});
    kvp_list.emplace("resultType",std::vector<string>{root_element.attribute_value("resultType")});
    auto id_list=root_element.element_list("Id");
    if (id_list.size() == 0) {
      print_exception_report("MissingParameterValue","Id");
    }
    kvp_list.emplace("Id",std::vector<string>());
    for (const auto& id : id_list) {
      kvp_list["Id"].emplace_back(id.content());
    }
    auto element_set_names=root_element.element_list("ElementSetName");
    if (element_set_names.size() > 0) {
      kvp_list.emplace("elementSetName",std::vector<string>());
      for (const auto& e : element_set_names) {
        kvp_list["elementSetName"].emplace_back(e.content());
      }
    }
  }
  string query;
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
cout << "Content-type: text/plain" << endl << endl;
cout << query << endl;
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
    } else if (service != "csw") {
      print_exception_report("InvalidParameterValue","service");
    }
  } else {
    print_exception_report("MissingParameterValue","REQUEST");
  }
}

string response_root()
{
  if (request == strutils::to_lower("getrecords")) {
    return "GetRecords";
  } else if (request == strutils::to_lower("getrecordbyid")) {
    return "GetRecordById";
  } else {
    return "";
  }
}

void get_summary_records(Server& server,size_t max_records)
{
  LocalQuery query;
  Row row;
  stringstream qspec;

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
  qspec << "select s.dsid as dsid,concat('edu.ucar.gdex:ds',s.dsid) as `dc:identifier1`,s.title as `dc:title`,s.summary as `dct:abstract`,concat('doi:',v.doi) as `dc:identifier2`,d.date_change as `dct:modified`";
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
std::cerr << query.show() << endl;
  if (query.submit(server) < 0) {
    std::cerr << "CSW Server Error (summary) - " << query.show() << endl;
    process_query_error(query.error());
  }
  cout << "Content-type: application/xml" << endl << endl;
  cout << "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" << endl;
  cout << "<csw:" << response_root() << "Response xmlns:csw=\"http://www.opengis.net/cat/csw/2.0.2\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.opengis.net/cat/csw/2.0.2 http://schemas.opengis.net/csw/2.0.2/CSW-discovery.xsd\">" << endl;
  cout << "  <csw:SearchStatus timestamp=\"" << dateutils::current_date_time().to_string("%ISO8601") << "\" />" << endl;
  cout << "  <csw:SearchResults elementSet=\"summary\" numberOfRecordsMatched=\"" << query.num_rows() << "\" numberOfRecordsReturned=\"" << query.num_rows() << "\" nextRecord=\"0\">" << endl;
  while (query.fetch_row(row)) {
    cout << "    <csw:SummaryRecord>" << endl;
    cout << "      <dc:identifier>" << row[1] << "</dc:identifier>" << endl;
    if (!row[4].empty()) {
      cout << "      <dc:identifier>" << row[4] << "</dc:identifier>" << endl;
    }
    cout << "      <dc:title>" << row[2] << "</dc:title>" << endl;
    cout << "      <dc:type>Dataset</dc:type>" << endl;
    if (constraint.contains_subject) {
      auto sp=strutils::split(row[constraint.subject_index],",");
      for (auto& p : sp) {
        cout << "      <dc:subject>" << p << "</dc:subject>" << endl;
      }
    } else {
      LocalQuery query2;
      query2.set("keyword","search.variables","dsid = '"+row[0]+"' and vocabulary = 'GCMD'");
      if (query2.submit(server) == 0) {
        Row row2;
        while (query2.fetch_row(row2)) {
          cout << "      <dc:subject>" << row2[0] << "</dc:subject>" << endl;
        }
      }
    }
    if (constraint.contains_format) {
      auto sp=strutils::split(row[constraint.format_index],",");
      for (auto& p : sp) {
        cout << "      <dc:format>" << p << "</dc:format>" << endl;
      }
    } else {
      LocalQuery query2;
      query2.set("keyword","search.formats","dsid = '"+row[0]+"'");
      if (query2.submit(server) == 0) {
        Row row2;
        while (query2.fetch_row(row2)) {
          cout << "      <dc:format>" << strutils::to_capital(strutils::substitute(row2[0],"proprietary_","")) << "</dc:format>" << endl;
        }
      }
    }
    cout << "      <dct:modified>" << row[5] << "</dct:modified>" << endl;
    auto abstract=htmlutils::convert_html_summary_to_ascii("<summary>"+row[3]+"</summary>",74,6);
    strutils::trim(abstract);
    cout << "      <dct:abstract>" << abstract << "</dct:abstract>" << endl;
    cout << "    </csw:SummaryRecord>" << endl;
  }
  cout << "  </csw:SearchResults>" << endl;
  cout << "</csw:" << response_root() << "Response>" << endl;
}

void get_brief_records(Server& server,size_t max_records)
{
  LocalQuery query;
  Row row;
  stringstream qspec;

  qspec << "select s.dsid,concat('edu.ucar.gdex:ds',s.dsid) as `dc:identifier1`,s.title as `dc:title`,concat('doi:',v.doi) as `dc:identifier2`,d.date_change as `dct:modified` from search.datasets as s left join dssdb.dsvrsn as v on v.dsid = concat('ds',s.dsid) and v.status = 'A' left join dssdb.dataset as d on d.dsid = concat('ds',s.dsid) where (s.type = 'P' or s.type = 'H')";
  if (!constraint.predicate.empty()) {
    qspec << " having (" << constraint.predicate << ")";
  }
  qspec << " order by s.dsid";
  query.set(qspec.str());
  if (query.submit(server) < 0) {
    std::cerr << "CSW Server Error (brief) - " << query.show() << endl;
    process_query_error(query.error());
  }
  cout << "Content-type: application/xml" << endl << endl;
  cout << "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" << endl;
  cout << "<csw:" << response_root() << "Response xmlns:csw=\"http://www.opengis.net/cat/csw/2.0.2\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:ows=\"http://www.opengis.net/ows\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.opengis.net/cat/csw/2.0.2 http://schemas.opengis.net/csw/2.0.2/CSW-discovery.xsd\">" << endl;
  cout << "  <csw:SearchStatus timestamp=\"" << dateutils::current_date_time().to_string("%ISO8601") << "\" />" << endl;
  cout << "  <csw:SearchResults elementSet=\"brief\" numberOfRecordsMatched=\"" << query.num_rows() << "\" numberOfRecordsReturned=\"" << query.num_rows() << "\" nextRecord=\"0\">" << endl;
  while (query.fetch_row(row)) {
    cout << "    <csw:BriefRecord>" << endl;
    cout << "      <dc:identifier>" << row[1] << "</dc:identifier>" << endl;
    if (!row[3].empty()) {
      cout << "      <dc:identifier>" << row[3] << "</dc:identifier>" << endl;
    }
    cout << "      <dc:title>" << row[2] << "</dc:title>" << endl;
    cout << "      <dc:type>Dataset</dc:type>" << endl;
    static Server geo_server(metautils::directives.metadb_config);
    XMLDocument xdoc("/data/web/datasets/ds"+row[0]+"/metadata/dsOverview.xml");
    double min_west_lon,min_south_lat,max_east_lon,max_north_lat;
    bool is_grid;
    metadataExport::fill_geographic_extent_data(geo_server,row[0],xdoc,min_west_lon,min_south_lat,max_east_lon,max_north_lat,is_grid);
    if (min_west_lon < 9999.) {
      if (min_west_lon > 180.) {
        min_west_lon-=360.;
      } else if (min_west_lon < -180.) {
        min_west_lon+=360.;
      }
    }
    if (max_east_lon > -9999.) {
      if (max_east_lon > 180.) {
        max_east_lon-=360.;
      } else if (max_east_lon < -180.) {
        max_east_lon+=360.;
      }
    }
    cout << "      <ows:BoundingBox>" << endl;
    cout << "        <ows:LowerCorner>" << min_west_lon << " " << min_south_lat << "</ows:LowerCorner>" << endl;
    cout << "        <ows:UpperCorner>" << max_east_lon << " " << max_north_lat << "</ows:UpperCorner>" << endl;
    cout << "      </ows:BoundingBox>" << endl;
    cout << "    </csw:BriefRecord>" << endl;
  }
  cout << "  </csw:SearchResults>" << endl;
  cout << "</csw:" << response_root() << "Response>" << endl;
}

void get_full_records(Server& server,size_t max_records)
{
  stringstream qspec;
  qspec << "select s.dsid,concat('edu.ucar.gdex:ds',s.dsid) as `dc:identifier1`,s.title as `dc:title`,s.summary as `dct:abstract`,concat('doi:',v.doi) as `dc:identifier2`,d.date_change as `dct:modified` from search.datasets as s left join dssdb.dsvrsn as v on v.dsid = concat('ds',s.dsid) and v.status = 'A' left join dssdb.dataset as d on d.dsid = concat('ds',s.dsid) where (s.type = 'P' or s.type = 'H')";
  if (!constraint.predicate.empty()) {
    qspec << " having (" << constraint.predicate << ")";
  }
  qspec << " order by s.dsid";
  LocalQuery query(qspec.str());
  if (query.submit(server) < 0) {
    std::cerr << "CSW Server Error (full) - " << query.show() << "; " << query.error() << endl;
    process_query_error(query.error());
  }
  cout << "Content-type: application/xml" << endl << endl;
  cout << "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" << endl;
  cout << "<csw:" << response_root() << "Response xmlns:csw=\"http://www.opengis.net/cat/csw/2.0.2\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.opengis.net/cat/csw/2.0.2 http://schemas.opengis.net/csw/2.0.2/CSW-discovery.xsd\">" << endl;
  cout << "  <csw:SearchStatus timestamp=\"" << dateutils::current_date_time().to_string("%ISO8601") << "\" />" << endl;
  cout << "  <csw:SearchResults elementSet=\"full\" numberOfRecordsMatched=\"" << query.num_rows() << "\" numberOfRecordsReturned=\"" << query.num_rows() << "\" nextRecord=\"0\">" << endl;
  Row row;
  while (query.fetch_row(row)) {
    cout << "    <csw:Record>" << endl;
    cout << "      <dc:identifier>" << row[1] << "</dc:identifier>" << endl;
    if (!row[4].empty()) {
      cout << "      <dc:identifier>" << row[4] << "</dc:identifier>" << endl;
    }
    cout << "      <dc:title>" << row[2] << "</dc:title>" << endl;
    cout << "      <dc:type>Dataset</dc:type>" << endl;
    LocalQuery query2;
    query2.set("select g.path from search.variables_new as v left join search.GCMD_sciencekeywords as g on g.uuid = v.keyword where v.vocabulary = 'GCMD' and v.dsid = '"+row[0]+"'");
    if (query2.submit(server) == 0) {
      Row row2;
      while (query2.fetch_row(row2)) {
        cout << "      <dc:subject>" << row2[0] << "</dc:subject>" << endl;
      }
    }
    query2.set("distinct keyword","search.formats","dsid = '"+row[0]+"'");
    if (query2.submit(server) == 0) {
      Row row2;
      while (query2.fetch_row(row2)) {
        cout << "      <dc:format>" << strutils::to_capital(strutils::substitute(row2[0],"proprietary_","")) << "</dc:format>" << endl;
      }
    }
    cout << "      <dct:modified>" << row[5] << "</dct:modified>" << endl;
    auto abstract=htmlutils::convert_html_summary_to_ascii("<summary>"+row[3]+"</summary>",74,6);
    strutils::trim(abstract);
    cout << "      <dct:abstract>" << abstract << "</dct:abstract>" << endl;
    cout << "    </csw:Record>" << endl;
  }
  cout << "  </csw:SearchResults>" << endl;
  cout << "</csw:" << response_root() << "Response>" << endl;
}

void get_records()
{
  auto version=query_string.value("version");
  if (version.empty()) {
    print_exception_report("MissingParameterValue","version");
  } else if (version != "2.0.2") {
    print_exception_report("InvalidParameterValue","version");
  }
  auto element_set_name=query_string.value("ElementSetName");
  if (element_set_name.empty()) {
    element_set_name="summary";
  } else if (element_set_name != "brief" && element_set_name != "summary" && element_set_name != "full") {
    print_exception_report("InvalidParametervalue","ElementSetName");
  }
  auto result_type=query_string.value("resultType");
  if (result_type.empty()) {
    result_type="hits";
  } else if (result_type != "hits" && result_type != "results") {
    print_exception_report("InvalidParameterValue","resultType");
  }
  auto type_names=strutils::to_lower(query_string.value("typeNames"));
  if (type_names.empty()) {
    print_exception_report("MissingParameterValue","typeNames");
  } else if (type_names != strutils::to_lower("csw:Record")) {
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
        stringstream p;
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
  int max_records=10;
  auto max_records_s=query_string.value("maxRecords");
  if (!max_records_s.empty()) {
    if (!strutils::is_numeric(max_records_s)) {
      print_exception_report("InvalidParameterValue","maxRecords","Value specified, '"+max_records_s+"', is not a number");
    }
    max_records=std::stoi(max_records_s);
    if (max_records < 0) {
      print_exception_report("InvalidParameterValue","maxRecords","Negative values are not allowed");
    }
  }
  if (max_records == 0) {
    result_type="hits";
  }
  auto start_position_s=query_string.value("startPosition");
  int start_position=0;
  if (!start_position_s.empty()) {
    start_position=std::stoi(start_position_s)-1;
  }
  Server server(metautils::directives.metadb_config);
  if (!server) {
    print_exception_report("NoApplicableCode","","Database connection failure");
  }
  if (result_type == "hits") {
    LocalQuery query;
    Row row;
    stringstream qspec;
    qspec << "select count(x.`dc:identifier1`) from (select concat('edu.ucar.gdex:ds',s.dsid) as `dc:identifier1`,s.title as `dc:title`,s.summary as `dct:abstract`,concat('doi:',v.doi) as `dc:identifier2`,d.date_change as `dct:modified` from search.datasets as s left join dssdb.dsvrsn as v on v.dsid = concat('ds',s.dsid) and v.status = 'A' left join dssdb.dataset as d on d.dsid = concat('ds',s.dsid) where (s.type = 'P' or s.type = 'H')";
    if (!constraint.predicate.empty()) {
      qspec << " having (" << constraint.predicate << ")";
    }
    qspec << ") as x";
    query.set(qspec.str());
    if (query.submit(server) < 0 || !query.fetch_row(row)) {
      std::cerr << "CSW Server Error (hits) - " << query.show() << endl;
      process_query_error(query.error());
    }
    cout << "Content-type: application/xml" << endl << endl;
    cout << "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" << endl;
    cout << "<csw:" << response_root() << "Response xmlns:csw=\"http://www.opengis.net/cat/csw/2.0.2\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.opengis.net/cat/csw/2.0.2 http://schemas.opengis.net/csw/2.0.2/CSW-discovery.xsd\">" << endl;
    cout << "  <csw:SearchStatus timestamp=\"" << dateutils::current_date_time().to_string("%ISO8601") << "\" />" << endl;
    cout << "  <csw:SearchResults elementSet=\"" << element_set_name << "\" numberOfRecordsMatched=\"" << row[0] << "\" numberOfRecordsReturned=\"0\" nextRecord=\"1\" />" << endl;
    cout << "</csw:" << response_root() << "Response>" << endl;
  } else {
    if (element_set_name == "summary") {
      get_summary_records(server,max_records);
    } else if (element_set_name == "brief") {
      get_brief_records(server,max_records);
    } else if (element_set_name == "full") {
      get_full_records(server,max_records);
    }
  }
  server.disconnect();
}

void get_record_by_id()
{
  auto id=query_string.value("id");
  if (id.empty()) {
    print_exception_report("MissingParameterValue","Id");
  }
  auto ids=strutils::split(id,",");
  string constraint;
  for (const auto& id : ids) {
    if (!constraint.empty()) {
      constraint+=" or ";
    }
    constraint+="dc:identifier = '"+id+"'";
  }
  query_string.fill(query_string.to_string()+"&typeNames=csw:Record&CONSTRAINTLANGUAGE=CQL_TEXT&Constraint="+constraint);
  get_records();
}

int main(int argc, char **argv) {
  parse_query();
  metautils::read_config("csw", "", false);
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
    TokenDocument tdoc("/data/web/html/csw/capabilities.xml");
    auto section_list=query_string.value("Sections");
    if (!section_list.empty()) {
      auto sections=strutils::split(section_list,",");
      for (const auto& s : sections) {
        auto l=strutils::to_lower(s);
        if (l == "serviceidentification") {
          tdoc.add_if("__PRINT_SERVICE_IDENTIFICATION__");
        } else if (l == "operationsmetadata") {
          tdoc.add_if("__PRINT_OPERATIONS_METADATA__");
        }
      }
    } else {
      tdoc.add_if("__PRINT_SERVICE_IDENTIFICATION__");
      tdoc.add_if("__PRINT_OPERATIONS_METADATA__");
    }
    for (const auto& item : TYPE_NAMES) {
      tdoc.add_repeat("__TYPE_NAME__",item.first);
      tdoc.add_repeat("__OUTPUT_SCHEMA__",item.second);
    }
    for (const auto& output_format : OUTPUT_FORMATS) {
      tdoc.add_repeat("__OUTPUT_FORMAT__",output_format);
    }
    for (const auto& constraint_language : CONSTRAINTLANGUAGES) {
      tdoc.add_repeat("__CONSTRAINTLANGUAGE__",constraint_language);
    }
    for (const auto& dc_queryable : DUBLIN_CORE_QUERYABLES) {
      tdoc.add_repeat("__DUBLIN_CORE_QUERYABLE__",dc_queryable);
    }
    cout << "Content-type: application/xml" << endl << endl;
    cout << tdoc << endl;
  } else if (request == strutils::to_lower("GetRecords")) {
    get_records();
  } else if (request == strutils::to_lower("GetRecordById")) {
    get_record_by_id();
  } else {
    print_exception_report("InvalidParameterValue","REQUEST");
  }
}
