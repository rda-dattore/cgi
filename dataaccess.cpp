#include <sys/stat.h>
#include <pthread.h>
#include <signal.h>
#include <sstream>
#include <string>
#include <list>
#include <unordered_map>
#include <regex>
#include <web/web.hpp>
#include <MySQL.hpp>
#include <xml.hpp>
#include <strutils.hpp>
#include <myerror.hpp>

std::string myerror="";
std::string mywarning="";
webutils::cgi::Directives directives;

struct GroupControlEntry {
  struct Data {
    Data() : subset_url(),dap_url(),format_conversion(),format_conversion_url(),has_hpss_staged_access(false) {}

    std::string subset_url,dap_url;
    std::string format_conversion,format_conversion_url;
    bool has_hpss_staged_access;
  };
  GroupControlEntry() : data(nullptr) {}

  std::shared_ptr<Data> data;
};
struct SpanData {
  SpanData() : data_file_downloads(0),ncar_only_access(0) {}
  size_t num_columns() const { return (data_file_downloads+ncar_only_access); }

  int data_file_downloads,ncar_only_access;
};

std::string dsnum;
MySQL::Server server;

void print_bad_input()
{
  web_error2("bad input","400 Bad Request");
}

void print_server_trouble()
{
  std::cout << "Content-type: text/html" << std::endl;
  std::cout << "Status: 500 Internal Server Error" << std::endl;
  std::cout << std::endl;
  std::cout << "<span class=\"fs20px bold\">Server Trouble</span>" << std::endl;
  std::cout << "<p>We are currently experiencing trouble with the disks that serve our data.  We hope to have the problem resolved shortly and apologize for the inconvenience.</p>" << std::endl;
  exit(0);
}

void print_not_authorized()
{
  std::cout << "Content-type: text/html" << std::endl;
  std::cout << "Status: 401 Unauthorized" << std::endl;
  std::cout << std::endl;
  std::cout << "<span class=\"fs20px bold\">Not Authorized</span>" << std::endl;
  std::cout << "<p>You are not authorized to access this information.  If you believe this to be in error, please <a href=\"mailto:rdahelp@ucar.edu\">contact us</a> for assistance.</p>" << std::endl;
  exit(0);
}

void print_static_glade_list(std::string gindex,std::stringstream& matrix_ss)
{
  matrix_ss << "<a class=\"matrix\" href=\"/datasets/ds" << dsnum << "/index.html#!sfol-gl?g=" << gindex << "\">GLADE File<br />Listing</a>";
}

std::string ajaxify_url(std::string url,std::string nvp = "")
{
  size_t idx;
  if (strutils::has_beginning(url,"/cgi-bin")) {
    std::string ajax_url="/datasets/ds"+dsnum+"/index.html#!"+url.substr(1);
    if (strutils::contains(ajax_url,"?")) {
	ajax_url+="&_da=y";
    }
    else {
	ajax_url+="?_da=y";
    }
    if (!nvp.empty()) {
	ajax_url+="&"+nvp;
    }
    return ajax_url;
  }
  else if (strutils::has_beginning(url,"/datasets/ds") && (idx=url.find("/ds"+dsnum)) != std::string::npos) {
    return "/datasets/ds"+dsnum+"/#!"+url.substr(idx+9);
  }
  else {
    return url;
  }
}

void parse_query_string()
{
  QueryString query_string(QueryString::GET);
  dsnum=query_string.value("dsnum");
  if (dsnum.empty()) {
    web_error2("no dataset number specified","400 Bad Request");
  }
  if (dsnum.length() != 5 || dsnum.find(".") != 3) {
    print_bad_input();
  }
}

void verify_authorization(std::string doc_root,std::string ruser,std::string& access_type)
{
  MySQL::PreparedStatement pstmt;
  std::string pstmt_error;
  if (!run_prepared_statement(server,"select access_type from dataset where dsid = concat('ds',?)",std::vector<enum_field_types>{MYSQL_TYPE_STRING},std::vector<std::string>{dsnum},pstmt,pstmt_error)) {
    print_server_trouble();
  }
  else if (pstmt.num_rows() == 0) {
    print_bad_input();
  }
  MySQL::Row row;
  pstmt.fetch_row(row);
  if (row["access_type"].empty()) {
    access_type="g";
  }
  else {
    access_type=row["access_type"];
  }
  if (!has_access("<"+access_type+">")) {
    if (!ruser.empty()) {
// user is signed in
	std::ifstream ifs;
	ifs.open((doc_root+"/web/.htaccess").c_str());
	if (ifs.is_open()) {
	  char line[32768];
	  ifs.getline(line,32768);
	  while (!ifs.eof()) {
	    std::string sline=line;
	    if (strutils::contains(sline,ruser)) {
		web_error2("Your data access privileges have been suspended.  Please contact rdahelp@ucar.edu to have your privileges restored.","401 Unauthorized");
	    }
	    ifs.getline(line,32768);
	  }
	  ifs.close();
	  ifs.clear();
	}
	struct stat buf;
	if (stat((doc_root+"/data_tous/"+access_type+"_tou.html").c_str(),&buf) == 0 && stat((doc_root+"/data_tous/"+access_type+".access").c_str(),&buf) == 0) {
	  MySQL::LocalQuery query("description,delayed_approval","access_types","acode = '"+access_type+"'");
	  if (query.submit(server) < 0) {
	    print_server_trouble();
	  }
	  if (query.num_rows() == 0) {
	    print_not_authorized();
	  }
	  else if (!query.fetch_row(row)) {
	    print_server_trouble();
	  }
	  std::cout << "Content-type: text/html" << std::endl << std::endl;
	  std::cout << "<span class=\"fs20px bold\">Additional Qualifications Required</span>" << std::endl;
	  std::cout << "<p>Access to these data requires that you be eligible to use the data and you accept the Terms of Use for the data.  To do this:<ul><li><a href=\"javascript:void(0)\" onClick=\"location='https://rda.ucar.edu/index.html?hash=data_user&action=edit&url='+constructPath()\">edit your profile</a></li><li>scroll down to the list of restricted datasets</li><li>if you are eligible to use the data, you will be able to click the entry for <span class=\"underline\">" << row["description"] << "</span><ul><li>you can view the terms of eligibility for this dataset in the <b>Access Restrictions</b> and/or <b>Usage Restrictions</b> section(s) under the <i>Description</i> tab</li></ul></li>";
	  if (row["delayed_approval"] == "N") {
	    std::cout << "<li>agree to the Terms of Use and provide any additional required information</li><li>click the \"Update Profile\" button</li><li>return here to access the data</li>";
	  }
	  else {
	    std::cout << "<li>fill out and submit the registration form</li><li>if you are granted access, you will be notified by email; at that time, you can return here and access the data</li></p>";
	  }
	  std::cout << "</ul></p>" << std::endl;
	  exit(1);
	}
	else {
	  if (stat("/glade/p/rda/data",&buf) == 0) {
	    print_not_authorized();
	  }
	  else {
	    print_server_trouble();
	  }
	}
    }
    else {
// anonymous user
	std::cout << "Content-type: text/html" << std::endl;
	std::cout << "Status: 401 Unauthorized" << std::endl;
	std::cout << std::endl;
	std::cout << "<span class=\"fs20px bold\">Authorization Required</span>" << std::endl;
	std::cout << "<p>You must be a registered data user and be signed in to access this information.  To gain access, please sign in at the top of the page or register as a new user.  Once your registration is activated, you will be able to sign in and then you will be able to access this information.  (If there are additional qualifications to meet, you will be informed about what they are and you will need to meet those additional qualifications.)</p>" << std::endl;
	exit(1);
    }
  }
}

void get_web_data(std::string& webhome, std::string& locflag, SpanData& span_data)
{
  MySQL::PreparedStatement pstmt;
  std::string pstmt_error;
  if (!run_prepared_statement(server,"select dwebcnt, inet_access, locflag from dataset as d left join search.datasets as s on concat('ds',s.dsid) = d.dsid where s.dsid = ?",std::vector<enum_field_types>{MYSQL_TYPE_STRING},std::vector<std::string>{dsnum},pstmt,pstmt_error)) {
    print_server_trouble();
  }
  else if (pstmt.num_rows() == 0) {
    print_bad_input();
  }
  MySQL::Row row;
  pstmt.fetch_row(row);
  if (row["dwebcnt"] != "0" && row["inet_access"] == "Y") {
    webhome="/data/ds"+dsnum;
    locflag = row["locflag"];

    // web file listing
    ++span_data.data_file_downloads;

    // globus transfer request is not available for Object Store files
    if (locflag != "O") {
      ++span_data.data_file_downloads;
    }

    // glade holdings
    ++span_data.ncar_only_access;
  }
}

void build_matrix(std::string doc_root,std::string webhome, std::string locflag, std::string ruser,std::string access_type,SpanData& span_data,std::stringstream& matrix_ss)
{
  matrix_ss << "Content-type: text/html" << std::endl << std::endl;
  if (span_data.num_columns() == 0) {
//    matrix_ss << "<p>This dataset contains data files that are not currently publicly accessible. For assistance, please submit a request on the <a href=\"https://helpdesk.ucar.edu/plugins/servlet/desk/portal/6\">RDA Support Portal</a> for access to the data in this dataset. Be sure to include the dataset title in your request.</p>";
matrix_ss << "<p>This dataset contains data files that are not currently publicly accessible. For assistance, please submit a request to <a href=\"mailto:rdahelp@ucar.edu\">rdahelp@ucar.edu</a> for access to the data in this dataset. Be sure to include the dataset title in your request.</p>";
    return;
  }
  matrix_ss << "<link rel=\"stylesheet\" type=\"text/css\" href=\"/css/matrix.css\" />" << std::endl;
  if (!webhome.empty()) {
    matrix_ss << "<script id=\"globus_script\" src=\"/js/rda_globus.js\" type=\"text/javascript\"></script>" << std::endl;
  }
  std::string format_conversion,format_conversion_url;
  MySQL::PreparedStatement pstmt;
  std::string pstmt_error;
  if (run_prepared_statement(server,"select rqsttype,url from rcrqst where dsid = concat('ds',?) and (rqsttype = 'F' or rqsttype = 'H')",std::vector<enum_field_types>{MYSQL_TYPE_STRING},std::vector<std::string>{dsnum},pstmt,pstmt_error)) {
    if (pstmt.num_rows() > 0 || dsnum == "999.9") { 
	MySQL::Row row;
	if (pstmt.num_rows() == 1 && pstmt.fetch_row(row)) {
	  format_conversion=row["rqsttype"];
	  format_conversion_url=row["url"];
	  strutils::replace_all(format_conversion_url,"http://rda.ucar.edu","");
	}
	else {
	  format_conversion="XXX";
	}
	++span_data.data_file_downloads;
    }
  }
  auto has_hpss_staged_access=false;
  if ((run_prepared_statement(server,"select rqsttype from rcrqst where dsid = concat('ds',?) and rqsttype = 'M'",std::vector<enum_field_types>{MYSQL_TYPE_STRING},std::vector<std::string>{dsnum},pstmt,pstmt_error) && pstmt.num_rows() > 0) || dsnum == "999.9") {
    has_hpss_staged_access=true;
    ++span_data.data_file_downloads;
  }
  std::string subset_url,dap_url;
  enum class DAPURLType {unspecified,THREDDS,OPeNDAP};
  DAPURLType dap_url_type=DAPURLType::unspecified;
  if (run_prepared_statement(server,"select url,rqsttype from rcrqst where dsid = concat('ds',?) and gindex = 0",std::vector<enum_field_types>{MYSQL_TYPE_STRING},std::vector<std::string>{dsnum},pstmt,pstmt_error)) {
    for (const auto& row : pstmt) {
	if (!row["url"].empty()) {
	  if (row["rqsttype"] == "S" || row["rqsttype"] == "T") {
	    subset_url=row["url"];
	    strutils::replace_all(subset_url,"http://rda.ucar.edu","");
	  }
	  else if (row["rqsttype"] == "N") {
	    dap_url=row["url"];
	    strutils::replace_all(dap_url,"http://rda.ucar.edu","");
	    strutils::replace_all(dap_url,"/datasets/ds"+dsnum,"");
	    if (strutils::contains(dap_url,"/thredds")) {
		dap_url_type=DAPURLType::THREDDS;
	    }
	    else if (strutils::contains(dap_url,"getOPeNDAP")) {
		dap_url_type=DAPURLType::OPeNDAP;
	    }
	  }
	}
    }
  }
  if (dsnum == "999.9") {
    subset_url="XXX";
    dap_url="XXX";
  }
  if (access_type != "g") {
    matrix_ss << "<script id=\"tou_script\" language=\"javascript\">" << std::endl;
    matrix_ss << "function popTOU() {" << std::endl;
    matrix_ss << "  var h=getContentFromSynchronousRequest('tou_html','/data_tous/" << access_type << "_tou.html');" << std::endl;
    matrix_ss << "  var idx1=h.indexOf(\"<span\");" << std::endl;
    matrix_ss << "  var idx2=h.indexOf(\"<form\");" << std::endl;
    matrix_ss << "  h=h.substring(idx1,idx2);" << std::endl;
    matrix_ss << "  popModalWindowWithHTML(h,640,480);" << std::endl;
    matrix_ss << "}" << std::endl;
    matrix_ss << "</script>" << std::endl;
    matrix_ss << "<p><img src=\"/images/alert.gif\" width=\"16\" height=\"16\" />&nbsp;You have previously agreed to the access and use restrictions for this dataset.  You may review them <a href=\"javascript:void(0)\" onClick=\"popTOU()\"><strong>here</strong></a>.</p>" << std::endl;
  }
  matrix_ss << "<center><div style=\"width: 980px\">" << std::endl;
  matrix_ss << "<span style=\"font-size: 14px\">Mouse over the table headings for detailed descriptions</span>" << std::endl;
  std::unordered_map<std::string,GroupControlEntry> group_control_table;
  auto found_group_subset_url=false;
  auto found_group_dap_url=false;
  auto found_group_hpss_staged_access=false;
  auto found_group_format_conversion=false;
  MySQL::PreparedStatement group_pstmt;
  if (run_prepared_statement(server,"select gindex,title,dwebcnt,nwebcnt,mnote,wnote,grpid from dsgroup where dsid = concat('ds',?) and pindex = 0 and (dwebcnt > 0 or nwebcnt > 0) order by gindex",std::vector<enum_field_types>{MYSQL_TYPE_STRING},std::vector<std::string>{dsnum},group_pstmt,pstmt_error) && group_pstmt.num_rows() > 1) {
    if (run_prepared_statement(server,"select gindex,rqsttype,url from rcrqst where dsid = concat('ds',?) and gindex != 0",std::vector<enum_field_types>{MYSQL_TYPE_STRING},std::vector<std::string>{dsnum},pstmt,pstmt_error) && pstmt.num_rows() > 0) {
	for (const auto& row : pstmt) {
	  if (group_control_table.find(row["gindex"]) == group_control_table.end()) {
	    group_control_table[row["gindex"]].data.reset(new GroupControlEntry::Data);
	  }
	  GroupControlEntry gce;
	  gce=group_control_table[row["gindex"]];
	  auto rqsttype=row["rqsttype"];
	  if (rqsttype == "S" || rqsttype == "T") {
	    gce.data->subset_url=row["url"];
	    strutils::replace_all(gce.data->subset_url,"http://rda.ucar.edu","");
	    found_group_subset_url=true;
	  }
	  else if (rqsttype == "N") {
	    gce.data->dap_url=row["url"];
	    strutils::replace_all(gce.data->dap_url,"http://rda.ucar.edu","");
	    strutils::replace_all(gce.data->dap_url,"/datasets/ds"+dsnum,"");
	    found_group_dap_url=true;
	    if (strutils::contains(gce.data->dap_url,"/thredds")) {
		dap_url_type=DAPURLType::THREDDS;
	    }
	    else if (strutils::contains(gce.data->dap_url,"getOPeNDAP")) {
		dap_url_type=DAPURLType::OPeNDAP;
	    }
	  }
	  else if (rqsttype == "M") {
	    gce.data->has_hpss_staged_access=true;
	    found_group_hpss_staged_access=true;
	  }
	  else if (rqsttype == "F") {
	    gce.data->format_conversion="F";
	    gce.data->format_conversion_url=row["url"];
	    strutils::replace_all(gce.data->format_conversion_url,"http://rda.ucar.edu","");
	    found_group_format_conversion=true;
	  }
	  else if (rqsttype == "H" && gce.data->format_conversion.empty()) {
	    gce.data->format_conversion="H";
	    gce.data->format_conversion_url=row["url"];
	    strutils::replace_all(gce.data->format_conversion_url,"http://rda.ucar.edu","");
	    found_group_format_conversion=true;
	  }
	}
    }
  }
  matrix_ss << "<table style=\"margin-top: 5px\" cellspacing=\"0\" cellpadding=\"5\" border=\"0\">" << std::endl;
// header rows
  matrix_ss << "<tr>";
  bool no_left_border_yet;
  if (group_pstmt.num_rows() > 1) {
    matrix_ss << "<th width=\"40%\" class=\"thick-border-top thick-border-left thick-border-right\" style=\"background-color: #b5ceff\" colspan=\"2\">&nbsp;Data Description</th>";
    no_left_border_yet=false;
  }
  else {
    no_left_border_yet=true;
  }
  if (!webhome.empty() || !format_conversion.empty() || has_hpss_staged_access) {
    matrix_ss << "<th class=\"thick-border-top";
    if (no_left_border_yet) {
	matrix_ss << " thick-border-left";
	no_left_border_yet=false;
    }
    matrix_ss << " thick-border-right\" style=\"background-color: #effee1\" colspan=\"" << span_data.data_file_downloads << "\">Data File Downloads</th>";
  }
  if (!subset_url.empty() || found_group_subset_url) {
    matrix_ss << "<th class=\"thick-border-top";
    if (no_left_border_yet) {
	matrix_ss << " thick-border-left";
	no_left_border_yet=false;
    }
    matrix_ss << " thick-border-right\" style=\"background-color: #f0f7ff\">Customizable Data Requests</th>";
  }
  if (!dap_url.empty() || found_group_dap_url) {
    matrix_ss << "<th class=\"thick-border-top";
    if (no_left_border_yet) {
	matrix_ss << " thick-border-left";
	no_left_border_yet=false;
    }
    matrix_ss << " thick-border-right\" style=\"background-color: #c6f0ff\">Other Access Methods</th>";
  }
  if (span_data.ncar_only_access > 0) {
    matrix_ss << "<th>&nbsp;</th><th class=\"thick-border-top thick-border-left thick-border-right\" style=\"background-color: #b8edab\" colspan=\"" << span_data.ncar_only_access << "\">NCAR-Only Access</th>";
  }
  matrix_ss << "</tr>" << std::endl;
  matrix_ss << "<tr>";
  if (group_pstmt.num_rows() > 1) {
    matrix_ss << "<th class=\"thick-border-left thick-border-right\" style=\"background-color: #b5ceff\" colspan=\"2\">&nbsp;</th>";
    no_left_border_yet=false;
  }
  else {
    no_left_border_yet=true;
  }
  if (!webhome.empty() || !format_conversion.empty() || has_hpss_staged_access) {
    if (!webhome.empty()) {
	matrix_ss << "<th class=\"thin-border-top";
	if (no_left_border_yet) {
	  matrix_ss << " thick-border-left";
	  no_left_border_yet=false;
	}
	if (locflag == "O" && format_conversion.empty() && !has_hpss_staged_access) {
	  matrix_ss << " thick-border-right";
	} else {
	  matrix_ss << " thin-border-right";
	}
	matrix_ss << "\" style=\"background-color: #effee1\"><div style=\"position: relative\" onMouseOver=\"popInfo(this,'iwebhold',null,'center-30','bottom+10')\" onMouseOut=\"hideInfo('iwebhold')\"><span style=\"border-bottom: black 1px dashed; cursor: pointer\">Web Server Holdings</span></div></th>";
	if (locflag != "O") {
	  matrix_ss << "<th class=\"thin-border-top";
	  if (format_conversion.empty() && !has_hpss_staged_access) {
	    matrix_ss << " thick-border-right";
	  }
	  else {
	    matrix_ss << " thin-border-right";
	  }
	  matrix_ss << "\" style=\"background-color: #effee1\"><div style=\"position: relative\" onMouseOver=\"popInfo(this,'iglobus',null,'center-30','bottom+10')\" onMouseOut=\"hideInfo('iglobus')\"><span style=\"border-bottom: black 1px dashed; cursor: pointer\">Globus Transfer Service (GridFTP)</span></div></th>";
	}
    }
    if (!format_conversion.empty()) {
	matrix_ss << "<th class=\"thin-border-top";
	if (no_left_border_yet) {
	  matrix_ss << " thick-border-left";
	  no_left_border_yet=false;
	}
	if (!has_hpss_staged_access) {
	  matrix_ss << " thick-border-right";
	}
	else {
	  matrix_ss << " thin-border-right";
	}
	matrix_ss << "\" style=\"background-color: #effee1\"><div style=\"position: relative\" onMouseOver=\"popInfo(this,'ifmtconv',null,'center-30','bottom+10')\" onMouseOut=\"hideInfo('ifmtconv')\"><span style=\"border-bottom: black 1px dashed; cursor: pointer\">Data Format Conversion</span></div></th>";
    }
    if (has_hpss_staged_access) {
	matrix_ss << "<th class=\"thin-border-top";
	if (no_left_border_yet) {
	  matrix_ss << " thick-border-left";
	  no_left_border_yet=false;
	}
	matrix_ss << " thick-border-right\" style=\"background-color: #effee1\"><div style=\"position: relative\" onMouseOver=\"popInfo(this,'istaged',null,'center-30','bottom+10')\" onMouseOut=\"hideInfo('istaged')\"><span style=\"border-bottom: black 1px dashed; cursor: pointer\">Staged Access from Tape Archive</span></div></th>";
    }
  }
  if (!subset_url.empty() || found_group_subset_url) {
    matrix_ss << "<th class=\"thin-border-top";
    if (no_left_border_yet) {
	matrix_ss << " thick-border-left";
	no_left_border_yet=false;
    }
    matrix_ss << " thick-border-right\" style=\"background-color: #f0f7ff\"><div style=\"position: relative\" onMouseOver=\"popInfo(this,'isubset',null,'center-30','bottom+10')\" onMouseOut=\"hideInfo('isubset')\"><span style=\"border-bottom: black 1px dashed; cursor: pointer\">Subsetting</span></div></th>";
  }
  if (!dap_url.empty() || found_group_dap_url) {
    matrix_ss << "<th class=\"thin-border-top";
    if (no_left_border_yet) {
	matrix_ss << " thick-border-left";
	no_left_border_yet=false;
    }
    matrix_ss << " thick-border-right\" style=\"background-color: #c6f0ff\"><div style=\"position: relative\" onMouseOver=\"popInfo(this,'idap',null,'center-30','bottom+10')\" onMouseOut=\"hideInfo('idap')\"><span style=\"border-bottom: black 1px dashed; cursor: pointer\">";
    if (dap_url_type == DAPURLType::THREDDS) {
	matrix_ss << "THREDDS Data<br />Server";
    }
    else if (dap_url_type == DAPURLType::OPeNDAP) {
	matrix_ss << "OPeNDAP<br />Access";
    }
    matrix_ss << "</span></div></th>";
  }
  matrix_ss << "<th>&nbsp;</th>";
  if (!webhome.empty()) {
    matrix_ss << "<th class=\"thin-border-top thick-border-left thick-border-right\" style=\"background-color: #b8edab\"><div style=\"position: relative\" onMouseOver=\"popInfo(this,'iglade',null,'rcenter+30','bottom+10')\" onMouseOut=\"hideInfo('iglade')\"><span style=\"border-bottom: black 1px dashed; cursor: pointer\">Central File System (GLADE) Holdings</span></div></th>";
  }
  matrix_ss << "</tr>" << std::endl;
// row for the full dataset
  matrix_ss << "<tr>";
  if (group_pstmt.num_rows() > 1) {
    matrix_ss << "<th height=\"50\" class=\"thick-border-top thick-border-left thick-border-right thin-border-bottom\" style=\"background-color: #b5ceff\" colspan=\"2\">Union of Available Products</th>";
    no_left_border_yet=false;
  }
  else {
    no_left_border_yet=true;
  }
  auto found_hpss_cache_file=false;
  std::vector<std::string> hpss_dblist{"GrML","ObML","FixML","SatML"};
  for (const auto& db : hpss_dblist) {
    struct stat buf;
    if (stat((doc_root+"/datasets/ds"+dsnum+"/metadata/customize."+db).c_str(),&buf) == 0) {
	found_hpss_cache_file=true;
	break;
    }
  }
  std::vector<std::string> Web_dblist{"WGrML","WObML","WFixML"};
  auto web_it=Web_dblist.begin(),web_end=Web_dblist.end();
  if (!webhome.empty() || !format_conversion.empty() || has_hpss_staged_access) {
    if (!webhome.empty()) {
	matrix_ss << "<td align=\"center\" class=\"thick-border-top";
	if (no_left_border_yet) {
	  matrix_ss << " thick-border-left";
	  no_left_border_yet=false;
	}
	if (locflag == "O" && format_conversion.empty() && !has_hpss_staged_access) {
	  matrix_ss << " thick-border-right";
	} else {
	  matrix_ss << " thin-border-right";
	}
	matrix_ss << " thin-border-bottom\" style=\"background-color: #effee1\">";
//	if (!webhome.empty()) {
	  for (; web_it != web_end; ++web_it) {
	    struct stat buf;
	    if (stat((doc_root+"/datasets/ds"+dsnum+"/metadata/customize."+*web_it).c_str(),&buf) == 0) {
		break;
	    }
	  }
	  matrix_ss << "<a class=\"matrix\" href=\"/datasets/ds" << dsnum << "/index.html#!";
	  if (web_it != web_end) {
	    matrix_ss << "cgi-bin/datasets/getWebList?dsnum=" << dsnum;
	  }
	  else {
	    matrix_ss << "sfol-wl-" << webhome;
	  }
	  matrix_ss << "\">Web File<br />Listing</a>";
//	}
//	else {
//	  matrix_ss << "&nbsp;";
//	}
	matrix_ss << "</td>";
	if (locflag != "O") {
	  matrix_ss << "<td align=\"center\" class=\"thick-border-top";
	  if (format_conversion.empty() && !has_hpss_staged_access) {
	    matrix_ss << " thick-border-right";
	  }
	  else {
	    matrix_ss << " thin-border-right";
	  }
	  matrix_ss << " thin-border-bottom\" style=\"background-color: #effee1\"><a class=\"matrix\" ";
	  MySQL::Row row;
	  if (run_prepared_statement(server,"select globus_url from goshare where email = '"+ruser+"' and dsid = concat('ds',?) and status = 'ACTIVE'",std::vector<enum_field_types>{MYSQL_TYPE_STRING},std::vector<std::string>{dsnum},pstmt,pstmt_error) && pstmt.fetch_row(row)) {
	    matrix_ss << "href=\"" << row[0] << "\" target=\"_globus\">Globus Transfer";
	  }
	  else {
	    matrix_ss << "href=\"javascript:void(0)\" onClick=\"requestGlobusInvite(2,undefined,'ds" << dsnum << "');win.onunload=function(){ win.opener.location='/datasets/ds" << dsnum << "/index.html#!access?r='+Math.random(); }\">Request Globus Transfer";
	  }
	  matrix_ss << "</a></td>";
	}
    }
    if (!format_conversion.empty()) {
	matrix_ss << "<td align=\"center\" class=\"thick-border-top";
	if (no_left_border_yet) {
	  matrix_ss << " thick-border-left";
	  no_left_border_yet=false;
	}
	if (!has_hpss_staged_access) {
	  matrix_ss << " thick-border-right";
	}
	else {
	  matrix_ss << " thin-border-right";
	}
	matrix_ss << " thin-border-bottom\" style=\"background-color: #effee1\">";
	if (format_conversion != "XXX") {
	  matrix_ss << "<a class=\"matrix\" href=\"";
	  if (!format_conversion_url.empty()) {
	    matrix_ss << ajaxify_url(format_conversion_url);
	  }
	  else {
	    matrix_ss << "/datasets/ds" << dsnum << "/index.html#!sfol-f";
	    if (format_conversion == "F") {
		matrix_ss << "w";
	    }
	    else if (format_conversion == "H") {
		matrix_ss << "h";
	    }
	  }
	  matrix_ss << "\">Get Converted<br />Files</a>";
	}
	else if (dsnum == "999.9") {
	  matrix_ss << "<a class=\"matrix\" href=\"javascript:void(0)\">Get Converted<br />Files</a>";
	}
	else {
	  matrix_ss << "&nbsp;";
	}
	matrix_ss << "</td>";
    }
    if (has_hpss_staged_access) {
	matrix_ss << "<td align=\"center\" class=\"thick-border-top";
	if (no_left_border_yet) {
	  matrix_ss << " thick-border-left";
	  no_left_border_yet=false;
	}
	matrix_ss << " thick-border-right thin-border-bottom\" style=\"background-color: #effee1\"><a class=\"matrix\" href=\"#!";
	if (found_hpss_cache_file) {
	  matrix_ss << "cgi-bin/datasets/getMssList?dsnum=" << dsnum << "&disp=hr";
	}
	else {
	  matrix_ss << "sfol-hr";
	}
	matrix_ss << "\">Request<br />Access</a></td>";
    }
  }
  if (!subset_url.empty() || found_group_subset_url) {
    matrix_ss << "<td align=\"center\" class=\"thick-border-top";
    if (no_left_border_yet) {
	matrix_ss << " thick-border-left";
	no_left_border_yet=false;
    }
    matrix_ss << " thick-border-right thin-border-bottom\" style=\"background-color: #f0f7ff\">";
    if (!subset_url.empty()) {
	matrix_ss << "<a class=\"matrix\" href=\"" << ajaxify_url(subset_url) << "\">Get a<br />Subset</a>";
    }
    else {
	matrix_ss << "&nbsp;";
    }
    matrix_ss << "</td>";
  }
  if (!dap_url.empty() || found_group_dap_url) {
    matrix_ss << "<td align=\"center\" class=\"thick-border-top";
    if (no_left_border_yet) {
	matrix_ss << " thick-border-left";
	no_left_border_yet=false;
    }
    matrix_ss << " thick-border-right thin-border-bottom\" style=\"background-color: #c6f0ff\">";
    if (!dap_url.empty()) {
	if (dap_url_type == DAPURLType::THREDDS) {
	  matrix_ss << "<a class=\"matrix\" href=\"" << ajaxify_url(dap_url) << "\">TDS<br />Access</a>";
	}
	else if (dap_url_type == DAPURLType::OPeNDAP) {
	  matrix_ss << "<a class=\"matrix\" href=\"" << ajaxify_url(dap_url) << "\">Get OPeNDAP<br />Access</a>";
	}
    }
    else {
	matrix_ss << "&nbsp;";
    }
    matrix_ss << "</td>";
  }
  matrix_ss << "<td>&nbsp;</td>";
  if (!webhome.empty()) {
    matrix_ss << "<td align=\"center\" class=\"thick-border-top thick-border-left thin-border-bottom thick-border-right\" style=\"background-color: #b8edab\"><a class=\"matrix\" href=\"/datasets/ds" << dsnum << "/index.html#!";
    if (web_it != web_end) {
	matrix_ss << "cgi-bin/datasets/getGladeList?dsnum=" << dsnum;
    }
    else {
	matrix_ss << "sfol-gl";
    }
    matrix_ss << "\">GLADE File<br />Listing</a></td>";
  }
  matrix_ss << "</tr>" << std::endl;
// add the individual top-level groups
  if (group_pstmt.num_rows() > 1) {
    auto n=0;
    for (const auto& row : group_pstmt) {
	auto gindex=row["gindex"];
	GroupControlEntry gce;
	auto found_group_control=false;
	if (group_control_table.find(gindex) != group_control_table.end()) {
	  gce=group_control_table[gindex];
	  found_group_control=true;
	}
	matrix_ss << "<tr>";
	if (n == 0) {
	  matrix_ss << "<th class=\"thick-border-left thin-border-top\" style=\"background-color: #d5eaff\" rowspan=\"" << group_pstmt.num_rows() << "\"><span style=\"font-size: 14px\">&nbsp;P&nbsp;<br>R<br>O<br>D<br>U<br>C<br>T<br>S</span></th>";
	}
	matrix_ss << "<td height=\"90%\" class=\"thin-border-top thin-border-left thick-border-right\" style=\"background-color: #b5ceff\">";
	auto title=row["title"];
	if (title.empty()) {
	  title=row["grpid"];
	}
	if (row["dwebcnt"] != "0" && !row["wnote"].empty()) {
	  matrix_ss << "<div style=\"position: relative\" onMouseOver=\"popInfo(this,'inote" << gindex << "',null,'left','bottom+10')\" onMouseOut=\"hideInfo('inote" << gindex << "')\"><span style=\"border-bottom: black 1px dashed; cursor: pointer\">" << title << "</span></div><div id=\"inote" << gindex << "\" class=\"bubble-top-left-arrow\" style=\"width: 600px\">" << row["wnote"] << "</div>";
	}
	else {
	  matrix_ss << title;
	}
	matrix_ss << "</td>";
	if (!webhome.empty()) {
	  matrix_ss << "<td align=\"center\" class=\"thin-border-top";
	  if (locflag == "O" && format_conversion.empty() && !has_hpss_staged_access) {
	    matrix_ss << " thick-border-right";
	  } else {
	    matrix_ss << " thin-border-right";
	  }
	  matrix_ss << "\" style=\"background-color: #effee1\">";
	  if (row["dwebcnt"] != "0") {
	    web_it=Web_dblist.begin();
	    for (; web_it != web_end; ++web_it) {
		struct stat buf;
		if (stat((doc_root+"/datasets/ds"+dsnum+"/metadata/customize."+*web_it+"."+gindex).c_str(),&buf) == 0) {
		  break;
		}
	    }
	    matrix_ss << "<a class=\"matrix\" href=\"/datasets/ds" << dsnum << "/index.html#!";
	    if (web_it != web_end) {
		matrix_ss << "cgi-bin/datasets/getWebList?dsnum=" << dsnum << "&gindex=" << gindex;
	    }
	    else {
		matrix_ss << "sfol-wl-" << webhome << "?g=" << gindex;
	    }
	    matrix_ss << "\">Web File<br />Listing</a>";
	  }
	  else {
	    matrix_ss << "&nbsp;";
	  }
	  matrix_ss << "</td>";
	  if (locflag != "O") {
	    matrix_ss << "<td class=\"thin-border-top";
	    if (format_conversion.empty() && !has_hpss_staged_access) {
	      matrix_ss << " thick-border-right";
	    }
	    else {
	      matrix_ss << " thin-border-right";
	    }
	    matrix_ss << "\" style=\"background-color: #effee1\">&nbsp;</td>";
	  }
	}
	if (!format_conversion.empty()) {
	  matrix_ss << "<td align=\"center\" class=\"thin-border-top";
	  if (!has_hpss_staged_access) {
	    matrix_ss << " thick-border-right";
	  }
	  else {
	    matrix_ss << " thin-border-right";
	  }
	  matrix_ss << "\" style=\"background-color: #effee1\">";
	  if (!found_group_format_conversion || (found_group_control && !gce.data->format_conversion.empty())) {
	    matrix_ss << "<a class=\"matrix\" href=\"/datasets/ds" << dsnum << "/index.html#!sfol-f";
	    if (!found_group_format_conversion) {
		if (format_conversion == "F") {
		  matrix_ss << "w";
		}
		else if (format_conversion == "H") {
		  matrix_ss << "h";
		}
	    }
	    else {
		if (gce.data->format_conversion == "F") {
		  matrix_ss << "w";
		}
		else if (gce.data->format_conversion == "H") {
		  matrix_ss << "h";
		}
	    }
	    matrix_ss << "?g=" << gindex << "\">Get Converted<br />Files</a>";
	  }
	  else {
	    matrix_ss << "&nbsp;";
	  }
	  matrix_ss << "</td>";
	}
	if (has_hpss_staged_access) {
	  matrix_ss << "<td align=\"center\" class=\"thin-border-top thick-border-right\" style=\"background-color: #effee1\">";
	  if (!found_group_hpss_staged_access || (found_group_control && gce.data->has_hpss_staged_access)) {
	    matrix_ss << "<a class=\"matrix\" href=\"/datasets/ds" << dsnum << "/index.html#!" << "sfol-hr?g=" << gindex << "\">Request<br />Access</a>";
	  }
	  else {
	    matrix_ss << "&nbsp;";
	  }
	  matrix_ss << "</td>";
	}
	if (!subset_url.empty() || found_group_subset_url) {
	  matrix_ss << "<td align=\"center\" class=\"thin-border-top thick-border-right\" style=\"background-color: #f0f7ff\">";
	  if (found_group_control && !gce.data->subset_url.empty()) {
	    matrix_ss << "<a class=\"matrix\" href=\"" << ajaxify_url(gce.data->subset_url,"gindex="+gindex) << "\">Get a<br />Subset</a>";
	  }
	  else if (row["dwebcnt"] != "0" && !subset_url.empty()) {
	    std::vector<std::string> inv_dblist{"IGrML","IObML"};
	    auto inv_it=inv_dblist.begin(),inv_end=inv_dblist.end();
	    for (; inv_it != inv_end; ++inv_it) {
		struct stat buf;
		if (stat((doc_root+"/datasets/ds"+dsnum+"/metadata/customize."+*inv_it+"."+gindex).c_str(),&buf) == 0) {
		  break;
		}
	    }
	    if (inv_it != inv_end) {
		matrix_ss << "<a class=\"matrix\" href=\"" << ajaxify_url(subset_url,"gindex="+gindex) << "\">Get a<br />Subset</a>";
	    }
	    else {
		matrix_ss << "&nbsp;";
	    }
	  }
	  else {
	    matrix_ss << "&nbsp;";
	  }
	  matrix_ss << "</td>";
	}
	if (!dap_url.empty() || found_group_dap_url) {
	  matrix_ss << "<td align=\"center\" class=\"thin-border-top thick-border-right\" style=\"background-color: #c6f0ff\">";
	  if (found_group_control && !gce.data->dap_url.empty()) {
	    if (dap_url_type == DAPURLType::THREDDS) {
		matrix_ss << "<a class=\"matrix\" href=\"" << ajaxify_url(gce.data->dap_url) << "\">TDS<br />Access</a>";
	    }
	    else if (dap_url_type == DAPURLType::OPeNDAP) {
		matrix_ss << "<a class=\"matrix\" href=\"" << ajaxify_url(gce.data->dap_url) << "\">Get OPeNDAP<br />Access</a>";
	    }
	  }
	  else {
	    matrix_ss << "&nbsp;";
	  }
	  matrix_ss << "</td>";
	}
	matrix_ss << "<td>&nbsp;</td>";
	if (!webhome.empty()) {
	  matrix_ss << "<td align=\"center\" class=\"thin-border-top thick-border-left thick-border-right\" style=\"background-color: #b8edab\">";
	  if (row["dwebcnt"] != "0") {
	    if (web_it != web_end) {
		matrix_ss << "<a class=\"matrix\" href=\"/datasets/ds" << dsnum << "/index.html#!cgi-bin/datasets/getGladeList?dsnum=" << dsnum << "&gindex=" << gindex << "\">GLADE File<br />Listing</a>";
	    }
	    else {
		print_static_glade_list(gindex,matrix_ss);
	    }
	  }
	  else if (row["nwebcnt"] != "0") {
	    print_static_glade_list(gindex,matrix_ss);
	  }
	  else {
	    matrix_ss << "&nbsp;";
	  }
	}
	else if (row["nwebcnt"] != "0") {
	    print_static_glade_list(gindex,matrix_ss);
	}
	else {
	  matrix_ss << "&nbsp;";
	}
	matrix_ss << "</td>";
	matrix_ss << "</tr>" << std::endl;
	++n;
    }
  }
// add the bottom border to the matrix
  if (!subset_url.empty() || found_group_subset_url) {
    ++span_data.data_file_downloads;
  }
  if (!dap_url.empty() || found_group_dap_url) {
    ++span_data.data_file_downloads;
  }
  if (group_pstmt.num_rows() > 1) {
    span_data.data_file_downloads+=2;
    matrix_ss << "<tr><th class=\"thick-border-top\" colspan=\"" << span_data.data_file_downloads << "\">&nbsp;</th>";
    if (span_data.ncar_only_access > 0) {
	matrix_ss << "<th>&nbsp;</th><th class=\"thick-border-top\" colspan=\"" << span_data.ncar_only_access << "\">&nbsp;</th></tr>" << std::endl;
    }
  }
  else {
    matrix_ss << "<tr>";
    if (span_data.data_file_downloads > 0) {
	matrix_ss << "<th class=\"thin-border-top\" colspan=\"" << span_data.data_file_downloads << "\">&nbsp;</th>";
    }
    if (span_data.ncar_only_access > 0) {
	matrix_ss << "<th>&nbsp;</th><th class=\"thin-border-top\" colspan=\"" << span_data.ncar_only_access << "\">&nbsp;</th>";
    }
    matrix_ss << "</tr>" << std::endl;
  }
  matrix_ss << "</table>" << std::endl;
  matrix_ss << "</div></center>" << std::endl;
  matrix_ss << "<div id=\"iwebhold\" class=\"bubble-top-left-arrow\" style=\"width: 200px\">Download RDA data files from this web server.</div>" << std::endl;
  matrix_ss << "<div id=\"iglobus\" class=\"bubble-top-left-arrow\" style=\"width: 200px\">Transfer RDA data files to your computer via the Globus transfer service (GridFTP). See http://www.globus.org for more information.</div>" << std::endl;
  matrix_ss << "<div id=\"ifmtconv\" class=\"bubble-top-left-arrow\" style=\"width: 200px\">Request that RDA data files be converted from their native format to a new format (e.g. from GRIB to netCDF).</div>" << std::endl;
  matrix_ss << "<div id=\"istaged\" class=\"bubble-top-left-arrow\" style=\"width: 200px\">Request that RDA Tape Archive files be staged to disk for download.</div>" << std::endl;
  matrix_ss << "<div id=\"idap\" class=\"bubble-top-left-arrow\" style=\"width: 210px\">";
  if (dap_url_type == DAPURLType::THREDDS) {
    matrix_ss << "Access the data through additional protocols, including OPeNDAP.";
  }
  else if (dap_url_type == DAPURLType::OPeNDAP) {
    matrix_ss << "Create a customized subset that you can then access via OPeNDAP.";
  }
  matrix_ss << "</div>" << std::endl;
  matrix_ss << "<div id=\"isubset\" class=\"bubble-top-left-arrow\" style=\"width: 300px\">Make selections (e.g. temporal range, spatial area, parameters, etc.) to be extracted from the files in this dataset.  The option to receive the output in a different data format may also be available.</div>" << std::endl;
  matrix_ss << "<div id=\"iglade\" class=\"bubble-top-right-arrow\" style=\"width: 180px\">Find the locations of RDA holdings residing on GLADE disk.</div>" << std::endl;
  matrix_ss << "<div id=\"ihpss\" class=\"bubble-top-right-arrow\" style=\"width: 190px\">Find the locations of RDA holdings residing on NCAR's HPSS.</div>" << std::endl;
}

int main(int argc,char **argv)
{
// read the cgi-bin configuration
  if (!webutils::cgi::read_config(directives)) {
    web_error2("unable to read configuration","500 Internal Server Error");
  }
// parse the query string
  parse_query_string();
// connect to RDADB
  server.connect(directives.database_server,directives.rdadb_username,directives.rdadb_password,"dssdb");
  if (!server) {
    print_server_trouble();
  }
  auto doc_root=webutils::cgi::server_variable("DOCUMENT_ROOT");
  auto ruser=rda_username();
  std::string access_type;
// verify that the user is authorized to view the data access matrix
  verify_authorization(doc_root,ruser,access_type);
  std::string webhome, locflag;
  SpanData span_data;
// get information about web-accessible files
  get_web_data(webhome, locflag, span_data);
  std::stringstream matrix_ss;
// build the data access matrix
  build_matrix(doc_root,webhome, locflag, ruser,access_type,span_data,matrix_ss);
  server.disconnect();
// send the matrix as the response
  std::cout << matrix_ss.str();
}
