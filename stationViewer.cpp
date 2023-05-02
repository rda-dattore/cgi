#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <string>
#include <list>
#include <deque>
#include <MySQL.hpp>
#include <web/web.hpp>
#include <metadata.hpp>
#include <mymap.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <myerror.hpp>

using std::cout;
using std::endl;
using std::stof;
using std::stoi;
using std::string;
using strutils::append;
using strutils::replace_all;
using strutils::split;
using strutils::substitute;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
string myerror = "";
string mywarning = "";

struct GetDataEntry {
  GetDataEntry() : key(), script(), target(), nvp_list() { }

  string key;
  string script, target;
  std::list<string> nvp_list;
};

struct Local_Args {
  Local_Args() : dsnum2(), gindex(), opList(), start(), end(), ID(), tspec(),
      title(), info(), display(), sstn(), spart(), slat(), nlat(), wlon(), elon(),
      zl(), wlon_f(-999.), elon_f(999.9), getdata_methods(), getdata_table() { }

  string dsnum2, gindex;
  std::list<string> opList;
  string start, end;
  string ID, tspec;
  string title, info, display;
  string sstn, spart;
  string slat, nlat, wlon, elon, zl;
  float wlon_f, elon_f;
  std::list<string> getdata_methods;
  my::map<GetDataEntry> getdata_table;
} local_args;

string requestURI, script_url;
MySQL::Server server;

void parseQuery() {
  QueryString queryString(QueryString::POST);
  if (!queryString) {
    queryString.fill(QueryString::GET);
    if (!queryString) {
      web_error("no input");
    }
  } else {
    requestURI = getenv("REQUEST_URI");
    auto idx = requestURI.find("?");
    if (idx != string::npos) {
      script_url = requestURI.substr(0, idx);
    } else {
      script_url = requestURI;
    }
  }
  metautils::args.dsnum = queryString.value("dsnum");
  if (metautils::args.dsnum.empty()) {
    web_error("missing dataset number");
  }
  if (metautils::args.dsnum.length() != 5 || metautils::args.dsnum.find(".") != 3) {
    web_error("invalid dataset number");
  }
if (metautils::args.dsnum == "132.0" || metautils::args.dsnum == "337.0" || metautils::args.dsnum == "461.0") {
web_error("service unavailable");
}
  local_args.dsnum2 = substitute(metautils::args.dsnum, ".", "");
  local_args.gindex = queryString.value("gindex");
  local_args.opList = queryString.values("op");
  local_args.start = queryString.value("sd");
  replace_all(local_args.start, "-", "");
  local_args.end = queryString.value("ed");
  replace_all(local_args.end, "-", "");
  local_args.ID = queryString.value("ID");
  local_args.tspec = queryString.value("tspec");
  local_args.zl = queryString.value("zl");
  if (local_args.zl.empty()) {
    local_args.zl = "2";
  }
  local_args.getdata_methods = queryString.values("getdata");
  for (const auto& method : local_args.getdata_methods) {
    GetDataEntry gde;
    gde.key = method;
    if (!local_args.getdata_table.found(gde.key, gde)) {
      gde.script = queryString.value(gde.key + "_script");
      gde.target = queryString.value(gde.key + "_window");
      gde.nvp_list = queryString.values(gde.key + "_nvp");
      local_args.getdata_table.insert(gde);
    }
  }
  local_args.title = queryString.value("title");
  local_args.info = queryString.value("info");
  local_args.display = queryString.value("display");
  local_args.slat = queryString.value("slat");
  local_args.nlat = queryString.value("nlat");
  local_args.wlon = queryString.value("wlon");
  if (!local_args.wlon.empty()) {
    local_args.wlon_f = stof(local_args.wlon);
  } else {
    local_args.wlon_f = -999.;
  }
  local_args.elon = queryString.value("elon");
  if (!local_args.elon.empty()) {
    local_args.elon_f = stof(local_args.elon);
  } else {
    local_args.elon_f = 999.;
  }
  local_args.sstn = queryString.value("sstn");
  local_args.spart = queryString.value("spart");
}

struct PlatformEntry {
  struct Data {
    Data() : description() {}

    string description;
  };
  PlatformEntry() : key(),data(nullptr) {}

  string key;
  std::shared_ptr<Data> data;
};
struct ObservationTypeEntry {
  struct Data {
    Data() : description(),platform_table() {}

    string description;
    my::map<PlatformEntry> platform_table;
  };
  ObservationTypeEntry() : key(),data(nullptr) {}

  string key;
  std::shared_ptr<Data> data;
};

void start()
{
  std::ifstream ifs;
  char line[256];
  MySQL::Query query;
  MySQL::Row row;
  string sline,db;
  std::deque<string> sp;
  size_t n;
  string start="1000-01-01",end="3000-12-31";
  my::map<ObservationTypeEntry> obs_table;
  ObservationTypeEntry ote;
  PlatformEntry pe;
  bool started;
  GetDataEntry gde;
  string referer,server_name=getenv("SERVER_NAME");

  db="ObML";
  if (local_args.getdata_table.size() > 0) {
    if (duser().empty()) {
      referer=getenv("HTTP_REFERER");
      if (!referer.empty()) {
        replace_all(referer,"https://","");
        replace_all(referer,"http://","");
        replace_all(referer,"rda.ucar.edu","");
        cout << "Location: /cgi-bin/error?code=403&directory="+referer << endl << endl;
      }
      else
        cout << "Location: /cgi-bin/error?code=403&directory="+requestURI << endl << endl;
      exit(1);
    }
    db="W"+db;
  }
  ifs.open((strutils::token("/"+unixutils::host_name(),".",0)+"/web/datasets/ds"+metautils::args.dsnum+"/metadata/customize."+db).c_str());
  if (ifs.is_open()) {
    ifs.getline(line,256);
    sp=split(line);
    start=dateutils::string_ll_to_date_string(sp[0]);
    end=dateutils::string_ll_to_date_string(sp[1]);
    ifs.close();
  }
  query.set("select i.observationType_code,o.obs_type,i.platformType_code,p.platform_type from (select distinct observationType_code,platformType_code from "+db+".ds"+local_args.dsnum2+"_IDList2) as i left join "+db+".obs_types as o on o.code = i.observationType_code left join "+db+".platform_types as p on p.code = i.platformType_code order by o.obs_type,p.platform_type");
  if (query.submit(server) < 0) {
    std::cerr << "STATIONVIEWER: error: '" << query.error() << "' for query '" <<
        query.show() << "'" << endl;
    web_error("A database error occurred. Please try again later.");
  }
  while (query.fetch_row(row)) {
    if (!obs_table.found(row[0],ote)) {
      ote.key=row[0];
      ote.data.reset(new ObservationTypeEntry::Data);
      ote.data->description=row[1];
      obs_table.insert(ote);
    }
    if (!ote.data->platform_table.found(row[2],pe)) {
      pe.key=row[2];
      pe.data.reset(new PlatformEntry::Data);
      pe.data->description=row[3];
      ote.data->platform_table.insert(pe);
    }
  }
  cout << "Access-Control-Allow-Origin: https://rda.ucar.edu" << endl;
  cout << "Content-type: text/html" << endl << endl;
  cout << "<script id=\"gmap_script\" src=\"/js/gmaps3.js\" type=\"text/javascript\"></script>" << endl;
  cout << webutils::php_execute("$mapType=\"Cluster\";$mapDivId=\"map\";$controlSize=\"large\";$zoomLevel=2;include(\"gmap3_key.inc\");") << endl;
  cout << "<script id=\"calendar_script\" src=\"/js/calendar.js\" type=\"text/javascript\"></script>" << endl;
  cout << "<link rel=\"stylesheet\" type=\"text/css\" href=\"/css/calendar.css\">" << endl;
  cout << "<script id=\"map_script\" language=\"javascript\">" << endl;
  cout << "var dsnum='"+metautils::args.dsnum+"';" << endl;
  cout << "var start='"+start+"';" << endl;
  cout << "var end='"+end+"';" << endl;
  cout << "var op,qs;" << endl;
  cout << "function doMapUpdate() {" << endl;
  cout << "  op='';" << endl;
  cout << "  qs='';" << endl;
  cout << "  var num_boxes=0,num_checked=0;" << endl;
  cout << "  for (n=0; n < document.selections.elements.length; n++) {" << endl;
  cout << "    if (document.selections.elements[n].type == 'checkbox') {" << endl;
  cout << "      num_boxes++;" << endl;
  cout << "      if (document.selections.elements[n].checked) {" << endl;
  cout << "        if (op.length > 0)" << endl;
  cout << "          op+='&';" << endl;
  cout << "        op+='op='+document.selections.elements[n].value;" << endl;
  cout << "        num_checked++;" << endl;
  cout << "      }" << endl;
  cout << "    }" << endl;
  cout << "  }" << endl;
  cout << "  var re_date=/^\\s*(\\d{4})\\-(\\d{2})\\-(\\d{2})\\s*$/;" << endl;
  cout << "  if (!re_date.exec(document.selections.sd.value) || !re_date.exec(document.selections.ed.value))" << endl;
  cout << "    return alert(\"Dates must be entered as YYYY-MM-DD\");" << endl;
  cout << "  if (document.selections.ed.value < document.selections.sd.value)" << endl;
  cout << "    return alert(\"The end date cannot precede the start date\");" << endl;
  cout << "  if (num_checked == 0)" << endl;
  cout << "    alert(\"You must choose at least one observation type/platform type for which to display stations\");" << endl;
  cout << "  else {" << endl;
  cout << "    if (num_boxes != num_checked) {" << endl;
  cout << "      if (qs.length > 0)" << endl;
  cout << "        qs+='&';" << endl;
  cout << "      qs+=op;" << endl;
  cout << "    }" << endl;
  cout << "    if (document.selections.sd.value != start || document.selections.ed.value != end || !document.selections.tspec[0].checked) {" << endl;
  cout << "      if (qs.length > 0)" << endl;
  cout << "        qs+='&';" << endl;
  cout << "      qs+='sd='+document.selections.sd.value+'&ed='+document.selections.ed.value;" << endl;
  cout << "      for (n=0; n < document.selections.tspec.length; n++) {" << endl;
  cout << "        if (document.selections.tspec[n].checked)" << endl;
  cout << "          qs+='&tspec='+document.selections.tspec[n].value;" << endl;
  cout << "      }" << endl;
  cout << "    }" << endl;
  cout << "    if (qs.length == 0)" << endl;
  cout << "      qs='x=y';" << endl;
  cout << "    qs+='&slat='+Math.round(map.handles.marker.getBounds().getSouthWest().lat())+'&wlon='+Math.round(map.handles.marker.getBounds().getSouthWest().lng())+'&nlat='+Math.round(map.handles.marker.getBounds().getNorthEast().lat())+'&elon='+Math.round(map.handles.marker.getBounds().getNorthEast().lng())+'&zl='+map.handles.marker.getZoom();" << endl;
  cout << "    updateMap('https://" << server_name << script_url << "?dsnum=" << metautils::args.dsnum << "&gindex=" << local_args.gindex << "&'+qs,marker_click_function);" << endl;
  cout << "  }" << endl;
//  cout << "  scrollTo('viewer_top');" << endl;
  cout << "}" << endl;
  cout << "function doMapUpdate1() {" << endl;
  cout << "  if (document.sstation.sstn.value.length == 0) {" << endl;
  cout << "    alert(\"You must enter a value for 'Station ID' if you want to search for specific stations\");" << endl;
  cout << "    return;" << endl;
  cout << "  }" << endl;
  cout << "  var spart_val;" << endl;
  cout << "  for (n=0; n < document.sstation.spart.length; n++) {" << endl;
  cout << "    if (document.sstation.spart[n].checked)" << endl;
  cout << "      spart_val=document.sstation.spart[n].value;" << endl;
  cout << "  }" << endl;
  cout << "  var protocol='http';" << endl;
  cout << "  if (window.location.href.charAt(4) == 's') {" << endl;
  cout << "    protocol+='s';" << endl;
  cout << "  }" << endl;
  cout << "  updateMap(protocol+'://" << server_name << script_url << "?dsnum=" << metautils::args.dsnum << "&gindex=" << local_args.gindex << "&sstn='+document.sstation.sstn.value+'&spart='+spart_val,marker_click_function);" << endl;
//  cout << "  scrollTo('viewer_top');" << endl;
  cout << "  return false;" << endl;
  cout << "}" << endl;
  cout << "function fillStationInfoWindow() {" << endl;
  cout << "  if (xhr.readyState == 4) {" << endl;
  cout << "    var data=eval('('+xhr.responseText+')');" << endl;
  cout << "    var content='<small>';" << endl;
  cout << "    for (n=0; n < data.length; ++n) {" << endl;
  cout << "      if (n > 0) {" << endl;
  cout << "        content+='<hr noshade />';" << endl;
  cout << "      }" << endl;
  cout << "      content+='<b>ID: </b>'+data[n].ID+'<br /><b>ID Type: </b>'+data[n].t+'<br /><b>Temporal range: </b>'+data[n].sd+' to '+data[n].ed+'<br /><b>Total number of observations: </b>'+data[n].n;" << endl;
  cout << "      if (typeof(data[n].f) != \"undefined\") {" << endl;
  cout << "          content+='<br /><b>Approximate frequency: </b>'+data[n].f+' per '+data[n].u;" << endl;
  cout << "      }" << endl;
  if (local_args.getdata_table.size() > 0)
    cout << "      content+='<table class=small cellspacing=0 cellpadding=0 border=0><tr valign=top><td><b>Data:</b>&nbsp;</td><td>";
  n=0;
  for (auto method : local_args.getdata_methods) {
    if (n > 0) {
      cout << "<br />";
    }
    local_args.getdata_table.found(method,gde);
    cout << "<a href=\""+gde.script+"?station='+data[n].ID+'&sd='+document.selections.sd.value+'&ed='+document.selections.ed.value+'";
    for (auto item : gde.nvp_list) {
      sp=split(item,"=");
      if (sp.size() == 2)
        cout << "&"+sp[0]+"="+sp[1];
    }
    cout << "\"";
    if (gde.target == "new")
      cout << " target=\"_"+strutils::to_lower(substitute(gde.script," ","_"))+"\"";
    cout << ">"+method+"</a>";
    n++;
  }
  if (local_args.getdata_table.size() > 0) {
    cout << "</td></tr></table>';" << endl;
  }
  cout << "    }" << endl;
  cout << "    content+='</small>';" << endl;
  cout << "    infoWindow.setContent(content);" << endl;
  cout << "    if (typeof(data[0].b) != \"undefined\") {" << endl;
  cout << "        var start=new google.maps.LatLng(data[0].b[0],data[0].b[1]);" << endl;
  cout << "        var coords=[start,new google.maps.LatLng(data[0].b[2],data[0].b[1]),new google.maps.LatLng(data[0].b[2],data[0].b[3]),new google.maps.LatLng(data[0].b[0],data[0].b[3]),start];" << endl;
  cout << "        box=new google.maps.Polygon({paths: coords, strokeColor: \"#ff0000\", strokeWeight: 1, fillColor: \"#ff0000\", fillOpacity: 0.2});" << endl;
  cout << "        box.setMap(map.handles.marker);" << endl;
  cout << "        boxArray.push(box);" << endl;
  cout << "    }" << endl;
  cout << "  }" << endl;
  cout << "}" << endl;
  cout << "function marker_click_function() {" << endl;
//cout << "alert(this.icon.anchor.x);" << endl;
  cout << "  if (infoWindow != null)" << endl;
  cout << "    infoWindow.close();" << endl;
  cout << "  if (typeof(infoWindowArray[this.getTitle()]) == \"undefined\") {" << endl;
  cout << "    var x_offset=0,y_offset=0;" << endl;
  cout << "    if (this.icon.anchor.x == 16 && this.icon.anchor.y == 32) {" << endl;
  cout << "      x_offset=0;" << endl;
  cout << "      y_offset=10;" << endl;
  cout << "    }" << endl;
  cout << "    else if (this.icon.anchor.x == 0 && this.icon.anchor.y == 16) {" << endl;
  cout << "      x_offset=6;" << endl;
  cout << "      y_offset=15;" << endl;
  cout << "    }" << endl;
  cout << "    if (this.icon.anchor.x == 16 && this.icon.anchor.y == 0) {" << endl;
  cout << "      x_offset=0;" << endl;
  cout << "      y_offset=22;" << endl;
  cout << "    }" << endl;
  cout << "    else if (this.icon.anchor.x == 32 && this.icon.anchor.y == 16) {" << endl;
  cout << "      x_offset=-6;" << endl;
  cout << "      y_offset=15;" << endl;
  cout << "    }" << endl;
  cout << "    infoWindow=new google.maps.InfoWindow({pixelOffset: new google.maps.Size(x_offset,y_offset)});" << endl;
  cout << "    infoWindow.setContent('<center><table><tr valign=\"middle\"><td><img src=\"/images/wait.gif\"></td><td>Loading...</td></tr></table></center>');" << endl;
  cout << "    infoWindowArray[this.getTitle()]=infoWindow;" << endl;
  cout << "    var u='" << script_url << "?dsnum='+dsnum+'&ID='+this.getTitle();" << endl;
  cout << "    if (typeof(op) != \"undefined\" && op.length > 0)" << endl;
  cout << "      u+='&'+op;" << endl;
cout << "console.log(u);" << endl;
  cout << "    submitRequest(u,fillStationInfoWindow);" << endl;
  cout << "  }" << endl;
  cout << "  else" << endl;
  cout << "    infoWindow=infoWindowArray[this.getTitle()];" << endl;
  cout << "  infoWindow.open(map.handles.marker,this);" << endl;
  cout << "}" << endl;
  cout << "</script>" << endl;
  cout << "<div id=\"viewer_top\" style=\"font-size: 20px; font-weight: bold; width: 100%; margin-top: 10px; margin-bottom: 15px\">";
  if (!local_args.title.empty()) {
    cout << local_args.title;
  }
  else {
    cout << "Interactive Station Viewer";
  }
  cout << "</div>" << endl;
  cout << "<p>";
  if (!local_args.info.empty()) {
    cout << local_args.info;
  }
  else {
    cout << "Use the interactive map to pan and zoom to your area of interest.  Then make selections in the panel to the right.  Stations that match your selections will be displayed on the map.  You can then click individual station markers to get detailed information about each station.";
  }
  cout << "</p>" << endl;
  cout << "<table width=\"100%\" cellspacing=\"0\" cellpadding=\"5\" border=\"0\"><tr valign=\"top\">" << endl;
  cout << "<td width=\"700\"><div id=\"map\" style=\"width: 700px; height: 700px; border: thin solid black\"></div><p class=\"nice\">Legend:<br><img src=\"/images/gmaps/blue-dot.png\" width=\"20\" height=\"20\">&nbsp;indicates a station that has not moved for the entire specified temporal period<br><img src=\"/images/gmaps/red.png\" width=\"20\" height=\"20\">&nbsp;indicates a station that has moved within a box: the marker location (which will appear when you click the marker) is the center of the bounding box; the station may never have actually been located at this position<br><table cellspacing=\"0\" cellpadding=\"0\" border=\"0\"><tr valign=\"middle\"><td style=\"background-image: url('/images/gmaps/cluster-purple_legend.png'); width: 40px; height: 40px; font-size: 10px; font-family: helvetica,arial,verdana,sans-serif; text-align: center\"><b>37</b></td><td class=\"nice\">&nbsp;icons like these indicate clusters of stations; click on a cluster icon to zoom in to more detail</td></tr></table></p></td>" << endl;
  cout << "<td><p>Use the interactive map to pan and zoom to your area of interest.  Then, show stations on the map for the following selections:</p>" << endl;
  cout << "<hr noshade>" << endl;
  cout << "<form class=\"ds\" name=\"selections\"><div style=\"padding-bottom: 10px\"><b>Temporal Range:</b><br /><input type=\"text\" class=\"fixedWidth14\" name=\"sd\" value=\"" << start << "\" size=\"10\" maxlength=\"10\">&nbsp;<img class=\"calendar\" src=\"/images/calendar/cal.gif\" onClick=\"javascript:showCalendar('calendar','selections.sd')\"><br />to<br /><input type=\"text\" class=\"fixedWidth14\" name=\"ed\" value=\"" << end << "\" size=\"10\" maxlength=\"10\">&nbsp;<img class=\"calendar\" src=\"/images/calendar/cal.gif\" onClick=\"javascript:showCalendar('calendar','selections.ed')\"><br>portion of temporal range:<br /><input type=\"radio\" name=\"tspec\" value=\"any\" checked>&nbsp;any<br /><input type=\"radio\" name=\"tspec\" value=\"all\">&nbsp;all</div>" << endl;
  cout << "<table cellspacing=\"2\" cellpadding=\"5\" border=\"0\"><tr bgcolor=\"#c8daff\"><th>Type of Observation</th><th align=\"left\">Type of Platform</th></tr>" << endl;
  for (auto& key : obs_table.keys()) {
    started=false;
    obs_table.found(key,ote);
    for (auto& key2 : ote.data->platform_table.keys()) {
      if (!started) {
        cout << "<tr bgcolor=\"#e1eaff\" valign=\"middle\"><td align=\"center\" rowspan=\"" << ote.data->platform_table.size() << "\">" << strutils::to_capital(ote.data->description) << "</td>";
        started=true;
      }
      else {
        cout << "<tr bgcolor=\"#e1eaff\">";
      }
      ote.data->platform_table.found(key2,pe);
      cout << "<td><input type=\"checkbox\" value=\"" << ote.key << "," << pe.key << "\"";
      if (obs_table.size() == 1 && ote.data->platform_table.size() == 1) {
        cout << " checked";
      }
      cout << ">&nbsp;" << strutils::to_capital(pe.data->description) << "</td></tr>" << endl;
    }
  }
  cout << "</table></form><input type=\"button\" value=\"Show Stations\" onClick=\"doMapUpdate()\">" << endl;
  cout << "<hr noshade>" << endl;
  cout << "<center><b>- OR -</b></center>" << endl;
  cout << "<hr noshade>" << endl;
  cout << "<form class=\"ds\" name=\"sstation\" onSubmit=\"return doMapUpdate1()\"><div style=\"padding-bottom: 10px\"><b>Station ID:</b>&nbsp;<input type=\"text\" class=\"fixedWidth14\" name=\"sstn\" value=\"\" size=\"10\" maxlength=\"10\"><br /><input type=\"radio\" name=\"spart\" value=\"exact\" checked>&nbsp;exact match<br /><input type=\"radio\" name=\"spart\" value=\"any\">&nbsp;any part</form></div><input type=\"button\" value=\"Show Station(s)\" onClick=\"doMapUpdate1()\">" << endl;
  cout << "<hr noshade>" << endl;
  cout << "<div id=\"info\"></div><div id=\"display\" style=\"display: none\">" << local_args.display << "</div></td>" << endl;
  cout << "</tr></table>" << endl;
  cout << "<div id=\"loadwaittext\" style=\"position: absolute; visibility: hidden; width: 200px; height: 50px; border: thin solid black; background: #ffffff; opacity: 0.5; font-size: 24px; font-family: helvetica,arial,verdana,sans-serif; padding: 5px\"><center><table><tr valign=\"middle\"><td><img src=\"/images/wait.gif\" width=\"50\" height=\"50\"></td><td>Searching...</td></tr></table></center></div>" << endl;
  cout << "<div id=\"warn\" style=\"position: absolute; visibility: hidden; width: 300px; height: 60px; border: thin solid black; background: #ffffff; padding: 5px\"></div>" << endl;
  cout << "<div class=\"calendar\" id=\"calendar\"></div>" << endl;
}

void get_id_info() {
  string db = "WObML";
  string option_conditions;
  for (const auto& item : local_args.opList) {
    auto sp = split(item, ",");
    if (!option_conditions.empty()) {
      option_conditions+=" or ";
    }
    append(option_conditions, "(l.observationType_code = " + sp[0] + " and l."
        "platformType_code = " + sp[1] + ")", " or ");
  }
  cout << "Content-type: text/html" << endl << endl;
  cout << "[";
  int sw_lat = 900000, sw_lon = 1800000, ne_lat = -900000, ne_lon = -1800000;
  auto sp = split(local_args.ID, ";");
  for (size_t n = 0; n < sp.size(); ++n) {
    auto sp2 = split(sp[n], ",");
    MySQL::LocalQuery id_query;
    if (option_conditions.empty()) {
      id_query.set("select code, sw_lat, sw_lon, ne_lat, ne_lon from " + db +
          ".ds" + local_args.dsnum2 + "_IDs2 where id_type_code = " + sp2[0] +
          " and id = '" + sp2[1] + "'");
    } else {
      id_query.set("select code, sw_lat, sw_lon, ne_lat, ne_lon from " + db +
          ".ds" + local_args.dsnum2 + "_IDs2 as i left join " + db + ".ds" +
          local_args.dsnum2 + "_IDList2 as l on l.ID_code = i.code where "
          "id_type_code = " + sp2[0] + " and id = '" + sp2[1] + "' and (" +
          option_conditions + ")");
    }
//std::cerr << id_query.show() << endl;
    if (id_query.submit(server) == 0) {
      string id_conditions;
      for (const auto& row : id_query) {
        append(id_conditions, "ID_code = " + row[0], " or ");
        auto val = stoi(row[1]);
        if (val < sw_lat) {
          sw_lat = val;
        }
        val = stoi(row[2]);
        if (val < sw_lon) {
          sw_lon = val;
        }
        val = stoi(row[3]);
        if (val > ne_lat) {
          ne_lat = val;
        }
        val = stoi(row[4]);
        if (val > ne_lon) {
          ne_lon = val;
        }
      }
      string qselect = "select min(start_date), max(end_date), sum("
          "num_observations), any_value(t.id_type) from " + db + ".ds" + local_args.
          dsnum2 + "_IDList2 as l left join " + db + ".id_types as t on t.code = " +
          sp2[0] + " where (" + id_conditions + ")";
      if (!option_conditions.empty()) {
        qselect += " and (" + option_conditions + ")";
      }
      MySQL::Query query(qselect);
//std::cerr << query.show() << endl;
      if (query.submit(server) == 0) {
        MySQL::Row row;
        if (query.fetch_row(row)) {
          if (n > 0) {
            cout << ", ";
          }
          cout << "{\"ID\":\"" << sp2[1] << "\",\"t\":\"" << row[3] << "\",\"sd\":\"" << dateutils::string_ll_to_date_string(row[0].substr(0,8)) << "\",\"ed\":\"" << dateutils::string_ll_to_date_string(row[1].substr(0,8)) << "\",\"n\":\"" << strutils::number_with_commas(row[2]) << "\"";
          if (stoi(row[2]) > 1) {
            double obsper;
            string unit;
            metautils::obs_per("",stoi(row[2]),DateTime(std::stoll(row[0])),DateTime(std::stoll(row[1])),obsper,unit);
            cout << ",\"f\":" << lroundf(obsper) << ",\"u\":\"" << unit << "\"";
          }
          if (sw_lat != sw_lon || ne_lat != ne_lon) {
            cout << ",\"b\":[" << sw_lat/10000. << "," << sw_lon/10000. << "," << ne_lat/10000. << "," << ne_lon/10000. << "]";
          }
          cout << "}";
        }
      }
    }
  }
  cout << "]" << endl;
}

void get_stations()
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  size_t num_stns=0,num_ignored=0;
  bool started=false;
  float avg_lat=0.,avg_lon=0.;
  size_t avg_cnt=0;
  my::map<metautils::StringEntry> rdafile_hash,unique_id_table;
  metautils::StringEntry se;

  if (!local_args.gindex.empty()) {
    query.set("wfile","dssdb.wfile","dsid = 'ds"+metautils::args.dsnum+"' and type = 'D' and status = 'P' and tindex = "+local_args.gindex);
    if (query.submit(server) < 0) {
      std::cerr << "STATIONVIEWER: error: '" << server.error() << "' for query '" <<
          query.show() << "'" << std::endl;
      web_error("A database error occurred. Please try again later.");
    }
    while (query.fetch_row(row)) {
      se.key=row[0];
      rdafile_hash.insert(se);
    }
  }
  string db="WObML";
/*
  if (local_args.opList.size() > 0 || local_args.start.length() > 0) {
    for (auto item : local_args.opList) {
      if (opConditions.length() > 0)
        opConditions+=" or ";
      sp=split(item,",");
      opConditions+="(observationType_code = "+sp[0]+" and platformType_code = "+sp[1]+")";
    }
    if (local_args.start.length() > 0) {
      if (local_args.tspec == "any")
        dateConditions="start_date <= "+local_args.end+"235959 and end_date >= "+local_args.start+"000000";
      else if (local_args.tspec == "all")
        dateConditions="start_date <= "+local_args.start+"000000 and end_date >= "+local_args.end+"235959";
    }
    whereConditions+=opConditions;
    if (dateConditions.length() > 0) {
      if (whereConditions.length() > 0)
        whereConditions+=" and ";
      whereConditions+=dateConditions;
    }
    if (whereConditions.length() > 0)
      whereConditions="where "+whereConditions;
      if (local_args.wlon <= local_args.elon)
        lon_conditions="sw_lon >= "+local_args.wlon+"0000 and ne_lon <= "+local_args.elon+"0000";
      else
        lon_conditions="((sw_lon >= "+local_args.wlon+"0000 and sw_lon <= 1800000) or (sw_lon >= -1800000 and sw_lon <= "+local_args.elon+"0000)) and ((ne_lon >= "+local_args.wlon+"0000 and ne_lon <= 1800000) or (ne_lon >= -1800000 and ne_lon <= "+local_args.elon+"0000))";
      query.set("select id_type_code,id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000. from "+db+".ds"+local_args.dsnum2+"_IDList2 as l left join "+db+".ds"+local_args.dsnum2+"_IDs2 as i on i.code = l.ID_code "+whereConditions+" and !isnull(i.id) and sw_lat >= "+local_args.slat+"0000 and ne_lat <= "+local_args.nlat+"0000 and "+lon_conditions+" group by id_type_code,id");
  }
  else if (local_args.sstn.length() > 0) {
    if (local_args.spart == "any")
      query.set("select id_type_code,id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000. from "+db+".ds"+local_args.dsnum2+"_IDs2 where id like '%"+local_args.sstn+"%' group by id_type_code,id");
    else
      query.set("select id_type_code,id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000. from "+db+".ds"+local_args.dsnum2+"_IDs2 where id = '"+local_args.sstn+"' group by id_type_code,id");
  }
  else {
    query.set("select id_type_code,id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000. from "+db+".ds"+local_args.dsnum2+"_IDs2 group by id_type_code,id");
  }
*/
  if (!local_args.sstn.empty()) {
    if (local_args.spart == "any") {
      if (local_args.gindex.empty()) {
        query.set("select id_type_code,id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000. from "+db+".ds"+local_args.dsnum2+"_IDs2 where id like '%"+local_args.sstn+"%' group by id_type_code,id");
      }
      else {
        query.set("select i.id_type_code,i.id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000.,w.id from "+db+".ds"+local_args.dsnum2+"_IDs2 as i left join "+db+".ds"+local_args.dsnum2+"_IDList2 as i2 on i2.ID_code = i.code left join "+db+".ds"+local_args.dsnum2+"_webfiles2 as w on w.code = i2.webID_code where i.id like '%"+local_args.sstn+"%' group by i.id_type_code,i.id,w.id");
      }
    }
    else {
      if (local_args.gindex.empty()) {
        query.set("select id_type_code,id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000. from "+db+".ds"+local_args.dsnum2+"_IDs2 where id = '"+local_args.sstn+"' group by id_type_code,id");
      }
      else {
        query.set("select i.id_type_code,i.id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000.,w.id from "+db+".ds"+local_args.dsnum2+"_IDs2 as i left join "+db+".ds"+local_args.dsnum2+"_IDList2 as i2 on i2.ID_code = i.code left join "+db+".ds"+local_args.dsnum2+"_webfiles2 as w on w.code = i2.webID_code where i.id = '"+local_args.sstn+"' group by i.id_type_code,i.id,w.id");
      }
    }
  }
  else {
    string op_join;
    if (local_args.opList.size() > 0 || !local_args.start.empty()) {
      for (auto item : local_args.opList) {
        if (!op_join.empty()) {
          op_join+=" or ";
        }
        auto sp=split(item,",");
        op_join+="(observationType_code = "+sp[0]+" and platformType_code = "+sp[1]+")";
      }
    }
    if (!op_join.empty()) {
      op_join=" and ("+op_join+")";
    }
    string where_conditions;
    if (!local_args.start.empty()) {
      string date_conditions;
      if (local_args.tspec == "any") {
        date_conditions="((l.start_date >= "+local_args.start+"000000 and l.start_date <= "+local_args.end+"000000) or (l.end_date >= "+local_args.start+"000000 and l.end_date <= "+local_args.end+"000000))";
      }
      else if (local_args.tspec == "all") {
        date_conditions="(l.start_date <= "+local_args.start+"000000 and l.end_date >= "+local_args.end+"000000)";
      }
      if (!where_conditions.empty()) {
        where_conditions+=" and ";
      }
      where_conditions+=date_conditions;
    }
    if (local_args.wlon_f > -999. && local_args.elon_f < 999.) {
      string lon_conditions;
      if (local_args.wlon_f <= local_args.elon_f) {
        lon_conditions="sw_lon >= "+local_args.wlon+"0000 and ne_lon <= "+local_args.elon+"0000";
      }
      else {
        lon_conditions="((sw_lon >= "+local_args.wlon+"0000 and sw_lon <= 1800000) or (sw_lon >= -1800000 and sw_lon <= "+local_args.elon+"0000)) and ((ne_lon >= "+local_args.wlon+"0000 and ne_lon <= 1800000) or (ne_lon >= -1800000 and ne_lon <= "+local_args.elon+"0000))";
      }
      if (!where_conditions.empty()) {
        where_conditions+=" and ";
      }
      where_conditions+=lon_conditions;
    }
    if (where_conditions.empty()) {
      where_conditions="(1)";
    }
    string lat_conditions;
    if (local_args.wlon_f > -999. && local_args.elon_f < 999.) {
      lat_conditions="sw_lat >= "+local_args.slat+"0000 and ne_lat <= "+local_args.nlat+"0000";
    }
    else {
      lat_conditions="(1)";
    }
    if (local_args.gindex.empty()) {
      query.set("select id_type_code,id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000. from "+db+".ds"+local_args.dsnum2+"_IDList2 as l left join "+db+".ds"+local_args.dsnum2+"_IDs2 as i on i.code = l.ID_code"+op_join+" where "+where_conditions+" and !isnull(i.id) and "+lat_conditions+" group by id_type_code,id");
    }
    else {
      query.set("select i.id_type_code,i.id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000.,w.id from "+db+".ds"+local_args.dsnum2+"_IDList2 as l left join "+db+".ds"+local_args.dsnum2+"_webfiles2 as w on w.code = l.webID_code left join "+db+".ds"+local_args.dsnum2+"_IDs2 as i on i.code = l.ID_code"+op_join+" where "+where_conditions+" and !isnull(i.id) and "+lat_conditions+" group by i.id_type_code,i.id,w.id");
    }
  }
//std::cerr << query.show() << endl;
  if (query.submit(server) < 0) {
    std::cerr << "STATIONVIEWER: error: '" << query.error() << "' for query '" <<
        query.show() << "'" << endl;
    web_error("A database error occurred. Please try again later.");
  }
  cout << "Access-Control-Allow-Origin: https://rda.ucar.edu" << endl;
  cout << "Content-type: text/plain" << endl << endl;
  if (query.num_rows() == 0) {
    cout << "[{\"warn\":\"No stations were found that matched your selections.  Please adjust your selections and try again.\"}]" << endl;
  }
  else if (query.num_rows() > 50000) {
    cout << "[{\"warn\":\"Your selections returned too many stations to plot.  Please narrow your selections.\"}]" << endl;
  }
  else {
    std::unordered_map<string,string> fixed_station_duplicate_map;
    cout << "[{" << "\"m\":[";
    while (query.fetch_row(row)) {
      if (local_args.gindex.empty() || (!unique_id_table.found(row[0]+","+row[1],se) && rdafile_hash.found(row[6],se))) {
        auto sw_lat=stof(row[2]);
        auto sw_lon=stof(row[3]);
        if (row[2] == row[4] && row[3] == row[5]) {
          auto ttl=row[0]+","+row[1];
          auto dupe_key=row[2]+","+row[3];
          if (fixed_station_duplicate_map.find(dupe_key) == fixed_station_duplicate_map.end()) {
            fixed_station_duplicate_map.emplace(dupe_key,ttl);
          }
          else {
            fixed_station_duplicate_map[dupe_key]+=";"+ttl;
          }
/*
          if (started) {
            cout << ",";
          }
          cout << "{\"a\":" << row[2] << ",\"o\":" << row[3] << ",\"t\":" << rotation*900 << ",\"c\":\"blue\",\"ttl\":\"" << row[0] << "," << row[1] << "\"}";
          ++num_stns;
*/
        }
        else {
          auto ne_lat=stof(row[4]);
          auto ne_lon=stof(row[5]);
          avg_lat+=ne_lat;
          avg_lon+=ne_lon;
          ++avg_cnt;
//          if ((fabs(ne_lat-sw_lat) <= 0.5 && fabs(ne_lon-sw_lon) <= 0.5) || query.num_rows() == 1) {
if ((fabs(ne_lat-sw_lat) <= 1. && fabs(ne_lon-sw_lon) <= 1.) || query.num_rows() == 1) {
            if (started) {
              cout << ",";
            }
            cout << "{\"a\":" << strutils::ftos((sw_lat+ne_lat)/2.,4) << ",\"o\":" << strutils::ftos((sw_lon+ne_lon)/2.,4) << ",\"t\":1,\"c\":\"red\",\"ttl\":\"" << row[0] << "," << row[1] << "\"}";
            num_stns++;
            started=true;
          }
          else {
            ++num_ignored;
          }
        }
        avg_lat+=sw_lat;
        avg_lon+=sw_lon;
        ++avg_cnt;
        if (!local_args.gindex.empty()) {
          se.key=row[0]+","+row[1];
          unique_id_table.insert(se);
        }
      }
    }
    num_stns+=fixed_station_duplicate_map.size();
    for (const auto& station : fixed_station_duplicate_map) {
      if (started) {
        cout << ",";
      }
      auto loc=split(station.first,",");
      cout << "{\"a\":" << loc.front() << ",\"o\":" << loc.back() << ",\"t\":0,\"c\":\"blue\",\"ttl\":\"" << station.second << "\"}";
      started=true;
    }
    cout << "],\"n\":" << num_stns << ",\"u\":" << num_ignored;
    if (!local_args.slat.empty() && !local_args.nlat.empty() > 0 && !local_args.wlon.empty() && !local_args.elon.empty()) {
      avg_lat=lroundf((stoi(local_args.slat)+stoi(local_args.nlat))/2.);
      avg_lon=lroundf((stoi(local_args.wlon)+stoi(local_args.elon))/2.);
      if (local_args.wlon_f > local_args.elon_f) {
        avg_lon-=180.;
        if (avg_lon < -180.) {
          avg_lon+=360.;
        }
      }
      cout << ",\"aa\":" << avg_lat << ",\"ao\":" << avg_lon;
    }
    else {
      cout << ",\"aa\":" << avg_lat/avg_cnt << ",\"ao\":" << avg_lon/avg_cnt;
    }
    cout << ",\"zl\":" << local_args.zl << "}]" << endl;
//    cout << "[{" << "\"m\":[" << markers << "],\"n\":" << num_stns << ",\"u\":" << num_ignored << "}]" << endl;
  }
}

int main(int argc, char **argv) {
  parseQuery();
  metautils::read_config("stationViewer", "", false);
  server.connect(metautils::directives.database_server, metautils::directives.
      metadb_username, metautils::directives.metadb_password, "");
  if (!requestURI.empty()) {
    start();
  } else {
    if (!local_args.ID.empty()) {
      get_id_info();
    } else {
      get_stations();
    }
  }
}
