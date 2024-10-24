#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <string>
#include <list>
#include <deque>
#include <PostgreSQL.hpp>
#include <web/web.hpp>
#include <metadata.hpp>
#include <mymap.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <tokendoc.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using std::cout;
using std::endl;
using std::stof;
using std::stoi;
using std::string;
using strutils::append;
using strutils::replace_all;
using strutils::split;
using strutils::substitute;
using strutils::to_lower;

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
  Local_Args() : gindex(), opList(), start(), end(), ID(), tspec(), title(),
      info(), display(), sstn(), spart(), slat(), nlat(), wlon(), elon(),
      zl(), wlon_f(-999.), elon_f(999.9), getdata_methods(), getdata_table() { }

  string gindex;
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
Server server;

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
  metautils::args.dsid = queryString.value("dsid");
  if (metautils::args.dsid.empty()) {
    metautils::args.dsid = queryString.value("dsnum");
  }
  if (metautils::args.dsid.empty()) {
    web_error("missing dataset number");
  }
  if (metautils::args.dsid[0] != 'd' || metautils::args.dsid.length() != 7) {
    web_error("invalid dataset number '" + metautils::args.dsid + "'");
  }
if (metautils::args.dsid == "d132000" || metautils::args.dsid == "d337000" || metautils::args.dsid == "d461000") {
web_error("service unavailable");
}
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

void start() {
  string db = "ObML";
  if (!local_args.getdata_table.empty()) {
    if (duser().empty()) {
      string referer = getenv("HTTP_REFERER");
      if (!referer.empty()) {
        replace_all(referer, "https://", "");
        replace_all(referer, "http://", "");
        replace_all(referer, "rda.ucar.edu", "");
        cout << "Location: /cgi-bin/error?code=403&directory=" << referer <<
            endl << endl;
      } else
        cout << "Location: /cgi-bin/error?code=403&directory=" << requestURI <<
            endl << endl;
      exit(1);
    }
    db = "W" + db;
  }
  string start = "1000-01-01", end = "3000-12-31";
  std::ifstream ifs((strutils::token("/" + unixutils::host_name(), ".", 0) +
      "/web/datasets/" + metautils::args.dsid + "/metadata/customize." + db).
      c_str());
  if (ifs.is_open()) {
    char line[256];
    ifs.getline(line, 256);
    auto sp = split(line);
    start = dateutils::string_ll_to_date_string(sp[0]);
    end = dateutils::string_ll_to_date_string(sp[1]);
    ifs.close();
  }
  Query query("select i.observation_type_code, o.obs_type, i."
      "platform_type_code, p.platform_type from (select distinct "
      "observation_type_code, platform_type_code from \"" + db + "\"." +
      metautils::args.dsid + "_id_list) as i left join \"" + db +
      "\".obs_types as o on o.code = i.observation_type_code left join \"" + db
      + "\".platform_types as p on p.code = i.platform_type_code order by o."
      "obs_type,p.platform_type");
//std::cerr << query.show() << std::endl;
  if (query.submit(server) < 0) {
    std::cerr << "STATIONVIEWER: error: '" << query.error() << "' for query '"
        << query.show() << "'" << endl;
    web_error("A database error occurred. Please try again later.");
  }
  my::map<ObservationTypeEntry> obs_table;
  for (const auto& row : query) {
    ObservationTypeEntry ote;
    if (!obs_table.found(row[0], ote)) {
      ote.key=row[0];
      ote.data.reset(new ObservationTypeEntry::Data);
      ote.data->description=row[1];
      obs_table.insert(ote);
    }
    PlatformEntry pe;
    if (!ote.data->platform_table.found(row[2], pe)) {
      pe.key = row[2];
      pe.data.reset(new PlatformEntry::Data);
      pe.data->description=row[3];
      ote.data->platform_table.insert(pe);
    }
  }
  TokenDocument tdoc("/data/web/html/stationViewer/start.tdoc");
  cout << "Content-type: text/html" << endl << endl;
  cout << webutils::php_execute("$mapType=\"Cluster\";$mapDivId=\"map\";$controlSize=\"large\";$zoomLevel=2;include(\"gmap3_key.inc\");") << endl;
  tdoc.add_replacement("__DSID__", metautils::args.dsid);
  tdoc.add_replacement("__START_DATE__", start);
  tdoc.add_replacement("__END_DATE__", end);
  tdoc.add_replacement("__SERVER_NAME__", getenv("SERVER_NAME"));
  tdoc.add_replacement("__SCRIPT_URL__", script_url);
  tdoc.add_replacement("__GINDEX__", local_args.gindex);
  if (!local_args.getdata_table.empty()) {
    tdoc.add_if("__HAS_METHODS__");
    string methods;
    for (auto method : local_args.getdata_methods) {
      GetDataEntry gde;
      local_args.getdata_table.found(method, gde);
      append(methods, "<a href=\"" + gde.script + "?station=' + data[n].ID + "
          "'&sd=' + document.selections.sd.value + '&ed=' + "
          "document.selections.ed.value + '", "<br>");
      for (auto item : gde.nvp_list) {
        auto sp = split(item, "=");
        if (sp.size() == 2) {
          methods += "&" + sp[0] + "=" + sp[1];
        }
      }
      methods += "\"";
      if (gde.target == "new") {
        methods += " target=\"_" + to_lower(substitute(gde.script, " ", "_")) +
            "\"";
      }
      methods += ">" + method + "</a>";
    }
    tdoc.add_replacement("__METHODS__", methods);
  }
  if (!local_args.title.empty()) {
    tdoc.add_replacement("__TITLE__", local_args.title);
  } else {
    tdoc.add_replacement("__TITLE__", "Interactive Station Viewer");
  }
  if (!local_args.info.empty()) {
    tdoc.add_replacement("__INFO__", local_args.info);
  } else {
    tdoc.add_replacement("__INFO__", "Use the interactive map to pan and zoom to your area of interest.  Then make selections in the panel to the right.  Stations that match your selections will be displayed on the map.  You can then click individual station markers to get detailed information about each station.");
  }
  cout << tdoc << endl;
  for (auto& key : obs_table.keys()) {
    auto started = false;
    ObservationTypeEntry ote;
    obs_table.found(key, ote);
    for (auto& key2 : ote.data->platform_table.keys()) {
      if (!started) {
        cout << "<tr bgcolor=\"#e1eaff\" valign=\"middle\"><td align=\"center\" rowspan=\"" << ote.data->platform_table.size() << "\">" << strutils::to_capital(ote.data->description) << "</td>";
        started=true;
      }
      else {
        cout << "<tr bgcolor=\"#e1eaff\">";
      }
      PlatformEntry pe;
      ote.data->platform_table.found(key2, pe);
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
  string option_conditions;
  for (const auto& item : local_args.opList) {
    auto sp = split(item, ",");
    if (!option_conditions.empty()) {
      option_conditions+=" or ";
    }
    append(option_conditions, "(l.observation_type_code = " + sp[0] + " and l."
        "platform_type_code = " + sp[1] + ")", " or ");
  }
  cout << "Content-type: text/html" << endl << endl;
  cout << "[";
  int sw_lat = 900000, sw_lon = 1800000, ne_lat = -900000, ne_lon = -1800000;
  auto sp = split(local_args.ID, ";");
  for (size_t n = 0; n < sp.size(); ++n) {
    auto sp2 = split(sp[n], ",");
    LocalQuery id_query;
    if (option_conditions.empty()) {
      id_query.set("select code, sw_lat, sw_lon, ne_lat, ne_lon from \"WObML\""
          "." + metautils::args.dsid + "_ids where id_type_code = " + sp2[0] +
          " and id = '" + sp2[1] + "'");
    } else {
      id_query.set("select code, sw_lat, sw_lon, ne_lat, ne_lon from \"WObML\""
          "." + metautils::args.dsid + "_ids as i left join \"WObML\"." +
          metautils::args.dsid + "_id_list as l on l.id_code = i.code where "
          "id_type_code = " + sp2[0] + " and id = '" + sp2[1] + "' and (" +
          option_conditions + ")");
    }
//std::cerr << id_query.show() << endl;
    if (id_query.submit(server) == 0) {
      string id_conditions;
      for (const auto& row : id_query) {
        append(id_conditions, "id_code = " + row[0], " or ");
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
          "num_observations), id_type from \"WObML\"." + metautils::args.dsid +
          "_id_list as l left join \"WObML\".id_types as t on t.code = " + sp2[
          0] + " where (" + id_conditions + ")";
      if (!option_conditions.empty()) {
        qselect += " and (" + option_conditions + ")";
      }
      qselect += " group by t.id_type";
      Query query(qselect);
//std::cerr << query.show() << endl;
      if (query.submit(server) == 0) {
        Row row;
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

void get_stations() {
  LocalQuery query;
  size_t num_stns=0,num_ignored=0;
  bool started=false;
  float avg_lat=0.,avg_lon=0.;
  size_t avg_cnt=0;
  my::map<metautils::StringEntry> rdafile_hash,unique_id_table;
  metautils::StringEntry se;

  if (!local_args.gindex.empty()) {
    query.set("wfile","dssdb.wfile_" + metautils::args.dsid,"type = 'D' and status = 'P' and tindex = "+local_args.gindex);
    if (query.submit(server) < 0) {
      std::cerr << "STATIONVIEWER: error: '" << server.error() << "' for query '" <<
          query.show() << "'" << std::endl;
      web_error("A database error occurred. Please try again later.");
    }
    for (const auto& row : query) {
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
      opConditions+="(observation_type_code = "+sp[0]+" and platform_type_code = "+sp[1]+")";
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
      query.set("select id_type_code,id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000. from \""+db+"\"."+metautils::args.dsid+"_id_list as l left join \""+db+"\"."+metautils::args.dsid+"_ids as i on i.code = l.id_code "+whereConditions+" and i.id is not null and sw_lat >= "+local_args.slat+"0000 and ne_lat <= "+local_args.nlat+"0000 and "+lon_conditions+" group by id_type_code,id");
  }
  else if (local_args.sstn.length() > 0) {
    if (local_args.spart == "any")
      query.set("select id_type_code,id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000. from \""+db+"\"."+metautils::args.dsid+"_ids where id like '%"+local_args.sstn+"%' group by id_type_code,id");
    else
      query.set("select id_type_code,id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000. from \""+db+"\"."+metautils::args.dsid+"_ids where id = '"+local_args.sstn+"' group by id_type_code,id");
  }
  else {
    query.set("select id_type_code,id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000. from \""+db+"\"."+metautils::args.dsid+"_ids group by id_type_code,id");
  }
*/
  if (!local_args.sstn.empty()) {
    if (local_args.spart == "any") {
      if (local_args.gindex.empty()) {
        query.set("select id_type_code,id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000. from \""+db+"\"."+metautils::args.dsid+"_ids where id like '%"+local_args.sstn+"%' group by id_type_code,id");
      }
      else {
        query.set("select i.id_type_code,i.id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000.,w.id from \""+db+"\"."+metautils::args.dsid+"_ids as i left join \""+db+"\"."+metautils::args.dsid+"_id_list as i2 on i2.id_code = i.code left join \""+db+"\"."+metautils::args.dsid+"_webfiles2 as w on w.code = i2.file_code where i.id like '%"+local_args.sstn+"%' group by i.id_type_code,i.id,w.id");
      }
    }
    else {
      if (local_args.gindex.empty()) {
        query.set("select id_type_code,id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000. from \""+db+"\"."+metautils::args.dsid+"_ids where id = '"+local_args.sstn+"' group by id_type_code,id");
      }
      else {
        query.set("select i.id_type_code,i.id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000.,w.id from \""+db+"\"."+metautils::args.dsid+"_ids as i left join \""+db+"\"."+metautils::args.dsid+"_id_list as i2 on i2.id_code = i.code left join \""+db+"\"."+metautils::args.dsid+"_webfiles2 as w on w.code = i2.file_code where i.id = '"+local_args.sstn+"' group by i.id_type_code,i.id,w.id");
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
        op_join+="(observation_type_code = "+sp[0]+" and platform_type_code = "+sp[1]+")";
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
      query.set("select id_type_code,id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000. from \""+db+"\"."+metautils::args.dsid+"_id_list as l left join \""+db+"\"."+metautils::args.dsid+"_ids as i on i.code = l.id_code"+op_join+" where "+where_conditions+" and i.id is not null and "+lat_conditions+" group by id_type_code,id");
    }
    else {
      query.set("select i.id_type_code,i.id,min(sw_lat)/10000.,min(sw_lon)/10000.,max(ne_lat)/10000.,max(ne_lon)/10000.,w.id from \""+db+"\"."+metautils::args.dsid+"_id_list as l left join \""+db+"\"."+metautils::args.dsid+"_webfiles2 as w on w.code = l.file_code left join \""+db+"\"."+metautils::args.dsid+"_ids as i on i.code = l.id_code"+op_join+" where "+where_conditions+" and i.id is not null and "+lat_conditions+" group by i.id_type_code,i.id,w.id");
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
    for (const auto& row : query) {
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
      metadb_username, metautils::directives.metadb_password, "rdadb");
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
