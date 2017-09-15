#include <iostream>
#include <fstream>
#include <sstream>
#include <unistd.h>
#include <string>
#include <regex>
#include <deque>
#include <list>
#include <web/web.hpp>
#include <MySQL.hpp>
#include <mymap.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <bits.hpp>
#include <datetime.hpp>
#include <grid.hpp>

struct ConfigData {
  ConfigData() : version(),db_host(),db_username(),db_password() {}

  std::string version;
  std::string db_host,db_username,db_password;
} config_data;
struct StringEntry {
  StringEntry() : key() {}

  std::string key;
};
struct ParameterData {
  struct Data {
    Data() : code(),format_code(),format(),long_name(),units() {}

    std::string code,format_code,format,long_name,units;
  };
  ParameterData() : key(),data(nullptr) {}

  std::string key;
  std::shared_ptr<Data> data;
};
struct DapArgs {
  DapArgs() : ID(),ext(),rinfo(),dsnum(),dsnum2(),parameters(),grid_definition() {}

  std::string ID,ext,rinfo,dsnum,dsnum2;
  my::map<ParameterData> parameters;
  struct GridDefinition {
    GridDefinition() : type(),code(),lat_index(),lon_index() {}

    std::string type,code;
    struct LatIndex {
	LatIndex() : start(-99),end(99) {}

	int start,end;
    } lat_index;
    struct LonIndex {
	LonIndex() : start(-999),end(999) {}

	int start,end;
    } lon_index;
  } grid_definition;
} dap_args;
gridSubset::Args grid_subset_args;
MySQL::Server server;
struct Dimension {
  Dimension() : length(0),start(0.),stop(0.),increment(0.) {}

  size_t length;
  double start,stop,increment;
} lat,lon;
struct DimensionIndex {
  DimensionIndex() : length(0),start(0),stop(0),increment(0) {}

  int length;
  int start,stop,increment;
};
struct ProjectionEntry {
  struct Data {
    struct DimensionID {
	DimensionID() : name(),size() {}

	std::string name,size;
    };
    Data() : member(),idx(),ref_time_dim(),time_dim(),fcst_hr_dim(),level_dim() {}

    std::string member;
    std::vector<DimensionIndex> idx;
    DimensionID ref_time_dim,time_dim,fcst_hr_dim,level_dim;
  };
  ProjectionEntry() : key(),data(nullptr) {}

  std::string key;
  std::shared_ptr<Data> data;
};
my::map<ProjectionEntry> projection_table;
union Value {
  float f;
  int i;
};
my::map<Grid::GLatEntry> *gaus_lats=NULL;
std::string myerror="";
std::string mywarning="";
struct timespec tp;

void printDateAndVersion(std::ostream& outs)
{
  outs << "Date: " << getCurrentDateTime().toString("%a, %d %h %Y %H:%MM:%SS GMT") << std::endl;
  outs << "XDODS-Server: " << config_data.version << std::endl;
}

void dapError(std::string status,std::string message)
{
  std::cout << "Status: " << status << std::endl;
  std::cout << "Content-type: text/plain" << std::endl;
  std::cout << "Content-Description: dods-error" << std::endl;
  printDateAndVersion(std::cout);
  std::cout << std::endl;
  std::cout << "Error {" << std::endl;
  std::cout << "  code = " << status.substr(0,status.find(" ")) << ";" << std::endl;
  std::cout << "  message = \"" << message << "\";" << std::endl;
  std::cout << "};" << std::endl;
  server.disconnect();
  exit(0);
}

void parse_config()
{
  std::ifstream ifs("/usr/local/www/server_root/web/cgi-bin/internal/conf/opendap.conf");
  if (!ifs.is_open()) {
    dapError("500 Internal Server Error","Missing configuration");
  }
  auto line=new char[4096];
  ifs.getline(line,4096);
  while (!ifs.eof()) {
    if (line[0] != '#') {
	auto nvp=strutils::split(line,"=");
	strutils::trim(nvp.front());
	strutils::trim(nvp.back());
	if (nvp.front() == "version") {
	  config_data.version=nvp.back();
	}
	else if (nvp.front() == "db_host") {
	  config_data.db_host=nvp.back();
	}
	else if (nvp.front() == "db_username") {
	  config_data.db_username=nvp.back();
	}
	else if (nvp.front() == "db_password") {
	  config_data.db_password=nvp.back();
	}
    }
    ifs.getline(line,4096);
  }
  ifs.close();
}

void decodeRequestData()
{
  xmlutils::ParameterMapper parameter_mapper;
  MySQL::LocalQuery query;
  MySQL::Row row;
  gridSubset::Parameter param;
  ParameterData pdata;

  decodeGridSubsetString(dap_args.rinfo,grid_subset_args);
  dap_args.dsnum=grid_subset_args.dsnum;
  dap_args.dsnum2=strutils::substitute(dap_args.dsnum,".","");
  strutils::replace_all(grid_subset_args.startdate,"-","");
  strutils::replace_all(grid_subset_args.startdate,":","");
  strutils::replace_all(grid_subset_args.startdate," ","");
  strutils::replace_all(grid_subset_args.enddate,"-","");
  strutils::replace_all(grid_subset_args.enddate,":","");
  strutils::replace_all(grid_subset_args.enddate," ","");
  if (grid_subset_args.parameters.size() == 0) {
    dapError("500 Internal Server Error","Bad aggregation specification (1)");
  }
  for (const auto& key : grid_subset_args.parameters.keys()) {
    grid_subset_args.parameters.found(key,param);
    if (param.format_code != nullptr) {
	query.set("select format from WGrML.formats where code = "+*param.format_code);
	if (query.submit(server) == 0 && query.fetch_row(row)) {
	  pdata.key=strutils::substitute(parameter_mapper.getShortName(row[0],key)," ","_");
	  pdata.data.reset(new ParameterData::Data);
	  pdata.data->code=key;
	  pdata.data->format_code=*param.format_code;
	  pdata.data->format=row[0];
	  pdata.data->long_name=parameter_mapper.getDescription(row[0],key);
	  pdata.data->units=parameter_mapper.getUnits(row[0],key);
	  dap_args.parameters.insert(pdata);
	}
    }
    else {
	dapError("500 Internal Server Error","Bad aggregation specification (2)");
    }
  }
  dap_args.grid_definition.code=grid_subset_args.grid_definition_code;
}

void writeRefTimes(std::ostream& outs,const ProjectionEntry& pe,int ref_time_index)
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  DateTime base_dt;

  query.set("base_time","metautil.custom_dap_ref_times","ID = '"+dap_args.ID+"' and dim_name = '"+pe.key+"'");
  if (query.submit(server) == 0 && query.fetch_row(row)) {
    base_dt.set(std::stoll(row[0])*100);
  }
  std::string query_spec="select distinct ref_time from metautil.custom_dap_grid_index where ID = '"+dap_args.ID+"' and ref_time > 0";
  if (pe.key != "ref_time") {
    query.set("select g.param,g.level_code from (select ID,param,level_code,count(ref_time) as cnt from metautil.custom_dap_grid_index where ID = '"+dap_args.ID+"' group by param,level_code) as g left join metautil.custom_dap_ref_times as t on t.ID = g.ID where t.dim_name = '"+pe.key+"' and g.cnt = t.dim_size");
    if (query.submit(server) == 0 && query.fetch_row(row)) {
	query_spec+=" and param = '"+row[0]+"' and level_code = "+row[1];
    }
  }
  query_spec+=" order by ref_time";
  if (ref_time_index >= 0) {
    query_spec+=" limit "+strutils::itos(pe.data->idx[ref_time_index].start)+","+strutils::itos(pe.data->idx[ref_time_index].stop-pe.data->idx[ref_time_index].start+1);
  }
  query.set(query_spec);
  if (query.submit(server) == 0) {
    char buf[4];
    if (ref_time_index < 0) {
	setBits(buf,query.num_rows(),0,32);
    }
    else {
	setBits(buf,pe.data->idx[ref_time_index].length,0,32);
    }
    outs.write(buf,4);
    outs.write(buf,4);
    while (query.fetch_row(row)) {
	setBits(buf,DateTime(std::stoll(row[0])*100).getMinutesSince(base_dt),0,32);
	outs.write(buf,4);
    }
  }
}

void writeTimes(std::ostream& outs,const ProjectionEntry& pe,int time_index)
{
  DateTime base_dt(std::stoll(grid_subset_args.startdate)*100);
  std::string query_spec="select distinct valid_date from metautil.custom_dap_grid_index where ID = '"+dap_args.ID+"'";
  if (pe.key != "time") {
    MySQL::LocalQuery query("select g.param,g.level_code from (select ID,param,level_code,count(valid_date) as cnt from metautil.custom_dap_grid_index where ID = '"+dap_args.ID+"' group by param,level_code) as g left join metautil.custom_dap_times as t on t.ID = g.ID where t.dim_name = '"+pe.key+"' and g.cnt = t.dim_size");
    MySQL::Row row;
    if (query.submit(server) == 0 && query.fetch_row(row)) {
	query_spec+=" and param = '"+row[0]+"' and level_code = "+row[1];
    }
  }
  if (time_index >= 0) {
    query_spec+=" order by valid_date limit "+strutils::itos(pe.data->idx[time_index].start)+","+strutils::itos(pe.data->idx[time_index].stop-pe.data->idx[time_index].start+1);
  }
  MySQL::LocalQuery query(query_spec);
  if (query.submit(server) == 0) {
    char buf[4];
    if (time_index < 0) {
	setBits(buf,query.num_rows(),0,32);
    }
    else {
	setBits(buf,pe.data->idx[time_index].length,0,32);
    }
    outs.write(buf,4);
    outs.write(buf,4);
    MySQL::Row row;
    while (query.fetch_row(row)) {
	setBits(buf,DateTime(std::stoll(row[0])*100).getMinutesSince(base_dt),0,32);
	outs.write(buf,4);
    }
  }
}

void writeFcstHrs(std::ostream& outs,const ProjectionEntry& pe,int fcst_hr_index)
{
  MySQL::LocalQuery query;
  MySQL::Row row;

  std::string query_spec="select distinct fcst_hr from metautil.custom_dap_grid_index where ID = '"+dap_args.ID+"' and ref_time > 0";
  if (pe.key != "fcst_hr") {
    query.set("select g.param,g.level_code from (select ID,param,level_code,count(distinct fcst_hr) as cnt from metautil.custom_dap_grid_index where ID = '"+dap_args.ID+"' group by param,level_code) as g left join metautil.custom_dap_fcst_hrs as f on f.ID = g.ID where f.dim_name = '"+pe.key+"' and g.cnt = f.dim_size limit 0,1");
    if (query.submit(server) == 0 && query.fetch_row(row)) {
	query_spec+=" and param = '"+row[0]+"' and level_code = "+row[1];
    }
  }
  query_spec+=" order by fcst_hr";
  if (fcst_hr_index >= 0) {
    query_spec+=" limit "+strutils::itos(pe.data->idx[fcst_hr_index].start)+","+strutils::itos(pe.data->idx[fcst_hr_index].stop-pe.data->idx[fcst_hr_index].start+1);
  }
  query.set(query_spec);
  if (query.submit(server) == 0) {
    char buf[4];
    if (fcst_hr_index < 0) {
	setBits(buf,query.num_rows(),0,32);
    }
    else {
	setBits(buf,pe.data->idx[fcst_hr_index].length,0,32);
    }
    outs.write(buf,4);
    outs.write(buf,4);
    while (query.fetch_row(row)) {
	setBits(buf,std::stoi(row[0]),0,32);
	outs.write(buf,4);
    }
  }
}

void writeLevels(std::ostream& outs,const ProjectionEntry& pe,int lev_index)
{
  std::string query_spec;
  MySQL::LocalQuery query;
  MySQL::Row row;
  char buf[4];
  Value value;

  query_spec="select l.value from metautil.custom_dap_level_index as i left join metautil.custom_dap_levels as v on v.ID = i.ID and v.uID = i.uID left join WGrML.levels as l on l.code = i.level_code where i.ID = '"+dap_args.ID+"'";
  if (lev_index >= 0) {
    query_spec+=" and i.slice_index >= "+strutils::itos(pe.data->idx[1].start)+" and i.slice_index <= "+strutils::itos(pe.data->idx[1].stop);
  }
  else {
    query_spec+=" and v.dim_name = '"+pe.key+"'";
  }
  query_spec+=" order by i.slice_index";
  query.set(query_spec);
  if (query.submit(server) == 0) {
    setBits(buf,query.num_rows(),0,32);
    outs.write(buf,4);
    outs.write(buf,4);
    while (query.fetch_row(row)) {
	if (strutils::contains(row[0],",")) {
	  auto sp=strutils::split(row[0],",");
	  value.f=(std::stof(sp[0])+std::stof(sp[1]))/2.;
	}
	else {
	  value.f=std::stof(row[0]);
	}
	setBits(buf,value.i,0,32);
	outs.write(buf,4);
    }
  }
}

void decodeGridDefinition()
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  Grid::GLatEntry gle;

  if (dap_args.grid_definition.type.length() == 0) {
    query.set("select definition,defParams from WGrML.gridDefinitions where code = "+dap_args.grid_definition.code);
    if (query.submit(server) < 0) {
	std::cerr << "opendap printDDS(1): " << query.error() << " for " << query.show() << std::endl;
	dapError("500 Internal Server Error","Database error printDDS(1)");
    }
    if (!query.fetch_row(row)) {
	std::cerr << "opendap printDDS(2): " << query.error() << " for " << query.show() << std::endl;
	dapError("500 Internal Server Error","Database error printDDS(2)");
    }
    dap_args.grid_definition.type=row[0];
    std::deque<std::string> sp=strutils::split(row[1],":");
    lat.start=std::stof(sp[2].substr(0,sp[2].length()-1));
    if (sp[2].back() == 'S') {
	lat.start=-lat.start;
    }
    lat.stop=std::stof(sp[4].substr(0,sp[4].length()-1));
    if (sp[4].back() == 'S') {
	lat.stop=-lat.stop;
    }
    lat.length=std::stoi(sp[1]);
    if (dap_args.grid_definition.type == "latLon") {
	lat.increment=(lat.stop-lat.start)/(lat.length-1.);
    }
    else {
	lat.increment=std::stoi(sp[7]);
    }
    if (grid_subset_args.subset_bounds.nlat > 90.) {
	dap_args.grid_definition.lat_index.start=0;
	dap_args.grid_definition.lat_index.end=lat.length-1;
    }
    else {
	if (dap_args.grid_definition.type == "gaussLatLon") {
	  if (gaus_lats == nullptr) {
	    gaus_lats=new my::map<Grid::GLatEntry>;
	    fillGaussianLatitudes(*gaus_lats,lat.increment,(lat.start > lat.stop));
	  }
	  gaus_lats->found(lat.increment,gle);
	  if (lat.start > lat.stop) {
	    for (size_t n=0; n < lat.increment*2; ++n) {
		if (gle.lats[n] > grid_subset_args.subset_bounds.nlat) {
		  dap_args.grid_definition.lat_index.start=n;
		}
		if (gle.lats[n] > grid_subset_args.subset_bounds.slat) {
		  dap_args.grid_definition.lat_index.end=n;
		}
	    }
	    ++dap_args.grid_definition.lat_index.start;
	  }
	  else {
	  }
	}
	else {
	  if (lat.start > lat.stop) {
	    for (size_t n=0; n < lat.length; ++n) {
		auto f=lat.start+n*lat.increment;
		if (f > grid_subset_args.subset_bounds.nlat) {
		  dap_args.grid_definition.lat_index.start=n;
		}
		if (f >= grid_subset_args.subset_bounds.slat) {
		  dap_args.grid_definition.lat_index.end=n;
		}
	    }
	    ++dap_args.grid_definition.lat_index.start;
	  }
	  else {
	  }
	}
	lat.length=dap_args.grid_definition.lat_index.end-dap_args.grid_definition.lat_index.start+1;
    }
    lon.start=std::stof(sp[3].substr(0,sp[3].length()-1));
    if (sp[3].back() == 'W') {
	lon.start=-lon.start;
    }
    lon.stop=std::stof(sp[5].substr(0,sp[5].length()-1));
    if (sp[5].back() == 'W') {
	lon.stop=-lon.stop;
    }
    lon.length=std::stoi(sp[0]);
    lon.increment=(lon.stop-lon.start)/(lon.length-1.);
    if (grid_subset_args.subset_bounds.nlat > 90.) {
	dap_args.grid_definition.lon_index.start=0;
	dap_args.grid_definition.lon_index.end=lon.length-1;
    }
    else {
	if (lon.start >= 0.) {
// 0 to 360 longitudes
	  if (grid_subset_args.subset_bounds.wlon < 0. && grid_subset_args.subset_bounds.elon > 0.) {
// crosses the Greenwich Meridian
	    grid_subset_args.subset_bounds.wlon+=360.;
	    dap_args.grid_definition.lon_index.start=floor(grid_subset_args.subset_bounds.wlon/lon.increment);
	    if (!myequalf(dap_args.grid_definition.lon_index.start*lon.increment,grid_subset_args.subset_bounds.wlon,0.001)) {
		dap_args.grid_definition.lon_index.start++;
	    }
	    dap_args.grid_definition.lon_index.end=floor(grid_subset_args.subset_bounds.elon/lon.increment)+lon.length;
	  }
	  else {
	    if (grid_subset_args.subset_bounds.wlon < 0.) {
		grid_subset_args.subset_bounds.wlon+=360.;
	    }
	    if (grid_subset_args.subset_bounds.elon < 0.) {
		grid_subset_args.subset_bounds.elon+=360.;
	    }
	    for (size_t n=0; n < lon.length; ++n) {
		float f=lon.start+n*lon.increment;
		if (f < grid_subset_args.subset_bounds.wlon) {
		  dap_args.grid_definition.lon_index.start=n;
		}
		if (f < grid_subset_args.subset_bounds.elon) {
		  dap_args.grid_definition.lon_index.end=n;
		}
	    }
	    dap_args.grid_definition.lon_index.start++;
	    if (myequalf((dap_args.grid_definition.lon_index.end+1)*lon.increment,grid_subset_args.subset_bounds.elon,0.001)) {
		dap_args.grid_definition.lon_index.end++;
	    }
	  }
	}
	else {
// -180 to 180 longitudes
	}
	lon.length=dap_args.grid_definition.lon_index.end-dap_args.grid_definition.lon_index.start+1;
    }
  }
}

void writeLatitudes(std::ostream& outs,const ProjectionEntry& pe,int lat_index)
{
  Value value;
  char buf[4];
  Grid::GLatEntry gle;
  int start,stop;

  decodeGridDefinition();
  if (lat_index < 0) {
    setBits(buf,lat.length,0,32);
    start=dap_args.grid_definition.lat_index.start;
    stop=dap_args.grid_definition.lat_index.end;
  }
  else {
    setBits(buf,pe.data->idx[lat_index].length,0,32);
    start=dap_args.grid_definition.lat_index.start+pe.data->idx[lat_index].start;
    stop=dap_args.grid_definition.lat_index.start+pe.data->idx[lat_index].stop;
  }
  outs.write(buf,4);
  outs.write(buf,4);
  if (dap_args.grid_definition.type == "latLon") {
    for (int n=dap_args.grid_definition.lat_index.start; n <= dap_args.grid_definition.lat_index.end; n++) {
	if (lat_index < 0 || (n >= start && n <= stop)) {
	  value.f=lat.start+n*lat.increment;
	  setBits(buf,value.i,0,32);
	  outs.write(buf,4);
	}
    }
  }
  else if (dap_args.grid_definition.type == "gaussLatLon") {
    if (gaus_lats == NULL) {
	gaus_lats=new my::map<Grid::GLatEntry>;
	fillGaussianLatitudes(*gaus_lats,lat.increment,(lat.start > lat.stop));
    }
    gaus_lats->found(lat.increment,gle);
    for (int n=dap_args.grid_definition.lat_index.start; n <= dap_args.grid_definition.lat_index.end; n++) {
	if (lat_index < 0 || (n >= start && n <= stop)) {
	  value.f=gle.lats[n];
	  setBits(buf,value.i,0,32);
	  outs.write(buf,4);
	}
    }
  }
}

void writeLongitudes(std::ostream& outs,const ProjectionEntry& pe,int lon_index)
{
  Value value;
  char buf[4];
  int start,stop;

  decodeGridDefinition();
  if (lon_index < 0) {
    setBits(buf,lon.length,0,32);
    start=dap_args.grid_definition.lon_index.start;
    stop=dap_args.grid_definition.lon_index.end;
  }
  else {
    setBits(buf,pe.data->idx[lon_index].length,0,32);
    start=dap_args.grid_definition.lon_index.start+pe.data->idx[lon_index].start;
    stop=dap_args.grid_definition.lon_index.start+pe.data->idx[lon_index].stop;
  }
  outs.write(buf,4);
  outs.write(buf,4);
  for (int n=dap_args.grid_definition.lon_index.start; n <= dap_args.grid_definition.lon_index.end; ++n) {
    if (lon_index < 0 || (n >= start && n <= stop)) {
	value.f=lon.start+n*lon.increment;
	if (value.f > 360.) {
	  value.f-=360.;
	}
	setBits(buf,value.i,0,32);
	outs.write(buf,4);
    }
  }
}

std::string addProductCodesToQuery(std::string column_name)
{
  std::string prod_codes;
  if (grid_subset_args.product_codes.size() > 1) {
    prod_codes+="(";
  }
  size_t n=0;
  for (const auto& code : grid_subset_args.product_codes) {
    if (n++ > 0) {
	prod_codes+=" or ";
    }
    prod_codes+=column_name+" = "+code;
  }
  if (grid_subset_args.product_codes.size() > 1) {
    prod_codes+=")";
  }
  return prod_codes;
}

void fillParameterQuery(MySQL::LocalQuery& query,std::string param_ID = "")
{
  std::string qspec="select distinct ";
  if (param_ID.length() == 0) {
    qspec+="x.param,";
  }
  qspec+="x.tdim_name,x.tdim_size,l.dim_name,l.dim_size,x.level_code,y.tdim_name,y.tdim_size from (select g.ID as ID,g.level_code as level_code,g.param as param,count(g.param) as pcnt,t.dim_name as tdim_name,t.dim_size as tdim_size from (select ID,dim_name,dim_size,(cast(dim_size as signed)-1) as tsize from metautil.custom_dap_times where ID = '"+dap_args.ID+"') as t left join metautil.custom_dap_grid_index as g on g.ID = t.ID and g.time_slice_index = tsize left join metautil.custom_dap_level_index as l on l.ID = g.ID and l.level_code = g.level_code";
  if (param_ID.length() > 0) {
    qspec+=" where g.param = '"+param_ID+"'";
  }
  qspec+=" group by g.param,l.uID) as x left join (select g.ID as ID,g.level_code as level_code,g.param as param,count(g.param) as pcnt,t.dim_name as tdim_name,t.dim_size as tdim_size from (select ID,dim_name,dim_size,(cast(dim_size as signed)-1) as tsize from metautil.custom_dap_ref_times where ID = '"+dap_args.ID+"') as t left join metautil.custom_dap_grid_index as g on g.ID = t.ID and g.time_slice_index = tsize left join metautil.custom_dap_level_index as l on l.ID = g.ID and l.level_code = g.level_code";
  if (param_ID.length() > 0) {
    qspec+=" where g.param = '"+param_ID+"'";
  }
  qspec+=" group by g.param,l.uID) as y on y.param = x.param and y.level_code = x.level_code left join metautil.custom_dap_level_index as i on i.ID = x.ID and i.level_code = x.level_code left join metautil.custom_dap_levels as l on l.ID = i.ID and l.level_key = i.level_key and l.dim_size = x.pcnt group by ";
  if (param_ID.length() == 0) {
    qspec+="x.param,";
  }
  qspec+="x.level_code";
  query.set(qspec);
}

bool hasReferenceTimes(MySQL::Server& server,MySQL::LocalQuery& query)
{
  query.set("select dim_name,dim_size from metautil.custom_dap_ref_times where ID = '"+dap_args.ID+"'");
  if (query.submit(server) < 0) {
    std::cerr << "opendap hasReferenceTimes: " << query.error() << " for " << query.show() << std::endl;
    dapError("500 Internal Server Error","Database error hasReferenceTimes");
  }
  if (query.num_rows() > 0) {
    return true;
  }
  else {
    return false;
  }
}

bool hasForecastHours(MySQL::Server& server,MySQL::LocalQuery& query)
{
  query.set("select dim_name,dim_size from metautil.custom_dap_fcst_hrs where ID = '"+dap_args.ID+"'");
  if (query.submit(server) < 0) {
    std::cerr << "opendap hasForecastHours: " << query.error() << " for " << query.show() << std::endl;
    dapError("500 Internal Server Error","Database error hasForecastHours");
  }
  if (query.num_rows() > 0) {
    return true;
  }
  else {
    return false;
  }
}

bool hasLevels(MySQL::Server& server,MySQL::LocalQuery& query)
{
  query.set("select dim_name,dim_size from metautil.custom_dap_levels where ID = '"+dap_args.ID+"'");
  if (query.submit(server) < 0) {
    std::cerr << "opendap hasLevels: " << query.error() << " for " << query.show() << std::endl;
    dapError("500 Internal Server Error","Database error hasLevels");
  }
  if (query.num_rows() > 0) {
    return true;
  }
  else {
    return false;
  }
}

void printDDS(std::ostream& outs,std::string content_type,std::string content_description)
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::stringstream dds;
  bool has_ref_times=false,has_fcst_hrs=false,has_levels=false;

  decodeRequestData();
  if (projection_table.size() == 0) {
    if ( (has_ref_times=hasReferenceTimes(server,query))) {
	while (query.fetch_row(row)) {
	  dds << "  Int32 " << row[0] << "[" << row[0] << " = " << row[1] << "];" << std::endl;
	}
    }
    if ( (has_fcst_hrs=hasForecastHours(server,query))) {
	while (query.fetch_row(row)) {
	  dds << "  Int32 " << row[0] << "[" << row[0] << " = " << row[1] << "];" << std::endl;
	}
    }
    query.set("select dim_name,dim_size from metautil.custom_dap_times where ID = '"+dap_args.ID+"'");
    if (query.submit(server) < 0) {
	std::cerr << "opendap printDDS(3): " << query.error() << " for " << query.show() << std::endl;
	dapError("500 Internal Server Error","Database error printDDS(3)");
    }
    while (query.fetch_row(row)) {
	dds << "  Int32 " << row[0] << "[" << row[0] << " = " << row[1] << "];" << std::endl;
    }
    if ( (has_levels=hasLevels(server,query))) {
	while (query.fetch_row(row)) {
	  dds << "  Float32 " << row[0] << "[" << row[0] << " = " << row[1] << "];" << std::endl;
	}
    }
    decodeGridDefinition();
    dds << "  Float32 lat[lat = " << lat.length << "];" << std::endl;
    dds << "  Float32 lon[lon = " << lon.length << "];" << std::endl;
    std::string qspec;
    if (has_ref_times) {
	if (has_fcst_hrs) {
	  qspec="(select distinct g.param,r.dim_name,r.dim_size,f.dim_name,f.dim_size from metautil.custom_dap_ref_times as r left join metautil.custom_dap_grid_index as g on g.ID = r.ID and g.time_slice_index = r.max_dim left join metautil.custom_dap_fcst_hrs as f on f.ID = r.ID and concat('ref_time_',f.dim_name) = concat(concat(substr(r.dim_name,1,8),'_fcst_hr'),substr(r.dim_name,9)) where g.ID = '"+dap_args.ID+"' and ref_time > 0 order by g.param)";
	}
	else {
	  qspec="(select distinct g.param,r.dim_name,r.dim_size,t.dim_name,t.dim_size from metautil.custom_dap_ref_times as r left join metautil.custom_dap_grid_index as g on g.ID = r.ID and g.time_slice_index = r.max_dim left join metautil.custom_dap_times as t on t.ID = r.ID and concat('ref_',t.dim_name) = r.dim_name where g.ID = '"+dap_args.ID+"' and ref_time > 0 order by g.param)";
	}
	qspec+="UNION ";
    }
    qspec+="(select distinct g.param,t.dim_name,t.dim_size,NULL,NULL from metautil.custom_dap_times as t left join metautil.custom_dap_grid_index as g on g.ID = t.ID and g.time_slice_index = t.max_dim where g.ID = '"+dap_args.ID+"' and g.ref_time = 0 order by g.param,t.dim_size desc)";
    query.set(qspec);
//std::cerr << query.show() << std::endl;
    if (query.submit(server) < 0) {
	std::cerr << "opendap printDDS(4): " << query.error() << " for " << query.show() << std::endl;
	dapError("500 Internal Server Error","Database error printDDS(4)");
    }
    my::map<StringEntry> unique_param_table;
    StringEntry se;
    while (query.fetch_row(row)) {
	if (!unique_param_table.found(row[0],se)) {
	  std::stringstream dds_a,dds_m;
	  dds << "  Grid {" << std::endl;
	  dds_a << "    Array:" << std::endl;
	  dds_m << "    Maps:" << std::endl;
	  dds_a << "      Float32 " << strutils::substitute(row[0]," ","_") << "[" << row[1] << " = " << row[2] << "]";
	  dds_m << "      Int32 " << row[1] << "[" << row[1] << " = " << row[2] << "];" << std::endl;
	  if (row[3].length() > 0) {
	    dds_a << "[" << row[3] << " = " << row[4] << "]";
	    dds_m << "      Int32 " << row[3] << "[" << row[3] << " = " << row[4] << "];" << std::endl;
	  }
	  if (has_levels) {
	    MySQL::LocalQuery query2;
	    query2.set("select l.dim_name,l.dim_size from (select any_value(x.ID) as ID,any_value(x.uID) as uID,count(x.uID) as dim_size from (select distinct g.ID as ID,g.level_code,l.uID as uID from metautil.custom_dap_grid_index as g left join metautil.custom_dap_level_index as l on l.ID = g.ID where g.ID = '"+dap_args.ID+"' and g.param = '"+row[0]+"') as x) as y left join metautil.custom_dap_levels as l on l.ID = y.ID and l.uID = y.uID and l.dim_size = y.dim_size");
	    if (query2.submit(server) < 0) {
		std::cerr << "opendap printDDS(4a): " << query2.error() << " for " << query2.show() << std::endl;
		dapError("500 Internal Server Error","Database error printDDS(4a)");
	    }
	    MySQL::Row row2;
	    if (!query2.fetch_row(row2)) {
		std::cerr << "opendap printDDS(4b): " << query2.error() << " for " << query2.show() << std::endl;
		dapError("500 Internal Server Error","Database error printDDS(4b)");
	    }
	    if (row2[0].length() > 0) {
		dds_a << "[" << row2[0] << " = " << row2[1] << "]";
		dds_m << "      Float32 " << row2[0] << "[" << row2[0] << " = " << row2[1] << "];" << std::endl;
	    }
	  }
	  dds_a << "[lat = " << lat.length << "][lon = " << lon.length << "];" << std::endl;
	  dds_m << "      Float32 lat[lat = " << lat.length << "];" << std::endl;
	  dds_m << "      Float32 lon[lon = " << lon.length << "];" << std::endl;
	  dds << dds_a.str() << dds_m.str() << "  } " << strutils::substitute(row[0]," ","_") << ";" << std::endl;
	  se.key=row[0];
	  unique_param_table.insert(se);
	}
    }
  }
  else {
    for (const auto& key : projection_table.keys()) {
	ProjectionEntry pe;
	if (projection_table.found(key,pe)) {
	  if (std::regex_search(key,std::regex("^(v){0,1}time"))) {
	    if (pe.data->idx.size() > 0) {
		dds << "  Int32 " << key << "[" << key << " = " << pe.data->idx[0].length << "];" << std::endl;
	    }
	    else {
		query.set("select dim_size from metautil.custom_dap_times where ID = '"+dap_args.ID+"' and dim_name = '"+key+"'");
		if (query.submit(server) < 0 || !query.fetch_row(row)) {
		  std::cerr << "opendap printDDS(7a): " << query.error() << " for " << query.show() << std::endl;
		  dapError("500 Internal Server Error","Database error printDDS(7a)");
		}
		dds << "  Int32 " << key << "[" << key << " = " << row[0] << "];" << std::endl;
	    }
	  }
	  else if (std::regex_search(key,std::regex("^ref_time"))) {
	    if (pe.data->idx.size() > 0) {
		dds << "  Int32 " << key << "[" << key << " = " << pe.data->idx[0].length << "];" << std::endl;
	    }
	    else {
		query.set("select dim_size from metautil.custom_dap_ref_times where ID = '"+dap_args.ID+"' and dim_name = '"+key+"'");
		if (query.submit(server) < 0 || !query.fetch_row(row)) {
		  std::cerr << "opendap printDDS(7b): " << query.error() << " for " << query.show() << std::endl;
		  dapError("500 Internal Server Error","Database error printDDS(7b)");
		}
		dds << "  Int32 " << key << "[" << key << " = " << row[0] << "];" << std::endl;
	    }
	  }
	  else if (std::regex_search(key,std::regex("^fcst_hr"))) {
	    if (pe.data->idx.size() > 0) {
		dds << "  Int32 " << key << "[" << key << " = " << pe.data->idx[0].length << "];" << std::endl;
	    }
	    else {
		query.set("select dim_size from metautil.custom_dap_fcst_hrs where ID = '"+dap_args.ID+"' and dim_name = '"+key+"'");
		if (query.submit(server) < 0 || !query.fetch_row(row)) {
		  std::cerr << "opendap printDDS(7c): " << query.error() << " for " << query.show() << std::endl;
		  dapError("500 Internal Server Error","Database error printDDS(7c)");
		}
		dds << "  Int32 " << key << "[" << key << " = " << row[0] << "];" << std::endl;
	    }
	  }
	  else if (std::regex_search(key,std::regex("^level"))) {
	    query.set("select dim_size from metautil.custom_dap_levels where ID = '"+dap_args.ID+"' and dim_name = '"+key+"'");
	    if (query.submit(server) < 0 || !query.fetch_row(row)) {
		std::cerr << "opendap printDDS(8): " << query.error() << " for " << query.show() << std::endl;
		dapError("500 Internal Server Error","Database error printDDS(8)");
	    }
	    dds << "  Float32 " << key << "[" << key << " = " << row[0] << "];" << std::endl;
	  }
	  else if (std::regex_search(key,std::regex("^lat"))) {
	    decodeGridDefinition();
	    dds << "  Float32 lat[lat = " << lat.length << "];" << std::endl;
	  }
	  else if (std::regex_search(key,std::regex("^lon"))) {
	    decodeGridDefinition();
	    dds << "  Float32 lon[lon = " << lon.length << "];" << std::endl;
	  }
	  else {
	    if (pe.data->idx.size() == 0) {
		DimensionIndex di;
		if (hasReferenceTimes(server,query)) {
		  if (hasForecastHours(server,query)) {
		    query.set("select r.dim_name,r.dim_size,f.dim_name,f.dim_size from metautil.custom_dap_ref_times as r left join metautil.custom_dap_grid_index as g on g.ID = r.ID and g.time_slice_index = r.max_dim left join metautil.custom_dap_fcst_hrs as f on f.ID = r.ID and concat('ref_time_',f.dim_name) = concat(concat(substr(r.dim_name,1,8),'_fcst_hr'),substr(r.dim_name,9)) where g.ID = '"+dap_args.ID+"' and g.param = '"+key+"'");
		  }
		  else {
		    query.set("select r.dim_name,r.dim_size,t.dim_name,t.dim_size from metautil.custom_dap_ref_times as r left join metautil.custom_dap_grid_index as g on g.ID = r.ID and g.time_slice_index = r.max_dim left join metautil.custom_dap_times as t on t.ID = r.ID and concat('ref_',t.dim_name) = r.dim_name where g.ID = '"+dap_args.ID+"' and g.param = '"+key+"'");
		  }
		  if (query.submit(server) == 0 && query.fetch_row(row)) {
		    di.length=std::stoi(row[1]);
		    di.start=0;
		    di.stop=di.length-1;
		    di.increment=1;
		    pe.data->idx.emplace_back(di);
		    di.length=std::stoi(row[3]);
		    di.start=0;
		    di.stop=di.length-1;
		    di.increment=1;
		    pe.data->idx.emplace_back(di);
		  }
		}
		else {
		  query.set("select t.dim_name,t.dim_size from metautil.custom_dap_times as t left join metautil.custom_dap_grid_index as g on g.ID = t.ID and g.time_slice_index = t.max_dim where g.ID = '"+dap_args.ID+"' and g.param = '"+key+"'");
		  if (query.submit(server) == 0 && query.fetch_row(row)) {
		    di.length=std::stoi(row[1]);
		    di.start=0;
		    di.stop=di.length-1;
		    di.increment=1;
		    pe.data->idx.emplace_back(di);
		  }
		}
		if (hasLevels(server,query)) {
		  query.set("select l.dim_name,l.dim_size from (select x.ID,x.uID as uID,count(x.uID) as dim_size from (select distinct g.ID as ID,g.level_code,l.uID as uID from metautil.custom_dap_grid_index as g left join metautil.custom_dap_level_index as l on l.ID = g.ID where g.ID = '"+dap_args.ID+"' and g.param = '"+key+"') as x) as y left join metautil.custom_dap_levels as l on l.ID = y.ID and l.uID = y.uID and l.dim_size = y.dim_size");
		  if (query.submit(server) == 0 && query.fetch_row(row)) {
		    di.length=std::stoi(row[1]);
		    di.start=0;
		    di.stop=di.length-1;
		    di.increment=1;
		    pe.data->idx.emplace_back(di);
		  }
		}
		decodeGridDefinition();
		di.length=lat.length;
		di.start=0;
		di.stop=di.length-1;
		di.increment=1;
		pe.data->idx.emplace_back(di);
		di.length=lon.length;
		di.start=0;
		di.stop=di.length-1;
		di.increment=1;
		pe.data->idx.emplace_back(di);
	    }
	    if (pe.data->idx.size() >= 3 && pe.data->idx.size() <= 5) {
		if (pe.data->member.length() > 0) {
		  dds << "  Structure {" << std::endl;
		  dds << "    Float32 " << pe.key;
		}
		else {
		  dds << "  Grid {" << std::endl;
		  dds << "    Array:" << std::endl;
		  dds << "      Float32 " << pe.key;
		}
		if (pe.data->ref_time_dim.name.length() > 0) {
		  dds << "[" << pe.data->ref_time_dim.name << " = ";
		  if (pe.data->idx[0].increment > 1) {
		    dds << (pe.data->idx[0].length+pe.data->idx[0].increment-1)/pe.data->idx[0].increment;
		  }
		  else {
		    dds << pe.data->idx[0].length;
		  }
		  dds << "]";
		  if (pe.data->time_dim.name.length() > 0) {
		    dds << "[" << pe.data->time_dim.name << " = ";
		    if (pe.data->idx[1].increment > 1) {
			dds << (pe.data->idx[1].length+pe.data->idx[1].increment-1)/pe.data->idx[1].increment;
		    }
		    else {
			dds << pe.data->idx[1].length;
		    }
		    dds << "]";
		  }
		  else {
		    dds << "[" << pe.data->fcst_hr_dim.name << " = ";
		    if (pe.data->idx[1].increment > 1) {
			dds << (pe.data->idx[1].length+pe.data->idx[1].increment-1)/pe.data->idx[1].increment;
		    }
		    else {
			dds << pe.data->idx[1].length;
		    }
		    dds << "]";
		  }
		  if (pe.data->idx.size() == 5) {
		    dds << "[" << pe.data->level_dim.name << " = ";
		    if (pe.data->idx[2].increment > 1) {
			dds << (pe.data->idx[2].length+pe.data->idx[2].increment-1)/pe.data->idx[2].increment;
		    }
		    else {
			dds << pe.data->idx[2].length;
		    }
		    dds << "]";
		  }
		}
		else {
		  dds << "[time = " << pe.data->idx[0].length << "]";
		  if (pe.data->idx.size() == 4) {
		    dds << "[" << pe.data->level_dim.name << " = ";
		    if (pe.data->idx[1].increment > 1) {
			dds << (pe.data->idx[1].length+pe.data->idx[1].increment-1)/pe.data->idx[1].increment;
		    }
		    else {
			dds << pe.data->idx[1].length;
		    }
		    dds << "]";
		  }
		}
		dds << "[lat = ";
		auto idx=pe.data->idx.size()-2;
		if (pe.data->idx[idx].increment > 1) {
		  dds << (pe.data->idx[idx].length+pe.data->idx[idx].increment-1)/pe.data->idx[idx].increment;
		}
		else {
		  dds << pe.data->idx[idx].length;
		}
		dds << "][lon = ";
		++idx;
		if (pe.data->idx[idx].increment > 1) {
		  dds << (pe.data->idx[idx].length+pe.data->idx[idx].increment-1)/pe.data->idx[idx].increment;
		}
		else {
		  dds << pe.data->idx[idx].length;
		}
		dds << "];" << std::endl;
		if (pe.data->member.length() == 0) {
		  dds << "    Maps:" << std::endl;
		  if (pe.data->ref_time_dim.name.length() > 0) {
		    dds << "      Int32 " << pe.data->ref_time_dim.name << "[" << pe.data->ref_time_dim.name << " = ";
		    if (pe.data->idx[0].increment > 1) {
			dds << (pe.data->idx[0].length+pe.data->idx[0].increment-1)/pe.data->idx[0].increment;
		    }
		    else {
			dds << pe.data->idx[0].length;
		    }
		    dds << "];";
		    if (pe.data->time_dim.name.length() > 0) {
			dds << "      Int32 " << pe.data->time_dim.name << "[" << pe.data->time_dim.name << " = ";
			if (pe.data->idx[1].increment > 1) {
			  dds << (pe.data->idx[1].length+pe.data->idx[1].increment-1)/pe.data->idx[1].increment;
			}
			else {
			  dds << pe.data->idx[1].length;
			}
			dds << "];";
		    }
		    else {
			dds << "      Int32 " << pe.data->fcst_hr_dim.name << "[" << pe.data->fcst_hr_dim.name << " = ";
			if (pe.data->idx[1].increment > 1) {
			  dds << (pe.data->idx[1].length+pe.data->idx[1].increment-1)/pe.data->idx[1].increment;
			}
			else {
			  dds << pe.data->idx[1].length;
			}
			dds << "];";
		    }
		    if (pe.data->idx.size() == 5) {
			dds << "      Float32 " << pe.data->level_dim.name << "[" << pe.data->level_dim.name << " = ";
			if (pe.data->idx[2].increment > 1) {
			  dds << (pe.data->idx[2].length+pe.data->idx[2].increment-1)/pe.data->idx[2].increment;
			}
			else {
			  dds << pe.data->idx[2].length;
			}
			dds << "];" << std::endl;
		    }
		  }
		  else {
		    dds << "      Int32 " << pe.data->time_dim.name << "[" << pe.data->time_dim.name << " = ";
		    if (pe.data->idx[0].increment > 1) {
			dds << (pe.data->idx[0].length+pe.data->idx[0].increment-1)/pe.data->idx[0].increment;
		    }
		    else {
			dds << pe.data->idx[0].length;
		    }
		    dds << "];" << std::endl;
		    if (pe.data->idx.size() == 4) {
			dds << "      Float32 " << pe.data->level_dim.name << "[" << pe.data->level_dim.name << " = ";
			if (pe.data->idx[1].increment > 1) {
			  dds << (pe.data->idx[1].length+pe.data->idx[1].increment-1)/pe.data->idx[1].increment;
			}
			else {
			  dds << pe.data->idx[1].length;
			}
			dds << "];" << std::endl;
		    }
		  }
		  auto idx=pe.data->idx.size()-2;
		  dds << "      Float32 lat[lat = ";
		  if (pe.data->idx[idx].increment > 1) {
		    dds << (pe.data->idx[idx].length+pe.data->idx[idx].increment-1)/pe.data->idx[idx].increment;
		  }
		  else {
		    dds << pe.data->idx[idx].length;
		  }
		  dds << "];" << std::endl;
		  ++idx;
		  dds << "      Float32 lon[lon = ";
		  if (pe.data->idx[idx].increment > 1) {
		    dds << (pe.data->idx[idx].length+pe.data->idx[idx].increment-1)/pe.data->idx[idx].increment;
		  }
		  else {
		    dds << pe.data->idx[idx].length;
		  }
		  dds << "];" << std::endl;
		}
		dds << "  } " << pe.key << ";" << std::endl;
	    }
	  }
	}
    }
  }
  outs << "Content-Type: " << content_type << std::endl;
  outs << "Content-Description: " << content_description << std::endl;
  printDateAndVersion(outs);
  outs << std::endl;
  outs << "Dataset {" << std::endl;
  outs << dds.str();
  outs << "} " << dap_args.ID << ";" << std::endl;
}

struct FormatEntry {
  FormatEntry() : key(),format(nullptr) {}

  std::string key;
  std::shared_ptr<std::string> format;
};
struct ParameterEntry {
  struct Data {
    Data() : format(),long_name(),units() {}

    std::string format,long_name,units;
  };
  ParameterEntry() : key(),data(nullptr) {}

  std::string key;
  std::shared_ptr<Data> data;
};

void printGlobalAttributes()
{
  MySQL::LocalQuery query;
  MySQL::Row row;

  query.set("title","search.datasets","dsid = '"+dap_args.dsnum+"'");
  if (query.submit(server) == 0 && query.fetch_row(row)) {
    if (dap_args.ext == ".das") {
	std::cout << "    String CISL_RDA_source_dataset_title \"" << row[0] << "\";" << std::endl;
    }
    else if (dap_args.ext == ".info") {
	std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>CISL_RDA_source_dataset_title:</strong></td><td>&nbsp;</td><td align=\"left\">" << row[0] << "</td></tr>" << std::endl;
    }
  }
  else {
    if (dap_args.ext == ".das") {
	std::cout << "    String CISL_RDA_source_dataset_number \"" << dap_args.dsnum << "\";" << std::endl;
    }
    else if (dap_args.ext == ".info") {
	std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>CISL_RDA_source_dataset_number:</strong></td><td>&nbsp;</td><td align=\"left\">" << dap_args.dsnum << "</td></tr>" << std::endl;
    }
  }
  if (dap_args.ext == ".das") {
    std::cout << "    String CISL_RDA_source_dataset_URL \"http://rda.ucar.edu/datasets/ds" << dap_args.dsnum << "/\";" << std::endl;
  }
  else if (dap_args.ext == ".info") {
    std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>CISL_RDA_source_dataset_URL:</strong></td><td>&nbsp;</td><td align=\"left\">http://rda.ucar.edu/datasets/ds" << dap_args.dsnum << "/</td></tr>" << std::endl;
  }
  if (grid_subset_args.inittime.length() > 0) {
    if (dap_args.ext == ".das") {
	std::cout << "    String model_initialization_time \"" << grid_subset_args.inittime << " UTC\";" << std::endl;
    }
    else if (dap_args.ext == ".info") {
	std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>model_initialization_time:</strong></td><td>&nbsp;</td><td align=\"left\">" << grid_subset_args.inittime << " UTC</td></tr>" << std::endl;
    }
  }
}

void printRefTimes()
{
  MySQL::LocalQuery query;

  query.set("select dim_name,dim_size,base_time from metautil.custom_dap_ref_times where ID = '"+dap_args.ID+"'");
  if (query.submit(server) == 0) {
    MySQL::Row row;
    while (query.fetch_row(row)) {
	if (dap_args.ext == ".das") {
	  std::cout << "  " << row[0] << " {" << std::endl;
	  std::cout << "    String long_name \"forecast_reference_time\";" << std::endl;
	  std::cout << "    String units \"minutes since " << DateTime(std::stoll(row[2])*100).toString("%Y-%m-%d %H:%MM +0:00") << "\";" << std::endl;
	  std::cout << "    String calendar \"standard\";" << std::endl;
	  std::cout << "  }" << std::endl;
	}
	else if (dap_args.ext == ".info") {
	  std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>" << row[0] << ":</strong></td><td>&nbsp;</td><td align=\"left\">Array of 32 bit Integers [" << row[0] << " = 0.." << row[1] << "]</td></tr>" << std::endl;
	  std::cout << "<tr valign=\"top\"><td></td><td></td><td>";
	  std::cout << "<strong>long_name:</strong>&nbsp;forecast_reference_time<br />";
	  std::cout << "<strong>units:</strong>&nbsp;minutes since " << DateTime(std::stoll(row[2])*100).toString("%Y-%m-%d %H:%MM +0:00") << "<br />";
	  std::cout << "<strong>calendar:</strong>&nbsp;standard<br />";
	  std::cout << "</td></tr>" << std::endl;
	}
    }
  }
}

void printTimes(std::string time_ID = "")
{
  MySQL::LocalQuery query;
  MySQL::Row row;

  auto qspec="select dim_name,dim_size from metautil.custom_dap_times where ID = '"+dap_args.ID+"'";
  if (time_ID.length() > 0) {
    qspec+=" and dim_name = '"+time_ID+"'";
  }
  query.set(qspec);
  if (query.submit(server) == 0) {
    while (query.fetch_row(row)) {
	if (dap_args.ext == ".das") {
	  std::cout << "  " << row[0] << " {" << std::endl;
	  if (row[0][0] == 'v') {
	    std::cout << "    String long_name \"valid_time\";" << std::endl;
	  }
	  else {
	    std::cout << "    String long_name \"time\";" << std::endl;
	  }
	  std::cout << "    String units \"minutes since " << DateTime(std::stoll(grid_subset_args.startdate)*100).toString("%Y-%m-%d %H:%MM +0:00") << "\";" << std::endl;
	  std::cout << "    String calendar \"standard\";" << std::endl;
	  std::cout << "  }" << std::endl;
	}
	else if (dap_args.ext == ".info") {
	  std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>" << row[0] << ":</strong></td><td>&nbsp;</td><td align=\"left\">Array of 32 bit Integers [" << row[0] << " = 0.." << row[1] << "]</td></tr>" << std::endl;
	  std::cout << "<tr valign=\"top\"><td></td><td></td><td>";
	  std::cout << "<strong>long_name:</strong>&nbsp;time<br />";
	  std::cout << "<strong>units:</strong>&nbsp;minutes since " << DateTime(std::stoll(grid_subset_args.startdate)*100).toString("%Y-%m-%d %H:%MM +0:00") << "<br />";
	  std::cout << "<strong>calendar:</strong>&nbsp;standard<br />";
	  std::cout << "</td></tr>" << std::endl;
	}
    }
  }
}

void printFcstHrs(std::string fcst_hr_ID = "")
{
  MySQL::LocalQuery query;

  auto qspec="select dim_name,dim_size from metautil.custom_dap_fcst_hrs where ID = '"+dap_args.ID+"'";
  if (fcst_hr_ID.length() > 0) {
    qspec+=" and dim_name = '"+fcst_hr_ID+"'";
  }
  query.set(qspec);
  if (query.submit(server) == 0) {
    MySQL::Row row;
    while (query.fetch_row(row)) {
	if (dap_args.ext == ".das") {
	  std::cout << "  " << row[0] << " {" << std::endl;
	  std::cout << "    String long_name \"forecast_period\";" << std::endl;
	  std::cout << "    String units \"hours\";" << std::endl;
	  std::cout << "  }" << std::endl;
	}
	else if (dap_args.ext == ".info") {
	  std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>" << row[0] << ":</strong></td><td>&nbsp;</td><td align=\"left\">Array of 32 bit Floats [" << row[0] << " = 0.." << row[1] << "]</td></tr>" << std::endl;
	  std::cout << "<tr valign=\"top\"><td></td><td></td><td>";
	  std::cout << "<strong>long_name:</strong>&nbsp;forecast_period<br />";
	  std::cout << "<strong>units:</strong>&nbsp;hours<br />";
	  std::cout << "</td></tr>" << std::endl;
	}
    }
  }
}

void printLevels(std::string format,xmlutils::LevelMapper& level_mapper,std::string level_ID = "")
{
  std::string qspec;
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::deque<std::string> sp;

  qspec="select distinct i.level_key,l.dim_name,l.dim_size from metautil.custom_dap_level_index as i left join metautil.custom_dap_levels as l on l.ID = i.ID and l.level_key = i.level_key where i.ID = '"+dap_args.ID+"'";
  if (level_ID.length() > 0) {
    qspec+=" and l.dim_name = '"+level_ID+"'";
  }
  query.set(qspec);
  if (query.submit(server) == 0) {
    while (query.fetch_row(row)) {
	sp=strutils::split(row[0],":");
	auto sdum=level_mapper.getDescription(format,sp[1],sp[0]);
	if (dap_args.ext == ".das") {
	  std::cout << "  " << row[1] << " {" << std::endl;
	  std::cout << "    String description \"" << sdum << "\";" << std::endl;
	  if (std::regex_search(sdum,std::regex("^Layer"))) {
	    std::cout << "    String comment \"Values are the midpoints of the layers\";" << std::endl;
	  }
	  std::cout << "    String units \"" << level_mapper.getUnits(format,sp[1],sp[0]) << "\";" << std::endl;
	  std::cout << "  }" << std::endl;
	}
	else if (dap_args.ext == ".info") {
	  std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>" << row[1] << ":</strong></td><td>&nbsp;</td><td align=\"left\">Array of 32 bit Reals [" << row[1] << " = 0.." << row[2] << "]</td></tr>";
	  std::cout << "<tr valign=\"top\"><td></td><td></td><td>";
	  std::cout << "<strong>description:</strong>&nbsp;" << sdum << "<br />";
	  if (std::regex_search(sdum,std::regex("^Layer"))) {
	    std::cout << "<strong>comment:</strong>&nbsp;degrees_north<br />";
	  }
	  std::cout << "<strong>units:</strong>&nbsp;" << level_mapper.getUnits(format,sp[1],sp[0]) << "<br />";
	  std::cout << "</td></tr>" << std::endl;
	}
    }
  }
}

void printLatitudeAndLongitude()
{
  if (dap_args.ext == ".das") {
    std::cout << "  lat {" << std::endl;
    std::cout << "    String long_name \"latitude\";" << std::endl;
    std::cout << "    String units \"degrees_north\";" << std::endl;
    std::cout << "  }" << std::endl;
    std::cout << "  lon {" << std::endl;
    std::cout << "    String long_name \"longitude\";" << std::endl;
    std::cout << "    String units \"degrees_east\";" << std::endl;
    std::cout << "  }" << std::endl;
  }
  else if (dap_args.ext == ".info") {
    decodeGridDefinition();
    std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>lat:</strong></td><td>&nbsp;</td><td align=\"left\">Array of 32 bit Reals [lat = 0.." << lat.length << "]</td></tr>";
    std::cout << "<tr valign=\"top\"><td></td><td></td><td>";
    std::cout << "<strong>long_name:</strong>&nbsp;latitude<br />";
    std::cout << "<strong>units:</strong>&nbsp;degrees_north<br />";
    std::cout << "</td></tr>" << std::endl;
    std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>lon:</strong></td><td>&nbsp;</td><td align=\"left\">Array of 32 bit Reals [lon = 0.." << lon.length << "]</td></tr>";
    std::cout << "<tr valign=\"top\"><td></td><td></td><td>";
    std::cout << "<strong>long_name:</strong>&nbsp;longitude<br />";
    std::cout << "<strong>units:</strong>&nbsp;degrees_east<br />";
    std::cout << "</td></tr>" << std::endl;
  }
}
void printParameterAttributes(std::string format,std::string param_ID,std::string description,std::string units)
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  xmlutils::LevelMapper level_mapper;

  if (dap_args.ext == ".das") {
    std::cout << "  " << param_ID << " {" << std::endl;
    if (description.length() > 0) {
	std::cout << "    String long_name \"" << description << "\";" << std::endl;
	std::cout << "    String units \"" << units << "\";" << std::endl;
	if (format == "WMO_GRIB1" || format == "WMO_GRIB2") {
	  std::cout << "    Float32 _FillValue " << Grid::missingValue << ";" << std::endl;
	}
	if (grid_subset_args.inittime.length() > 0) {
	  std::cout << "    String model_initialization_time \"" << grid_subset_args.inittime << " UTC\";" << std::endl;
	}
    }
    std::cout << "  }" << std::endl;
  }
  else if (dap_args.ext == ".info") {
    std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>" << param_ID << ":</strong></td><td>&nbsp;</td><td align=\"left\">Grid</td></tr>";
    if (description.length() > 0) {
	std::cout << "<tr valign=\"top\"><td></td><td></td><td>";
	std::cout << "<strong>long_name:</strong>&nbsp;" << description << "<br />";
	std::cout << "<strong>units:</strong>&nbsp;" << units << "<br />";
	if (format == "WMO_GRIB1" || format == "WMO_GRIB2") {
	  std::cout << "<strong>_FillValue:</strong>&nbsp;" << Grid::missingValue << "<br />";
	}
	if (grid_subset_args.inittime.length() > 0) {
	  std::cout << "<strong>model_initialization_time:</strong>&nbsp;" << grid_subset_args.inittime << "<br />";
	}
	std::cout << "<table cellspacing=\"0\" cellpadding=\"0\" border=\"0\">";
	fillParameterQuery(query,param_ID);
	if (query.submit(server) == 0 && query.fetch_row(row)) {
	  std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>" << param_ID << ":</strong></td><td>&nbsp;</td><td align=\"left\">Array of 32 bit Reals ";
	  if (row.length() > 5) {
	    std::cout << "[" << row[5] << " = 0.." << row[6] << "]";
	  }
	  std::cout << "[" << row[0] << " = 0.." << row[1] << "]";
	  if (row[2].length() > 0) {
	    std::cout << "[" << row[2] << " = 0.." << row[3] << "]";
	  }
	  std::cout << "[lat = 0.." << lat.length << "][lon = 0.." << lon.length << "]</td></tr>";
	  std::cout << "<tr valign=\"top\"><td></td><td></td><td align=\"left\"><strong>long_name:</strong>&nbsp;" << description << "</td></tr>";
	  std::cout << "<tr valign=\"top\"><td></td><td></td><td align=\"left\"><strong>units:</strong>&nbsp;" << units << "</td></tr>";
	  if (format == "WMO_GRIB1" || format == "WMO_GRIB2") {
	    std::cout << "<tr valign=\"top\"><td></td><td></td><td align=\"left\"><strong>_FillValue:</strong>&nbsp;" << Grid::missingValue << "</td></tr>";
	  }
	  if (grid_subset_args.inittime.length() > 0) {
	    std::cout << "<tr valign=\"top\"><td></td><td></td><td align=\"left\"><strong>model_initialization_time:</strong>&nbsp;" << grid_subset_args.inittime << "</td></tr>";
	  }
	  printRefTimes();
	  printTimes(row[0]);
	  if (row[2].length() > 0) {
	    printLevels(format,level_mapper,row[2]);
	  }
	  printLatitudeAndLongitude();
	}
	std::cout << "</table>";
	std::cout << "</td></tr>" << std::endl;
    }
  }
}

void printParameters(std::string& format,xmlutils::LevelMapper& level_mapper)
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  my::map<FormatEntry> unique_formats_table;
  my::map<ParameterEntry> parameter_table;
  ParameterEntry pe;

  for (const auto& key : grid_subset_args.parameters.keys()) {
    gridSubset::Parameter param;
    grid_subset_args.parameters.found(key,param);
    if (param.format_code != nullptr) {
	FormatEntry fe;
	if (!unique_formats_table.found(*param.format_code,fe)) {
	  query.set("select format from WGrML.formats where code = "+*param.format_code);
	  if (query.submit(server) == 0 && query.fetch_row(row)) {
	    fe.key=*param.format_code;
	    fe.format.reset(new std::string);
	    *fe.format=row[0];
	    unique_formats_table.insert(fe);
	  }
	}
	xmlutils::ParameterMapper parameter_mapper;
	pe.key=strutils::substitute(parameter_mapper.getShortName(*fe.format,key)," ","_");
	pe.data.reset(new ParameterEntry::Data);
	pe.data->format=*fe.format;
	pe.data->long_name=parameter_mapper.getDescription(*fe.format,key);
	pe.data->units=parameter_mapper.getUnits(*fe.format,key);
	parameter_table.insert(pe);
    }
  }
  query.set("select distinct param from metautil.custom_dap_grid_index where ID = '"+dap_args.ID+"' and time_slice_index = 0");
  if (query.submit(server) < 0) {
    std::cerr << "opendap printDAS(1): " << query.error() << " for " << query.show() << std::endl;
    dapError("500 Internal Server Error","Database error printDAS(1)");
  }
  while (query.fetch_row(row)) {
    parameter_table.found(row[0].substr(0,row[0].rfind("_")),pe);
    MySQL::LocalQuery query2;
    query2.set("select l.level_key,y.level_values from (select any_value(x.ID) as ID,any_value(x.uID) as uID,count(x.uID) as dim_size,group_concat(x.level_code separator '!') as level_values from (select distinct g.ID as ID,g.level_code,l.uID as uID from metautil.custom_dap_grid_index as g left join metautil.custom_dap_level_index as l on l.ID = g.ID where g.ID = '"+dap_args.ID+"' and g.time_slice_index < 100 and g.param = '"+row[0]+"') as x) as y left join metautil.custom_dap_levels as l on l.ID = y.ID and l.uID = y.uID and l.dim_size = y.dim_size");
    if (query2.submit(server) < 0) {
	std::cerr << "opendap printDAS(1a): " << query2.error() << " for " << query2.show() << std::endl;
	dapError("500 Internal Server Error","Database error printDAS(1a)");
    }
    MySQL::Row row2;
    while (query2.fetch_row(row2)) {
	if (row2[0].length() == 0) {
	  MySQL::LocalQuery query3;
	  MySQL::Row row3;
	  query3.set("select distinct l.type,l.map,l.value from metautil.custom_dap_grid_index as g left join WGrML.levels as l on l.code = g.level_code where g.param = '"+row[0]+"' and g.ID = '"+dap_args.ID+"' and g.time_slice_index = 0");
	  if (query3.submit(server) < 0) {
	    std::cerr << "opendap printDAS(1b): " << query2.error() << " for " << query2.show() << std::endl;
	    dapError("500 Internal Server Error","Database error printDAS(1b)");
	  }
	  while (query3.fetch_row(row3)) {
	    auto lunits=level_mapper.getUnits(pe.data->format,row3[0],row3[1]);
	    if (lunits.length() > 0) {
		printParameterAttributes(pe.data->format,strutils::substitute(row[0]," ","_"),pe.data->long_name+" at "+row3[2]+" "+lunits,pe.data->units);
	    }
	    else {
		std::string ldes="";
		if (std::regex_search(row3[0],std::regex("-")) && std::regex_search(row3[2],std::regex(","))) {
		  ldes+=" in "+level_mapper.getDescription(pe.data->format,row3[0],row3[1]);
		  auto sp=strutils::split(row3[0],"-");
		  auto sp2=strutils::split(row3[2],",");
		  ldes+=" - top: "+sp2[1]+" "+level_mapper.getUnits(pe.data->format,sp[1],row3[1])+" bottom: "+sp2[0]+" "+level_mapper.getUnits(pe.data->format,sp[0],row3[1]);
		}
		else {
		  ldes+=" at "+level_mapper.getDescription(pe.data->format,row3[0],row3[1]);
		  if (row3[2] != "0") {
		    ldes+=" "+row3[2];
		  }
		}
		printParameterAttributes(pe.data->format,strutils::substitute(row[0]," ","_"),pe.data->long_name+ldes,pe.data->units);
	    }
	  }
	}
	else {
	  auto sp=strutils::split(row2[0],":");
	  auto ldes=level_mapper.getDescription(pe.data->format,sp[1],sp[0]);
	  sp=strutils::split(row2[1],"!");
	  auto sdum=" at ";
	  for (const auto& p : sp) {
	    if (std::regex_search(p,std::regex(","))) {
		sdum=" in ";
	    }
	  }
	  printParameterAttributes(pe.data->format,strutils::substitute(row[0]," ","_"),pe.data->long_name+sdum+ldes,pe.data->units);
	}
    }
  }
  format=pe.data->format;
}

void printDAS()
{
  xmlutils::LevelMapper level_mapper;
  std::string format;

  decodeRequestData();
  std::cout << "Content-Type: text/plain" << std::endl;
  std::cout << "Content-Description: dods-das" << std::endl;
  printDateAndVersion(std::cout);
  std::cout << std::endl;
  std::cout << "Attributes {" << std::endl;
  std::cout << "  NC_GLOBAL {" << std::endl;
  printGlobalAttributes();
  std::cout << "  }" << std::endl;
  printRefTimes();
  printTimes();
  printFcstHrs();
  printLatitudeAndLongitude();
  printParameters(format,level_mapper);
  printLevels(format,level_mapper);
  std::cout << "}" << std::endl;
}

void printInfo()
{
  xmlutils::LevelMapper level_mapper;
  std::string format;

  decodeRequestData();
  std::cout << "Content-type: text/html" << std::endl << std::endl;
  std::cout << "<h3>Dataset Information</h3>" << std::endl;
  std::cout << "<center>" << std::endl;
  std::cout << "<table cellpadding=\"0\" cellspacing=\"0\" border=\"0\">" << std::endl;
  printGlobalAttributes();
  std::cout << "</table>" << std::endl;
  std::cout << "</center>" << std::endl;
  std::cout << "<p></p>" << std::endl;
  std::cout << "<hr />" << std::endl;
  std::cout << "<h3>Variables in this Dataset</h3>" << std::endl;
  std::cout << "<center>" << std::endl;
  std::cout << "<table cellpadding=\"0\" cellspacing=\"0\" border=\"0\">" << std::endl;
  printRefTimes();
  printTimes();
  printLatitudeAndLongitude();
  printParameters(format,level_mapper);
  printLevels(format,level_mapper);
  std::cout << "</table>" << std::endl;
  std::cout << "</center>" << std::endl;
}

void outputDODSData(std::stringstream& dds,const ProjectionEntry& pe)
{
  int n;
  ParameterData pdata;
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::ifstream ifs;
  long long num_bytes;
  char buf[4],*filebuf=nullptr;
  int filebuf_len=0;
  GRIBMessage msg;
  GRIB2Message msg2;
  int lat_idx=pe.data->idx.size()-2,lon_idx=pe.data->idx.size()-1;
  std::string last_file;
  long long last_offset=0,offset;
  size_t idx;

clock_gettime(CLOCK_REALTIME,&tp);
  size_t grid_size;
  if (pe.data->idx[lat_idx].increment > 1) {
    grid_size=(pe.data->idx[lat_idx].length+pe.data->idx[lat_idx].increment-1)/pe.data->idx[lat_idx].increment;
  }
  else {
    grid_size=pe.data->idx[lat_idx].length;
  }
  if (pe.data->idx[lon_idx].increment > 1) {
    grid_size*=(pe.data->idx[lon_idx].length+pe.data->idx[lon_idx].increment-1)/pe.data->idx[lon_idx].increment;
  }
  else {
    grid_size*=pe.data->idx[lon_idx].length;
  }
  float *fpoints=new float[grid_size];
  char *buffer=new char[grid_size*4];
  char *mbuffer=nullptr;
  long long num_points=grid_size;
  for (n=0; n < lat_idx; ++n) {
    if (pe.data->idx[n].increment > 1) {
	num_points*=(pe.data->idx[n].length+pe.data->idx[n].increment-1)/pe.data->idx[n].increment;
    }
    else {
	num_points*=pe.data->idx[n].length;
    }
  }
  if ((num_points*4) > 0x7fffffff) {
    dapError("400 Bad Request","Array size exceeds 2GB");
  }
  else {
    std::cout << dds.str();
  }
  setBits(buf,num_points,0,32);
  std::cout.write(buf,4);
  std::cout.write(buf,4);
  if (!dap_args.parameters.found(pe.key,pdata)) {
    if ( (idx=pe.key.rfind("_")) != std::string::npos) {
	if (!dap_args.parameters.found(pe.key.substr(0,idx),pdata)) {
// this really should never happen
std::cerr << "but it did happen (1)" << std::endl;
	  exit(0);
	}
	else {
/*
	  grid_subset_args.level_codes.clear();
	  grid_subset_args.level_codes.push_back(pe.key.substr(idx+1));
*/
	}
    }
    else {
// this really should never happen
std::cerr << "but it did happen (2)" << std::endl;
	exit(0);
    }
  }
/*
select f.format,w.webID,i.byte_offset,i.byte_length,i.valid_date,g.ref_time from metautil.custom_dap_grid_index as g left join (select valid_date from metautil.custom_dap_time_index where ID = 'sVVpDypxRV' and slice_index = 3) as t on t.valid_date = g.valid_date left join metautil.custom_dap_level_index as l on l.ID = g.ID and l.level_code = g.level_code left join IGrML.`ds3350_inventory_3!7-0.2-1:0.3.5` as i on i.valid_date = t.valid_date and i.webID_code = g.webID_code and i.timeRange_code = g.timeRange_code and i.level_code = g.level_code left join WGrML.ds3350_webfiles as w on w.code = i.webID_code left join WGrML.formats as f on f.code = w.format_code where g.ID = 'sVVpDypxRV' and g.param = 'HGT_ISBL' and g.time_slice_index = 0 and l.slice_index = 0 and i.gridDefinition_code = 5;
*/
  std::stringstream query_spec;
  query_spec << "select f.format,w.webID,i.byte_offset,i.byte_length,i.valid_date";
  if (pe.data->ref_time_dim.name.length() > 0) {
    query_spec << ",g.ref_time";
    if (pe.data->idx[0].start != pe.data->idx[0].stop) {
	query_spec << ",g.time_slice_index";
    }
    if  (pe.data->idx.size() == 5 && pe.data->idx[2].start != pe.data->idx[2].stop) {
	query_spec << ",l.slice_index";
    }
  }
  else {
    if (pe.data->idx[0].start != pe.data->idx[0].stop) {
	query_spec << ",g.time_slice_index";
    }
    if  (pe.data->idx.size() == 4 && pe.data->idx[1].start != pe.data->idx[1].stop) {
	query_spec << ",l.slice_index";
    }
  }
  query_spec << " from";
  if (pe.data->ref_time_dim.name.length() > 0) {
    query_spec << " (select ";
    if (pe.data->time_dim.name.length() > 0) {
	query_spec << "valid_date";
    }
    else {
	query_spec << "fcst_hr";
    }
    if (pe.data->idx[1].start != pe.data->idx[1].stop) {
	query_spec << ",slice_index";
    }
    query_spec << " from metautil.custom_dap_";
    if (pe.data->time_dim.name.length() > 0) {
	query_spec << "time";
    }
    else {
	query_spec << "fcst_hr";
    }
    query_spec << "_index where ID = '" << dap_args.ID << "'";
    if (pe.data->idx[1].start == pe.data->idx[1].stop) {
	query_spec << " and slice_index = " << pe.data->idx[1].start;
    }
    else {
	query_spec << " and slice_index >= " << pe.data->idx[1].start << " and slice_index <= " << pe.data->idx[1].stop << " order by slice_index";
    }
    query_spec << ") as t left join metautil.custom_dap_grid_index as g on g.";
    if (pe.data->time_dim.name.length() > 0) {
	query_spec << "valid_date";
    }
    else {
	query_spec << "fcst_hr";
    }
    query_spec << " = t.";
    if (pe.data->time_dim.name.length() > 0) {
	query_spec << "valid_date";
    }
    else {
	query_spec << "fcst_hr";
    }
  }
  else {
    query_spec << " metautil.custom_dap_grid_index as g";
  }
  query_spec << " left join metautil.custom_dap_level_index as l on l.ID = g.ID and l.level_code = g.level_code left join IGrML.`ds" << dap_args.dsnum2 << "_inventory_" << pdata.data->format_code << "!" << pdata.data->code << "` as i on i.valid_date = g.valid_date and i.webID_code = g.webID_code and i.timeRange_code = g.timeRange_code and i.level_code = g.level_code left join WGrML.ds" << dap_args.dsnum2 << "_webfiles as w on w.code = i.webID_code left join WGrML.formats as f on f.code = w.format_code where g.ID = '" << dap_args.ID << "' and g.param = '" << pe.key << "'";
  std::string order_by;
  if (pe.data->ref_time_dim.name.length() > 0) {
    if (pe.data->idx[0].start == pe.data->idx[0].stop) {
	query_spec << " and g.time_slice_index = " << pe.data->idx[0].start;
    }
    else {
	query_spec << " and g.time_slice_index >= " << pe.data->idx[0].start << " and g.time_slice_index <= " << pe.data->idx[0].stop;
	if (order_by.length() > 0) {
	  order_by+=",";
	}
	order_by+="g.time_slice_index";
    }
//    if (pe.data->idx.size() == 5 && grid_subset_args.level_codes.size() > 1) {
if (pe.data->idx.size() == 5) {
	if (pe.data->idx[2].start == pe.data->idx[2].stop) {
	  query_spec << " and l.slice_index = " << pe.data->idx[2].start;
	}
	else {
	  query_spec << " and l.slice_index >= " << pe.data->idx[2].start << " and l.slice_index <= " << pe.data->idx[2].stop;
	  if (order_by.length() > 0) {
	    order_by+=",";
	  }
	  order_by+="l.slice_index";
	}
    }
  }
  else {
    if (pe.data->idx[0].start == pe.data->idx[0].stop) {
	query_spec << " and g.time_slice_index = " << pe.data->idx[0].start;
    }
    else {
	query_spec << " and g.time_slice_index >= " << pe.data->idx[0].start << " and g.time_slice_index <= " << pe.data->idx[0].stop;
	if (order_by.length() > 0) {
	  order_by+=",";
	}
	order_by+="g.time_slice_index";
    }
    if (pe.data->idx.size() == 4) {
	if (pe.data->idx[1].start == pe.data->idx[1].stop) {
	  query_spec << " and l.slice_index = " << pe.data->idx[1].start;
	}
	else {
	  query_spec << " and l.slice_index >= " << pe.data->idx[1].start << " and l.slice_index <= " << pe.data->idx[1].stop;
	  if (pe.data->idx[1].increment > 1) {
	    query_spec << " and (l.slice_index % " << pe.data->idx[1].increment << ") = 0";
	  }
	  if (order_by.length() > 0) {
	    order_by+=",";
	  }
	  order_by+="l.slice_index";
	}
    }
  }
  query_spec << " and i.gridDefinition_code = " << dap_args.grid_definition.code;
  if (order_by.length() > 0) {
    query_spec << " order by " << order_by;
  }
  query.set(query_spec.str());
//std::cerr << query.show() << std::endl;
  if (query.submit(server) == 0) {
    decodeGridDefinition();
    size_t next_off=0;
    while (query.fetch_row(row)) {
	offset=std::stoll(row[2]);
	if (row[0] == "WMO_GRIB1" || row[0] == "WMO_GRIB2") {
	  if (row[1] != last_file) {
	    if (ifs.is_open()) {
		ifs.close();
		ifs.clear();
	    }
	    ifs.open(("/glade/p/rda/data/ds"+dap_args.dsnum+"/"+row[1]));
	    last_offset=0;
	  }
	  if (ifs.is_open()) {
//	    ifs.seekg(std::stoll(row[2]),std::ios::beg);
ifs.seekg(offset-last_offset,std::ios::cur);
	    num_bytes=std::stoll(row[3]);
	    if (num_bytes > filebuf_len) {
		if (filebuf != nullptr) {
		  delete[] filebuf;
		}
	      filebuf_len=num_bytes;
	      filebuf=new char[filebuf_len];
	    }
	    ifs.read(filebuf,num_bytes);
	    Grid *grid=nullptr;
	    if (row[0] == "WMO_GRIB1") {
		msg.fill(reinterpret_cast<unsigned char *>(filebuf),false);
		grid=msg.getGrid(0);
	    }
	    else {
		msg2.quickFill(reinterpret_cast<unsigned char *>(filebuf));
		if (msg2.getNumberOfGrids() > 1) {
		  for (size_t n=0; n < msg2.getNumberOfGrids(); ++n) {
		    std::stringstream ss;
		    auto g=msg2.getGrid(n);
		    ss << reinterpret_cast<GRIB2Grid *>(g)->getDiscipline() << "." << reinterpret_cast<GRIB2Grid *>(g)->getParameterCategory() << "." << g->getParameter();
		    if (std::regex_search(pdata.data->code,std::regex(ss.str()+"$"))) {
			grid=g;
			break;
		    }
		  }
		}
		else {
		  grid=msg2.getGrid(0);
		}
	    }
	    if (grid != nullptr) {
		double **gridpoints=grid->getGridpoints();
		int n_end=dap_args.grid_definition.lat_index.start+pe.data->idx[lat_idx].start+pe.data->idx[lat_idx].length;
		int m_end=dap_args.grid_definition.lon_index.start+pe.data->idx[lon_idx].start+pe.data->idx[lon_idx].length;
		size_t foff;
		if (pe.data->ref_time_dim.name.length() > 0) {
		  auto mult=0;
		  size_t roff=6;
		  if (pe.data->idx[0].start != pe.data->idx[0].stop) {
		    mult=(std::stoi(row[roff++])-pe.data->idx[0].start)/pe.data->idx[0].increment;
		  }
		  if (pe.data->idx[1].start != pe.data->idx[1].stop) {
		    if (pe.data->idx[1].increment > 1) {
			mult*=(pe.data->idx[1].length+pe.data->idx[1].increment-1)/pe.data->idx[1].increment;
		    }
		    else {
			mult*=pe.data->idx[1].length;
		    }
		    mult+=(std::stoi(row[roff++])-pe.data->idx[1].start)/pe.data->idx[1].increment;
		  }
		  if (pe.data->idx.size() == 5 && pe.data->idx[2].start != pe.data->idx[2].stop) {
		    if (pe.data->idx[2].increment > 1) {
			mult*=(pe.data->idx[2].length+pe.data->idx[2].increment-1)/pe.data->idx[2].increment;
		    }
		    else {
			mult*=pe.data->idx[2].length;
		    }
		    mult+=(std::stoi(row[roff++])-pe.data->idx[2].start)/pe.data->idx[2].increment;
		  }
		  foff=mult*grid_size;
		}
		else {
		  auto mult=0;
		  size_t roff=5;
		  if (pe.data->idx[0].start != pe.data->idx[0].stop) {
		    mult=(std::stoi(row[roff++])-pe.data->idx[0].start)/pe.data->idx[0].increment;
		  }
		  if (pe.data->idx.size() == 4 && pe.data->idx[1].start != pe.data->idx[1].stop) {
		    if (pe.data->idx[1].increment > 1) {
			mult*=(pe.data->idx[1].length+pe.data->idx[1].increment-1)/pe.data->idx[1].increment;
		    }
		    else {
			mult*=pe.data->idx[1].length;
		    }
		    mult+=(std::stoi(row[roff++])-pe.data->idx[1].start)/pe.data->idx[1].increment;
		  }
		  foff=mult*grid_size;
		}
		if (foff != next_off) {
		  if (mbuffer == nullptr) {
		    mbuffer=new char[grid_size*4];
		    for (size_t n=0; n < grid_size; ++n) {
			fpoints[n]=Grid::missingValue;
		    }
		    setBits(mbuffer,reinterpret_cast<int *>(fpoints),0,32,0,grid_size);
		  }
		  while (next_off < foff) {
		    std::cout.write(mbuffer,grid_size*4);
		    next_off+=grid_size;
		  }
		}
		short dimx=grid->getDimensions().x;
		auto idx=0;
		for (int n=dap_args.grid_definition.lat_index.start+pe.data->idx[lat_idx].start; n < n_end; n+=pe.data->idx[lat_idx].increment) {
		  for (int m=dap_args.grid_definition.lon_index.start+pe.data->idx[lon_idx].start; m < m_end; m+=pe.data->idx[lon_idx].increment) {
		    if (m >= dimx) {
			fpoints[idx++]=gridpoints[n][m-dimx];
		    }
		    else {
			fpoints[idx++]=gridpoints[n][m];
		    }
		  }
		}
		setBits(buffer,reinterpret_cast<int *>(fpoints),0,32,0,grid_size);
		std::cout.write(buffer,grid_size*4);
		next_off=foff+grid_size;
	    }
	    last_offset=offset+num_bytes;
	  }
	}
	last_file=row[1];
    }
  }
  if (pe.data->member.length() == 0) {
    if (pe.data->ref_time_dim.name.length() > 0) {
	writeRefTimes(std::cout,pe,0);
	if (pe.data->time_dim.name.length() > 0) {
	  writeTimes(std::cout,pe,1);
	}
	else {
	  writeFcstHrs(std::cout,pe,1);
	}
	if (pe.data->idx.size() == 5) {
	  writeLevels(std::cout,pe,2);
	}
    }
    else {
	writeTimes(std::cout,pe,0);
	if (pe.data->idx.size() == 4) {
	  writeLevels(std::cout,pe,1);
	}
    }
    writeLatitudes(std::cout,pe,lat_idx);
    writeLongitudes(std::cout,pe,lon_idx);
  }
}

void outputDODS()
{
  MySQL::Server server("rda-db.ucar.edu","metadata","metadata","");
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::string query_spec;
  std::deque<std::string> sp;
  ProjectionEntry pe;
  DateTime base_dt,first_dt;

  std::stringstream dds;
  printDDS(dds,"application/octet-stream","dods-data");
  dds << std::endl;
  dds << "Data:" << std::endl;
  if (projection_table.size() == 0) {
  }
  else {
    for (const auto& key : projection_table.keys()) {
	projection_table.found(key,pe);
	if (std::regex_search(pe.key,std::regex("^(v){0,1}time"))) {
	  std::cout << dds.str();
	  if (pe.data->idx.size() == 1) {
	    writeTimes(std::cout,pe,0);
	  }
	  else {
	    writeTimes(std::cout,pe,-1);
	  }
	}
	else if (std::regex_search(pe.key,std::regex("^ref_time"))) {
	  std::cout << dds.str();
	  if (pe.data->idx.size() == 1) {
	    writeRefTimes(std::cout,pe,0);
	  }
	  else {
	    writeRefTimes(std::cout,pe,-1);
	  }
	}
	else if (std::regex_search(pe.key,std::regex("^fcst_hr"))) {
	  std::cout << dds.str();
	  if (pe.data->idx.size() == 1) {
	    writeFcstHrs(std::cout,pe,0);
	  }
	  else {
	    writeFcstHrs(std::cout,pe,-1);
	  }
	}
	else if (std::regex_search(pe.key,std::regex("^level"))) {
	  std::cout << dds.str();
	  if (pe.data->idx.size() > 0) {
	  }
	  else {
	    writeLevels(std::cout,pe,-1);
	  }
	}
	else if (pe.key == "lat") {
	  std::cout << dds.str();
	  if (pe.data->idx.size() == 1) {
	    writeLatitudes(std::cout,pe,0);
	  }
	  else {
	    writeLatitudes(std::cout,pe,-1);
	  }
	}
	else if (pe.key == "lon") {
	  std::cout << dds.str();
	  if (pe.data->idx.size() == 1) {
	    writeLongitudes(std::cout,pe,0);
	  }
	  else {
	    writeLongitudes(std::cout,pe,-1);
	  }
	}
	else if (pe.data->idx.size() >= 3 && pe.data->idx.size() <= 5) {
	  outputDODSData(dds,pe);
	}
	dds.str("");
    }
  }
}

int main(int argc,char **argv)
{
  parse_config();
  std::string query_string,passwd;
  MySQL::LocalQuery query,query2;
  MySQL::Row row,row2;
  int idx;
  ProjectionEntry pe;

  query_string=getenv("QUERY_STRING");
  if (query_string.length() == 0) {
    dapError("400 Bad Request","Bad request (1)");
  }
  convertCGICodes(query_string);
  auto sp=strutils::split(query_string,"&");
  if ( (idx=sp[0].find(".")) > 0) {
    dap_args.ID=sp[0].substr(0,idx);
    dap_args.ext=sp[0].substr(idx);
  }
  else {
    dap_args.ID=sp[0];
  }
/*
  if (sp.size() == 1) {
    if (dap_args.ext != ".info") {
	dapError("400 Bad Request","missing password");
    }
  }
  else {
    passwd=sp[1];
    strutils::replace_all(passwd,"passwd=","");
    if (passwd.length() == 0) {
	dapError("400 Bad Request","empty password");
    }
  }
*/
  server.connect(config_data.db_host,config_data.db_username,config_data.db_password,"");
  if (!server) {
    dapError("500 Internal Server Error","Database error main(1)");
  }
//  if (sp.size() > 2) {
if (sp.size() > 1) {
// projections
//    auto projs=strutils::split(sp[2],",");
auto projs=strutils::split(sp[1],",");
    for (size_t n=0; n < projs.size(); ++n) {
	pe.data.reset(new ProjectionEntry::Data);
	auto pparts=strutils::split(projs[n],".");
	std::string hyperslab;
	if (pparts.size() == 2) {
	  pe.key=pparts[0];
	  pe.data->member=pparts[1];
	  if ( (idx=pe.data->member.find("[")) > 0) {
	    hyperslab=pe.data->member.substr(idx);
	    pe.data->member=pe.data->member.substr(0,idx);
	  }
	}
	else {
	  pe.key=projs[n];
	  if ( (idx=pe.key.find("[")) > 0) {
	    hyperslab=pe.key.substr(idx);
	    pe.key=pe.key.substr(0,idx);
	  }
	}
	if (hyperslab.length() > 0) {
	  pe.data->idx.resize(strutils::occurs(hyperslab,"["));
	  strutils::chop(hyperslab);
	  auto hyops=strutils::split(hyperslab,"]");
	  for (size_t m=0; m < hyops.size(); ++m) {
	    auto hdata=strutils::split(hyops[m].substr(1),":");
	    pe.data->idx[m].start=std::stoi(hdata[0]);
	    switch (hdata.size()) {
		case 1:
		{
		  pe.data->idx[m].increment=1;
		  pe.data->idx[m].stop=pe.data->idx[m].start;
		  pe.data->idx[m].length=1;
		  break;
		}
		case 2:
		{
		  pe.data->idx[m].increment=1;
		  pe.data->idx[m].stop=std::stoi(hdata[1]);
		  pe.data->idx[m].length=pe.data->idx[m].stop-pe.data->idx[m].start+1;
		  break;
		}
		case 3:
		{
		  pe.data->idx[m].increment=std::stoi(hdata[1]);
		  pe.data->idx[m].stop=std::stoi(hdata[2]);
		  pe.data->idx[m].length=pe.data->idx[m].stop-pe.data->idx[m].start+1;
		  break;
		}
	    }
	  }
	}
	if (!projection_table.found(pe.key,pe)) {
	  query.set("select distinct r.dim_name,r.dim_size,t.dim_name,t.dim_size,f.dim_name,f.dim_size from metautil.custom_dap_ref_times as r left join metautil.custom_dap_grid_index as g on g.ID = r.ID and g.time_slice_index = r.max_dim left join metautil.custom_dap_times as t on t.ID = r.ID and concat('ref_',t.dim_name) = r.dim_name left join metautil.custom_dap_fcst_hrs as f on f.ID = r.ID and concat(concat(substr(r.dim_name,1,8),'_fcst_hr'),substr(r.dim_name,9)) = concat('ref_time_',f.dim_name) where g.ID = '"+dap_args.ID+"' and g.param = '"+pe.key+"' and g.ref_time > 0");
	  if (query.submit(server) == 0) {
	    if (query.num_rows() > 0) {
		if (query.fetch_row(row)) {
		  pe.data->ref_time_dim.name=row[0];
		  pe.data->ref_time_dim.size=row[1];
		  if (row[2].length() > 0) {
		    pe.data->time_dim.name=row[2];
		    pe.data->time_dim.size=row[3];
		  }
		  if (row[4].length() > 0) {
		    pe.data->fcst_hr_dim.name=row[4];
		    pe.data->fcst_hr_dim.size=row[5];
		  }
		}
	    }
	    else {
		query.set("select distinct t.dim_name,t.dim_size from metautil.custom_dap_times as t left join metautil.custom_dap_grid_index as g on g.ID = t.ID and g.time_slice_index = t.max_dim where g.ID = '"+dap_args.ID+"' and g.param = '"+pe.key+"' and g.ref_time = 0 order by t.dim_size desc");
		if (query.submit(server) == 0 && query.fetch_row(row)) {
		  pe.data->time_dim.name=row[0];
		  pe.data->time_dim.size=row[1];
		}
	    }
	  }
	  query.set("select count(distinct g.level_code) as lcnt,any_value(l.dim_name),any_value(l.dim_size) as dsize from metautil.custom_dap_grid_index as g left join metautil.custom_dap_levels as l on l.ID = g.ID where g.ID = '"+dap_args.ID+"' and g.param = '"+pe.key+"' having lcnt = dsize");
	  if (query.submit(server) == 0 && query.fetch_row(row)) {
	    pe.data->level_dim.name=row[1];
	    pe.data->level_dim.size=row[2];
	  }
	  projection_table.insert(pe);
	}
    }
  }
//  if (sp.size() > 3) {
if (sp.size() > 2) {
// selections
//    for (size_t n=3; n < sp.size(); n++) {
for (size_t n=2; n < sp.size(); n++) {
    }
  }
  query.set("select rinfo,duser from metautil.custom_dap where ID = '"+dap_args.ID+"'");
  if (query.submit(server) < 0) {
    std::cerr << "opendap main(2): " << query.error() << " for " << query.show() << std::endl;
    dapError("500 Internal Server Error","Database error main(2)");
  }
  if (!query.fetch_row(row)) {
    dapError("400 Bad Request","Dataset does not exist");
  }
/*
  if (dap_args.ext != ".info") {
    query2.set("password","dssdb.ruser","email = '"+row[1]+"' and isnull(end_date)");
    if (query2.submit(server) == 0 && query2.fetch_row(row2)) {
	if (crypt(passwd.substr(0,8).c_str(),row2[0].substr(0,2).c_str()) != row2[0]) {
	  dapError("400 Bad Request","Incorrect password");
	}
    }
    else {
	dapError("400 Bad Request","Invalid user");
    }
  }
*/
  dap_args.rinfo=row[0];
/*
clock_gettime(CLOCK_REALTIME,&tp);
std::cerr << tp.tv_sec << " " << tp.tv_nsec << std::endl;
*/
  if (dap_args.ext == ".dds") {
    printDDS(std::cout,"text/plain","dods-dds");
  }
  else if (dap_args.ext == ".das") {
    if (projection_table.size() > 0) {
	dapError("400 Bad Request","Bad request (2)");
    }
    else {
	printDAS();
    }
  }
  else if (dap_args.ext == ".dods") {
    outputDODS();
  }
  else if (dap_args.ext == ".ver") {
    if (projection_table.size() > 0) {
	dapError("400 Bad Request","Bad request (3)");
    }
    else {
	std::cout << "Content-type: text/plain" << std::endl;
	printDateAndVersion(std::cout);
	std::cout << std::endl;
	std::cout << "Core version: DAP/2.0" << std::endl;
	std::cout << "Server version: " << config_data.version << std::endl;
    }
  }
  else if (dap_args.ext == ".help") {
    if (projection_table.size() > 0) {
	dapError("400 Bad Request","Bad request (4)");
    }
    else {
	dapError("501 Not Implemented","Help is not available");
    }
  }
  else if (dap_args.ext == ".info") {
    if (projection_table.size() > 0) {
	dapError("400 Bad Request","Bad request (5)");
    }
    else {
	printInfo();
    }
  }
  else if (dap_args.ext == ".html") {
    if (projection_table.size() > 0) {
	dapError("400 Bad Request","Bad request (6)");
    }
    else {
	dapError("501 Not Implemented","A dataset access form is not available");
    }
  }
  else {
    dapError("400 Bad Request","Bad request (7)");
  }
  server.disconnect();
}
