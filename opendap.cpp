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
#include <gridutils.hpp>

struct ConfigData {
  ConfigData() : version(),db_host(),db_username(),db_password(),rdadata_home() {}

  std::string version;
  std::string db_host,db_username,db_password;
  std::string rdadata_home;
} config_data;
struct StringEntry {
  StringEntry() : key() {}

  std::string key;
};
struct ParameterData {
  struct Data {
    Data() : codes(),format_codes(),format(),long_name(),units() {}

    std::vector<std::string> codes,format_codes;
    std::string format,long_name,units;
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
gridutils::gridSubset::Args grid_subset_args;
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

void print_date_and_version(std::ostream& outs)
{
  outs << "Date: " << dateutils::current_date_time().to_string("%a, %d %h %Y %H:%MM:%SS GMT") << std::endl;
  outs << "XDODS-Server: " << config_data.version << std::endl;
}

void dap_error(std::string status,std::string message)
{
  std::cout << "Status: " << status << std::endl;
  std::cout << "Content-type: text/plain" << std::endl;
  std::cout << "Content-Description: dods-error" << std::endl;
  print_date_and_version(std::cout);
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
    dap_error("500 Internal Server Error","Missing configuration");
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
	else if (nvp.front() == "rdadata_home") {
	  config_data.rdadata_home=nvp.back();
	}
    }
    ifs.getline(line,4096);
  }
  ifs.close();
}

void decode_request_data()
{
  decode_grid_subset_string(dap_args.rinfo,grid_subset_args);
  dap_args.dsnum=grid_subset_args.dsnum;
  dap_args.dsnum2=strutils::substitute(dap_args.dsnum,".","");
  strutils::replace_all(grid_subset_args.startdate,"-","");
  strutils::replace_all(grid_subset_args.startdate,":","");
  strutils::replace_all(grid_subset_args.startdate," ","");
  strutils::replace_all(grid_subset_args.enddate,"-","");
  strutils::replace_all(grid_subset_args.enddate,":","");
  strutils::replace_all(grid_subset_args.enddate," ","");
  if (grid_subset_args.parameters.size() == 0) {
    dap_error("500 Internal Server Error","Bad aggregation specification (1)");
  }
  for (const auto& key : grid_subset_args.parameters.keys()) {
    gridutils::gridSubset::Parameter param;
    grid_subset_args.parameters.found(key,param);
    if (param.format_code != nullptr) {
	MySQL::LocalQuery query("select format from WGrML.formats where code = "+*param.format_code);
	MySQL::Row row;
	if (query.submit(server) == 0 && query.fetch_row(row)) {
	  ParameterData pdata;
	  xmlutils::ParameterMapper parameter_mapper(config_data.rdadata_home+"/share/metadata/ParameterTables");
	  pdata.key=strutils::substitute(parameter_mapper.short_name(row[0],key)," ","_");
	  if (!dap_args.parameters.found(pdata.key,pdata)) {
	    pdata.data.reset(new ParameterData::Data);
	    dap_args.parameters.insert(pdata);
	    pdata.data->format=row[0];
	    pdata.data->long_name=parameter_mapper.description(row[0],key);
	    pdata.data->units=parameter_mapper.units(row[0],key);
	  }
	  pdata.data->codes.emplace_back(key);
	  pdata.data->format_codes.emplace_back(*param.format_code);
	}
    }
    else {
	dap_error("500 Internal Server Error","Bad aggregation specification (2)");
    }
  }
  dap_args.grid_definition.code=grid_subset_args.grid_definition_code;
}

void write_reference_times(std::ostream& outs,const ProjectionEntry& pe,int ref_time_index)
{
  DateTime base_dt;
  MySQL::LocalQuery query("base_time","metautil.custom_dap_ref_times","id = '"+dap_args.ID+"' and dim_name = '"+pe.key+"'");
  MySQL::Row row;
  if (query.submit(server) == 0 && query.fetch_row(row)) {
    base_dt.set(std::stoll(row[0])*100);
  }
  std::string query_spec="select distinct ref_time from metautil.custom_dap_grid_index where id = '"+dap_args.ID+"' and ref_time > 0";
  if (pe.key != "ref_time") {
    query.set("select g.param,g.level_code from (select id,param,level_code,count(ref_time) as cnt from metautil.custom_dap_grid_index where id = '"+dap_args.ID+"' group by param,level_code) as g left join metautil.custom_dap_ref_times as t on t.id = g.id where t.dim_name = '"+pe.key+"' and g.cnt = t.dim_size");
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
	bits::set(buf,query.num_rows(),0,32);
    }
    else {
	bits::set(buf,pe.data->idx[ref_time_index].length,0,32);
    }
    outs.write(buf,4);
    outs.write(buf,4);
    while (query.fetch_row(row)) {
	bits::set(buf,DateTime(std::stoll(row[0])*100).minutes_since(base_dt),0,32);
	outs.write(buf,4);
    }
  }
}

void write_times(std::ostream& outs,const ProjectionEntry& pe,int time_index)
{
  DateTime base_dt(std::stoll(grid_subset_args.startdate)*100);
  std::string query_spec="select distinct valid_date from metautil.custom_dap_grid_index where id = '"+dap_args.ID+"'";
  if (pe.key != "time") {
    MySQL::LocalQuery query("select g.param,g.level_code from (select id,param,level_code,count(valid_date) as cnt from metautil.custom_dap_grid_index where id = '"+dap_args.ID+"' group by param,level_code) as g left join metautil.custom_dap_times as t on t.id = g.id where t.dim_name = '"+pe.key+"' and g.cnt = t.dim_size");
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
    if (dap_args.ext == ".ascii") {
	outs << "^" << pe.data->time_dim.name << std::endl;
    }
    else {
	char buf[4];
	if (time_index < 0) {
	  bits::set(buf,query.num_rows(),0,32);
	}
	else {
	  bits::set(buf,pe.data->idx[time_index].length,0,32);
	}
	outs.write(buf,4);
	outs.write(buf,4);
    }
    MySQL::Row row;
    auto cnt=0;
    while (query.fetch_row(row)) {
	if (dap_args.ext == ".ascii") {
	  if (cnt > 0) {
	    outs << ", ";
	  }
	  outs << DateTime(std::stoll(row[0])*100).minutes_since(base_dt);
	}
	else {
	  char buf[4];
	  bits::set(buf,DateTime(std::stoll(row[0])*100).minutes_since(base_dt),0,32);
	  outs.write(buf,4);
	}
	++cnt;
    }
    if (dap_args.ext == ".ascii") {
	outs << std::endl;
    }
  }
}

void write_forecast_hours(std::ostream& outs,const ProjectionEntry& pe,int fcst_hr_index)
{
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::string query_spec="select distinct fcst_hr from metautil.custom_dap_grid_index where id = '"+dap_args.ID+"' and ref_time > 0";
  if (pe.key != "fcst_hr") {
    query.set("select g.param,g.level_code from (select id,param,level_code,count(distinct fcst_hr) as cnt from metautil.custom_dap_grid_index where id = '"+dap_args.ID+"' group by param,level_code) as g left join metautil.custom_dap_fcst_hrs as f on f.id = g.id where f.dim_name = '"+pe.key+"' and g.cnt = f.dim_size limit 0,1");
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
	bits::set(buf,query.num_rows(),0,32);
    }
    else {
	bits::set(buf,pe.data->idx[fcst_hr_index].length,0,32);
    }
    outs.write(buf,4);
    outs.write(buf,4);
    while (query.fetch_row(row)) {
	bits::set(buf,std::stoi(row[0]),0,32);
	outs.write(buf,4);
    }
  }
}

void write_levels(std::ostream& outs,const ProjectionEntry& pe,int lev_index)
{
  std::string query_spec="select l.value from metautil.custom_dap_level_index as i left join metautil.custom_dap_levels as v on v.id = i.id and v.uid = i.uid left join WGrML.levels as l on l.code = i.level_code where i.id = '"+dap_args.ID+"'";
  if (lev_index >= 0) {
    query_spec+=" and i.slice_index >= "+strutils::itos(pe.data->idx[1].start)+" and i.slice_index <= "+strutils::itos(pe.data->idx[1].stop);
  }
  else {
    query_spec+=" and v.dim_name = '"+pe.key+"'";
  }
  query_spec+=" order by i.slice_index";
  MySQL::LocalQuery query(query_spec);
  if (query.submit(server) == 0) {
    if (dap_args.ext == ".ascii") {
	outs << "^" << pe.data->level_dim.name << std::endl;
    }
    else {
	char buf[4];
	bits::set(buf,query.num_rows(),0,32);
	outs.write(buf,4);
	outs.write(buf,4);
    }
    MySQL::Row row;
    auto cnt=0;
    while (query.fetch_row(row)) {
	Value value;
	if (strutils::contains(row[0],",")) {
	  auto sp=strutils::split(row[0],",");
	  value.f=(std::stof(sp[0])+std::stof(sp[1]))/2.;
	}
	else {
	  value.f=std::stof(row[0]);
	}
	if (dap_args.ext == ".ascii") {
	  if (cnt > 0) {
	    outs << ", ";
	  }
	  outs << value.f;
	}
	else {
	  char buf[4];
	  bits::set(buf,value.i,0,32);
	  outs.write(buf,4);
	}
	++cnt;
    }
    if (dap_args.ext == ".ascii") {
	outs << std::endl;
    }
  }
}

void decode_grid_definition()
{
  if (dap_args.grid_definition.type.length() == 0) {
    MySQL::LocalQuery query("select definition,defParams from WGrML.gridDefinitions where code = "+dap_args.grid_definition.code);
    if (query.submit(server) < 0) {
	std::cerr << "opendap print_DDS(1): " << query.error() << " for " << query.show() << std::endl;
	dap_error("500 Internal Server Error","Database error print_DDS(1)");
    }
    MySQL::Row row;
    if (!query.fetch_row(row)) {
	std::cerr << "opendap print_DDS(2): " << query.error() << " for " << query.show() << std::endl;
	dap_error("500 Internal Server Error","Database error print_DDS(2)");
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
	    gridutils::fill_gaussian_latitudes(config_data.rdadata_home+"/share/GRIB",*gaus_lats,lat.increment,(lat.start > lat.stop));
	  }
	  Grid::GLatEntry gle;
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
	    if (!floatutils::myequalf(dap_args.grid_definition.lon_index.start*lon.increment,grid_subset_args.subset_bounds.wlon,0.001)) {
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
	    if (floatutils::myequalf((dap_args.grid_definition.lon_index.end+1)*lon.increment,grid_subset_args.subset_bounds.elon,0.001)) {
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

void write_latitudes(std::ostream& outs,const ProjectionEntry& pe,int lat_index)
{
  decode_grid_definition();
  char buf[4];
  int start,stop;
  if (lat_index < 0) {
    if (dap_args.ext != ".ascii") {
	bits::set(buf,lat.length,0,32);
    }
    start=dap_args.grid_definition.lat_index.start;
    stop=dap_args.grid_definition.lat_index.end;
  }
  else {
    if (dap_args.ext != ".ascii") {
	bits::set(buf,pe.data->idx[lat_index].length,0,32);
    }
    start=dap_args.grid_definition.lat_index.start+pe.data->idx[lat_index].start;
    stop=dap_args.grid_definition.lat_index.start+pe.data->idx[lat_index].stop;
  }
  if (dap_args.ext == ".ascii") {
    outs << "^lat" << std::endl;
  }
  else {
    outs.write(buf,4);
    outs.write(buf,4);
  }
  Value value;
  if (dap_args.grid_definition.type == "latLon") {
    for (int n=dap_args.grid_definition.lat_index.start; n <= dap_args.grid_definition.lat_index.end; n++) {
	if (lat_index < 0 || (n >= start && n <= stop)) {
	  value.f=lat.start+n*lat.increment;
	  if (dap_args.ext == ".ascii") {
	    if (n > dap_args.grid_definition.lat_index.start) {
		outs << ", ";
	    }
	    outs << value.f;
	  }
	  else {
	    bits::set(buf,value.i,0,32);
	    outs.write(buf,4);
	  }
	}
    }
  }
  else if (dap_args.grid_definition.type == "gaussLatLon") {
    if (gaus_lats == NULL) {
	gaus_lats=new my::map<Grid::GLatEntry>;
	gridutils::fill_gaussian_latitudes(config_data.rdadata_home+"/share/GRIB",*gaus_lats,lat.increment,(lat.start > lat.stop));
    }
    Grid::GLatEntry gle;
    gaus_lats->found(lat.increment,gle);
    for (int n=dap_args.grid_definition.lat_index.start; n <= dap_args.grid_definition.lat_index.end; n++) {
	if (lat_index < 0 || (n >= start && n <= stop)) {
	  value.f=gle.lats[n];
	  if (dap_args.ext == ".ascii") {
	    if (n > dap_args.grid_definition.lat_index.start) {
		outs << ", ";
	    }
	    outs << value.f;
	  }
	  else {
	    bits::set(buf,value.i,0,32);
	    outs.write(buf,4);
	  }
	}
    }
  }
  if (dap_args.ext == ".ascii") {
    outs << std::endl;
  }
}

void write_longitudes(std::ostream& outs,const ProjectionEntry& pe,int lon_index)
{
  decode_grid_definition();
  char buf[4];
  int start,stop;
  if (lon_index < 0) {
    if (dap_args.ext != ".ascii") {
	bits::set(buf,lon.length,0,32);
    }
    start=dap_args.grid_definition.lon_index.start;
    stop=dap_args.grid_definition.lon_index.end;
  }
  else {
    if (dap_args.ext != ".ascii") {
	bits::set(buf,pe.data->idx[lon_index].length,0,32);
    }
    start=dap_args.grid_definition.lon_index.start+pe.data->idx[lon_index].start;
    stop=dap_args.grid_definition.lon_index.start+pe.data->idx[lon_index].stop;
  }
  if (dap_args.ext == ".ascii") {
    outs << "^lon" << std::endl;
  }
  else {
    outs.write(buf,4);
    outs.write(buf,4);
  }
  for (int n=dap_args.grid_definition.lon_index.start; n <= dap_args.grid_definition.lon_index.end; ++n) {
    if (lon_index < 0 || (n >= start && n <= stop)) {
	Value value;
	value.f=lon.start+n*lon.increment;
	if (value.f > 360.) {
	  value.f-=360.;
	}
	if (dap_args.ext == ".ascii") {
	  if (n > dap_args.grid_definition.lon_index.start) {
	    outs << ", ";
	  }
	  outs << value.f;
	}
	else {
	  bits::set(buf,value.i,0,32);
	  outs.write(buf,4);
	}
    }
  }
  if (dap_args.ext == ".ascii") {
    outs << std::endl;
  }
}

std::string add_product_codes_to_query(std::string column_name)
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

std::string fill_parameter_query(std::string param_ID = "")
{
  std::string qspec="select distinct ";
  if (param_ID.length() == 0) {
    qspec+="x.param,";
  }
  qspec+="x.tdim_name,x.tdim_size,any_value(l.dim_name),any_value(l.dim_size),x.level_code,any_value(y.tdim_name),any_value(y.tdim_size) from (select any_value(g.id) as id,any_value(g.level_code) as level_code,g.param as param,count(g.param) as pcnt,any_value(t.dim_name) as tdim_name,any_value(t.dim_size) as tdim_size from (select id,dim_name,dim_size,(cast(dim_size as signed)-1) as tsize from metautil.custom_dap_times where id = '"+dap_args.ID+"') as t left join metautil.custom_dap_grid_index as g on g.id = t.id and g.time_slice_index = tsize left join metautil.custom_dap_level_index as l on l.id = g.id and l.level_code = g.level_code";
  if (param_ID.length() > 0) {
    qspec+=" where g.param = '"+param_ID+"'";
  }
  qspec+=" group by g.param,l.uid) as x left join (select any_value(g.id) as id,any_value(g.level_code) as level_code,g.param as param,count(g.param) as pcnt,any_value(t.dim_name) as tdim_name,any_value(t.dim_size) as tdim_size from (select id,dim_name,dim_size,(cast(dim_size as signed)-1) as tsize from metautil.custom_dap_ref_times where id = '"+dap_args.ID+"') as t left join metautil.custom_dap_grid_index as g on g.id = t.id and g.time_slice_index = tsize left join metautil.custom_dap_level_index as l on l.id = g.id and l.level_code = g.level_code where ";
  if (param_ID.length() > 0) {
    qspec+="g.param = '"+param_ID+"' and ";
  }
  qspec+=" g.ref_time > 0 group by g.param,l.uid) as y on y.param = x.param and y.level_code = x.level_code left join metautil.custom_dap_level_index as i on i.id = x.id and i.level_code = x.level_code left join metautil.custom_dap_levels as l on l.id = i.id and l.level_key = i.level_key and l.dim_size = x.pcnt group by ";
  if (param_ID.length() == 0) {
    qspec+="x.param,";
  }
  qspec+="x.level_code";
  return qspec;
}

bool hasReferenceTimes(MySQL::Server& server,MySQL::LocalQuery& query)
{
  query.set("select dim_name,dim_size from metautil.custom_dap_ref_times where id = '"+dap_args.ID+"'");
  if (query.submit(server) < 0) {
    std::cerr << "opendap hasReferenceTimes: " << query.error() << " for " << query.show() << std::endl;
    dap_error("500 Internal Server Error","Database error hasReferenceTimes");
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
  query.set("select dim_name,dim_size from metautil.custom_dap_fcst_hrs where id = '"+dap_args.ID+"'");
  if (query.submit(server) < 0) {
    std::cerr << "opendap hasForecastHours: " << query.error() << " for " << query.show() << std::endl;
    dap_error("500 Internal Server Error","Database error hasForecastHours");
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
  query.set("select dim_name,dim_size from metautil.custom_dap_levels where id = '"+dap_args.ID+"'");
  if (query.submit(server) < 0) {
    std::cerr << "opendap hasLevels: " << query.error() << " for " << query.show() << std::endl;
    dap_error("500 Internal Server Error","Database error hasLevels");
  }
  if (query.num_rows() > 0) {
    return true;
  }
  else {
    return false;
  }
}

void print_DDS(std::ostream& outs,std::string content_type,std::string content_description)
{
  decode_request_data();
  std::stringstream dds;
  MySQL::LocalQuery query;
  MySQL::Row row;
  if (projection_table.size() == 0) {
    bool has_ref_times=false,has_fcst_hrs=false,has_levels=false;
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
    query.set("select dim_name,dim_size from metautil.custom_dap_times where id = '"+dap_args.ID+"'");
    if (query.submit(server) < 0) {
	std::cerr << "opendap print_DDS(3): " << query.error() << " for " << query.show() << std::endl;
	dap_error("500 Internal Server Error","Database error print_DDS(3)");
    }
    while (query.fetch_row(row)) {
	dds << "  Int32 " << row[0] << "[" << row[0] << " = " << row[1] << "];" << std::endl;
    }
    if ( (has_levels=hasLevels(server,query))) {
	while (query.fetch_row(row)) {
	  dds << "  Float32 " << row[0] << "[" << row[0] << " = " << row[1] << "];" << std::endl;
	}
    }
    decode_grid_definition();
    dds << "  Float32 lat[lat = " << lat.length << "];" << std::endl;
    dds << "  Float32 lon[lon = " << lon.length << "];" << std::endl;
    std::string qspec;
    if (has_ref_times) {
	if (has_fcst_hrs) {
	  qspec="(select distinct g.param,r.dim_name,r.dim_size,f.dim_name,f.dim_size from metautil.custom_dap_ref_times as r left join metautil.custom_dap_grid_index as g on g.id = r.id and g.time_slice_index = r.max_dim left join metautil.custom_dap_fcst_hrs as f on f.id = r.id and concat('ref_time_',f.dim_name) = concat(concat(substr(r.dim_name,1,8),'_fcst_hr'),substr(r.dim_name,9)) where g.id = '"+dap_args.ID+"' and ref_time > 0 order by g.param)";
	}
	else {
	  qspec="(select distinct g.param,r.dim_name,r.dim_size,t.dim_name,t.dim_size from metautil.custom_dap_ref_times as r left join metautil.custom_dap_grid_index as g on g.id = r.id and g.time_slice_index = r.max_dim left join metautil.custom_dap_times as t on t.id = r.id and concat('ref_',t.dim_name) = r.dim_name where g.id = '"+dap_args.ID+"' and ref_time > 0 order by g.param)";
	}
	qspec+="UNION ";
    }
    qspec+="(select distinct g.param,t.dim_name,t.dim_size,NULL,NULL from metautil.custom_dap_times as t left join metautil.custom_dap_grid_index as g on g.id = t.id and g.time_slice_index = t.max_dim where g.id = '"+dap_args.ID+"' and g.ref_time = 0 order by g.param,t.dim_size desc)";
    query.set(qspec);
//std::cerr << query.show() << std::endl;
    if (query.submit(server) < 0) {
	std::cerr << "opendap print_DDS(4): " << query.error() << " for " << query.show() << std::endl;
	dap_error("500 Internal Server Error","Database error print_DDS(4)");
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
	    query2.set("select l.dim_name,l.dim_size from (select any_value(x.id) as id,any_value(x.uid) as uid,count(x.uid) as dim_size from (select distinct g.id as id,g.level_code,l.uid as uid from metautil.custom_dap_grid_index as g left join metautil.custom_dap_level_index as l on l.id = g.id and l.level_code = g.level_code where g.id = '"+dap_args.ID+"' and g.time_slice_index < 100 and g.param = '"+row[0]+"') as x group by x.uid) as y left join metautil.custom_dap_levels as l on l.id = y.id and l.uid = y.uid and l.dim_size = y.dim_size");
	    if (query2.submit(server) < 0) {
		std::cerr << "opendap print_DDS(4a): " << query2.error() << " for " << query2.show() << std::endl;
		dap_error("500 Internal Server Error","Database error print_DDS(4a)");
	    }
	    MySQL::Row row2;
	    if (!query2.fetch_row(row2)) {
		std::cerr << "opendap print_DDS(4b): " << query2.error() << " for " << query2.show() << std::endl;
		dap_error("500 Internal Server Error","Database error print_DDS(4b)");
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
		query.set("select dim_size from metautil.custom_dap_times where id = '"+dap_args.ID+"' and dim_name = '"+key+"'");
		if (query.submit(server) < 0 || !query.fetch_row(row)) {
		  std::cerr << "opendap print_DDS(7a): " << query.error() << " for " << query.show() << std::endl;
		  dap_error("500 Internal Server Error","Database error print_DDS(7a)");
		}
		dds << "  Int32 " << key << "[" << key << " = " << row[0] << "];" << std::endl;
	    }
	  }
	  else if (std::regex_search(key,std::regex("^ref_time"))) {
	    if (pe.data->idx.size() > 0) {
		dds << "  Int32 " << key << "[" << key << " = " << pe.data->idx[0].length << "];" << std::endl;
	    }
	    else {
		query.set("select dim_size from metautil.custom_dap_ref_times where id = '"+dap_args.ID+"' and dim_name = '"+key+"'");
		if (query.submit(server) < 0 || !query.fetch_row(row)) {
		  std::cerr << "opendap print_DDS(7b): " << query.error() << " for " << query.show() << std::endl;
		  dap_error("500 Internal Server Error","Database error print_DDS(7b)");
		}
		dds << "  Int32 " << key << "[" << key << " = " << row[0] << "];" << std::endl;
	    }
	  }
	  else if (std::regex_search(key,std::regex("^fcst_hr"))) {
	    if (pe.data->idx.size() > 0) {
		dds << "  Int32 " << key << "[" << key << " = " << pe.data->idx[0].length << "];" << std::endl;
	    }
	    else {
		query.set("select dim_size from metautil.custom_dap_fcst_hrs where id = '"+dap_args.ID+"' and dim_name = '"+key+"'");
		if (query.submit(server) < 0 || !query.fetch_row(row)) {
		  std::cerr << "opendap print_DDS(7c): " << query.error() << " for " << query.show() << std::endl;
		  dap_error("500 Internal Server Error","Database error print_DDS(7c)");
		}
		dds << "  Int32 " << key << "[" << key << " = " << row[0] << "];" << std::endl;
	    }
	  }
	  else if (std::regex_search(key,std::regex("^level"))) {
	    query.set("select dim_size from metautil.custom_dap_levels where id = '"+dap_args.ID+"' and dim_name = '"+key+"'");
	    if (query.submit(server) < 0 || !query.fetch_row(row)) {
		std::cerr << "opendap print_DDS(8): " << query.error() << " for " << query.show() << std::endl;
		dap_error("500 Internal Server Error","Database error print_DDS(8)");
	    }
	    dds << "  Float32 " << key << "[" << key << " = " << row[0] << "];" << std::endl;
	  }
	  else if (std::regex_search(key,std::regex("^lat"))) {
	    decode_grid_definition();
	    dds << "  Float32 lat[lat = " << lat.length << "];" << std::endl;
	  }
	  else if (std::regex_search(key,std::regex("^lon"))) {
	    decode_grid_definition();
	    dds << "  Float32 lon[lon = " << lon.length << "];" << std::endl;
	  }
	  else {
	    if (pe.data->idx.size() == 0) {
		DimensionIndex di;
		if (hasReferenceTimes(server,query)) {
		  if (hasForecastHours(server,query)) {
		    query.set("select r.dim_name,r.dim_size,f.dim_name,f.dim_size from metautil.custom_dap_ref_times as r left join metautil.custom_dap_grid_index as g on g.id = r.id and g.time_slice_index = r.max_dim left join metautil.custom_dap_fcst_hrs as f on f.id = r.id and concat('ref_time_',f.dim_name) = concat(concat(substr(r.dim_name,1,8),'_fcst_hr'),substr(r.dim_name,9)) where g.id = '"+dap_args.ID+"' and g.param = '"+key+"'");
		  }
		  else {
		    query.set("select r.dim_name,r.dim_size,t.dim_name,t.dim_size from metautil.custom_dap_ref_times as r left join metautil.custom_dap_grid_index as g on g.id = r.id and g.time_slice_index = r.max_dim left join metautil.custom_dap_times as t on t.id = r.id and concat('ref_',t.dim_name) = r.dim_name where g.id = '"+dap_args.ID+"' and g.param = '"+key+"'");
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
		  query.set("select t.dim_name,t.dim_size from metautil.custom_dap_times as t left join metautil.custom_dap_grid_index as g on g.id = t.id and g.time_slice_index = t.max_dim where g.id = '"+dap_args.ID+"' and g.param = '"+key+"'");
		  if (query.submit(server) == 0 && query.fetch_row(row)) {
		    di.length=std::stoi(row[1]);
		    di.start=0;
		    di.stop=di.length-1;
		    di.increment=1;
		    pe.data->idx.emplace_back(di);
		  }
		}
		if (hasLevels(server,query)) {
		  query.set("select l.dim_name,l.dim_size from (select any_value(x.id) as id,any_value(x.uid) as uid,count(x.uid) as dim_size from (select distinct g.id as id,g.level_code,l.uid as uid from metautil.custom_dap_grid_index as g left join metautil.custom_dap_level_index as l on l.id = g.id and l.level_code = g.level_code where g.id = '"+dap_args.ID+"' and g.time_slice_index < 100 and g.param = '"+key+"') as x group by x.uid) as y left join metautil.custom_dap_levels as l on l.id = y.id and l.uid = y.uid and l.dim_size = y.dim_size");
		  if (query.submit(server) == 0 && query.fetch_row(row)) {
		    di.length=std::stoi(row[1]);
		    di.start=0;
		    di.stop=di.length-1;
		    di.increment=1;
		    pe.data->idx.emplace_back(di);
		  }
		}
		decode_grid_definition();
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
  print_date_and_version(outs);
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

void print_global_attributes()
{
  MySQL::LocalQuery query("title","search.datasets","dsid = '"+dap_args.dsnum+"'");
  MySQL::Row row;
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

void print_reference_times()
{
  MySQL::LocalQuery query("select dim_name,dim_size,base_time from metautil.custom_dap_ref_times where id = '"+dap_args.ID+"'");
  if (query.submit(server) == 0) {
    MySQL::Row row;
    while (query.fetch_row(row)) {
	if (dap_args.ext == ".das") {
	  std::cout << "  " << row["dim_name"] << " {" << std::endl;
	  std::cout << "    String long_name \"forecast_reference_time\";" << std::endl;
	  std::cout << "    String units \"minutes since " << DateTime(std::stoll(row["base_time"])*100).to_string("%Y-%m-%d %H:%MM +0:00") << "\";" << std::endl;
	  std::cout << "    String calendar \"standard\";" << std::endl;
	  std::cout << "  }" << std::endl;
	}
	else if (dap_args.ext == ".info") {
	  std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>" << row["dim_name"] << ":</strong></td><td>&nbsp;</td><td align=\"left\">Array of 32 bit Integers [" << row["dim_name"] << " = 0.." << (std::stoi(row["dim_size"])-1) << "]</td></tr>" << std::endl;
	  std::cout << "<tr valign=\"top\"><td></td><td></td><td>";
	  std::cout << "<strong>long_name:</strong>&nbsp;forecast_reference_time<br />";
	  std::cout << "<strong>units:</strong>&nbsp;minutes since " << DateTime(std::stoll(row["base_time"])*100).to_string("%Y-%m-%d %H:%MM +0:00") << "<br />";
	  std::cout << "<strong>calendar:</strong>&nbsp;standard<br />";
	  std::cout << "</td></tr>" << std::endl;
	}
    }
  }
}

void print_times(std::string time_ID = "")
{
  auto qspec="select dim_name,dim_size from metautil.custom_dap_times where id = '"+dap_args.ID+"'";
  if (time_ID.length() > 0) {
    qspec+=" and dim_name = '"+time_ID+"'";
  }
  MySQL::LocalQuery query(qspec);
  if (query.submit(server) == 0) {
    MySQL::Row row;
    while (query.fetch_row(row)) {
	if (dap_args.ext == ".das") {
	  std::cout << "  " << row["dim_name"] << " {" << std::endl;
	  if (row["dim_name"][0] == 'v') {
	    std::cout << "    String long_name \"valid_time\";" << std::endl;
	  }
	  else {
	    std::cout << "    String long_name \"time\";" << std::endl;
	  }
	  std::cout << "    String units \"minutes since " << DateTime(std::stoll(grid_subset_args.startdate)*100).to_string("%Y-%m-%d %H:%MM +0:00") << "\";" << std::endl;
	  std::cout << "    String calendar \"standard\";" << std::endl;
	  std::cout << "  }" << std::endl;
	}
	else if (dap_args.ext == ".info") {
	  std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>" << row["dim_name"] << ":</strong></td><td>&nbsp;</td><td align=\"left\">Array of 32 bit Integers [" << row["dim_name"] << " = 0.." << (std::stoi(row["dim_size"])-1) << "]</td></tr>" << std::endl;
	  std::cout << "<tr valign=\"top\"><td></td><td></td><td>";
	  std::cout << "<strong>long_name:</strong>&nbsp;time<br />";
	  std::cout << "<strong>units:</strong>&nbsp;minutes since " << DateTime(std::stoll(grid_subset_args.startdate)*100).to_string("%Y-%m-%d %H:%MM +0:00") << "<br />";
	  std::cout << "<strong>calendar:</strong>&nbsp;standard<br />";
	  std::cout << "</td></tr>" << std::endl;
	}
    }
  }
}

void print_forecast_hours(std::string fcst_hr_ID = "")
{
  auto qspec="select dim_name,dim_size from metautil.custom_dap_fcst_hrs where id = '"+dap_args.ID+"'";
  if (fcst_hr_ID.length() > 0) {
    qspec+=" and dim_name = '"+fcst_hr_ID+"'";
  }
  MySQL::LocalQuery query(qspec);
  if (query.submit(server) == 0) {
    MySQL::Row row;
    while (query.fetch_row(row)) {
	if (dap_args.ext == ".das") {
	  std::cout << "  " << row["dim_name"] << " {" << std::endl;
	  std::cout << "    String long_name \"forecast_period\";" << std::endl;
	  std::cout << "    String units \"hours\";" << std::endl;
	  std::cout << "  }" << std::endl;
	}
	else if (dap_args.ext == ".info") {
	  std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>" << row["dim_name"] << ":</strong></td><td>&nbsp;</td><td align=\"left\">Array of 32 bit Floats [" << row["dim_name"] << " = 0.." << (std::stoi(row["dim_size"])-1) << "]</td></tr>" << std::endl;
	  std::cout << "<tr valign=\"top\"><td></td><td></td><td>";
	  std::cout << "<strong>long_name:</strong>&nbsp;forecast_period<br />";
	  std::cout << "<strong>units:</strong>&nbsp;hours<br />";
	  std::cout << "</td></tr>" << std::endl;
	}
    }
  }
}

void print_levels(std::string format,xmlutils::LevelMapper& level_mapper,std::string level_ID = "")
{
  std::string qspec="select distinct i.level_key,l.dim_name,l.dim_size from metautil.custom_dap_level_index as i left join metautil.custom_dap_levels as l on l.id = i.id and l.level_key = i.level_key where i.id = '"+dap_args.ID+"'";
  if (level_ID.length() > 0) {
    qspec+=" and l.dim_name = '"+level_ID+"'";
  }
  MySQL::LocalQuery query(qspec);
  if (query.submit(server) == 0) {
    MySQL::Row row;
    while (query.fetch_row(row)) {
	auto sp=strutils::split(row[0],":");
	auto sdum=level_mapper.description(format,sp[1],sp[0]);
	if (dap_args.ext == ".das") {
	  std::cout << "  " << row[1] << " {" << std::endl;
	  std::cout << "    String description \"" << sdum << "\";" << std::endl;
	  if (std::regex_search(sdum,std::regex("^Layer"))) {
	    std::cout << "    String comment \"Values are the midpoints of the layers\";" << std::endl;
	  }
	  std::cout << "    String units \"" << level_mapper.units(format,sp[1],sp[0]) << "\";" << std::endl;
	  std::cout << "  }" << std::endl;
	}
	else if (dap_args.ext == ".info") {
	  std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>" << row["l.dim_name"] << ":</strong></td><td>&nbsp;</td><td align=\"left\">Array of 32 bit Reals [" << row["l.dim_name"] << " = 0.." << (std::stoi(row["l.dim_size"])-1) << "]</td></tr>";
	  std::cout << "<tr valign=\"top\"><td></td><td></td><td>";
	  std::cout << "<strong>description:</strong>&nbsp;" << sdum << "<br />";
	  if (std::regex_search(sdum,std::regex("^Layer"))) {
	    std::cout << "<strong>comment:</strong>&nbsp;degrees_north<br />";
	  }
	  std::cout << "<strong>units:</strong>&nbsp;" << level_mapper.units(format,sp[1],sp[0]) << "<br />";
	  std::cout << "</td></tr>" << std::endl;
	}
    }
  }
}

void print_latitude_and_longitude()
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
    decode_grid_definition();
    std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>lat:</strong></td><td>&nbsp;</td><td align=\"left\">Array of 32 bit Reals [lat = 0.." << (lat.length-1) << "]</td></tr>";
    std::cout << "<tr valign=\"top\"><td></td><td></td><td>";
    std::cout << "<strong>long_name:</strong>&nbsp;latitude<br />";
    std::cout << "<strong>units:</strong>&nbsp;degrees_north<br />";
    std::cout << "</td></tr>" << std::endl;
    std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>lon:</strong></td><td>&nbsp;</td><td align=\"left\">Array of 32 bit Reals [lon = 0.." << (lon.length-1) << "]</td></tr>";
    std::cout << "<tr valign=\"top\"><td></td><td></td><td>";
    std::cout << "<strong>long_name:</strong>&nbsp;longitude<br />";
    std::cout << "<strong>units:</strong>&nbsp;degrees_east<br />";
    std::cout << "</td></tr>" << std::endl;
  }
}
void print_parameter_attributes(std::string format,std::string param_ID,std::string description,std::string units)
{
  if (dap_args.ext == ".das") {
    std::cout << "  " << param_ID << " {" << std::endl;
    if (description.length() > 0) {
	std::cout << "    String long_name \"" << description << "\";" << std::endl;
	std::cout << "    String units \"" << units << "\";" << std::endl;
	if (format == "WMO_GRIB1" || format == "WMO_GRIB2") {
	  std::cout << "    Float32 _FillValue " << Grid::MISSING_VALUE << ";" << std::endl;
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
	  std::cout << "<strong>_FillValue:</strong>&nbsp;" << Grid::MISSING_VALUE << "<br />";
	}
	if (grid_subset_args.inittime.length() > 0) {
	  std::cout << "<strong>model_initialization_time:</strong>&nbsp;" << grid_subset_args.inittime << "<br />";
	}
	std::cout << "<table cellspacing=\"0\" cellpadding=\"0\" border=\"0\">";
	auto qspec=fill_parameter_query(param_ID);
	MySQL::LocalQuery query(qspec);
	if (query.submit(server) == 0) {
	  MySQL::Row row;
	  bool got_row;
	  if (!(got_row=query.fetch_row(row))) {
	    strutils::replace_all(qspec,"custom_dap_times","custom_dap_fcst_hrs");
	    query.set(qspec);
	    if (query.submit(server) == 0) {
		got_row=query.fetch_row(row);
	    }
	  }
	  if (got_row) {
	    std::cout << "<tr valign=\"top\"><td align=\"right\"><strong>" << param_ID << ":</strong></td><td>&nbsp;</td><td align=\"left\">Array of 32 bit Reals ";
	    if (!row[5].empty()) {
		std::cout << "[" << row[5] << " = 0.." << (std::stoi(row[6])-1) << "]";
	    }
	    std::cout << "[" << row[0] << " = 0.." << (std::stoi(row[1])-1) << "]";
	    if (!row[2].empty()) {
		std::cout << "[" << row[2] << " = 0.." << (std::stoi(row[3])-1) << "]";
	    }
	    std::cout << "[lat = 0.." << (lat.length-1) << "][lon = 0.." << (lon.length-1) << "]</td></tr>";
	    std::cout << "<tr valign=\"top\"><td></td><td></td><td align=\"left\"><strong>long_name:</strong>&nbsp;" << description << "</td></tr>";
	    std::cout << "<tr valign=\"top\"><td></td><td></td><td align=\"left\"><strong>units:</strong>&nbsp;" << units << "</td></tr>";
	    if (format == "WMO_GRIB1" || format == "WMO_GRIB2") {
		std::cout << "<tr valign=\"top\"><td></td><td></td><td align=\"left\"><strong>_FillValue:</strong>&nbsp;" << Grid::MISSING_VALUE << "</td></tr>";
	    }
	    if (grid_subset_args.inittime.length() > 0) {
		std::cout << "<tr valign=\"top\"><td></td><td></td><td align=\"left\"><strong>model_initialization_time:</strong>&nbsp;" << grid_subset_args.inittime << "</td></tr>";
	    }
	    if (!row[5].empty()) {
		print_reference_times();
	    }
	    if (std::regex_search(row[0],std::regex("^fcst_hr"))) {
		print_forecast_hours(row[0]);
	    }
	    else {
		print_times(row[0]);
	    }
	    if (row[2].length() > 0) {
		xmlutils::LevelMapper level_mapper(config_data.rdadata_home+"/share/metadata/LevelTables");
		print_levels(format,level_mapper,row[2]);
	    }
	    print_latitude_and_longitude();
	  }
	}
	std::cout << "</table>";
	std::cout << "</td></tr>" << std::endl;
    }
  }
}

void print_parameters(std::string& format,xmlutils::LevelMapper& level_mapper)
{
  ParameterEntry pe;
  MySQL::LocalQuery query;
  MySQL::Row row;
  my::map<FormatEntry> unique_formats_table;
  my::map<ParameterEntry> parameter_table;
  for (const auto& key : grid_subset_args.parameters.keys()) {
    gridutils::gridSubset::Parameter param;
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
	xmlutils::ParameterMapper parameter_mapper(config_data.rdadata_home+"/share/metadata/ParameterTables");
	pe.key=strutils::substitute(parameter_mapper.short_name(*fe.format,key)," ","_");
	if (!parameter_table.found(pe.key,pe)) {
	  pe.data.reset(new ParameterEntry::Data);
	  pe.data->format=*fe.format;
	  pe.data->long_name=parameter_mapper.description(*fe.format,key);
	  pe.data->units=parameter_mapper.units(*fe.format,key);
	  parameter_table.insert(pe);
	}
    }
  }
  format=pe.data->format;
  query.set("select distinct param from metautil.custom_dap_grid_index where id = '"+dap_args.ID+"' and time_slice_index = 0");
  if (query.submit(server) < 0) {
    std::cerr << "opendap print_DAS(1): " << query.error() << " for " << query.show() << std::endl;
    dap_error("500 Internal Server Error","Database error print_DAS(1)");
  }
  while (query.fetch_row(row)) {
    parameter_table.found(row[0].substr(0,row[0].rfind("_")),pe);
    MySQL::LocalQuery query2;
    query2.set("select l.level_key,y.level_codes from (select any_value(x.id) as id,any_value(x.uid) as uid,count(x.uid) as dim_size,group_concat(x.level_code separator '!') as level_codes from (select distinct g.id as id,g.level_code,l.uid as uid from metautil.custom_dap_grid_index as g left join metautil.custom_dap_level_index as l on l.id = g.id and l.level_code = g.level_code where g.id = '"+dap_args.ID+"' and g.time_slice_index < 100 and g.param = '"+row[0]+"') as x group by x.uid) as y left join metautil.custom_dap_levels as l on l.id = y.id and l.uid = y.uid and l.dim_size = y.dim_size");
    if (query2.submit(server) < 0) {
	std::cerr << "opendap print_DAS(1a): " << query2.error() << " for " << query2.show() << std::endl;
	dap_error("500 Internal Server Error","Database error print_DAS(1a)");
    }
    MySQL::Row row2;
    while (query2.fetch_row(row2)) {
	if (row2[0].length() == 0) {
	  MySQL::LocalQuery query3;
	  MySQL::Row row3;
	  query3.set("select distinct l.type,l.map,l.value,f.format from metautil.custom_dap_grid_index as g left join WGrML.levels as l on l.code = g.level_code left join WGrML.ds"+dap_args.dsnum2+"_webfiles2 as w on w.code = g.file_code left join WGrML.formats as f on f.code = w.format_code where g.param = '"+row[0]+"' and g.id = '"+dap_args.ID+"' and g.time_slice_index = 0");
	  if (query3.submit(server) < 0) {
	    std::cerr << "opendap print_DAS(1b): " << query2.error() << " for " << query2.show() << std::endl;
	    dap_error("500 Internal Server Error","Database error print_DAS(1b)");
	  }
	  while (query3.fetch_row(row3)) {
	    auto lunits=level_mapper.units(row3[3],row3[0],row3[1]);
	    if (lunits.length() > 0) {
		print_parameter_attributes(row3[3],strutils::substitute(row[0]," ","_"),pe.data->long_name+" at "+row3[2]+" "+lunits,pe.data->units);
	    }
	    else {
		std::string ldes="";
		if (std::regex_search(row3[0],std::regex("-")) && std::regex_search(row3[2],std::regex(","))) {
		  ldes+=" in "+level_mapper.description(row3[3],row3[0],row3[1]);
		  auto sp=strutils::split(row3[0],"-");
		  auto sp2=strutils::split(row3[2],",");
		  ldes+=" - top: "+sp2[1]+" "+level_mapper.units(row3[3],sp[1],row3[1])+" bottom: "+sp2[0]+" "+level_mapper.units(row3[3],sp[0],row3[1]);
		}
		else {
		  ldes+=" at "+level_mapper.description(row3[3],row3[0],row3[1]);
		  if (row3[2] != "0") {
		    ldes+=" "+row3[2];
		  }
		}
		print_parameter_attributes(row3[3],strutils::substitute(row[0]," ","_"),pe.data->long_name+ldes,pe.data->units);
	    }
	    format=row3[3];
	  }
	}
	else {
	  auto sp=strutils::split(row2[0],":");
	  auto ldes=level_mapper.description(format,sp[1],sp[0]);
	  sp=strutils::split(row2[1],"!");
	  auto sdum=" at ";
	  for (const auto& p : sp) {
	    if (std::regex_search(p,std::regex(","))) {
		sdum=" in ";
	    }
	  }
	  print_parameter_attributes(format,strutils::substitute(row[0]," ","_"),pe.data->long_name+sdum+ldes,pe.data->units);
	}
    }
  }
}

void print_DAS()
{
  decode_request_data();
  std::cout << "Content-Type: text/plain" << std::endl;
  std::cout << "Content-Description: dods-das" << std::endl;
  print_date_and_version(std::cout);
  std::cout << std::endl;
  std::cout << "Attributes {" << std::endl;
  std::cout << "  NC_GLOBAL {" << std::endl;
  print_global_attributes();
  std::cout << "  }" << std::endl;
  print_reference_times();
  print_times();
  print_forecast_hours();
  print_latitude_and_longitude();
  std::string format;
  xmlutils::LevelMapper level_mapper(config_data.rdadata_home+"/share/metadata/LevelTables");
  print_parameters(format,level_mapper);
  print_levels(format,level_mapper);
  std::cout << "}" << std::endl;
}

void print_info()
{
  decode_request_data();
  std::cout << "Content-type: text/html" << std::endl << std::endl;
  std::cout << "<h3>Dataset Information</h3>" << std::endl;
  std::cout << "<center>" << std::endl;
  std::cout << "<table cellpadding=\"0\" cellspacing=\"0\" border=\"0\">" << std::endl;
  print_global_attributes();
  std::cout << "</table>" << std::endl;
  std::cout << "</center>" << std::endl;
  std::cout << "<p></p>" << std::endl;
  std::cout << "<hr />" << std::endl;
  std::cout << "<h3>Variables in this Dataset</h3>" << std::endl;
  std::cout << "<center>" << std::endl;
  std::cout << "<table cellpadding=\"0\" cellspacing=\"0\" border=\"0\">" << std::endl;
  print_reference_times();
  print_times();
  print_forecast_hours();
  print_latitude_and_longitude();
  std::string format;
  xmlutils::LevelMapper level_mapper(config_data.rdadata_home+"/share/metadata/LevelTables");
  print_parameters(format,level_mapper);
  print_levels(format,level_mapper);
  std::cout << "</table>" << std::endl;
  std::cout << "</center>" << std::endl;
}

void output_DODS_data(std::stringstream& dds,const ProjectionEntry& pe)
{
clock_gettime(CLOCK_REALTIME,&tp);
  size_t lat_idx=pe.data->idx.size()-2,lon_idx=pe.data->idx.size()-1;
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
  long long num_points=grid_size;
  for (size_t n=0; n < lat_idx; ++n) {
    if (pe.data->idx[n].increment > 1) {
	num_points*=(pe.data->idx[n].length+pe.data->idx[n].increment-1)/pe.data->idx[n].increment;
    }
    else {
	num_points*=pe.data->idx[n].length;
    }
  }
  if ((num_points*4) > 0x7fffffff) {
    dap_error("400 Bad Request","Array size exceeds 2GB");
  }
  else {
    std::cout << dds.str();
  }
  ParameterData pdata;
  if (!dap_args.parameters.found(pe.key,pdata)) {
    size_t idx;
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
  if (dap_args.ext == ".dods") {
    char buf[4];
    bits::set(buf,num_points,0,32);
    std::cout.write(buf,4);
    std::cout.write(buf,4);
  }
  else {
    std::cout << "^" << pe.key << "." << pe.key << std::endl;
  }
  std::stringstream query_spec;
  query_spec << "select f.format,w.id,i.byte_offset,i.byte_length,i.valid_date";
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
    query_spec << "_index where id = '" << dap_args.ID << "'";
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
  query_spec << " left join metautil.custom_dap_level_index as l on l.id = g.id and l.level_code = g.level_code left join IGrML.`ds" << dap_args.dsnum2 << "_inventory_";
  if (pdata.data->format_codes.size() < 2 && pdata.data->codes.size() < 2) {
    query_spec << pdata.data->format_codes.front() << "!" << pdata.data->codes.front();
  }
  else {
    query_spec << "<FCODE>!<PCODE>";
  }
  query_spec << "` as i on i.valid_date = g.valid_date and i.webID_code = g.file_code and i.timeRange_code = g.time_range_code and i.level_code = g.level_code left join WGrML.ds" << dap_args.dsnum2 << "_webfiles2 as w on w.code = i.webID_code left join WGrML.formats as f on f.code = w.format_code where g.id = '" << dap_args.ID << "' and g.param = '" << pe.key << "'";
  std::string order_by;
  if (pe.data->ref_time_dim.name.length() > 0) {
    if (pe.data->idx[0].start == pe.data->idx[0].stop) {
	query_spec << " and g.time_slice_index = " << pe.data->idx[0].start;
    }
    else {
	query_spec << " and g.time_slice_index >= " << pe.data->idx[0].start << " and g.time_slice_index <= " << pe.data->idx[0].stop;
	if (!order_by.empty()) {
	  order_by+=",";
	}
	if (pdata.data->format_codes.size() < 2 && pdata.data->codes.size() < 2) {
	  order_by+="g.";
	}
	order_by+="time_slice_index";
    }
    if (pe.data->idx.size() == 5) {
	if (pe.data->idx[2].start == pe.data->idx[2].stop) {
	  query_spec << " and l.slice_index = " << pe.data->idx[2].start;
	}
	else {
	  query_spec << " and l.slice_index >= " << pe.data->idx[2].start << " and l.slice_index <= " << pe.data->idx[2].stop;
	  if (!order_by.empty()) {
	    order_by+=",";
	  }
	  if (pdata.data->format_codes.size() < 2 && pdata.data->codes.size() < 2) {
	    order_by+="l.";
	  }
	  order_by+="slice_index";
	}
    }
  }
  else {
    if (pe.data->idx[0].start == pe.data->idx[0].stop) {
	query_spec << " and g.time_slice_index = " << pe.data->idx[0].start;
    }
    else {
	query_spec << " and g.time_slice_index >= " << pe.data->idx[0].start << " and g.time_slice_index <= " << pe.data->idx[0].stop;
	if (!order_by.empty()) {
	  order_by+=",";
	}
	if (pdata.data->format_codes.size() < 2 && pdata.data->codes.size() < 2) {
	  order_by+="g.";
	}
	order_by+="time_slice_index";
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
	  if (!order_by.empty()) {
	    order_by+=",";
	  }
	  if (pdata.data->format_codes.size() < 2 && pdata.data->codes.size() < 2) {
	    order_by+="l.";
	  }
	  order_by+="slice_index";
	}
    }
  }
  query_spec << " and i.gridDefinition_code = " << dap_args.grid_definition.code;
  MySQL::LocalQuery query;
  if (pdata.data->format_codes.size() > 1 || pdata.data->codes.size() > 1) {
    std::string union_spec;
    for (const auto& format_code : pdata.data->format_codes) {
	for (const auto& code : pdata.data->codes) {
	  if (MySQL::table_exists(server,"IGrML.ds"+dap_args.dsnum2+"_inventory_"+format_code+"!"+code)) {
	    if (!union_spec.empty()) {
		union_spec+=" UNION ";
	    }
	    union_spec+=strutils::substitute(strutils::substitute(query_spec.str(),"<FCODE>",format_code),"<PCODE>",code);
	  }
	}
    }
    if (!order_by.empty()) {
	union_spec+=" order by "+order_by;
    }
    query.set(union_spec);
  }
  else {
    if (!order_by.empty()) {
	query_spec << " order by " << order_by;
    }
    query.set(query_spec.str());
  }
  if (query.submit(server) == 0) {
    if (query.num_rows() > 0) {
	float *fpoints=new float[grid_size];
	char *buffer=new char[grid_size*4];
	char *mbuffer=nullptr;
	decode_grid_definition();
	std::unique_ptr<GRIBMessage> msg(nullptr);
	std::unique_ptr<GRIB2Message> msg2(nullptr);
	char *filebuf=nullptr;
	int filebuf_len=0;
	std::ifstream ifs;
	std::string last_file;
	long long last_offset=0;
	size_t next_off=0;
	MySQL::Row row;
	while (query.fetch_row(row)) {
	  auto offset=std::stoll(row[2]);
	  if (row[0] == "WMO_GRIB1" || row[0] == "WMO_GRIB2") {
	    if (row[1] != last_file) {
		if (ifs.is_open()) {
		  ifs.close();
		  ifs.clear();
		}
		ifs.open(("/data/rda/data/ds"+dap_args.dsnum+"/"+row[1]));
		last_offset=0;
	    }
	    if (ifs.is_open()) {
		ifs.seekg(offset-last_offset,std::ios::cur);
		auto num_bytes=std::stoll(row[3]);
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
		  if (msg == nullptr) {
		    msg.reset(new GRIBMessage);
		  }
		  msg->fill(reinterpret_cast<unsigned char *>(filebuf),false);
		  grid=msg->grid(0);
		}
		else {
		  if (msg2 == nullptr) {
		    msg2.reset(new GRIB2Message);
		  }
		  msg2->quick_fill(reinterpret_cast<unsigned char *>(filebuf));
		  if (msg2->number_of_grids() > 1) {
		    for (size_t n=0; n < msg2->number_of_grids(); ++n) {
			std::stringstream ss;
			auto g=msg2->grid(n);
			ss << reinterpret_cast<GRIB2Grid *>(g)->discipline() << "." << reinterpret_cast<GRIB2Grid *>(g)->parameter_category() << "." << g->parameter();
			for (const auto& code : pdata.data->codes) {
			  if (std::regex_search(code,std::regex(":"+ss.str()+"$"))) {
			    grid=g;
			    n=msg2->number_of_grids();
			  }
			}
		    }
		  }
		  else {
		    grid=msg2->grid(0);
		  }
		}
		if (grid != nullptr) {
		  double **gridpoints=grid->gridpoints();
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
			  fpoints[n]=Grid::MISSING_VALUE;
			}
			bits::set(mbuffer,reinterpret_cast<int *>(fpoints),0,32,0,grid_size);
		    }
		    while (next_off < foff) {
			std::cout.write(mbuffer,grid_size*4);
			next_off+=grid_size;
		    }
		  }
		  short dimx=grid->dimensions().x;
		  auto idx=0;
		  for (int n=dap_args.grid_definition.lat_index.start+pe.data->idx[lat_idx].start; n < n_end; n+=pe.data->idx[lat_idx].increment) {
		    for (int m=dap_args.grid_definition.lon_index.start+pe.data->idx[lon_idx].start; m < m_end; m+=pe.data->idx[lon_idx].increment) {
			if (dap_args.ext == ".ascii" && idx > 0) {
			  std::cout << ", ";
			}
			if (m >= dimx) {
			  if (dap_args.ext == ".dods") {
			    fpoints[idx]=gridpoints[n][m-dimx];
			  }
			  else {
			    std::cout << gridpoints[n][m-dimx];
			  }
			}
			else {
			  if (dap_args.ext == ".dods") {
			    fpoints[idx]=gridpoints[n][m];
			  }
			  else {
			    std::cout << gridpoints[n][m];
			  }
			}
			++idx;
		    }
		    if (dap_args.ext == ".ascii") {
			std::cout << std::endl;
		    }
		  }
		  if (dap_args.ext == ".dods") {
		    bits::set(buffer,reinterpret_cast<int *>(fpoints),0,32,0,grid_size);
		    std::cout.write(buffer,grid_size*4);
		  }
		  next_off=foff+grid_size;
		}
		last_offset=offset+num_bytes;
	    }
	  }
	  last_file=row[1];
	}
    }
    else {
	auto mbuffer=new char[4];
	float f[]={static_cast<float>(Grid::MISSING_VALUE)};
	bits::set(mbuffer,reinterpret_cast<int *>(f),0,32,0,1);
	for (size_t n=0; n < grid_size; ++n) {
	  std::cout.write(mbuffer,4);
	}
    }
  }
  if (pe.data->member.length() == 0) {
    if (pe.data->ref_time_dim.name.length() > 0) {
	write_reference_times(std::cout,pe,0);
	if (pe.data->time_dim.name.length() > 0) {
	  write_times(std::cout,pe,1);
	}
	else {
	  write_forecast_hours(std::cout,pe,1);
	}
	if (pe.data->idx.size() == 5) {
	  write_levels(std::cout,pe,2);
	}
    }
    else {
	write_times(std::cout,pe,0);
	if (pe.data->idx.size() == 4) {
	  write_levels(std::cout,pe,1);
	}
    }
    write_latitudes(std::cout,pe,lat_idx);
    write_longitudes(std::cout,pe,lon_idx);
  }
}

void output_DODS()
{
  std::stringstream dds;
  print_DDS(dds,"application/octet-stream","dods-data");
  dds << std::endl;
  dds << "Data:" << std::endl;
  if (projection_table.size() == 0) {
    dap_error("400 Bad Request","Bad request (dods)");
  }
  else {
    for (const auto& key : projection_table.keys()) {
	ProjectionEntry pe;
	projection_table.found(key,pe);
	if (std::regex_search(pe.key,std::regex("^(v){0,1}time"))) {
	  std::cout << dds.str();
	  if (pe.data->idx.size() == 1) {
	    write_times(std::cout,pe,0);
	  }
	  else {
	    write_times(std::cout,pe,-1);
	  }
	}
	else if (std::regex_search(pe.key,std::regex("^ref_time"))) {
	  std::cout << dds.str();
	  if (pe.data->idx.size() == 1) {
	    write_reference_times(std::cout,pe,0);
	  }
	  else {
	    write_reference_times(std::cout,pe,-1);
	  }
	}
	else if (std::regex_search(pe.key,std::regex("^fcst_hr"))) {
	  std::cout << dds.str();
	  if (pe.data->idx.size() == 1) {
	    write_forecast_hours(std::cout,pe,0);
	  }
	  else {
	    write_forecast_hours(std::cout,pe,-1);
	  }
	}
	else if (std::regex_search(pe.key,std::regex("^level"))) {
	  std::cout << dds.str();
	  if (pe.data->idx.size() > 0) {
	  }
	  else {
	    write_levels(std::cout,pe,-1);
	  }
	}
	else if (pe.key == "lat") {
	  std::cout << dds.str();
	  if (pe.data->idx.size() == 1) {
	    write_latitudes(std::cout,pe,0);
	  }
	  else {
	    write_latitudes(std::cout,pe,-1);
	  }
	}
	else if (pe.key == "lon") {
	  std::cout << dds.str();
	  if (pe.data->idx.size() == 1) {
	    write_longitudes(std::cout,pe,0);
	  }
	  else {
	    write_longitudes(std::cout,pe,-1);
	  }
	}
	else if (pe.data->idx.size() >= 3 && pe.data->idx.size() <= 5) {
	  output_DODS_data(dds,pe);
	}
	dds.str("");
    }
  }
}

int main(int argc,char **argv)
{
  parse_config();
  MySQL::LocalQuery query;
  MySQL::Row row;
  std::string query_string=getenv("QUERY_STRING");
  if (query_string.length() == 0) {
    dap_error("400 Bad Request","Bad request (1)");
  }
  webutils::cgi::convert_codes(query_string);
  auto sp=strutils::split(query_string,"&");
  size_t idx;
  if ( (idx=sp[0].find(".")) != std::string::npos) {
    dap_args.ID=sp[0].substr(0,idx);
    dap_args.ext=sp[0].substr(idx);
  }
  else {
    dap_args.ID=sp[0];
  }
  server.connect(config_data.db_host,config_data.db_username,config_data.db_password,"");
  if (!server) {
    dap_error("500 Internal Server Error","Database error main(1)");
  }
  if (sp.size() > 1) {
// projections
    auto projs=strutils::split(sp[1],",");
    for (size_t n=0; n < projs.size(); ++n) {
	ProjectionEntry pe;
	pe.data.reset(new ProjectionEntry::Data);
	auto pparts=strutils::split(projs[n],".");
	std::string hyperslab;
	if (pparts.size() == 2) {
	  pe.key=pparts[0];
	  pe.data->member=pparts[1];
	  if ( (idx=pe.data->member.find("[")) != std::string::npos) {
	    hyperslab=pe.data->member.substr(idx);
	    pe.data->member=pe.data->member.substr(0,idx);
	  }
	}
	else {
	  pe.key=projs[n];
	  if ( (idx=pe.key.find("[")) != std::string::npos) {
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
	  query.set("select distinct r.dim_name,r.dim_size,t.dim_name,t.dim_size,f.dim_name,f.dim_size from metautil.custom_dap_ref_times as r left join metautil.custom_dap_grid_index as g on g.id = r.id and g.time_slice_index = r.max_dim left join metautil.custom_dap_times as t on t.id = r.id and concat('ref_',t.dim_name) = r.dim_name left join metautil.custom_dap_fcst_hrs as f on f.id = r.id and concat(concat(substr(r.dim_name,1,8),'_fcst_hr'),substr(r.dim_name,9)) = concat('ref_time_',f.dim_name) where g.id = '"+dap_args.ID+"' and g.param = '"+pe.key+"' and g.ref_time > 0");
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
		query.set("select distinct t.dim_name,t.dim_size from metautil.custom_dap_times as t left join metautil.custom_dap_grid_index as g on g.id = t.id and g.time_slice_index = t.max_dim where g.id = '"+dap_args.ID+"' and g.param = '"+pe.key+"' and g.ref_time = 0 order by t.dim_size desc");
		if (query.submit(server) == 0 && query.fetch_row(row)) {
		  pe.data->time_dim.name=row[0];
		  pe.data->time_dim.size=row[1];
		}
	    }
	  }
	  query.set("select dim_name,dim_size from metautil.custom_dap_level_list where id = '"+dap_args.ID+"' and param = '"+pe.key+"'");
	  if (query.submit(server) == 0) {
	    if (query.num_rows() == 0) {
		query.set("select count(distinct g.level_code) as lcnt,any_value(l.dim_name),any_value(l.dim_size) as dsize from metautil.custom_dap_grid_index as g left join metautil.custom_dap_levels as l on l.id = g.id where g.id = '"+dap_args.ID+"' and g.param = '"+pe.key+"' having lcnt = dsize");
		if (query.submit(server) == 0 && query.fetch_row(row)) {
		  server.insert("metautil.custom_dap_level_list","'"+dap_args.ID+"','"+pe.key+"','"+row[1]+"',"+row[2]);
		  pe.data->level_dim.name=row[1];
		  pe.data->level_dim.size=row[2];
		}
	    }
	    else {
		if (query.fetch_row(row)) {
		  pe.data->level_dim.name=row[0];
		  pe.data->level_dim.size=row[1];
		}
	    }
	  }
	  projection_table.insert(pe);
	}
    }
  }
  if (sp.size() > 2) {
// selections
    for (size_t n=2; n < sp.size(); n++) {
    }
  }
  query.set("select rinfo,duser from metautil.custom_dap where id = '"+dap_args.ID+"'");
  if (query.submit(server) < 0) {
    std::cerr << "opendap main(2): " << query.error() << " for " << query.show() << std::endl;
    dap_error("500 Internal Server Error","Database error main(2)");
  }
  if (!query.fetch_row(row)) {
    dap_error("400 Bad Request","Dataset does not exist");
  }
  dap_args.rinfo=row[0];
/*
clock_gettime(CLOCK_REALTIME,&tp);
std::cerr << tp.tv_sec << " " << tp.tv_nsec << std::endl;
*/
  if (dap_args.ext == ".dds") {
    print_DDS(std::cout,"text/plain","dods-dds");
  }
  else if (dap_args.ext == ".das") {
    if (projection_table.size() > 0) {
	dap_error("400 Bad Request","Bad request (2)");
    }
    else {
	print_DAS();
    }
  }
  else if (dap_args.ext == ".dods" || dap_args.ext == ".ascii") {
    output_DODS();
  }
  else if (dap_args.ext == ".ver") {
    if (projection_table.size() > 0) {
	dap_error("400 Bad Request","Bad request (3)");
    }
    else {
	std::cout << "Content-type: text/plain" << std::endl;
	print_date_and_version(std::cout);
	std::cout << std::endl;
	std::cout << "Core version: DAP/2.0" << std::endl;
	std::cout << "Server version: " << config_data.version << std::endl;
    }
  }
  else if (dap_args.ext == ".help") {
    if (projection_table.size() > 0) {
	dap_error("400 Bad Request","Bad request (4)");
    }
    else {
	dap_error("501 Not Implemented","Help is not available");
    }
  }
  else if (dap_args.ext == ".info") {
    if (projection_table.size() > 0) {
	dap_error("400 Bad Request","Bad request (5)");
    }
    else {
	print_info();
    }
  }
  else if (dap_args.ext == ".html") {
    if (projection_table.size() > 0) {
	dap_error("400 Bad Request","Bad request (6)");
    }
    else {
	dap_error("501 Not Implemented","A dataset access form is not available");
    }
  }
  else {
    dap_error("400 Bad Request","Bad request (7)");
  }
  server.disconnect();
}
