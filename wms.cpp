#include <iostream>
#include <deque>
#include <unordered_map>
#include <regex>
#include <unistd.h>
#include <utime.h>
#include <web/web.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <tokendoc.hpp>
#include <tempfile.hpp>
#include <xml.hpp>
#include <xmlutils.hpp>
#include <metadata.hpp>
#include <gridutils.hpp>
#include <myerror.hpp>

metautils::Directives metautils::directives;
metautils::Args metautils::args;
std::string myerror="";
std::string mywarning="";

struct CacheData {
  CacheData() : data_min(1.e37),data_max(-1.e37),data_format(),byte_offset(-1),byte_length(0) {}

  double data_min,data_max;
  std::string data_format;
  long long byte_offset,byte_length;
};
struct CapabilitiesParameters {
  CapabilitiesParameters() : service(),version() {}

  std::string service,version;
};
struct MapParameters {
  MapParameters() : version(),layers(),styles(),crs(),bbox(),s_width(),s_height(),width(),height(),format(),time(),bgcolor{255,255,255},transparent(false) {}

  std::string version;
  std::deque<std::string> layers,styles;
  std::string crs,bbox;
  std::string s_width,s_height;
  size_t width,height;
  std::string format,time;
  short bgcolor[3];
  bool transparent;
};

std::string request,resource,dsnum2,wms_directory;
std::unordered_map<std::string,std::string> image_formats={ {"image/png","png"} };
//std::unordered_map<std::string,double> coordinate_reference_systems={ {"CRS:84",0.},{"EPSG:3786",6371007.},{"EPSG:4326",0.} };
std::unordered_map<std::string,std::tuple<std::string,double,unsigned char>> coordinate_reference_systems={ {"EPSG:3857",std::make_tuple("Mercator",6378137.,0x0)}, {"EPSG:4326",std::make_tuple("CylindricalEquidistant",0.,0x1)} };
auto is_dev=false;

const double PI=3.141592653589793;
const double TWO_PI=2.*PI;
const double E=2.71828;

void print_exception_report(std::string exception_text,std::string exception_code = "",std::string locator = "")
{
  std::cerr << "WMS EXCEPTION: " << exception_text << std::endl;
  std::cout << "Content-type: application/xml" << std::endl << std::endl;
  TokenDocument tdoc("/usr/local/www/server_root/web/html/wms/exception.xml");
  if (!exception_code.empty()) {
    tdoc.add_replacement("__SERVICE_EXCEPTION_CODE__",exception_code);
    if (!locator.empty()) {
	tdoc.add_replacement("__LOCATOR__",locator);
	tdoc.add_if("__HAS_CODE_AND_LOCATOR__");
    }
    else {
	tdoc.add_if("__HAS_CODE__");
    }
  }
  else {
    tdoc.add_if("__HAS_TEXT__");
  }
  tdoc.add_replacement("__SERVICE_EXCEPTION_TEXT__",exception_text);
  std::cout << tdoc << std::endl;
  exit(1);
}

void fill_querystring(QueryString& query_string)
{
  query_string.fill(QueryString::GET);
  if (!query_string) {
    query_string.fill(QueryString::POST);
    if (!query_string) {
	print_exception_report("Missing query");
    }
  }
  request=query_string.value("request");
  auto rparts=strutils::split(query_string.value("resource"),"/");
  if (rparts.size() > 0) {
    rparts.pop_front();
    if (rparts.front().substr(0,2) == "ds") {
	metautils::args.dsnum=rparts.front().substr(2);
	dsnum2=strutils::substitute(metautils::args.dsnum,".","");
	rparts.pop_front();
	resource=rparts.front();
	rparts.pop_front();
	while (rparts.size() > 0) {
	  if (rparts.front() == "legend") {
	    request="GetLegendGraphic<!>";
	    if (rparts.back() != rparts.front()) {
		request+=rparts.back();
	    }
	    break;
	  }
	  resource+="/"+rparts.front();
	  rparts.pop_front();
	}
    }
    else {
	print_exception_report("Invalid URL - bad data file specification");
    }
  }
  else {
    print_exception_report("Invalid URL - no data file specified");
  }
  if (request.empty()) {
    print_exception_report("A value for the parameter 'REQUEST' is required","MissingParameterValue","REQUEST");
  }
}
void fill_data_value_range(std::unique_ptr<char[]>& buffer,std::string format,double &min,double &max)
{
  if (format == "WMO_GRIB2") {
    GRIB2Message msg;
    msg.fill(reinterpret_cast<unsigned char *>(buffer.get()),false);
    auto g=msg.grid(0);
    max=g->statistics().max_val;
    min=g->statistics().min_val;
  }
  else {
    print_exception_report("Unrecognized format '"+format+"' (fill_data_value_range)");
  }
}

std::string make_work_directory()
{
/*
** make a working directory, and return the name if successful, or an empty
** string for a failure
*/
  static TempDir *tdir=nullptr;
  if (tdir == nullptr) {
    tdir=new TempDir;
    if (!tdir->create(metautils::directives.temp_path)) {
	print_exception_report("Error creating temporary directory");
    }
  }
  if (is_dev) {
    tdir->set_keep();
    std::cerr << tdir->name() << std::endl;
  }
  return tdir->name();
}

void build_resource_directory_tree()
{
  std::stringstream oss,ess;
  if (unixutils::mysystem2("/bin/mkdir -p "+wms_directory+resource.substr(0,resource.rfind("/")),oss,ess) < 0) {
    print_exception_report("Server directory error (make_resource_directory_tree)");
  }
}

std::string build_layer_cache(std::string layer_name)
{
  static const std::string THIS_FUNC=__func__;
  std::string cache_file=wms_directory+resource+"."+layer_name+".cache";
  struct stat buf;
  if (stat(cache_file.c_str(),&buf) != 0) {
    MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
    if (!server) {
	print_exception_report("A database error occurred ("+THIS_FUNC+")");
    }
    auto lparts=strutils::split(layer_name,";");
    std::string layer_time="";
    if (lparts.size() > 4) {
	layer_time=lparts[4];
    }
    MySQL::LocalQuery query("select i.byte_offset,i.byte_length,i.valid_date,f.format from IGrML.`ds"+dsnum2+"_inventory_"+lparts[3]+"` as i left join WGrML.ds"+dsnum2+"_webfiles as w on w.code = i.webID_code left join WGrML.formats as f on f.code = w.format_code where gridDefinition_code = "+lparts[0]+" and timeRange_code = "+lparts[1]+" and level_code = "+lparts[2]+" and webID = '"+resource+"'");
    if (query.submit(server) == 0) {
	if (query.num_rows() == 0) {
	  print_exception_report("Specified layer name is invalid","LayerNotDefined");
	}
	else {
	  build_resource_directory_tree();
	  std::ifstream ifs(metautils::web_home()+"/"+resource);
	  if (!ifs.is_open()) {
	    print_exception_report("The data file could not be opened");
	  }
	  std::unique_ptr<char[]> buffer;
	  size_t buffer_length=0;
	  double overall_min=1.e37,overall_max=-1.e37;
	  std::string data_format;
	  std::stringstream cache_ss;
	  for (const auto& row : query) {
	    if (data_format.empty()) {
		data_format=row[3];
	    }
	    size_t byte_length=std::stoi(row[1]);
	    if (byte_length > buffer_length) {
		buffer_length=byte_length;
		buffer.reset(new char[buffer_length]);
	    }
	    ifs.seekg(std::stoll(row[0]),std::ios::beg);
	    ifs.read(buffer.get(),byte_length);
	    double min,max;
	    fill_data_value_range(buffer,row[3],min,max);
	    if (min < overall_min) {
		overall_min=min;
	    }
	    if (max > overall_max) {
		overall_max=max;
	    }
	    cache_ss << row[2] << "," << row[0] << "," << row[1] << std::endl;
	  }
	  if (overall_min < 0. && overall_max > 0.) {
	    if ( (-overall_min/overall_max) < 0.02) {
		overall_min=0.;
	    }
	  }
	  std::ofstream ofs(cache_file.c_str());
	  if (!ofs.is_open()) {
	    print_exception_report("Cache creation error");
	  }
	  ofs << overall_min << "," << overall_max << std::endl;
	  ofs << data_format << std::endl;
	  ofs << cache_ss.str();
	  ofs.flush();
	  ofs.close();
	  ifs.close();
	}
    }
    else {
	print_exception_report("A database query error occurred ("+THIS_FUNC+")");
    }
    server.disconnect();
  }
  return cache_file;
}

void read_layer_cache(std::string layer_name,std::string layer_time,CacheData& cdata)
{
  auto cache_file=build_layer_cache(layer_name);
  std::ifstream ifs(cache_file.c_str());
  if (!ifs.is_open()) {
    print_exception_report("Cache access error");
  }
  char line[256];
  ifs.getline(line,256);
  auto parts=strutils::split(line,",");
  cdata.data_min=std::stod(parts[0]);
  cdata.data_max=std::stod(parts[1]);
  ifs.getline(line,256);
  cdata.data_format=line;
  ifs.getline(line,256);
  while (!ifs.eof()) {
    parts=strutils::split(line,",");
    if (layer_time.empty() || parts[0] == layer_time) {
	cdata.byte_offset=std::stoll(parts[1]);
	cdata.byte_length=std::stoll(parts[2]);
	break;
    }
    ifs.getline(line,256);
  }
  ifs.close();
  if (cdata.byte_length == 0) {
    if (!layer_time.empty()) {
	print_exception_report("The value for the parameter 'TIME' must match an exact value specified by the capabilities temporal extent","InvalidDimensionValue","TIME");
    }
    else {
	print_exception_report("Cache read error");
    }
  }
}

std::string grid_definition_parameters(const XMLElement& e)
{
  std::string def_params;
  auto definition=e.attribute_value("definition");
  if (definition == "latLon") {
    def_params=e.attribute_value("numX")+":"+e.attribute_value("numY")+":"+e.attribute_value("startLat")+":"+e.attribute_value("startLon")+":"+e.attribute_value("endLat")+":"+e.attribute_value("endLon")+":"+e.attribute_value("xRes")+":"+e.attribute_value("yRes");
  }
  else if (definition == "gaussLatLon") {
    def_params=e.attribute_value("numX")+":"+e.attribute_value("numY")+":"+e.attribute_value("startLat")+":"+e.attribute_value("startLon")+":"+e.attribute_value("endLat")+":"+e.attribute_value("endLon")+":"+e.attribute_value("xRes")+":"+e.attribute_value("circles");
  }
  return def_params;
}

void add_parameter_layers(std::stringstream& wms_layers,std::vector<std::string>& dates,std::string grid_code,std::string product_code,std::string level_code,std::string parameter_code)
{
  DateTime dt[2];
  dt[0].set(std::stoll(dates.front())*100);
  auto dt_index=1;
  auto last_diff=-1;
  for (size_t n=1; n < dates.size(); ++n) {
    dt[dt_index].set(std::stoll(dates[n])*100);
    auto diff=dt[dt_index].hours_since(dt[1-dt_index]);
    if (last_diff < 0) {
	last_diff=diff;
    }
    if (diff != last_diff) {
	last_diff=-1;
	break;
    }
    dt_index=1-dt_index;
  }
  if (last_diff > 0) {
    auto first_date=strutils::substitute(dateutils::string_ll_to_date_string(dates.front())," ","T")+"Z";
    auto last_date=strutils::substitute(dateutils::string_ll_to_date_string(dates.back())," ","T")+"Z";
    wms_layers << "            <Layer queryable=\"0\">" << std::endl;
    auto name=grid_code+";"+product_code+";"+level_code+";"+parameter_code;
    wms_layers << "              <Name>" << name << "</Name>" << std::endl;
    wms_layers << "              <Title>" << first_date << " to " << last_date << "</Title>" << std::endl;
    wms_layers << "              <Dimension name=\"time\" units=\"ISO8601\" default=\"" << first_date << "\" nearestValue=\"0\">" << first_date << "/" << last_date << "/PT" << last_diff << "H</Dimension>" << std::endl;
    wms_layers << "              <Style>" << std::endl;
    wms_layers << "                <Name>Legend</Name>" << std::endl;
    wms_layers << "                <Title>Legend Graphic</Title>" << std::endl;
    wms_layers << "                <LegendURL>" << std::endl;
    wms_layers << "                  <Format>image/png</Format>" << std::endl;
    wms_layers << "                  <OnlineResource xlink:type=\"simple\" xlink:href=\"__SERVICE_RESOURCE_GET_URL__/legend/" << webutils::url_encode(name) << "\" />" << std::endl;
    wms_layers << "                </LegendURL>" << std::endl;
    wms_layers << "              </Style>" << std::endl;
    wms_layers << "            </Layer>" << std::endl;
  }
  else {
    for (auto& date : dates) {
	wms_layers << "            <Layer queryable=\"0\">" << std::endl;
	auto name=grid_code+";"+product_code+";"+level_code+";"+parameter_code+";"+date;
	wms_layers << "              <Name>" << name << "</Name>" << std::endl;
	date.insert(4,1,'-');
	date.insert(7,1,'-');
	date.insert(10,1,'T');
	date.insert(13,1,':');
	wms_layers << "              <Title>" << date << "Z</Title>" << std::endl;
	wms_layers << "              <Style>" << std::endl;
	wms_layers << "                <Name>Legend</Name>" << std::endl;
	wms_layers << "                <Title>Legend Graphic</Title>" << std::endl;
	wms_layers << "                <LegendURL>" << std::endl;
	wms_layers << "                  <Format>image/png</Format>" << std::endl;
	wms_layers << "                  <OnlineResource xlink:type=\"simple\" xlink:href=\"__SERVICE_RESOURCE_GET_URL__/legend/" << webutils::url_encode(name) << "\" />" << std::endl;
	wms_layers << "                </LegendURL>" << std::endl;
	wms_layers << "              </Style>" << std::endl;
	wms_layers << "            </Layer>" << std::endl;
    }
  }
  dates.clear();
}

bool converted_inventory_to_wms(const std::stringstream& inventory,std::stringstream& wms_layers) {
  auto lines=strutils::split(inventory.str(),"\n");
  std::unordered_map<int,std::tuple<std::string,std::string>> products,grids,levels,parameters;
  std::vector<std::tuple<std::string,std::string,std::string,std::string,std::string>> inv_lines;
  auto inventory_started=false;
  for (const auto& line : lines) {
    if (line == "-----") {
	inventory_started=true;
    }
    else if (inventory_started) {
	auto parts=strutils::split(line,"|");
	if (parts.size() > 0) {
	  for (size_t n=3; n < 7; ++n) {
	    if (parts[n].length() < 3) {
		parts[n].insert(0,3-parts[n].length(),'0');
	    }
	  }
	  inv_lines.emplace_back(std::make_tuple(parts[4],parts[3],parts[5],parts[6],parts[2]));
	}
    }
    else {
	switch (line[0]) {
	  case 'U':
	  {
	    auto p_parts=strutils::split(line,"<!>");
	    products.emplace(std::stoi(p_parts[1]),std::make_tuple(p_parts[2],p_parts[3]));
	    break;
	  }
	  case 'G':
	  {
	    auto g_parts=strutils::split(line,"<!>");
	    grids.emplace(std::stoi(g_parts[1]),std::make_tuple(g_parts[2],g_parts[3]));
	    break;
	  }
	  case 'L':
	  {
	    auto l_parts=strutils::split(line,"<!>");
	    levels.emplace(std::stoi(l_parts[1]),std::make_tuple(l_parts[2],l_parts[3]));
	    break;
	  }
	  case 'P':
	  {
	    auto p_parts=strutils::split(line,"<!>");
	    parameters.emplace(std::stoi(p_parts[1]),std::make_tuple(p_parts[2],p_parts[3]));
	    break;
	  }
	}
    }
  }
  std::sort(inv_lines.begin(),inv_lines.end(),
  [](const std::tuple<std::string,std::string,std::string,std::string,std::string>& left,const std::tuple<std::string,std::string,std::string,std::string,std::string>& right) -> bool
  {
    if (std::get<0>(left) < std::get<0>(right)) {
	return true;
    }
    else if (std::get<0>(left) > std::get<0>(right)) {
	return false;
    }
    else {
	if (std::get<1>(left) < std::get<1>(right)) {
	  return true;
	}
	else if (std::get<1>(left) > std::get<1>(right)) {
	  return false;
	}
	else {
	  if (std::get<2>(left) < std::get<2>(right)) {
	    return true;
	  }
	  else if (std::get<2>(left) > std::get<2>(right)) {
	    return false;
	  }
	  else {
	    if (std::get<3>(left) < std::get<3>(right)) {
		return true;
	    }
	    else if (std::get<3>(left) > std::get<3>(right)) {
		return false;
	    }
	    else {
		return (std::get<4>(left) < std::get<4>(right));
	    }
	  }
	}
    }
  });
  std::string last_grid,last_product,last_level,last_parameter;
  wms_layers.setf(std::ios::fixed);
  wms_layers.precision(4);
  std::vector<std::string> dates;
  for (const auto& inv_line : inv_lines) {
    if (std::get<0>(inv_line) != last_grid) {
	if (!last_grid.empty()) {
	  if (!last_product.empty()) {
	    if (!last_level.empty()) {
		if (!last_parameter.empty()) {
		  add_parameter_layers(wms_layers,dates,std::get<0>(grids[std::stoi(last_grid)]),std::get<0>(products[std::stoi(last_product)]),std::get<0>(levels[std::stoi(last_level)]),std::get<0>(parameters[std::stoi(last_parameter)]));
		  wms_layers << "          </Layer>" << std::endl;
		}
		wms_layers << "        </Layer>" << std::endl;
	    }
	    wms_layers << "      </Layer>" << std::endl;
	  }
	  wms_layers << "    </Layer>" << std::endl;
	}
	last_grid=std::get<0>(inv_line);
	auto g_index=std::stoi(last_grid);
	wms_layers << "    <Layer>" << std::endl;
	auto grid_definition=strutils::substitute(std::get<1>(grids[g_index]),",",":");
	auto idx=grid_definition.find(":");
	grid_definition=grid_definition.substr(0,idx)+"<!>"+grid_definition.substr(idx+1);
	double west_lon,south_lat,east_lon,north_lat;
	if (!gridutils::fill_spatial_domain_from_grid_definition(grid_definition,"primeMeridian",west_lon,south_lat,east_lon,north_lat)) {
	  myerror="Error getting spatial domain";
	  return false;
	}
	auto g_parts=strutils::split(std::get<1>(grids[g_index]),",");
	wms_layers << "      <Title>" << g_parts[0] << "_" << g_parts[1] << "x" << g_parts[2] << "</Title>" << std::endl;
	wms_layers << "#REPEAT __CRS__" << g_index << "__" << std::endl;
	wms_layers << "      <CRS>__CRS__" << g_index << "__.CRS</CRS>" << std::endl;
	wms_layers << "#ENDREPEAT __CRS__" << g_index << "__" << std::endl;
	wms_layers << "      <EX_GeographicBoundingBox>" << std::endl;
	wms_layers << "        <westBoundLongitude>" << west_lon << "</westBoundLongitude>" << std::endl;
	wms_layers << "        <eastBoundLongitude>" << east_lon << "</eastBoundLongitude>" << std::endl;
	wms_layers << "        <southBoundLatitude>" << south_lat << "</southBoundLatitude>" << std::endl;
	wms_layers << "        <northBoundLatitude>" << north_lat << "</northBoundLatitude>" << std::endl;
	wms_layers << "      </EX_GeographicBoundingBox>" << std::endl;
	wms_layers << "#REPEAT __CRS__" << g_index << "__" << std::endl;
	wms_layers << "      <BoundingBox CRS=\"__CRS__" << g_index << "__.CRS\" minx=\"__CRS__" << g_index << "__.minx\" miny=\"__CRS__" << g_index << "__.miny\" maxx=\"__CRS__" << g_index << "__.maxx\" maxy=\"__CRS__" << g_index << "__.maxy\" />" << std::endl;
	wms_layers << "#ENDREPEAT __CRS__" << g_index << "__" << std::endl;
	last_product="";
    }
    if (std::get<1>(inv_line) != last_product) {
	if (!last_product.empty()) {
	  if (!last_level.empty()) {
	    if (!last_parameter.empty()) {
		add_parameter_layers(wms_layers,dates,std::get<0>(grids[std::stoi(last_grid)]),std::get<0>(products[std::stoi(last_product)]),std::get<0>(levels[std::stoi(last_level)]),std::get<0>(parameters[std::stoi(last_parameter)]));
		wms_layers << "          </Layer>" << std::endl;
	    }
	    wms_layers << "        </Layer>" << std::endl;
	  }
	  wms_layers << "      </Layer>" << std::endl;
	}
	last_product=std::get<1>(inv_line);
	auto p_index=std::stoi(last_product);
	wms_layers << "      <Layer>" << std::endl;
	wms_layers << "        <Title>" << std::get<1>(products[p_index]) << "</Title>" << std::endl;
	last_level="";
    }
    if (std::get<2>(inv_line) != last_level) {
	if (!last_level.empty()) {
	  if (!last_parameter.empty()) {
	    add_parameter_layers(wms_layers,dates,std::get<0>(grids[std::stoi(last_grid)]),std::get<0>(products[std::stoi(last_product)]),std::get<0>(levels[std::stoi(last_level)]),std::get<0>(parameters[std::stoi(last_parameter)]));
	    wms_layers << "          </Layer>" << std::endl;
	  }
	  wms_layers << "        </Layer>" << std::endl;
	}
	last_level=std::get<2>(inv_line);
	auto l_index=std::stoi(last_level);
	wms_layers << "        <Layer>" << std::endl;
	wms_layers << "          <Title>" << std::get<1>(levels[l_index]) << "</Title>" << std::endl;
	last_parameter="";
    }
    if (std::get<3>(inv_line) != last_parameter) {
	if (!last_parameter.empty()) {
	  add_parameter_layers(wms_layers,dates,std::get<0>(grids[std::stoi(last_grid)]),std::get<0>(products[std::stoi(last_product)]),std::get<0>(levels[std::stoi(last_level)]),std::get<0>(parameters[std::stoi(last_parameter)]));
	  wms_layers << "          </Layer>" << std::endl;
	}
	last_parameter=std::get<3>(inv_line);
	auto p_index=std::stoi(last_parameter);
	wms_layers << "          <Layer>" << std::endl;
        wms_layers << "            <Title>" << std::get<1>(parameters[p_index]) << "</Title>" << std::endl;
    }
    dates.emplace_back(std::get<4>(inv_line));
  }
  add_parameter_layers(wms_layers,dates,std::get<0>(grids[std::stoi(last_grid)]),std::get<0>(products[std::stoi(last_product)]),std::get<0>(levels[std::stoi(last_level)]),std::get<0>(parameters[std::stoi(last_parameter)]));
  wms_layers << "          </Layer>" << std::endl;
  wms_layers << "        </Layer>" << std::endl;
  wms_layers << "      </Layer>" << std::endl;
  wms_layers << "    </Layer>";
  wms_layers.flush();
  return true;
}

void fill_capabilities_parameters(const QueryString& query_string,CapabilitiesParameters& cp)
{
  cp.service=query_string.value("service");
  if (cp.service != "WMS") {
    if (cp.service.empty()) {
	print_exception_report("The value for the parameter 'SERVICE' must be 'WMS'","MissingParameterValue","SERVICE");
    }
    else {
	print_exception_report("The value for the parameter 'SERVICE' must be 'WMS'","InvalidParameterValue","SERVICE");
    }
  }
  cp.version=query_string.value("version");
}

void build_capabilities_cache(const QueryString& query_string)
{
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (!server) {
    print_exception_report("Database connection error");
  }
  MySQL::LocalQuery query("select uflag from WGrML.ds"+dsnum2+"_webfiles2 where webID = '"+resource+"'");
  MySQL::Row row;
  if (query.submit(server) < 0) {
    print_exception_report("Database server error");
  }
  else {
    if (query.num_rows() == 0) {
	print_exception_report("The specified resource does not exist");
    }
    else if (!query.fetch_row(row)) {
	print_exception_report("Database query error");
    }
  }
  auto uflag=row[0]+"\n";
  server.disconnect();
  auto capabilities_file=wms_directory+resource+".xml";
  auto versions_match=false;
  struct stat buf;
  if (stat((capabilities_file+".vrsn").c_str(),&buf) == 0) {
    std::stringstream oss,ess;
    unixutils::mysystem2("/bin/tcsh -c \"cat "+capabilities_file+".vrsn\"",oss,ess);
    versions_match=(oss.str() == uflag);
  }
  if (stat(capabilities_file.c_str(),&buf) != 0 || !versions_match) {
    std::stringstream inventory;
    if (webutils::filled_inventory(metautils::args.dsnum,resource,"GrML",inventory) < 0) {
	print_exception_report("The resource was not found");
    }
    std::stringstream wms_layers;
    if (!converted_inventory_to_wms(inventory,wms_layers)) {
	print_exception_report("The resource is not accessible");
    }
    build_resource_directory_tree();
    TempFile tfile("/data/ptmp");
    XMLSnippet xmls("<Layers>"+wms_layers.str()+"</Layers>");
    auto bbox_list=xmls.element_list("Layers/Layer/EX_GeographicBoundingBox");
    std::stringstream tss;
    TokenDocument *tdoc=new TokenDocument(tfile.name());
    tdoc=new TokenDocument("/usr/local/www/server_root/web/html/wms/capabilities.xml");
    tdoc->add_replacement("__UPDATE_SEQUENCE__",dateutils::current_date_time().to_string("%Y%m%d%H%MM"));
    std::string server_name;
    char *env;
    if ( (env=getenv("SERVER_NAME")) != nullptr) {
	server_name=env;
    }
    tdoc->add_replacement("__SERVICE_RESOURCE_GET_URL__","https://"+server_name+"/wms"+strutils::substitute(query_string.value("resource"),"%","%25"));
    for (const auto& format : image_formats) {
	tdoc->add_repeat("__IMAGE_FORMAT__",format.first);
    }
    tdoc->add_replacement("__LAYERS__",wms_layers.str());
    tss << *tdoc << std::endl;
    std::ofstream ofs(tfile.name().c_str());
    if (!ofs.is_open()) {
	print_exception_report("Resource access error (3)");
    }
    ofs << tss.str() << std::endl;
    ofs.close();
    delete tdoc;
    tdoc=new TokenDocument(tfile.name());
    tdoc->add_replacement("__SERVICE_RESOURCE_GET_URL__","https://"+server_name+"/wms"+strutils::substitute(query_string.value("resource"),"%","%25"));
    auto gcount=0;
    for (const auto& bbox : bbox_list) {
	auto wb=bbox.element("westBoundLongitude").content();
	auto eb=bbox.element("eastBoundLongitude").content();
	auto sb=bbox.element("southBoundLatitude").content();
	auto nb=bbox.element("northBoundLatitude").content();
	for (const auto& crs : coordinate_reference_systems) {
	  auto earth_radius=std::get<1>(crs.second);
	  if (earth_radius == 0.) {
	    switch (std::get<2>(crs.second)) {
		case 0x1: {
		  tdoc->add_repeat("__CRS__"+strutils::itos(gcount)+"__","CRS[!]"+crs.first+"<!>minx[!]"+sb+"<!>maxx[!]"+nb+"<!>miny[!]"+wb+"<!>maxy[!]"+eb);
		  break;
		}
		default: {
		  tdoc->add_repeat("__CRS__"+strutils::itos(gcount)+"__","CRS[!]"+crs.first+"<!>minx[!]"+wb+"<!>maxx[!]"+eb+"<!>miny[!]"+sb+"<!>maxy[!]"+nb);
		}
	    }
	  }
	  else {
	    auto wbr=strutils::dtos(std::stod(wb)*TWO_PI*earth_radius/360.,4);
	    auto ebr=strutils::dtos(std::stod(eb)*TWO_PI*earth_radius/360.,4);
	    auto sbr=strutils::dtos(std::stod(sb)*TWO_PI*earth_radius/360.,4);
	    auto nbr=strutils::dtos(std::stod(nb)*TWO_PI*earth_radius/360.,4);
	    switch (std::get<2>(crs.second)) {
		case 0x1: {
		  tdoc->add_repeat("__CRS__"+strutils::itos(gcount)+"__","CRS[!]"+crs.first+"<!>minx[!]"+sbr+"<!>maxx[!]"+nbr+"<!>miny[!]"+wbr+"<!>maxy[!]"+ebr);
		  break;
		}
		default: {
		  tdoc->add_repeat("__CRS__"+strutils::itos(gcount)+"__","CRS[!]"+crs.first+"<!>minx[!]"+wbr+"<!>maxx[!]"+ebr+"<!>miny[!]"+sbr+"<!>maxy[!]"+nbr);
		}
	    }
	  }
	}
	++gcount;
    }
    ofs.open(capabilities_file.c_str());
    ofs << *tdoc;
    ofs.close();
    ofs.open((capabilities_file+".vrsn").c_str());
    ofs << uflag;
    ofs.close();
  }
  else {
    utime((capabilities_file+".vrsn").c_str(),nullptr);
  }
}

void read_capabilities_cache(const QueryString& query_string,std::stringstream& capabilities_ss)
{
  build_capabilities_cache(query_string);
  auto capabilities_file=wms_directory+resource+".xml";
  std::stringstream ess;
  if (unixutils::mysystem2("/bin/tcsh -c \"cat "+capabilities_file+"\"",capabilities_ss,ess) < 0) {
    print_exception_report("Resource access error (2)");
  }
}

void output_capabilities(const QueryString& query_string)
{
  std::stringstream capabilities_ss;
  read_capabilities_cache(query_string,capabilities_ss);
  std::cout << "Content-type: application/xml" << std::endl << std::endl;
  std::cout << capabilities_ss.str();
}

size_t color_map_size(std::string color_map)
{
  std::stringstream oss,ess;
  if (unixutils::mysystem2("/bin/tcsh -c \"head -1 /usr/local/www/server_root/web/html/wms/"+color_map+".colormap |awk -F= '{print $2}'\"",oss,ess) < 0) {
    print_exception_report("There was an error reading the color map: '"+ess.str()+"'");
  }
  size_t num_colors=std::stoi(oss.str());
  if (num_colors > 254) {
    num_colors=254;
  }
  return num_colors;
}

std::vector<double> precipitation_contours()
{
  return std::move(std::vector<double>{0.254,2.54,6.35,12.7,19.05,25.4,31.75,38.1,44.45,50.8,63.5,76.2,88.9,101.6,127.,177.8,254.,381.,508.});
}

std::vector<unsigned char> precipitation_contour_types()
{
  return std::move(std::vector<unsigned char>{0x2,0x1,0x2,0x1,0x2,0x1,0x2,0x2,0x2,0x1,0x2,0x2,0x2,0x1,0x2,0x2,0x1,0x2,0x1});
}

double y_to_latitude(std::string projection,double y,double semi_major_axis)
{
  double latitude=0.;
  if (projection == "Mercator") {
     latitude=(360/PI)*(atan(pow(E,y/semi_major_axis))-(PI/4));
  }
  return latitude;
}

void get_contours_from_data(double data_min,double data_max,std::string color_map,int first_color_index,double central_value,std::vector<double>& levels,std::vector<short>& fill_colors,std::vector<unsigned char>& types,std::vector<short>& dash_patterns)
{
  if (central_value != 1.e18) {
    auto right_range=data_max-central_value;
    auto left_range=central_value-data_min;
    if (right_range > left_range) {
	data_min=central_value-right_range;
    }
    else {
	data_max=central_value+left_range;
    }
  }
// aim for 10 to 20 major (labeled) contours
  auto num_filled_contours=color_map_size(color_map)-first_color_index;
  size_t major_contour_count=0;
  for (size_t n=20; n >= 10; --n) {
    if ( (num_filled_contours % n) == 0) {
	major_contour_count=n;
	break;
    }
  }
  if (major_contour_count == 0) {
    major_contour_count=15;
    --num_filled_contours;
    while (num_filled_contours > 0 && (num_filled_contours % 15) != 0) {
	--num_filled_contours;
    }
  }
  auto minor_contour_count=num_filled_contours/major_contour_count;
  auto contour_increment=(data_max-data_min)/major_contour_count;
  auto fill_color_index=first_color_index;
  auto vector_size=major_contour_count*minor_contour_count;
  levels.clear();
  levels.reserve(vector_size);
  fill_colors.clear();
  fill_colors.reserve(vector_size);
  types.clear();
  types.reserve(vector_size);
  dash_patterns.clear();
  dash_patterns.reserve(vector_size);
  for (size_t n=0; n < major_contour_count; ++n) {
    levels.emplace_back(n*contour_increment+data_min);
    fill_colors.emplace_back(fill_color_index++);
    types.emplace_back(0x1);
    if (central_value != 1.e18) {
	if (levels.back() <= central_value) {
	  dash_patterns.emplace_back(5);
	}
	else {
	  dash_patterns.emplace_back(0);
	}
    }
    for (size_t m=1; m < minor_contour_count; ++m) {
	levels.emplace_back(contour_increment*(n+static_cast<double>(m)/minor_contour_count)+data_min);
	fill_colors.emplace_back(fill_color_index++);
	types.emplace_back(0x2);
	if (central_value != 1.e18) {
	  if (levels.back() <= central_value) {
	    dash_patterns.emplace_back(5);
	  }
	  else {
	    dash_patterns.emplace_back(0);
	  }
	}
    }
  }
// pad the contours so the legend displays properly
  levels.emplace_back(levels.back());
  levels.emplace_back(levels.back());
  fill_colors.emplace_back(fill_colors.back());
  fill_colors.emplace_back(fill_colors.back());
  types.emplace_back(0x2);
  types.emplace_back(0x2);
  dash_patterns.emplace_back(0);
  dash_patterns.emplace_back(0);
}

void set_contours_from_specified_values(const std::vector<double>& specified_contour_levels,const std::vector<unsigned char>& specified_contour_level_types,std::string color_map,size_t first_color_index,TokenDocument& tdoc)
{
  tdoc.add_if("__SPECIFIED_CONTOUR_LEVELS__");
  auto data_range=specified_contour_levels.back()-specified_contour_levels.front();
  auto label_precision=2;
  while (label_precision < 5 && data_range < 1.) {
    ++label_precision;
    data_range*=10.;
  }
  auto num_colors=color_map_size(color_map)-first_color_index+1; 
  auto num_minor_levels=(num_colors-1)/specified_contour_levels.size();
  std::stringstream contours_ss,fill_colors_ss,flags_ss;
  if (label_precision < 5) {
    contours_ss.setf(std::ios::fixed);
    contours_ss.precision(label_precision);
  }
  else {
    contours_ss.setf(std::ios::scientific);
    contours_ss.precision(3);
  }
  for (size_t n=0; n < specified_contour_levels.size()-1; ++n) {
    if (!contours_ss.str().empty()) {
	contours_ss << ",";
    }
    contours_ss << specified_contour_levels[n];
    if (!fill_colors_ss.str().empty()) {
	fill_colors_ss << ",";
    }
    fill_colors_ss << first_color_index++;
    if (!flags_ss.str().empty()) {
	flags_ss << ",";
    }
    if (specified_contour_level_types[n] == 0x1) {
	flags_ss << "\"LineAndLabel\"";
    }
    else {
	flags_ss << "\"NoLine\"";
    }
    for (size_t m=1; m <= num_minor_levels; ++m) {
	auto minor_diff=(specified_contour_levels[n+1]-specified_contour_levels[n])/(num_minor_levels+1);
	if (!contours_ss.str().empty()) {
	  contours_ss << ",";
	}
	contours_ss << specified_contour_levels[n]+m*minor_diff;
	if (!fill_colors_ss.str().empty()) {
	  fill_colors_ss << ",";
	}
	fill_colors_ss << first_color_index++;
	if (!flags_ss.str().empty()) {
	  flags_ss << ",";
	}
	flags_ss << "\"NoLine\"";
    }
  }
  contours_ss << "," << specified_contour_levels.back();
  fill_colors_ss << "," << first_color_index;
  if (specified_contour_level_types.back() == 0x1) {
    flags_ss << ",\"LineAndLabel\"";
  }
  else {
    flags_ss << ",\"NoLine\"";
  }
  tdoc.add_replacement("__SPECIFIED_CONTOUR_LEVELS__",contours_ss.str());
  tdoc.add_replacement("__SPECIFIED_CONTOUR_FILL_COLORS__",fill_colors_ss.str());
  tdoc.add_replacement("__SPECIFIED_CONTOUR_LEVEL_FLAGS__",flags_ss.str());
}

void set_contours_from_data(double data_min,double data_max,std::string color_map,size_t first_color_index,double central_value,TokenDocument& tdoc)
// set first_color_index to 1 if the first filled contour should be the
//   background color
{
  tdoc.add_if("__SPECIFIED_CONTOUR_LEVELS__");
  auto data_range=data_max-data_min;
  auto label_precision=2;
  while (label_precision < 5 && data_range < 1.) {
    ++label_precision;
    data_range*=10.;
  }
  std::vector<double> contour_levels;
  std::vector<short> contour_fill_colors,contour_dash_patterns;
  std::vector<unsigned char> contour_types;
  get_contours_from_data(data_min,data_max,color_map,first_color_index,central_value,contour_levels,contour_fill_colors,contour_types,contour_dash_patterns);
  std::stringstream contours_ss;
  if (label_precision < 5) {
    contours_ss.setf(std::ios::fixed);
    contours_ss.precision(label_precision);
  }
  else {
    contours_ss.setf(std::ios::scientific);
    contours_ss.precision(3);
  }
  for (const auto& contour_level : contour_levels) {
    if (!contours_ss.str().empty()) {
	contours_ss << ",";
    }
    contours_ss << contour_level;
  }
  tdoc.add_replacement("__SPECIFIED_CONTOUR_LEVELS__",contours_ss.str());
  std::stringstream fill_colors_ss;
  for (const auto& fill_color : contour_fill_colors) {
    if (!fill_colors_ss.str().empty()) {
	fill_colors_ss << ",";
    }
    fill_colors_ss << fill_color;
  }
  tdoc.add_replacement("__SPECIFIED_CONTOUR_FILL_COLORS__",fill_colors_ss.str());
  std::stringstream flags_ss;
  for (const auto& contour_type : contour_types) {
    if (!flags_ss.str().empty()) {
	flags_ss << ",";
    }
    if (contour_type == 0x1) {
	flags_ss << "\"LineAndLabel\"";
    }
    else {
	flags_ss << "\"NoLine\"";
    }
  }
  tdoc.add_replacement("__SPECIFIED_CONTOUR_LEVEL_FLAGS__",flags_ss.str());
  if (central_value != 1.e18) {
    tdoc.add_if("__SPECIFIED_CONTOUR_LINE_DASH_PATTERNS__");
    std::stringstream dash_ss;
    for (const auto& dash_pattern : contour_dash_patterns) {
	if (!dash_ss.str().empty()) {
	  dash_ss << ",";
	}
	dash_ss << dash_pattern;
    }
    tdoc.add_replacement("__SPECIFIED_CONTOUR_LINE_DASH_PATTERNS__",dash_ss.str());
  }
}

void output_map(const MapParameters& mp)
{
  auto tdir_name=make_work_directory();
  CacheData cdata;
  read_layer_cache(mp.layers.front(),mp.time,cdata);
  std::ifstream ifs(metautils::web_home()+"/"+resource);
  std::ofstream ofs((tdir_name+"/infile.grb2").c_str());
  if (!ifs.is_open() || !ofs.is_open()) {
    print_exception_report("The data file could not be opened");
  }
  ifs.seekg(cdata.byte_offset,std::ios::beg);
  std::unique_ptr<char []> buffer(new char[cdata.byte_length]);
  ifs.read(buffer.get(),cdata.byte_length);
  if (!ifs.good()) {
    print_exception_report("There was an error while reading the data file");
  }
  ifs.close();
  ofs.write(buffer.get(),cdata.byte_length);
  ofs.close();
  TokenDocument tdoc("/usr/local/www/server_root/web/html/wms/grib2.ncl");
  tdoc.add_replacement("__INFILE__",tdir_name+"/infile.grb2");
  if (is_dev) {
    tdoc.add_replacement("__OUTLINE_BOUNDARY_SETS__","AllBoundaries");
  }
  else {
    tdoc.add_replacement("__OUTLINE_BOUNDARY_SETS__","NoBoundaries");
  }
  MySQL::Server server(metautils::directives.database_server,metautils::directives.metadb_username,metautils::directives.metadb_password,"");
  if (!server) {
    print_exception_report("A database error occurred (output_map)");
  }
  auto lparts=strutils::split(mp.layers.front(),";");
  MySQL::LocalQuery query("definition","WGrML.gridDefinitions","code = "+lparts[0]);
  MySQL::Row row;
  if (query.submit(server) == 0 && query.fetch_row(row)) {
    if (row[0] == "lambertConformal") {
	tdoc.add_if("__LAMBERT_CONFORMAL__");
    }
    else if (row[0] == "latLon" || row[0] == "gaussLatLon") {
	tdoc.add_if("__LAT_LON__");
	tdoc.add_replacement("__MAP_PROJECTION__",std::get<0>(coordinate_reference_systems[mp.crs]));
    }
  }
  if (mp.width > mp.height) {
    size_t max=8640;
    tdoc.add_replacement("__WIDTH__",strutils::itos(max));
    tdoc.add_replacement("__HEIGHT__",strutils::itos(lround((static_cast<float>(mp.height))/mp.width*max)));
  }
  else {
    size_t max=8640;
    tdoc.add_replacement("__HEIGHT__",strutils::itos(max));
    tdoc.add_replacement("__WIDTH__",strutils::itos(lround((static_cast<float>(mp.width))/mp.height*max)));
  }
  tdoc.add_replacement("__OUTFILE__",tdir_name+"/outfile."+image_formats[mp.format]);
  auto bbox_parts=strutils::split(mp.bbox,",");
  std::string projection;
  double semi_major_axis;
  unsigned char flag;
  std::tie(projection,semi_major_axis,flag)=coordinate_reference_systems[mp.crs];
  if (semi_major_axis == 0.) {
    switch (flag) {
	case 0x1: {
	  tdoc.add_replacement("__MIN_LAT__",strutils::dtos(std::stod(bbox_parts[0]),4));
	  tdoc.add_replacement("__MIN_LON__",strutils::dtos(std::stod(bbox_parts[1]),4));
	  tdoc.add_replacement("__MAX_LAT__",strutils::dtos(std::stod(bbox_parts[2]),4));
	  tdoc.add_replacement("__MAX_LON__",strutils::dtos(std::stod(bbox_parts[3]),4));
	  break;
	}
	default: {
	  tdoc.add_replacement("__MIN_LON__",strutils::dtos(std::stod(bbox_parts[0]),4));
	  tdoc.add_replacement("__MIN_LAT__",strutils::dtos(std::stod(bbox_parts[1]),4));
	  tdoc.add_replacement("__MAX_LON__",strutils::dtos(std::stod(bbox_parts[2]),4));
	  tdoc.add_replacement("__MAX_LAT__",strutils::dtos(std::stod(bbox_parts[3]),4));
	}
    }
  }
  else {
    const double LON_METERS_PER_DEGREE=TWO_PI*semi_major_axis/360.;
    tdoc.add_replacement("__MIN_LON__",strutils::dtos(std::stod(bbox_parts[0])/LON_METERS_PER_DEGREE,4));
    tdoc.add_replacement("__MAX_LON__",strutils::dtos(std::stod(bbox_parts[2])/LON_METERS_PER_DEGREE,4));
    tdoc.add_replacement("__MIN_LAT__",strutils::dtos(y_to_latitude(projection,std::stod(bbox_parts[1]),semi_major_axis),4));
    tdoc.add_replacement("__MAX_LAT__",strutils::dtos(y_to_latitude(projection,std::stod(bbox_parts[3]),semi_major_axis),4));
  }
  std::unique_ptr<std::vector<double>> specified_contour_levels(nullptr);
  std::unique_ptr<std::vector<unsigned char>> specified_contour_level_types(nullptr);
  std::string color_map="NCV_bright";
  size_t first_color_index=2;
  double central_value=1.e18;
  auto param=lparts[3].substr(lparts[3].find(":")+1);
  if (std::regex_search(param,std::regex("^0\\.0\\.[045]$"))) {
// GRIB2 temperature
    central_value=273.15;
  }
  else if (std::regex_search(param,std::regex("^0\\.1\\.8$"))) {
// GRIB2 total precipitation
    color_map="precip_new";
    specified_contour_levels.reset(new std::vector<double>(precipitation_contours()));
    specified_contour_level_types.reset(new std::vector<unsigned char>(precipitation_contour_types()));
  }
  else if (std::regex_search(param,std::regex("^0\\.1\\.0$"))) {
// GRIB2 specific humidity
    color_map="brown_green";
  }
  else if (std::regex_search(param,std::regex("^0\\.2\\.[23]$"))) {
// GRIB2 u- and v- wind components
    color_map="NCV_jaisnd";
    central_value=0.;
  }
  if (specified_contour_levels == nullptr) {
    set_contours_from_data(cdata.data_min,cdata.data_max,color_map,first_color_index,central_value,tdoc);
  }
  else {
    set_contours_from_specified_values(*specified_contour_levels,*specified_contour_level_types,color_map,first_color_index,tdoc);
  }
  tdoc.add_replacement("__COLOR_MAP__",color_map);
  TokenDocument colormap("/usr/local/www/server_root/web/html/wms/"+color_map+".colormap");
  colormap.add_replacement("__BGRED__",strutils::itos(mp.bgcolor[0]));
  colormap.add_replacement("__BGGREEN__",strutils::itos(mp.bgcolor[1]));
  colormap.add_replacement("__BGBLUE__",strutils::itos(mp.bgcolor[2]));
  ofs.open((tdir_name+"/"+color_map+".rgb").c_str());
  if (!ofs.is_open()) {
    print_exception_report("There was an error writing the color map");
  }
  ofs << colormap << std::endl;
  ofs.close();
  tdoc.add_replacement("__MIN_VAL__",strutils::ftos(cdata.data_min,4));
  tdoc.add_replacement("__MAX_VAL__",strutils::ftos(cdata.data_max,4));
  ofs.open((tdir_name+"/outfile.ncl").c_str());
  if (!ofs.is_open()) {
    print_exception_report("There was an error writing the NCL file");
  }
  ofs << tdoc << std::endl;
  ofs.close();
  std::stringstream oss,ess;
  if (unixutils::mysystem2("/bin/tcsh -c \"source /etc/profile.d/ncarg.csh; setenv NCARG_COLORMAPS "+tdir_name+"; /usr/bin/ncl "+tdir_name+"/outfile.ncl\"",oss,ess) < 0 || std::regex_search(oss.str(),std::regex("fatal:"))) {
    print_exception_report("NCL error:\n-----\n"+oss.str()+"\n-----\n"+ess.str());
  }
  if (mp.transparent) {
    unixutils::mysystem2("/bin/tcsh -c \"/usr/bin/convert "+tdir_name+"/outfile."+image_formats[mp.format]+" -trim +repage -fuzz 1% -transparent 'rgb("+strutils::itos(mp.bgcolor[0])+","+strutils::itos(mp.bgcolor[1])+","+strutils::itos(mp.bgcolor[2])+")' -alpha set -background none -channel A -evaluate multiply 0.5 +channel -resize "+mp.s_width+"x"+mp.s_height+"! -\"",oss,ess);
  }
  else {
    unixutils::mysystem2("/bin/tcsh -c \"/usr/bin/convert "+tdir_name+"/outfile."+image_formats[mp.format]+" -trim -resize "+mp.s_width+"x"+mp.s_height+"! -\"",oss,ess);
  }
  std::cout << "Content-type: " << mp.format << std::endl << std::endl;
  std::cout << oss.str() << std::endl;
}

void fill_get_map_parameters(const QueryString& query_string,MapParameters& mp)
{
// VERSION
  mp.version=query_string.value("version");
  if (mp.version.empty()) {
    print_exception_report("A value for the parameter 'VERSION' is required","MissingParameterValue","VERSION");
  }
  else if (mp.version != "1.3.0") {
    print_exception_report("This server only supports version 1.3.0","InvalidParameterValue","VERSION");
  }
// LAYERS
  auto l=query_string.value("layers");
  mp.layers=strutils::split(l,",");
  if (mp.layers.size() > 1) {
    print_exception_report("Only one layer may be requested","OperationNotSupported","LAYERS");
  }
  else if (mp.layers.size() == 0) {
    print_exception_report("A value for the parameter 'LAYERS' is required","MissingParameterValue","LAYERS");
  }
// STYLES
  auto s=query_string.value("styles");
  mp.styles=strutils::split(s,",");
  if (mp.styles.size() != mp.layers.size()) {
    if (mp.styles.size() == 0 && mp.layers.size() == 1 && query_string.has_value("styles")) {
	mp.styles.emplace_back("default");
    }
    else {
	print_exception_report("Exactly one style must be requested for each layer");
    }
  }
// CRS
  mp.crs=query_string.value("crs");
  if (mp.crs.empty()) {
    print_exception_report("A value for the parameter 'CRS' is required","MissingParameterValue","CRS");
  }
  else if (coordinate_reference_systems.find(mp.crs) == coordinate_reference_systems.end()) {
    print_exception_report("The CRS '"+mp.crs+"' is not available from this server","InvalidCRS","CRS");
  }
// BBOX
  mp.bbox=query_string.value("bbox");
  if (mp.bbox.empty()) {
    print_exception_report("A value for the parameter 'BBOX' is required","MissingParameterValue","BBOX");
  }
// WIDTH
  mp.s_width=query_string.value("width");
  if (mp.s_width.empty()) {
    print_exception_report("A value for the parameter 'WIDTH' is required","MissingParameterValue","WIDTH");
  }
// HEIGHT
  mp.s_height=query_string.value("height");
  if (mp.s_height.empty()) {
    print_exception_report("A value for the parameter 'HEIGHT' is required","MissingParameterValue","HEIGHT");
  }
  mp.width=std::stoi(mp.s_width);
  mp.height=std::stoi(mp.s_height);
// FORMAT
  mp.format=query_string.value("format");
  if (mp.format.empty()) {
    print_exception_report("A value for the parameter 'FORMAT' is required","MissingParameterValue","FORMAT");
  }
  else if (image_formats.find(strutils::to_lower(mp.format)) == image_formats.end()) {
    print_exception_report("The image format '"+mp.format+"' is not available from this server","InvalidFormat","FORMAT");
  }
  mp.format=strutils::to_lower(mp.format);
// TRANSPARENT
  auto transparent=query_string.value("transparent");
  if (transparent == "TRUE") {
    mp.transparent=true;
  }
  else if (!transparent.empty() && transparent != "FALSE") {
    print_exception_report("The value '"+transparent+"' for 'TRANSPARENT' is not valid","InvalidParameterValue","TRANSPARENT");
  }
// BGCOLOR
  auto bgcolor=query_string.value("bgcolor");
  if (!bgcolor.empty()) {
    if (!std::regex_search(bgcolor,std::regex("^0[xX]([0-9a-fA-F]){6}$"))) {
	print_exception_report("The value '"+bgcolor+"' for 'BGCOLOR' is not valid","InvalidParameterValue","BGCOLOR");
    }
    else {
	mp.bgcolor[0]=xtox::htoi(bgcolor.substr(2,2));
	mp.bgcolor[1]=xtox::htoi(bgcolor.substr(4,2));
	mp.bgcolor[2]=xtox::htoi(bgcolor.substr(6,2));
    }
  }
// TIME
  mp.time=query_string.value("time");
  if (!mp.time.empty()) {
    auto parts=strutils::split(mp.time,"/");
    if (parts.size() > 1) {
	mp.time=parts.front();
    }
    strutils::replace_all(mp.time,"-","");
    strutils::replace_all(mp.time,"T","");
    strutils::replace_all(mp.time,":","");
    mp.time.pop_back();
  }
}

void set_legend_from_specified_values(const std::vector<double>& specified_contour_levels,const std::vector<unsigned char>& specified_contour_level_types,std::string color_map,size_t first_color_index,TokenDocument& tdoc)
{
  tdoc.add_if("__SPECIFIC_LABELS__");
  auto data_range=specified_contour_levels.back()-specified_contour_levels.front();
  auto label_precision=2;
  while (label_precision < 5 && data_range < 1.) {
    ++label_precision;
    data_range*=10.;
  }
  auto num_colors=color_map_size(color_map)-first_color_index+1; 
  auto num_minor_levels=(num_colors-1)/specified_contour_levels.size();
  std::stringstream labels_ss;
  if (label_precision < 5) {
    labels_ss.setf(std::ios::fixed);
    labels_ss.precision(label_precision);
  }
  else {
    labels_ss.setf(std::ios::scientific);
    labels_ss.precision(3);
  }
  auto num_boxes=0;
  for (size_t n=0; n < specified_contour_levels.size()-1; ++n) {
    if (!labels_ss.str().empty()) {
	labels_ss << ",";
    }
    labels_ss << "\"" << specified_contour_levels[n] << "\"";
    ++num_boxes;
    for (size_t m=1; m <= num_minor_levels; ++m) {
	auto minor_diff=(specified_contour_levels[n+1]-specified_contour_levels[n])/(num_minor_levels+1);
	if (!labels_ss.str().empty()) {
	  labels_ss << ",";
	}
	labels_ss << "\"" << specified_contour_levels[n]+m*minor_diff << "\"";
	++num_boxes;
    }
  }
  labels_ss << ",\"" << specified_contour_levels.back() << "\"";
  ++num_boxes;
// pad the labels so the legend displays properly
  labels_ss << ",\"" << specified_contour_levels.back() << "\"";
  ++num_boxes;
  labels_ss << ",\"" << specified_contour_levels.back() << "\"";
  ++num_boxes;
  tdoc.add_replacement("__LABELS__",labels_ss.str());
  tdoc.add_replacement("__NUM_LABEL_BOXES__",strutils::itos(num_boxes));
  std::stringstream fill_colors_ss;
  for (size_t n=0; n < specified_contour_levels.size()-1; ++n) {
    if (!fill_colors_ss.str().empty()) {
	fill_colors_ss << ",";
    }
    fill_colors_ss << first_color_index++;
    for (size_t m=1; m <= num_minor_levels; ++m) {
	if (!fill_colors_ss.str().empty()) {
	  fill_colors_ss << ",";
	}
	fill_colors_ss << first_color_index++;
    }
  }
  fill_colors_ss << "," << first_color_index;
// pad the fill colors so the legend displays properly
  fill_colors_ss << "," << first_color_index;
  fill_colors_ss << "," << first_color_index;
  tdoc.add_replacement("__FILL_COLORS__",fill_colors_ss.str());
  tdoc.add_replacement("__LABEL_STRIDE__",strutils::itos(color_map_size(color_map)/specified_contour_levels.size()));
}

void set_legend_from_data(double data_min,double data_max,std::string color_map,size_t first_color_index,double central_value,TokenDocument& tdoc)
// set first_color_index to 1 if the first filled contour should be the
//   background color
{
  tdoc.add_if("__SPECIFIC_LABELS__");
  auto data_range=data_max-data_min;
  auto label_precision=2;
  while (label_precision < 5 && data_range < 1.) {
    ++label_precision;
    data_range*=10.;
  }
  std::vector<double> contour_levels;
  std::vector<short> contour_fill_colors,contour_dash_patterns;
  std::vector<unsigned char> contour_types;
  get_contours_from_data(data_min,data_max,color_map,first_color_index,central_value,contour_levels,contour_fill_colors,contour_types,contour_dash_patterns);
  tdoc.add_replacement("__NUM_LABEL_BOXES__",strutils::itos(contour_levels.size()));
  std::stringstream labels_ss;
  if (label_precision < 5) {
    labels_ss.setf(std::ios::fixed);
    labels_ss.precision(label_precision);
  }
  else {
    labels_ss.setf(std::ios::scientific);
    labels_ss.precision(3);
  }
  for (const auto& contour_level : contour_levels) {
    if (!labels_ss.str().empty()) {
	labels_ss << ",";
    }
    labels_ss << contour_level;
  }
  tdoc.add_replacement("__LABELS__",labels_ss.str());
  std::stringstream fill_colors_ss;
  for (const auto& fill_color : contour_fill_colors) {
    if (!fill_colors_ss.str().empty()) {
	fill_colors_ss << ",";
    }
    fill_colors_ss << fill_color;
  }
  tdoc.add_replacement("__FILL_COLORS__",fill_colors_ss.str());
  size_t num_major_contours=0;
  for (const auto& contour_type : contour_types) {
    if (contour_type == 0x1) {
	++num_major_contours;
    }
  }
  tdoc.add_replacement("__LABEL_STRIDE__",strutils::itos(color_map_size(color_map)/num_major_contours));
}

void output_legend(MapParameters& mp)
{
  auto tdir_name=make_work_directory();
  CacheData cdata;
  read_layer_cache(mp.layers.front(),mp.time,cdata);
  TokenDocument tdoc("/usr/local/www/server_root/web/html/wms/legend.ncl");
  tdoc.add_replacement("__OUTFILE__",tdir_name+"/outfile."+image_formats["image/png"]);
  std::unique_ptr<std::vector<double>> specified_contour_levels(nullptr);
  std::unique_ptr<std::vector<unsigned char>> specified_contour_level_types(nullptr);
  std::string color_map="NCV_bright";
  size_t first_color_index=2;
  double central_value=1.e18;
  auto lparts=strutils::split(mp.layers.front(),";");
  auto param=lparts[3].substr(lparts[3].find(":")+1);
  if (std::regex_search(param,std::regex("^0\\.0\\.[045]$"))) {
// GRIB2 temperature
    central_value=273.15;
  }
  else if (std::regex_search(param,std::regex("^0\\.1\\.8$"))) {
// GRIB2 total precipitation
    color_map="precip_new";
    specified_contour_levels.reset(new std::vector<double>(precipitation_contours()));
    specified_contour_level_types.reset(new std::vector<unsigned char>(precipitation_contour_types()));
  }
  else if (std::regex_search(param,std::regex("^0\\.1\\.0$"))) {
// GRIB2 specific humidity
    color_map="brown_green";
  }
  else if (std::regex_search(param,std::regex("^0\\.2\\.[23]$"))) {
// GRIB2 u- and v- wind components
    color_map="NCV_jaisnd";
    central_value=0.;
  }
  if (specified_contour_levels == nullptr) {
    set_legend_from_data(cdata.data_min,cdata.data_max,color_map,first_color_index,central_value,tdoc);
  }
  else {
    set_legend_from_specified_values(*specified_contour_levels,*specified_contour_level_types,color_map,first_color_index,tdoc);
  }
  tdoc.add_replacement("__COLOR_MAP__",color_map);
  TokenDocument colormap("/usr/local/www/server_root/web/html/wms/"+color_map+".colormap");
  colormap.add_replacement("__BGRED__","248");
  colormap.add_replacement("__BGGREEN__","248");
  colormap.add_replacement("__BGBLUE__","248");
  std::ofstream ofs((tdir_name+"/"+color_map+".rgb").c_str());
  if (!ofs.is_open()) {
    print_exception_report("There was an error writing the color map");
  }
  ofs << colormap << std::endl;
  ofs.close();
  xmlutils::ParameterMapper pmapper(metautils::directives.parameter_map_path);
  auto pparts=strutils::split(lparts[3],"!");
  auto title=pmapper.description(cdata.data_format,pparts.back());
  auto units=pmapper.units(cdata.data_format,pparts.back());
  if (!units.empty()) {
    title+=" ("+units+")";
  }
  tdoc.add_replacement("__TITLE__",title);
  ofs.open((tdir_name+"/outfile.ncl").c_str());
  if (!ofs.is_open()) {
    print_exception_report("There was an error writing the NCL file");
  }
  ofs << tdoc << std::endl;
  ofs.close();
  std::stringstream oss,ess;
  if (unixutils::mysystem2("/bin/tcsh -c \"source /etc/profile.d/ncarg.csh; setenv NCARG_COLORMAPS "+tdir_name+"; /usr/bin/ncl "+tdir_name+"/outfile.ncl\"",oss,ess) < 0 || std::regex_search(oss.str(),std::regex("fatal:"))) {
    print_exception_report("NCL error:\n-----\n"+oss.str()+"\n-----\n"+ess.str());
  }
  unixutils::mysystem2("/bin/tcsh -c \"/usr/bin/convert -trim +repage "+tdir_name+"/outfile.png -\"",oss,ess);
  std::cout << "Content-type: image/png" << std::endl << std::endl;
  std::cout << oss.str() << std::endl;
}

void show_capabilities(const QueryString& query_string)
{
  CapabilitiesParameters cp;
  fill_capabilities_parameters(query_string,cp);
  output_capabilities(query_string);
}

void get_map(const QueryString& query_string)
{
  MapParameters mp;
  fill_get_map_parameters(query_string,mp);
  output_map(mp);
}

void get_legend(std::string layer)
{
  if (layer.empty()) {
    print_exception_report("A value for the parameter 'LAYER' is required");
  }
  MapParameters mp;
  mp.layers.emplace_back(layer);
  output_legend(mp);
}

int main(int argc,char **argv)
{
// sets the effective user so that the service has the correct directory/file
//   access permissions
  setreuid(15968,15968);

// use the server name to determine whether this is a development or operational
//   request
  auto env=getenv("HTTP_HOST");
  if (env != nullptr && std::string(env) == "rda-web-dev.ucar.edu") {
    is_dev=true;
  }

// parse the query string from the request URL
  QueryString query_string;
  fill_querystring(query_string);

// read the configuration
  metautils::read_config("wms","","");

// set the directory that holds the WMS cache information for the specified
//   resource
  wms_directory="/usr/local/www/server_root/web/datasets/ds"+metautils::args.dsnum+"/metadata/wms/";

// handle the request
  if (request == "GetCapabilities") {
    show_capabilities(query_string);
  }
  else if (request == "GetMap") {
    get_map(query_string);
  }
  else if (std::regex_search(request,std::regex("^GetLegendGraphic"))) {
    get_legend(request.substr(request.find("<!>")+3));
  }
  else if (request == "GetFeatureInfo") {
    print_exception_report("GetFeatureInfo requests are not supported","OperationNotSupported","REQUEST");
  }
  else {
    print_exception_report("The value '"+request+"' is not a valid value for the 'REQUEST' parameter","InvalidParameterValue","REQUEST");
  }
}
