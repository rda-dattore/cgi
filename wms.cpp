#include <iostream>
#include <deque>
#include <unordered_map>
#include <regex>
#include <web/web.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <tokendoc.hpp>
#include <tempfile.hpp>
#include <xml.hpp>
#include <xmlutils.hpp>
#include <metadata.hpp>
#include <myerror.hpp>

metautils::Directives directives;
metautils::Args args;
std::string myerror="";
std::string mywarning="";

struct MapParameters {
  MapParameters() : version(),layers(),styles(),crs(),bbox(),width(),height(),format(),time(),transparent(false) {}

  std::string version;
  std::deque<std::string> layers,styles;
  std::string crs,bbox,width,height,format,time;
  bool transparent;
};

QueryString query_string;
std::string request,resource,dsnum2,data_file;
std::unordered_map<std::string,std::string> image_formats={ {"image/png","png"} };
//std::unordered_map<std::string,float> color_maps={ {"NCV_bright_new",254.}, {"precip_new",39.} };
//std::vector<std::string> coordinate_reference_systems={"CRS:84","EPSG:4326"};
std::unordered_map<std::string,double> coordinate_reference_systems={ {"EPSG:3786",6371007.} };

const double PI=3.14159265358979;
const double METERS_PER_DEGREE=2.*3.141592654*6371007./360.;

void print_exception_report(std::string exception_text,std::string exception_code = "",std::string locator = "")
{
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

void fill_querystring()
{
  query_string.fill(QueryString::GET);
  if (!query_string) {
    query_string.fill(QueryString::POST);
    if (!query_string) {
	print_exception_report("Missing query");
    }
  }
  auto service=strutils::to_lower(query_string.value("service"));
  if (service != "wms") {
    print_exception_report("The value for the parameter 'SERVICE' must be 'WMS'");
  }
  request=query_string.value("request");
  if (request.empty()) {
    print_exception_report("A value for the parameter 'REQUEST' is required");
  }
  auto rparts=strutils::split(query_string.value("resource"),"/");
  rparts.pop_front();
  if (rparts.front().substr(0,2) == "ds") {
    args.dsnum=rparts.front().substr(2);
    dsnum2=strutils::substitute(args.dsnum,".","");
    rparts.pop_front();
    data_file=rparts.front();
    rparts.pop_front();
    for (const auto& part : rparts) {
	data_file+="/"+part;
    }
    resource="/ds"+args.dsnum+"/metadata/wms/"+strutils::substitute(data_file,"/","%");
    auto idx=data_file.rfind(".");
    data_file=data_file.substr(0,idx);
  }
  else {
    print_exception_report("Bad request");
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

void show_capabilities()
{
//std::cerr << "show_capabilities()" << std::endl;
  auto version=query_string.value("version");
  struct stat buf;
  if (stat(("/usr/local/www/server_root/web/datasets/"+resource+".gz").c_str(),&buf) != 0) {
    print_exception_report("The resource was not found");
  }
  std::stringstream oss,ess;
  if (mysystem2("/bin/sh -c 'gunzip -c /usr/local/www/server_root/web/datasets/"+resource+".gz'",oss,ess) < 0) {
    print_exception_report("The resource is not accessible (1)");
  }
  TempFile tfile("/data/ptmp");
  std::ofstream ofs(tfile.name().c_str());
  if (!ofs.is_open()) {
    print_exception_report("The resource is not accessible (2)");
  }
  ofs << oss.str() << std::endl;
  ofs.close();
  TokenDocument *tdoc=new TokenDocument(tfile.name());
  std::stringstream tss;
  tss << *tdoc << std::endl;
  delete tdoc;
  XMLSnippet xmls("<Layers>"+tss.str()+"</Layers>");
  auto bbox_list=xmls.element_list("Layers/Layer/EX_GeographicBoundingBox");
  tss.str("");
  tdoc=new TokenDocument("/usr/local/www/server_root/web/html/wms/capabilities.xml");
  tdoc->add_replacement("__LAYERS__",oss.str());
  tss << *tdoc << std::endl;
  ofs.open(tfile.name().c_str());
  if (!ofs.is_open()) {
    print_exception_report("The resource is not accessible (3)");
  }
  ofs << tss.str() << std::endl;
  ofs.close();
  delete tdoc;
  tdoc=new TokenDocument(tfile.name());
  tdoc->add_replacement("__UPDATE_SEQUENCE__",current_date_time().to_string("%Y%m%d%H%MM"));
  std::string server_name;
  char *env;
  if ( (env=getenv("SERVER_NAME")) != nullptr) {
    server_name=env;
  }
  tdoc->add_replacement("__SERVICE_RESOURCE_GET_URL__","https://"+server_name+"/wms"+strutils::substitute(query_string.value("resource"),"%","%25"));
  for (const auto& format : image_formats) {
    tdoc->add_repeat("__IMAGE_FORMAT__",format.first);
  }
  const double TWO_PI=6.283185307179586;
  auto gcount=0;
  for (const auto& bbox : bbox_list) {
    auto wb=bbox.element("westBoundLongitude").content();
    auto eb=bbox.element("eastBoundLongitude").content();
    auto sb=bbox.element("southBoundLatitude").content();
    auto nb=bbox.element("northBoundLatitude").content();
    for (const auto& crs : coordinate_reference_systems) {
	auto wbr=strutils::dtos(std::stod(wb)*TWO_PI*crs.second/360.,4);
	auto ebr=strutils::dtos(std::stod(eb)*TWO_PI*crs.second/360.,4);
	auto sbr=strutils::dtos(std::stod(sb)*TWO_PI*crs.second/360.,4);
	auto nbr=strutils::dtos(std::stod(nb)*TWO_PI*crs.second/360.,4);
	tdoc->add_repeat("__CRS__"+strutils::itos(gcount)+"__","CRS[!]"+crs.first+"<!>"+wb+"[!]"+wbr+"<!>"+eb+"[!]"+ebr+"<!>"+sb+"[!]"+sbr+"<!>"+nb+"[!]"+nbr);
    }
    ++gcount;
  }
  std::cout << "Content-type: application/xml" << std::endl << std::endl;
  std::cout << *tdoc << std::endl;
}

size_t num_colors(std::string color_map)
{
  std::stringstream oss,ess;
  if (mysystem2("/bin/tcsh -c \"head -1 /usr/lib/ncarg/colormaps/"+color_map+".rgb |awk -F= '{print $2}'\"",oss,ess) < 0) {
    print_exception_report("There was an error reading the color map");
  }
  size_t num_colors=std::stoi(oss.str());
  if (num_colors > 254) {
    num_colors=254;
  }
  return num_colors;
}

void output_map(MapParameters& mp)
{
  metautils::read_config("wms","","");
  TempDir tdir;
  if (!tdir.create(directives.temp_path)) {
    print_exception_report("Error creating temporary directory");
  }
/*
tdir.setKeep();
std::cerr << tdir.name() << std::endl;
*/
  MySQL::Server server;
  if (!metautils::connect_to_metadata_server(server)) {
    print_exception_report("A database error occurred (1m)");
  }
  auto lparts=strutils::split(mp.layers.front(),";");
mp.time=lparts[4];
  auto qspec="select byte_offset,byte_length from IGrML.`ds"+dsnum2+"_inventory_"+lparts[3]+"` as i left join WGrML.ds"+dsnum2+"_webfiles as w on w.code = i.webID_code where gridDefinition_code = "+lparts[0]+" and timeRange_code = "+lparts[1]+" and level_code = "+lparts[2]+" and webID = '"+data_file+"'";
  if (mp.time.empty()) {
    qspec+=" limit 0,1";
  }
  else {
    qspec+=" and valid_date = "+mp.time;
  }
  MySQL::LocalQuery query(qspec);
  MySQL::Row row;
  long long byte_offset,byte_length;
  if (query.submit(server) == 0 && query.fetch_row(row)) {
    byte_offset=std::stoll(row[0]);
    byte_length=std::stoll(row[1]);
  }
  else {
    print_exception_report("A database query error occurred (2m)");
  }
  std::ifstream ifs(metautils::web_home()+"/"+data_file);
  std::ofstream ofs((tdir.name()+"/infile.grb2").c_str());
  if (!ifs.is_open() || !ofs.is_open()) {
    print_exception_report("The data file could not be opened");
  }
  ifs.seekg(byte_offset,std::ios::beg);
  std::unique_ptr<char []> buffer(new char[byte_length]);
  ifs.read(buffer.get(),byte_length);
  if (!ifs.good()) {
    print_exception_report("There was an error while reading the data file");
  }
  ifs.close();
  ofs.write(buffer.get(),byte_length);
  ofs.close();
  TokenDocument tdoc("/usr/local/www/server_root/web/html/wms/grib2.ncl");
  tdoc.add_replacement("__INFILE__",tdir.name()+"/infile.grb2");
  query.set("definition","WGrML.gridDefinitions","code = "+lparts[0]);
  if (query.submit(server) == 0 && query.fetch_row(row)) {
    if (row[0] == "lambertConformal") {
	tdoc.add_if("__LAMBERT_CONFORMAL__");
    }
    else if (row[0] == "latLon" || row[0] == "gaussLatLon") {
	tdoc.add_if("__LAT_LON__");
    }
  }
  tdoc.add_replacement("__NCL_IMAGE_TYPE__",image_formats[mp.format]);
  if (mp.width == mp.height) {
    tdoc.add_replacement("__WIDTH__",mp.width);
    tdoc.add_replacement("__HEIGHT__",mp.height);
  }
  else {
    auto width=std::stof(mp.width);
    auto height=std::stof(mp.height);
    if (width > height) {
	auto r=width/height;
	height=width;
	width*=r;
    }
    else {
	auto r=height/width;
	width=height;
	height*=r;
    }
    tdoc.add_replacement("__WIDTH__",strutils::itos(lround(width)));
    tdoc.add_replacement("__HEIGHT__",strutils::itos(lround(height)));
  }
  tdoc.add_replacement("__OUTFILE__",tdir.name()+"/outfile."+image_formats[mp.format]);
  auto bbox_parts=strutils::split(mp.bbox,",");
  tdoc.add_replacement("__MIN_LON__",strutils::dtos(std::stod(bbox_parts[0])/METERS_PER_DEGREE,4));
  tdoc.add_replacement("__MIN_LAT__",strutils::dtos(std::stod(bbox_parts[1])/METERS_PER_DEGREE,4));
  tdoc.add_replacement("__MAX_LON__",strutils::dtos(std::stod(bbox_parts[2])/METERS_PER_DEGREE,4));
  tdoc.add_replacement("__MAX_LAT__",strutils::dtos(std::stod(bbox_parts[3])/METERS_PER_DEGREE,4));
  GRIB2Message msg;
  msg.fill(reinterpret_cast<unsigned char *>(buffer.get()),false);
  auto g=msg.grid(0);
  std::string color_map;
  if (myequalf(g->statistics().min_val,0.)) {
    color_map="precip_new";
    tdoc.add_replacement("__CONTOUR_INTERVAL__",strutils::ftos((g->statistics().max_val-g->statistics().min_val)/(num_colors(color_map)-1.),4));
  }
  else {
    color_map="NCV_bright_new";
    tdoc.add_replacement("__CONTOUR_INTERVAL__",strutils::ftos((g->statistics().max_val-g->statistics().min_val)/num_colors(color_map),4));
  }
  tdoc.add_replacement("__COLOR_MAP__",color_map);
  tdoc.add_replacement("__MAX_VAL__",strutils::ftos(g->statistics().max_val,4));
  tdoc.add_replacement("__MIN_VAL__",strutils::ftos(g->statistics().min_val,4));
  ofs.open((tdir.name()+"/outfile.ncl").c_str());
  if (!ofs.is_open()) {
    print_exception_report("There was an error writing the NCL file");
  }
  ofs << tdoc << std::endl;
  ofs.close();
  std::stringstream oss,ess;
  if (mysystem2("/bin/tcsh -c \"source /etc/profile.d/ncarg.csh; /usr/bin/ncl "+tdir.name()+"/outfile.ncl\"",oss,ess) < 0 || std::regex_search(oss.str(),std::regex("fatal:"))) {
    print_exception_report("NCL error:\n-----\n"+oss.str()+"\n-----\n"+ess.str());
  }
  if (mp.transparent) {
    mysystem2("/bin/tcsh -c \"/usr/bin/convert -trim +repage "+tdir.name()+"/outfile."+image_formats[mp.format]+" "+tdir.name()+"/outfile2."+image_formats[mp.format]+"; /usr/bin/mogrify -crop "+mp.width+"x"+mp.height+"+0+0! -transparent 'rgb(248,248,248)' "+tdir.name()+"/outfile2."+image_formats[mp.format]+"; /usr/bin/convert "+tdir.name()+"/outfile2."+image_formats[mp.format]+" -alpha set -background none -channel A -evaluate multiply 0.5 +channel -\"",oss,ess);
  }
  else {
    mysystem2("/bin/tcsh -c \"/usr/bin/convert -trim +repage "+tdir.name()+"/outfile."+image_formats[mp.format]+" "+tdir.name()+"/outfile2."+image_formats[mp.format]+"; /usr/bin/mogrify -crop "+mp.width+"x"+mp.height+"+0+0! -\"",oss,ess);
  }
  std::cout << "Content-type: " << mp.format << std::endl << std::endl;
  std::cout << oss.str() << std::endl;
}

void get_map()
{
  MapParameters mp;
  mp.version=query_string.value("version");
  if (mp.version.empty()) {
    print_exception_report("A value for the parameter 'VERSION' is required");
  }
  auto l=query_string.value("layers");
  mp.layers=strutils::split(l,",");
  if (mp.layers.size() > 1) {
    print_exception_report("Only one layer may be requested");
  }
  else if (mp.layers.size() == 0) {
    print_exception_report("A value for the parameter 'LAYERS' is required");
  }
  auto s=query_string.value("styles");
  mp.styles=strutils::split(s,",");
  if (mp.styles.size() != mp.layers.size()) {
    if (mp.styles.size() == 0 && mp.layers.size() == 1 && query_string.has_value("styles")) {
	mp.styles.emplace_back("default");
    }
    else {
	print_exception_report("One style must be requested for each layer");
    }
  }
  mp.crs=query_string.value("crs");
  if (mp.crs.empty()) {
    print_exception_report("A value for the parameter 'CRS' is required");
  }
  mp.bbox=query_string.value("bbox");
  if (mp.bbox.empty()) {
    print_exception_report("A value for the parameter 'BBOX' is required");
  }
  mp.width=query_string.value("width");
  if (mp.width.empty()) {
    print_exception_report("A value for the parameter 'WIDTH' is required");
  }
  mp.height=query_string.value("height");
  if (mp.height.empty()) {
    print_exception_report("A value for the parameter 'HEIGHT' is required");
  }
  mp.format=query_string.value("format");
  if (mp.format.empty()) {
    print_exception_report("A value for the parameter 'FORMAT' is required");
  }
  if (image_formats.find(strutils::to_lower(mp.format)) == image_formats.end()) {
    print_exception_report("The image format '"+mp.format+"' is not available from this server","InvalidFormat");
  }
  mp.format=strutils::to_lower(mp.format);
  mp.time=query_string.value("time");
  if (!mp.time.empty()) {
    strutils::replace_all(mp.time,"-","");
    strutils::replace_all(mp.time,"T","");
    strutils::replace_all(mp.time,":","");
    mp.time.pop_back();
  }
  auto transparent=strutils::to_lower(query_string.value("transparent"));
  if (transparent == "true") {
    mp.transparent=true;
  }
  output_map(mp);
}

void output_legend(std::string layer)
{
  metautils::read_config("wms","","");
  TempDir tdir;
  if (!tdir.create(directives.temp_path)) {
    print_exception_report("Error creating temporary directory");
  }
/*
tdir.setKeep();
std::cerr << tdir.name() << std::endl;
*/
  MySQL::Server server;
  if (!metautils::connect_to_metadata_server(server)) {
    print_exception_report("A database error occurred (1l)");
  }
  auto lparts=strutils::split(layer,";");
auto time=lparts[4];
  auto qspec="select byte_offset,byte_length from IGrML.`ds"+dsnum2+"_inventory_"+lparts[3]+"` as i left join WGrML.ds"+dsnum2+"_webfiles as w on w.code = i.webID_code where gridDefinition_code = "+lparts[0]+" and timeRange_code = "+lparts[1]+" and level_code = "+lparts[2]+" and webID = '"+data_file+"'";
  if (time.empty()) {
    qspec+=" limit 0,1";
  }
  else {
    qspec+=" and valid_date = "+time;
  }
  MySQL::LocalQuery query(qspec);
  MySQL::Row row;
  long long byte_offset,byte_length;
  if (query.submit(server) == 0 && query.fetch_row(row)) {
    byte_offset=std::stoll(row[0]);
    byte_length=std::stoll(row[1]);
  }
  else {
    print_exception_report("A database query error occurred (2l)");
  }
  std::ifstream ifs(metautils::web_home()+"/"+data_file);
  if (!ifs.is_open()) {
    print_exception_report("The data file could not be opened");
  }
  ifs.seekg(byte_offset,std::ios::beg);
  std::unique_ptr<char []> buffer(new char[byte_length]);
  ifs.read(buffer.get(),byte_length);
  if (!ifs.good()) {
    print_exception_report("There was an error while reading the data file");
  }
  ifs.close();
  auto pparts=strutils::split(lparts[3],"!");
  query.set("format","WGrML.formats","code = "+pparts.front());
  if (query.submit(server) == 0 && query.fetch_row(row)) {
    double max_val,min_val;
    if (row[0] == "WMO_GRIB2") {
	GRIB2Message msg;
	msg.quick_fill(reinterpret_cast<unsigned char *>(buffer.get()));
	auto g=msg.grid(0);
	max_val=g->statistics().max_val;
	min_val=g->statistics().min_val;
    }
    else {
	print_exception_report("Unsupported data format");
    }
    TokenDocument tdoc("/usr/local/www/server_root/web/html/wms/legend.ncl");
    tdoc.add_replacement("__OUTFILE__",tdir.name()+"/outfile."+image_formats["image/png"]);
    std::string color_map;
    if (myequalf(min_val,0.)) {
	color_map="precip_new";
	tdoc.add_replacement("__CONTOUR_INTERVAL__",strutils::ftos((max_val-min_val)/(num_colors(color_map)-1.),4));
    }
    else {
	color_map="NCV_bright_new";
	tdoc.add_replacement("__CONTOUR_INTERVAL__",strutils::ftos((max_val-min_val)/num_colors(color_map),4));
    }
    tdoc.add_replacement("__COLOR_MAP__",color_map);
    tdoc.add_replacement("__MIN_VAL__",strutils::ftos(min_val,4));
    xmlutils::ParameterMapper pmapper;
    auto title=pmapper.description(row[0],pparts.back());
    auto units=pmapper.units(row[0],pparts.back());
    if (!units.empty()) {
	title+=" ("+units+")";
    }
    tdoc.add_replacement("__TITLE__",title);
    std::ofstream ofs((tdir.name()+"/outfile.ncl").c_str());
    if (!ofs.is_open()) {
	print_exception_report("There was an error writing the NCL file");
    }
    ofs << tdoc << std::endl;
    ofs.close();
    std::stringstream oss,ess;
    if (mysystem2("/bin/tcsh -c \"source /etc/profile.d/ncarg.csh; /usr/bin/ncl "+tdir.name()+"/outfile.ncl\"",oss,ess) < 0 || std::regex_search(oss.str(),std::regex("fatal:"))) {
	print_exception_report("NCL error:\n-----\n"+oss.str()+"\n-----\n"+ess.str());
    }
    mysystem2("/bin/tcsh -c \"/usr/bin/convert -trim +repage "+tdir.name()+"/outfile.png -\"",oss,ess);
    std::cout << "Content-type: image/png" << std::endl << std::endl;
    std::cout << oss.str() << std::endl;
  }
  else {
    print_exception_report("A database query error occurred (3l)");
  }
}

void get_legend()
{
  auto layer=query_string.value("layer");
  if (layer.size() == 0) {
    print_exception_report("A value for the parameter 'LAYER' is required");
  }
  output_legend(layer);
}

int main(int argc,char **argv)
{
  fill_querystring();
  auto lrequest=strutils::to_lower(request);
  if (lrequest == "getcapabilities") {
    show_capabilities();
  }
  else if (lrequest == "getmap") {
    get_map();
  }
  else if (lrequest == "getlegendgraphic") {
    get_legend();
  }
  else if (lrequest == "getfeatureinfo") {
    print_exception_report("GetFeatureInfo requests are not supported","OperationNotSupported");
  }
  else {
    print_exception_report("The value '"+request+"' is not a valid value for the 'REQUEST' parameter");
  }
}
