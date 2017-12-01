#include <iostream>
#include <fstream>
#include <regex>
#include <sstream>
#include <tempfile.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <web/web.hpp>
#include <datetime.hpp>
#include <bsort.hpp>
#include <tokendoc.hpp>

struct Args {
  Args() : storm_number(),season(),ocean(),action(),resolution(),plot_type() {}

  std::string storm_number,season,ocean;
  std::string action,resolution,plot_type;
} args;
float minlat=90.,minlon=360.,maxlat=-90.,maxlon=-360.;
short colidx[]={0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},nidx=0;
std::string txt[]={"","","","","","","","","Hurricane - Cat 5","Hurricane - Cat 4","Hurricane - Cat 3","Hurricane - Cat 2","Hurricane - Cat 1","Tropical Storm","Tropical Depression","Extratropical/Remnant Low","Subtropical Storm"};
std::string server_root="/"+strutils::token(host_name(),".",0);
std::string doc_root=getenv("DOCUMENT_ROOT");
std::string data_root="/glade/p/rda/data";

void parse_query()
{
  QueryString query_string(QueryString::GET);
  if (!query_string) {
    query_string.fill(QueryString::POST);
  }
  args.storm_number=query_string.value("s");
  args.season=query_string.value("y");
  args.ocean=query_string.value("o");
  args.action=query_string.value("a");
  args.resolution=query_string.value("r");
  args.plot_type=query_string.value("p");
}

void add_intensity_color(char intensity_char,std::string& string,int wind_speed)
{
  if (intensity_char == 'S') {
    string+=",16";
    if (colidx[16] == 0) {
	++nidx;
    }
    colidx[16]=1;
  }
  else if (intensity_char == 'E' || intensity_char == 'L' || intensity_char == 'W') {
    string+=",15";
    if (colidx[15] == 0) {
	++nidx;
    }
    colidx[15]=1;
  }
  else {
    if (wind_speed < 35) {
	string+=",14";
	if (colidx[14] == 0) {
	  ++nidx;
	}
	colidx[14]=1;
    }
    else if (wind_speed < 65) {
	string+=",13";
	if (colidx[13] == 0) {
	  ++nidx;
	}
	colidx[13]=1;
    }
    else if (wind_speed < 85) {
	string+=",12";
	if (colidx[12] == 0) {
	  ++nidx;
	}
	colidx[12]=1;
    }
    else if (wind_speed < 100) {
	string+=",11";
	if (colidx[11] == 0) {
	  ++nidx;
	}
	colidx[11]=1;
    }
    else if (wind_speed < 115) {
	string+=",10";
	if (colidx[10] == 0) {
	  ++nidx;
	}
	colidx[10]=1;
    }
    else if (wind_speed < 140) {
	string+=",9";
	if (colidx[9] == 0) {
	  ++nidx;
	}
	colidx[9]=1;
    }
    else {
	string+=",8";
	if (colidx[8] == 0) {
	  ++nidx;
	}
	colidx[8]=1;
    }
  }
}

void start()
{
  std::cout << "Content-type: text/html" << std::endl << std::endl;
  std::cout << "<script id=\"tcplot_script\" language=\"javascript\">" << std::endl;
  std::cout << "function initializePage() {" << std::endl;
  std::cout << "  document.selections.o.selectedIndex=0;" << std::endl;
  std::cout << "}" << std::endl;
  std::cout << "function submitForm() {" << std::endl;
  std::cout << "  document.selections.submit_button.disabled=true;" << std::endl;
  std::cout << "  document.selections.submit_button.value='Plotting...';" << std::endl;
  std::cout << "  return true;" << std::endl;
  std::cout << "}" << std::endl;
  std::cout << "function resetSeason() {" << std::endl;
  std::cout << "  document.selections.y.selectedIndex=0;" << std::endl;
  std::cout << "}" << std::endl;
  std::cout << "function getSeason() {" << std::endl;
  std::cout << "  if (document.selections.o.selectedIndex > 0) {" << std::endl;
  std::cout << "    getContent('season','/cgi-bin/datasets/tcplot?a=season&o='+document.selections.o[document.selections.o.selectedIndex].value,null,resetSeason);" << std::endl;
  std::cout << "  }" << std::endl;
  std::cout << "}" << std::endl;
  std::cout << "function resetStormNumber() {" << std::endl;
  std::cout << "  document.selections.s.selectedIndex=0;" << std::endl;
  std::cout << "}" << std::endl;
  std::cout << "function getStormNumber() {" << std::endl;
  std::cout << "  if (document.selections.y.selectedIndex > 0)" << std::endl;
  std::cout << "    getContent('stormnum','/cgi-bin/datasets/tcplot?a=stormnum&o='+document.selections.o[document.selections.o.selectedIndex].value+'&y='+document.selections.y[document.selections.y.selectedIndex].value,null,resetStormNumber);" << std::endl;
  std::cout << "}" << std::endl;
  std::cout << "function resetPlotType() {" << std::endl;
  std::cout << "  if (document.selections && document.selections.p)" << std::endl;
  std::cout << "    document.selections.p.selectedIndex=0;" << std::endl;
  std::cout << "}" << std::endl;
  std::cout << "function getPlotType() {" << std::endl;
  std::cout << "  if (document.selections.s.selectedIndex > 0)" << std::endl;
  std::cout << "    getContent('plottype','/cgi-bin/datasets/tcplot?a=plottype&o='+document.selections.o[document.selections.o.selectedIndex].value+'&y='+document.selections.y[document.selections.y.selectedIndex].value+'&s='+document.selections.s[document.selections.s.selectedIndex].value,null,resetPlotType);" << std::endl;
  std::cout << "}" << std::endl;
  std::cout << "if (typeof registerAjaxCallback == \"function\")" << std::endl;
  std::cout << "  registerAjaxCallback('initializePage');" << std::endl;
  std::cout << "</script>" << std::endl;
  std::cout << "<center><span style=\"font-size: 22px; font-weight: bold\">Tropical Cyclone Track Plotting</span></center>" << std::endl;
  std::cout << "<center><form name=\"selections\" action=\"/cgi-bin/datasets/tcplot\" method=\"post\" onSubmit=\"return submitForm()\">" << std::endl;
  std::cout << "<br /><div id=\"ocean\">Choose an ocean basin: <select name=\"o\" onChange=\"getSeason()\"><option value=\"\" selected>choose one</option><option value=\"n_atl\">North Atlantic</option><option value=\"e_npac\">Eastern N. Pacific</option><option value=\"w_npac\">Western N. Pacific</option></select></div>" << std::endl;
  std::cout << "<div id=\"season\"></div>" << std::endl;
  std::cout << "<div id=\"stormnum\"></div>" << std::endl;
  std::cout << "<div id=\"plottype\"></div>" << std::endl;
  std::cout << "</form></center>" << std::endl;
}

bool compare_seasons(std::string& left,std::string& right)
{
  if (left >= right) {
    return true;
  }
  else {
    return false;
  }
}

void get_season()
{
  std::cout << "Content-type: text/html" << std::endl << std::endl;
  std::ifstream ifs((doc_root+"/datasets/ds824.1/inventories/"+args.ocean+".inv").c_str());
  if (ifs.is_open()) {
    std::cout << "Choose a season: <select name=\"y\" onChange=\"getStormNumber()\"><option value=\"\" selected>choose one</option>";
    char line[32768];
    ifs.getline(line,32768);
    std::vector<std::string> array;
    while (!ifs.eof()) {
	std::string sline=line;
	if (std::regex_search(sline,std::regex("^SEASON:"))) {
	  array.emplace_back(sline.substr(8,4));
	}
	ifs.getline(line,32768);
    }
    ifs.close();
    binary_sort(array,compare_seasons);
    for (const auto& season : array) {
	std::cout << "<option value=\"" << season << "\">" << season << "</option>";
    }
    std::cout << "</select>" << std::endl;
  }
}

void get_storm_number()
{
  std::cout << "Content-type: text/html" << std::endl << std::endl;
  std::ifstream ifs((doc_root+"/datasets/ds824.1/inventories/"+args.ocean+".inv").c_str());
  if (ifs.is_open()) {
    std::cout << "Choose a storm: <select name=\"s\" onChange=\"getPlotType()\")\"><option value=\"\" selected>choose one</option>";
    char line[32768];
    ifs.getline(line,32768);
    while (!ifs.eof()) {
	std::string sline=line;
	if (std::regex_search(sline,std::regex("^SEASON: "+args.season))) {
	  ifs.getline(line,32768);
	  sline=line;
	  while (!std::regex_search(sline,std::regex("^SEASON:")) && !std::regex_search(sline,std::regex("^LEGEND:"))) {
	    if (!sline.empty() && sline[3] != ' ') {
		auto number=sline.substr(0,4);
		strutils::trim(number);
		std::cout << "<option value=\"" << number << "\">" << sline.substr(6,16) << "</option>";
	    }
	    ifs.getline(line,32768);
	    sline=line;
	  }
	  break;
	}
	ifs.getline(line,32768);
    }
    ifs.close();
    std::cout << "</select>" << std::endl;
  }
}

void get_plot_type()
{
  std::vector<std::string> fcsts{"_f12","_f24","_f36","_f48","_f72","_f96","_f120"};
  strutils::replace_all(args.storm_number,"**","");
  std::cout << "Content-type: text/html" << std::endl << std::endl;
  std::cout << "Plot Type: <select name=\"p\"><option value=\"single\" selected>Track w/intensities</option>";
  for (const auto& fcst : fcsts) {
    auto found=false;
    std::ifstream ifs((doc_root+"/datasets/ds824.1/inventories/"+args.ocean+fcst+".inv").c_str());
    if (ifs.is_open()) {
	char line[32768];
	ifs.getline(line,32768);
	while (!ifs.eof()) {
	  std::string sline=line;
	  if (std::regex_search(sline,std::regex("^SEASON: "+args.season))) {
	    ifs.getline(line,32768);
	    sline=line;
	    while (!sline.empty()) {
		if (sline.substr(4-args.storm_number.length(),args.storm_number.length()) == args.storm_number) {
		  found=true;
		  break;
		}
		ifs.getline(line,32768);
		sline=line;
	    }
	    if (found) {
		break;
	    }
	  }
	  ifs.getline(line,32768);
	}
	ifs.close();
	if (found) {
	  std::cout << "<option value=\"vsfcst" << fcst.substr(2) << "\">Track vs. " << fcst.substr(2) << "-hour forecast</option>";
	}
    }
  }
  std::cout << "</select><br />Resolution: <select name=\"r\"><option value=\"2048x2048\" selected>High (2048x2048)</option><option value=\"1280x1280\">Medium (1280x1280)</option><option value=\"768x768\">Low (768x768)</option></select><br><br /><input type=\"submit\" name=\"submit_button\" value=\"Make the Plot\"><br />The plot will appear on the next screen.  Use your browser's <i>Back</i> button to return to this screen." << std::endl;
}

void add_forecast(std::string storm_name,std::string& fcst_string,int fcst_hr,DateTime& last_date)
{
  DateTime date(1000,1,1,0,0);
  std::ifstream ifs((data_root+"/ds824.1/"+args.ocean+"_f"+strutils::itos(fcst_hr)+".dat").c_str());
  if (ifs.is_open()) {
    storm_name=strutils::to_upper(storm_name);
    char line[32768];
    ifs.getline(line,32768);
    while (!ifs.eof()) {
	std::string sline=line;
	if (sline.substr(12,4) == args.season && sline.substr(35,storm_name.length()) == storm_name) {
	  auto nhist=std::stoi(sline.substr(19,2));
	  auto yr=std::stoi(sline.substr(12,4));
	  float latm;
	  if (sline[64] == 'N') {
	    latm=1.;
	  }
	  else {
	    latm=-1.;
	  }
	  float lonm;
	  if (sline[70] == 'E') {
	    lonm=1.;
	  }
	  else {
	    lonm=-1.;
	  }
	  for (auto n=0; n < nhist; ++n) {
	    ifs.getline(line,32768);
	    sline=line;
	    auto mo=std::stoi(sline.substr(6,2));
	    if (mo == 1 && date.month() == 12) {
		++yr;
	    }
	    auto dy=std::stoi(sline.substr(9,2));
	    auto off=11;
	    for (auto m=0; m < 4; ++m) {
		if (sline[off] != ' ') {
		  auto hr=m*6+3;
		  date.set(yr,mo,dy,hr*10000);
		  if (date > last_date && date.hours_since(last_date) == fcst_hr) {
		    auto val=std::stof(sline.substr(off+1,3))/10.*latm;
		    if (val < minlat) {
			minlat=val;
		    }
		    if (val > maxlat) {
			maxlat=val;
		    }
		    if (std::regex_search(fcst_string,std::regex("\\)$"))) {
			fcst_string+=",";
		    }
		    fcst_string+=" (/"+strutils::ftos(val,4,3,' ');
		    val=std::stof(sline.substr(off+4,4))/10.*lonm;
		    if (val < minlon) {
			minlon=val;
		    }
		    if (val > maxlon) {
			maxlon=val;
		    }
		    fcst_string+=","+strutils::ftos(val,4,3,' ');
		    add_intensity_color(sline[off],fcst_string,std::stoi(sline.substr(off+8,4)));
		    fcst_string+="/)";
		  }
		}
		off+=17;
	    }
	  }
	  break;
	}
	ifs.getline(line,32768);
    }
    ifs.close();
  }
}

void plot_image(std::string tfile_name,std::string image_name)
{
  std::stringstream oss,ess;
  if (mysystem2("/bin/tcsh -c \"source /etc/profile.d/ncarg.csh; /usr/bin/ncl < "+tfile_name+"\"",oss,ess) < 0 || std::regex_search(oss.str(),std::regex("fatal:"))) {
    std::cout << "Content-type: text/plain" << std::endl << std::endl;
    std::cout << "-----" << std::endl;
    std::cout << oss.str() << std::endl;
    std::cout << "-----" << std::endl;
    std::cout << ess.str() << std::endl;
    std::cout << "-----" << std::endl;
  }
  else {
    if (mysystem2("/bin/tcsh -c \"source /etc/profile.d/ncarg.csh; /usr/bin/ctrans -d sun -res "+args.resolution+" "+server_root+"/tmp/"+image_name+".ncgm |convert sun:- gif:-; rm "+server_root+"/tmp/"+image_name+".ncgm\"",oss,ess) < 0) {
	std::cout << "Content-type: text/plain" << std::endl << std::endl;
	std::cout << ess.str() << std::endl;
    }
    else {
	std::cout << "Content-type: image/gif" << std::endl << std::endl;
	std::cout << oss.str() << std::endl;
    }
  }
}

void plot_single()
{
  strutils::replace_all(args.storm_number,"**","");
  TokenDocument token_doc("/usr/local/www/server_root/web/html/tcplot/plot_single.ncl");
  token_doc.add_replacement("__SERVER_ROOT__",server_root);
  auto img=strutils::strand(12);
  token_doc.add_replacement("__IMAGE_NAME__",img);
  std::ifstream ifs((data_root+"/ds824.1/"+args.ocean+".dat").c_str());
  if (!ifs.is_open()) {
    web_error("unable to open data file");
  }
  DateTime first_date(1000,1,1,0,0),last_date(1000,1,1,0,0);
  std::string storm_name,xaxis_string;
  size_t cnt=0;
  auto started=false;
  char line[32768];
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    std::string sline=line;
    if (sline.substr(12,4) == args.season && sline.substr(24-args.storm_number.length(),args.storm_number.length()) == args.storm_number) {
	storm_name=sline.substr(35,12);
	strutils::trim(storm_name);
	float latm;
	if (sline[64] == 'N') {
	  latm=1.;
	}
	else {
	  latm=-1.;
	}
	float lonm;
	if (sline[70] == 'E') {
	  lonm=1.;
	}
	else {
	  lonm=-1.;
	}
	std::string point_string="  p=(/";
	size_t nhist=std::stoi(sline.substr(19,2));
	auto yr=std::stoi(sline.substr(12,4));
	for (size_t n=0; n < nhist; ++n) {
	  ifs.getline(line,32768);
	  sline=line;
	  auto off=11;
	  for (size_t m=0; m < 4; ++m) {
	    if (sline[off] != ' ') {
		auto mo=std::stoi(sline.substr(6,2));
		if (mo == 1 && last_date.month() == 12) {
		  ++yr;
		}
		auto dy=std::stoi(sline.substr(9,2));
		if (sline[5] == 'P') {
		  last_date.set(yr,mo,dy,(m*6+3)*10000);
		}
		else {
		  last_date.set(yr,mo,dy,m*60000);
		}
		if (first_date.year() == 1000) {
		  first_date=last_date;
		  if (sline[5] == 'P') {
		    xaxis_string="Preliminary 6-hourly positions (03Z, 09Z, 15Z, and 21Z)";
		  }
		  else {
		    xaxis_string="Final best track positions (00Z, 06Z, 12Z, and 18Z)";
		  }
		}
		if (started) {
		  point_string+=(",");
		}
		started=true;
		auto val=std::stof(sline.substr(off+1,3))/10.*latm;
		if (val < minlat) {
		  minlat=val;
		}
		if (val > maxlat) {
		  maxlat=val;
		}
		point_string+=" (/"+strutils::ftos(val,4,3,' ');
		val=std::stof(sline.substr(off+4,4))/10.*lonm;
		if (val < minlon) {
		  minlon=val;
		}
		if (val > maxlon) {
		  maxlon=val;
		}
		point_string+=","+strutils::ftos(val,4,3,' ');
		add_intensity_color(sline[off],point_string,std::stoi(sline.substr(off+8,4)));
		point_string+="/)";
		++cnt;
	    }
	    off+=17;
	  }
	}
	if (cnt < 2) {
	  point_string+=","+point_string.substr(6);
	}
	point_string+=" /)";
	token_doc.add_replacement("__POINT_STRING__",point_string);
	break;
    }
    ifs.getline(line,32768);
  }
  ifs.close();
  if (last_date.year() == 1000) {
    std::cout << "Content-type: text/plain" << std::endl << std::endl;
    std::cout << "Storm/season combination not found" << std::endl;
    exit(1);
  }
  std::string fcst_string;
  auto current_date=current_date_time();
  current_date=current_date.hours_added(-current_date.utc_offset()/100);
  if (current_date.hours_since(last_date) < 24) {
    add_forecast(storm_name,fcst_string,12,last_date);
    add_forecast(storm_name,fcst_string,24,last_date);
    add_forecast(storm_name,fcst_string,36,last_date);
    add_forecast(storm_name,fcst_string,48,last_date);
    add_forecast(storm_name,fcst_string,72,last_date);
    add_forecast(storm_name,fcst_string,96,last_date);
    add_forecast(storm_name,fcst_string,120,last_date);
    if (fcst_string.empty()) {
	token_doc.add_if("__HAS_NO_FORECASTS__");
    }
    else {
	token_doc.add_if("__HAS_FORECASTS__");
	if (strutils::occurs(fcst_string,",") < 4) {
	  fcst_string+=", "+fcst_string;
	}
	token_doc.add_replacement("__FCST_STRING__",fcst_string);
	++nidx;
    }
  }
  else {
    token_doc.add_if("__HAS_NO_FORECASTS__");
  }
  while ((maxlat-minlat)/(maxlon-minlon) < 0.895) {
    minlat-=0.1;
    maxlat+=0.1;
  }
  while ((maxlat-minlat)/(maxlon-minlon) > 0.895) {
    minlon-=0.1;
    maxlon+=0.1;
  }
  token_doc.add_replacement("__MIN_LAT__",strutils::ftos(minlat-10.,5,1,' '));
  token_doc.add_replacement("__MAX_LAT__",strutils::ftos(maxlat+10.,5,1,' '));
  token_doc.add_replacement("__MIN_LON__",strutils::ftos(minlon-10.,6,1,' '));
  token_doc.add_replacement("__MAX_LON__",strutils::ftos(maxlon+10.,6,1,' '));
  token_doc.add_replacement("__STORM_NAME__",storm_name);
  token_doc.add_replacement("__SEASON__",args.season);
  token_doc.add_replacement("__STORM_NUMBER__",args.storm_number);
  token_doc.add_replacement("__XAXIS_STRING__",xaxis_string);
  if (args.resolution == "2048x2048") {
    token_doc.add_if("__HIGH_RESOLUTION__");
  }
  else if (args.resolution == "1280x1280") {
    token_doc.add_if("__MEDIUM_RESOLUTION__");
  }
  else {
    token_doc.add_if("__LOW_RESOLUTION__");
  }
  auto line_space=90./(100.-(maxlat-minlat));
  auto y=minlat-9.5+nidx*line_space;
  for (size_t n=0; n <= 16; ++n) {
    if (colidx[n] == 1) {
	token_doc.add_repeat("__LEGEND_ENTRY__","MARKER_COLOR[!]"+strutils::itos(n)+"<!>Y[!]"+strutils::ftos(y,4)+"<!>TEXT[!]"+txt[n]);
	y-=line_space;
    }
  }
  if (!fcst_string.empty()) {
    token_doc.add_replacement("__Y__",strutils::ftos(y,4));
  }
  token_doc.add_replacement("__FIRST_DATE__",first_date.to_string("%m/%d %HZ"));
  token_doc.add_replacement("__LAST_DATE__",last_date.to_string("%m/%d %HZ"));
  TempFile tfile(server_root+"/tmp");
  std::ofstream ofs(tfile.name());
  if (!ofs.is_open()) {
    web_error("unable to create output");
  }
  ofs << token_doc << std::endl;
  ofs.close();
  plot_image(tfile.name(),img);
}

void plot_vs_forecast()
{
  TokenDocument token_doc("/usr/local/www/server_root/web/html/tcplot/plot_vs_forecast.ncl");
  auto img=strutils::strand(12);
  token_doc.add_replacement("__SERVER_ROOT__",server_root);
  token_doc.add_replacement("__IMAGE_NAME__",img);
  bool is_best;
  if (std::regex_search(args.storm_number,std::regex("^\\*\\*"))) {
    args.storm_number=args.storm_number.substr(2);
    is_best=false;
  }
  else {
    is_best=true;
  }
// get forecast positions
  auto fcst_hr=strutils::substitute(args.plot_type,"vsfcst","");
  std::ifstream ifs((data_root+"/ds824.1/"+args.ocean+"_f"+fcst_hr+".dat").c_str());
  if (!ifs.is_open()) {
    web_error("unable to open data file");
  }
  DateTime first_date(1000,1,1,0,0),last_date(1000,1,1,0,0);
  DateTime firstf_date(1000,1,1,0,0),lastf_date(1000,1,1,0,0);
  std::string storm_name;
  char line[32768];
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    std::string sline=line;
    if (sline.substr(12,4) == args.season && sline.substr(24-args.storm_number.length(),args.storm_number.length()) == args.storm_number) {
        auto yr=std::stoi(sline.substr(12,4));
	size_t nhist=std::stoi(sline.substr(19,2));
	storm_name=sline.substr(35,12);
	strutils::trim(storm_name);
	float latm;
	if (sline[64] == 'N') {
	  latm=1.;
	}
	else {
	  latm=-1.;
	}
	float lonm;
	if (sline[70] == 'E') {
	  lonm=1.;
	}
	else {
	  lonm=-1.;
	}
	std::string fpoint_string="  f=(/";
	auto firstf=-1,cnt=0;
	auto started=false;
	for (size_t n=0; n < nhist; ++n) {
	  ifs.getline(line,32768);
	  sline=line;
	  auto off=11;
	  for (size_t m=0; m < 4; ++m) {
	    if (sline[off] != ' ') {
		if (started) {
		  fpoint_string+=",";
		}
		started=true;
		auto lastf=(n*4+m);
		if (firstf < 0) {
		  firstf=lastf;
		}
		auto mo=std::stoi(sline.substr(6,2));
		if (mo == 1 && last_date.month() == 12) {
		  ++yr;
		}
		auto dy=std::stoi(sline.substr(9,2));
		lastf_date.set(yr,mo,dy,m*60000);
		if (firstf_date.year() == 1000) {
		  firstf_date=lastf_date;
		}
		auto val=std::stof(sline.substr(off+1,3))/10.*latm;
		if (val < minlat) {
		  minlat=val;
		}
		if (val > maxlat) {
		  maxlat=val;
		}
		fpoint_string+=" (/"+strutils::ftos(val,4,3,' ');
		val=std::stof(sline.substr(off+4,4))/10.*lonm;
		if (val < minlon) {
		  minlon=val;
		}
		if (val > maxlon) {
		  maxlon=val;
		}
		fpoint_string+=","+strutils::ftos(val,4,3,' ');
		add_intensity_color(sline[off],fpoint_string,std::stoi(sline.substr(off+8,4)));
		fpoint_string+="/)";
		++cnt;
	    }
	    off+=17;
	  }
	}
	if (cnt < 2) {
	  fpoint_string+=", (/-999,-999,-1/)";
	}
	fpoint_string+=" /)";
	token_doc.add_replacement("__FPOINT_STRING__",fpoint_string);
	break;
    }
    ifs.getline(line,32768);
  }
  if (ifs.eof()) {
    std::cout << "Content-type: text/html" << std::endl << std::endl;
    std::cout << "<html><body><p>Sorry, " << fcst_hr << "-hour forecast data for " << storm_name << " not available</p><p><br><a href=\"http://dss.ucar.edu/cgi-bin/datasets/tcplot\">New Plot</a>&nbsp;&nbsp;&nbsp;<a href=\"javascript:parent.close()\">Close Window</a></p></body></html>" << std::endl;
    exit(1);
  }
  ifs.close();
// get actual track
  if (is_best) {
    ifs.open((data_root+"/ds824.1/"+args.ocean+".dat").c_str());
  }
  else {
    ifs.open((data_root+"/ds824.1/"+args.ocean+"_f00.dat").c_str());
  }
  auto first_fpoint=999999;
  ifs.getline(line,32768);
  while (!ifs.eof()) {
    std::string sline=line;
    if (sline.substr(12,4) == args.season && sline.substr(24-args.storm_number.length(),args.storm_number.length()) == args.storm_number) {
        auto yr=std::stoi(sline.substr(12,4));
	size_t nhist=std::stoi(sline.substr(19,2));
	storm_name=sline.substr(35,12);
	strutils::trim(storm_name);
	float latm;
	if (sline[64] == 'N') {
	  latm=1.;
	}
	else {
	  latm=-1.;
	}
	float lonm;
	if (sline[70] == 'E') {
	  lonm=1.;
	}
	else {
	  lonm=-1.;
	}
	std::string point_string="  p=(/";
	auto first=-1,cnt=0;
	auto started=false;
	for (size_t n=0; n < nhist; ++n) {
	  ifs.getline(line,32768);
	  sline=line;
	  auto off=11;
	  for (size_t m=0; m < 4; ++m) {
	    if (sline[off] != ' ') {
		if (started) {
		  point_string+=",";
		}
		started=true;
		auto last=(n*4+m);
		if (first < 0) {
		  first=last;
		}
		auto mo=std::stoi(sline.substr(6,2));
		if (mo == 1 && last_date.month() == 12) {
		  ++yr;
		}
		auto dy=std::stoi(sline.substr(9,2));
		last_date.set(yr,mo,dy,m*60000);
		if (last_date == firstf_date) {
		  first_fpoint=cnt;
		}
		++cnt;
		if (first_date.year() == 1000) {
		  first_date=last_date;
		}
		auto val=std::stof(sline.substr(off+1,3))/10.*latm;
		if (val < minlat) {
		  minlat=val;
		}
		if (val > maxlat) {
		  maxlat=val;
		}
		point_string+=" (/"+strutils::ftos(val,4,3,' ');
		val=std::stof(sline.substr(off+4,4))/10.*lonm;
		if (val < minlon) {
		  minlon=val;
		}
		if (val > maxlon) {
		  maxlon=val;
		}
		point_string+=","+strutils::ftos(val,4,3,' ');
		if (is_best) {
		  add_intensity_color(sline[off],point_string,std::stoi(sline.substr(off+8,4)));
		}
		else {
		  point_string+=",1";
		}
		point_string+="/)";
	    }
	    off+=17;
	  }
	}
	point_string+=" /)";
	token_doc.add_replacement("__POINT_STRING__",point_string);
	break;
    }
    ifs.getline(line,32768);
  }
  if (ifs.eof()) {
    std::cout << "Content-type: text/html" << std::endl << std::endl;
    std::cout << "<html><body><p>Sorry, data for " << storm_name << " not available</p><p><br><a href=\"http://dss.ucar.edu/cgi-bin/datasets/tcplot\">New Plot</a>&nbsp;&nbsp;&nbsp;<a href=\"javascript:parent.close()\">Close Window</a></p></body></html>" << std::endl;
    exit(1);
  }
  ifs.close();
  token_doc.add_replacement("__FIRST_FPOINT__",strutils::itos(first_fpoint));
  while ((maxlat-minlat)/(maxlon-minlon) < 0.895) {
    minlat-=0.1;
    maxlat+=0.1;
  }
  while ((maxlat-minlat)/(maxlon-minlon) > 0.895) {
    minlon-=0.1;
    maxlon+=0.1;
  }
  token_doc.add_replacement("__MIN_LAT__",strutils::ftos(minlat-10.,5,1,' '));
  token_doc.add_replacement("__MAX_LAT__",strutils::ftos(maxlat+10.,5,1,' '));
  token_doc.add_replacement("__MIN_LON__",strutils::ftos(minlon-10.,6,1,' '));
  token_doc.add_replacement("__MAX_LON__",strutils::ftos(maxlon+10.,6,1,' '));
  token_doc.add_replacement("__STORM_NAME__",storm_name);
  token_doc.add_replacement("__SEASON__",args.season);
  token_doc.add_replacement("__STORM_NUMBER__",args.storm_number);
  token_doc.add_replacement("__FCST_HR__",fcst_hr);
  if (args.resolution == "2048x2048") {
    token_doc.add_if("__HIGH_RESOLUTION__");
  }
  else if (args.resolution == "1280x1280") {
    token_doc.add_if("__MEDIUM_RESOLUTION__");
  }
  else {
    token_doc.add_if("__LOW_RESOLUTION__");
  }
  token_doc.add_replacement("__FIRST_DATE__",first_date.to_string("%m/%d %HZ"));
  token_doc.add_replacement("__LAST_DATE__",last_date.to_string("%m/%d %HZ"));
  token_doc.add_replacement("__FIRSTF_DATE__",firstf_date.to_string("%m/%d %HZ"));
  token_doc.add_replacement("__LASTF_DATE__",lastf_date.to_string("%m/%d %HZ"));
  if (nidx > 0) {
    token_doc.add_if("__HAS_LEGEND__");
    auto line_space=90./(100.-(maxlat-minlat));
    float y;
    if (is_best) {
	y=minlat-6.5+nidx*line_space;
	token_doc.add_replacement("__LEGEND_MARKER_SIZE__","0.006");
	token_doc.add_replacement("__LEGEND_MARKER_INDEX__","6");
    }
    else {
	y=minlat-8.+nidx*line_space;
	token_doc.add_replacement("__LEGEND_MARKER_SIZE__","0.008");
	token_doc.add_replacement("__LEGEND_MARKER_INDEX__","5");
    }
    for (size_t n=0; n <= 16; ++n) {
	if (colidx[n] == 1) {
	  token_doc.add_repeat("__LEGEND__","MARKER_COLOR[!]"+strutils::itos(n)+"<!>Y[!]"+strutils::ftos(y,4)+"<!>TEXT[!]"+txt[n]);
	  y-=line_space;
	}
    }
    if (is_best) {
	token_doc.add_if("__IS_BEST__");
	token_doc.add_replacement("__Y__",strutils::ftos(y,4));
    }
  }
  TempFile tfile(server_root+"/tmp");
  std::ofstream ofs(tfile.name());
  if (!ofs.is_open()) {
    web_error("unable to create output");
  }
  ofs << token_doc << std::endl;
  ofs.close();
  plot_image(tfile.name(),img);
}

int main(int argc,char **argv)
{
  parse_query();
  if (!args.ocean.empty() && !args.season.empty() && !args.storm_number.empty() && !args.plot_type.empty()) {
    if (args.plot_type == "single") {
	plot_single();
    }
    else if (std::regex_search(args.plot_type,std::regex("^vsfcst"))) {
	plot_vs_forecast();
    }
  }
  else if (args.action == "season") {
    get_season();
  }
  else if (args.action == "stormnum") {
    get_storm_number();
  }
  else if (args.action == "plottype") {
    get_plot_type();
  }
  else {
    start();
  }
}
