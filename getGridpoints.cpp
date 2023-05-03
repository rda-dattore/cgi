#include <iostream>
#include <regex>
#include <web/web.hpp>
#include <MySQL.hpp>
#include <grid.hpp>
#include <gridutils.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <mymap.hpp>

using gridutils::fill_gaussian_latitudes;
using gridutils::fill_lat_lons_from_lambert_conformal_gridpoints;
using gridutils::fix_grid_definition;
using std::cout;
using std::endl;
using std::stod;
using std::stof;
using std::stoi;
using std::string;
using std::stringstream;
using std::vector;
using strutils::ftos;
using strutils::split;
using floatutils::myequalf;

int main(int argc, char **argv) {
  QueryString queryString(QueryString::GET);
  auto db = queryString.value("db");
  if (db.empty()) {
    web_error("No database specified.");
  }
  auto code = queryString.value("code");
  if (code.empty()) {
    web_error("No grid definition specified.");
  }
  auto sp = split(code, ",");
  code = sp[0];
  float wlon, elon, slat, nlat;
  auto v = queryString.value("wlon");
  if (!v.empty()) {
    wlon = stof(queryString.value("wlon"));
    slat = stof(queryString.value("slat"));
    elon = stof(queryString.value("elon"));
    nlat = stof(queryString.value("nlat"));
  } else {
    wlon = stof(queryString.value("lon"));
    elon = wlon;
    slat = stof(queryString.value("lat"));
    nlat = slat;
  }
  MySQL::Server server("rda-db.ucar.edu", "metadata", "metadata", "");
  if (!server) {
    web_error("Unable to connect to database.");
  }
  MySQL::Query query;
  query.set("definition, def_params", db + ".grid_definitions", "code = " +
      code);
  MySQL::Row row;
  if (query.submit(server) < 0 || !query.fetch_row(row)) {
    web_error("Database error.");
  }
  cout.setf(std::ios::fixed);
  cout.precision(6);
  auto def_params = split(row[1], ":");
  Grid::GridDimensions dim;
  dim.x = stoi(def_params[0]);
  dim.y = stoi(def_params[1]);
  Grid::GridDefinition def;
  def.slatitude = stof(def_params[2].substr(0, def_params[2].length() - 1));
  if (def_params[2].back() == 'S') {
    def.slatitude = -def.slatitude;
  }
  def.slongitude = stof(def_params[3].substr(0, def_params[3].length() - 1));
  if (def_params[3].back() == 'W') {
    def.slongitude = -def.slongitude;
  }
  if (row[0] == "latLon") {
    cout << "Content-type: text/plain" << endl << endl;
    def.elatitude = stof(def_params[4].substr(0, def_params[4].length() - 1));
    if (def_params[4].back() == 'S') {
      def.elatitude = -def.elatitude;
    }
    def.elongitude=stof(def_params[5].substr(0, def_params[5].length() - 1));
    if (def_params[5].back() == 'W') {
      def.elongitude = -def.elongitude;
    }
    auto crosses_greenwich = false;
    if (def.slongitude >= 0. && (def.elongitude - def.slongitude) > 180.) {
      if (myequalf(wlon, -180.) && myequalf(elon, 180.)) {
        wlon = 0;
        elon = 360.;
      } else {
        if (wlon < 0. && elon >= 0.) {
          crosses_greenwich = true;
        }
        if (wlon < 0) {
          wlon += 360.;
        }
        if (elon < 0.) {
          elon += 360.;
        }
      }
    }
    def.loincrement = stof(def_params[6]);
    def.laincrement = stof(def_params[7]);
    if (def.slatitude > def.elatitude) {
      def.laincrement = -def.laincrement;
    }
    def = fix_grid_definition(def, dim);
    cout << "{x: " << dim.x << ", y: " << dim.y << ", locdata: [";
    if (wlon != elon) {
      auto cnt = 0;
      stringstream locdata;
      for (int n = 0; n < dim.y; ++n) {
        auto lat=def.slatitude+n*def.laincrement;
        for (int m = 0; m < dim.x; ++m) {
          auto lon = def.slongitude + m * def.loincrement;
          if (lat >= slat && lat <= nlat && ((crosses_greenwich && (lon >= wlon
              || lon <= elon)) || (lon >= wlon && lon <= elon))) {
            if (cnt > 0) {
              locdata << ",";
            }
            locdata << "[" << n << "," << m << "," << lat << "," << lon << "]";
            ++cnt;
          }
          if (cnt > 400) {
            n = dim.y;
          }
        }
      }
      if (cnt <= 400) {
        cout << locdata.str();
      }
    } else {
      for (int n = 1; n < dim.y; ++n) {
        auto lat = def.slatitude + n * def.laincrement;
        auto lat_1 = lat - def.laincrement;
        for (int m = 1; m < dim.x; ++m) {
          if (slat >= lat && slat <= lat_1) {
            auto lon = def.slongitude + m * def.loincrement;
            auto lon_1 = lon - def.loincrement;
            if (wlon >= lon_1 && wlon <= lon) {
              cout << "[" << n-1 << "," << m-1 << "," << lat_1 << "," << lon_1
                  << "],[" << n-1 << "," << m << "," << lat_1 << "," << lon <<
                  "],[" << n << "," << m-1 << "," << lat << "," << lon_1 <<
                  "],[" << n << "," << m << "," << lat << "," << lon << "]";
              m = dim.x;
              n = dim.y;
            }
          }
        }
      }
    }
    cout << "]}" << endl;
  } else if (row[0] == "gaussLatLon") {
    cout << "Content-type: text/plain" << endl << endl;
    def.elatitude = stof(def_params[4].substr(0, def_params[4].length() - 1));
    if (def_params[4].back() == 'S') {
      def.elatitude = -def.elatitude;
    }
    def.elongitude = stof(def_params[5].substr(0, def_params[5].length() - 1));
    if (def_params[5].back() == 'W') {
      def.elongitude = -def.elongitude;
    }
    auto crosses_greenwich = false;
    if (def.slongitude >= 0. && (def.elongitude - def.slongitude) > 180.) {
      if (myequalf(wlon, -180.) && myequalf(elon, 180.)) {
        wlon = 0;
        elon = 360.;
      } else {
        if (wlon < 0. && elon >= 0.) {
          crosses_greenwich = true;
        }
        if (wlon < 0) {
          wlon += 360.;
        }
        if (elon < 0.) {
          elon += 360.;
        }
      }
    }
    def.loincrement = stof(def_params[6]);
    def.num_circles = stoi(def_params[7]);
    def = fix_grid_definition(def, dim);
    cout << "{x: " << dim.x << ", y: " << dim.y << ", locdata: [";
    my::map<Grid::GLatEntry> gaus_lats;
    if (fill_gaussian_latitudes("/glade/u/home/rdadata/share/GRIB", gaus_lats,
        def.num_circles, (def.slatitude > def.elatitude))) {
      Grid::GLatEntry glat_entry;
      gaus_lats.found(def.num_circles, glat_entry);
      if (wlon != elon) {
        auto cnt = 0;
        stringstream locdata;
        size_t end = def.num_circles * 2;
        for (size_t n = 0; n < end; ++n) {
          for (int m = 0; m < dim.x; ++m) {
            auto lon = def.slongitude + m * def.loincrement;
            if (glat_entry.lats[n] >= slat && glat_entry.lats[n] <= nlat &&
                ((crosses_greenwich && (lon >= wlon || lon <= elon)) || (lon >=
                wlon && lon <= elon))) {
              if (cnt > 0) {
                locdata << ",";
              }
              locdata << "[" << n << "," << m << "," << glat_entry.lats[n] <<
                  "," << lon << "]";
              ++cnt;
            }
            if (cnt > 400) {
              n = end;
            }
          }
        }
        if (cnt <= 400) {
          cout << locdata.str();
        }
      } else {
        for (int n = 1; n < static_cast<int>(def.num_circles*2); ++n) {
          for (int m = 1; m < dim.x; ++m) {
            if (slat >= glat_entry.lats[n] && slat <= glat_entry.lats[n - 1]) {
              auto lon = def.slongitude + m * def.loincrement;
              auto lon_1 = lon - def.loincrement;
              if (wlon >= lon_1 && wlon <= lon) {
                cout << "[" << n - 1 << "," << m - 1 << "," << glat_entry.lats[
                    n - 1] << "," << lon_1 << "],[" << n - 1 << "," << m << ","
                    << glat_entry.lats[n - 1] << "," << lon << "],[" << n << ","
                    << m - 1 << "," << glat_entry.lats[n] << "," << lon_1 <<
                    "],[" << n << "," << m << "," << glat_entry.lats[n] << ","
                    << lon << "]";
                m = dim.x;
                n = def.num_circles * 2;
              }
            }
          }
        }
      }
    }
    cout << "]}" << endl;
  } else if (row[0] == "lambertConformal") {
    auto elon1 = def.slongitude;
    if (elon1 < 0.) {
      elon1 += 360.;
    }
    auto elonv = stod(def_params[5]);
    if (elonv < 0.) {
      elonv += 360.;
    }
    auto tanlat = stof(def_params[4].substr(0, def_params[4].length() - 1));
    if (def_params[4].back() == 'S') {
      tanlat = -tanlat;
    }
    vector<float> lats, elons;
    fill_lat_lons_from_lambert_conformal_gridpoints(dim.x, dim.y, def.slatitude,
        elon1, stod(def_params[7]), elonv, tanlat, lats, elons);
    cout << "Content-type: text/plain" << endl << endl;
    cout << "{x: " << dim.x << ", y: " << dim.y << ", locdata: [";
    if (wlon < 0) {
      wlon += 360.;
    }
    if (elon < 0) {
      elon += 360.;
    }
    if (wlon != elon) {
      auto cnt = 0;
      stringstream locdata;
      for (size_t n = 0; n < lats.size(); ++n) {
        if (lats[n] >= slat && lats[n] <= nlat && elons[n] >= wlon && elons[n]
            <= elon) {
          if (cnt > 0) {
            locdata << ",";
          }
          locdata << "[" << (n/dim.x) << "," << (n % dim.x) << "," << lats[n] <<
              "," << elons[n] << "]";
          ++cnt;
          if (cnt > 400) {
            n = lats.size();
          }
        }
      }
      if (cnt <= 400) {
        cout << locdata.str();
      }
    } else {
      for (int n = 1; n < dim.y; ++n) {
        for (int m = 1; m < dim.x; ++m) {
          auto nn = n * dim.x;
          auto ur = nn + m;
          auto ul = ur - 1;
          nn -= dim.x;
          auto lr = nn + m;
          auto ll = lr - 1;
          auto min_lat = (lats[ll] < lats[lr]) ? lats[ll] : lats[lr];
          auto max_lat = (lats[ul] > lats[ur]) ? lats[ul] : lats[ur];
          auto min_lon = (elons[ll] < elons[ul]) ? elons[ll] : elons[ul];
          auto max_lon = (elons[ul] > elons[ur]) ? elons[ul] : elons[ur];
          if (slat >= min_lat && slat <= max_lat && wlon >= min_lon && wlon <=
              max_lon) {
            if (within_polygon("POLYGON((" + ftos(lats[ll], 4) + " " + ftos(
                elons[ll], 4) + ", " + ftos(lats[ul], 4) + " " + ftos(elons[ul],
                4) + ", " + ftos(lats[ur], 4) + " " + ftos(elons[ur], 4) + ", "
                + ftos(lats[lr], 4) + " " + ftos(elons[lr], 4) + ", " + ftos(
                lats[ll], 4) + " " + ftos(elons[ll], 4) + "))", "POINT(" + ftos(
                slat, 4) + " " + ftos(wlon, 4) + ")")) {
              cout << "[" << n-1 << "," << m-1 << "," << lats[ll] << "," <<
                  elons[ll] << "],[" << n-1 << "," << m << "," << lats[lr] <<
                  "," << elons[lr] << "],[" << n << "," << m-1 << "," << lats[
                  ul] << "," << elons[ul] << "],[" << n << "," << m << "," <<
                  lats[ur] << "," << elons[ur] << "]";
              n = dim.y;
              m = dim.x;
            }
          }
        }
      }
    }
    cout << "]}" << endl;
  } else {
    web_error("Unsupported grid - '" + row[0] + "'.");
  }
}
