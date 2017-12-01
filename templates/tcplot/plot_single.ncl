load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
begin
  wks = gsn_open_wks("ncgm","__SERVER_ROOT__/tmp/__IMAGE_NAME__")
  cmap=(/ (/1.,1.,1./), (/0.,0.,0./), (/.55294,.513725,.443137/), (/0.,.48627,.63922/), (/.4,.4,.4/), (/.65,.65,.65/), (/0.1,0.1,0.1/), (/.87,.87,.87/), (/0.3,0.,0./), (/0.6,0.,0./), (/0.9,0.,0./), (/1.,0.3,0./), (/1.,0.6,0./), (/1.,1.,0./), (/0.,1.,0./), (/0.,0.8,1./), (/0.,0.,1./) /)
  gsn_define_colormap(wks,cmap)
__POINT_STRING__
  dsizes=dimsizes(p)
  numpts=dsizes(0)
#IF __HAS_FORECASTS__
  f=(/ __FCST_STRING__  /)
  dsizes=dimsizes(f)
  numfpts=dsizes(0)
#ENDIF __HAS_FORECASTS__
  minlat=__MIN_LAT__
  maxlat=__MAX_LAT__
  minlon=__MIN_LON__
  maxlon=__MAX_LON__
  mpres = True
  mpres@mpCenterLonF = (minlon+maxlon)/2.
  mpres@gsnFrame = False
  mpres@gsnMaximize = True
  mpres@mpProjection = "CylindricalEquidistant"
  mpres@mpLimitMode = "LatLon"
  mpres@mpOutlineOn = True
  mpres@mpOutlineBoundarySets = "AllBoundaries"
  mpres@mpNationalLineColor = 4
  mpres@mpGeophysicalLineColor = 2
  mpres@mpGeophysicalLineThicknessF = 0.
  mpres@mpMinLatF   = minlat
  mpres@mpMaxLatF   = maxlat
  mpres@mpMinLonF   = minlon
  mpres@mpMaxLonF   = maxlon
  mpres@mpFillColors=(/0,3,2,3/)
  mpres@mpPerimOn = False
  mpres@mpLabelsOn = False
  mpres@mpGridAndLimbOn = True
;  mpres@mpGridMaskMode = "MaskNotOcean"
  mpres@mpGridLineColor = 5
  mpres@mpGridSpacingF = 5.
  mpres@tmBorderLineColor = 0
  mpres@tmXBOn = False
  mpres@tmXTOn = False
  mpres@tmYLOn = False
  mpres@tmYROn = False
  mpres@tiMainString="__STORM_NAME__ - __SEASON__ (#__STORM_NUMBER__)"
  mpres@tiMainFont="helvetica-bold"
  mpres@tiMainFontHeightF=0.015
  mpres@tiXAxisString="__XAXIS_STRING__"
  mpres@tiXAxisFont="helvetica"
  mpres@tiXAxisFontHeightF=0.009
  map = gsn_csm_map(wks,mpres)
  tres=True
  tres@txFont="helvetica"
  tres@txFontHeightF=0.012
  tres@txFontColor=7
  tres@txJust="CenterCenter"
  do i=floattoint(minlon)+1,floattoint(maxlon)-1
    if ( (i % 5) .eq. 0 .and. (i-minlon) .gt. 2 .and. (maxlon-i) .gt. 2) then
      if (i .ge. 0) then
        gsn_text(wks,map,sprinti("%dE",i),i,maxlat-1.5,tres)
      else
        gsn_text(wks,map,sprinti("%dW",-i),i,maxlat-1.5,tres)
      end if
    end if
  end do
  do i=floattoint(minlat)+1,floattoint(maxlat)-1
    if ( (i % 5) .eq. 0 .and. (i-minlat) .gt. 2 .and. (maxlat-i) .gt. 2) then
      if (i .ge. 0) then
        gsn_text(wks,map,sprinti("%dN",i),maxlon-2.,i,tres)
      else
        gsn_text(wks,map,sprinti("%dS",-i),maxlon-2.,i,tres)
      end if
    end if
  end do
  lres = True
  lres@gsLineColor = 1
  lres@gsLineDashPattern = 0
  mres = True
#IF __HIGH_RESOLUTION__
  lres@gsLineThicknessF = 2.
  mres@gsMarkerThicknessF = 4.
#ENDIF __HIGH_RESOLUTION__
#IF __MEDIUM_RESOLUTION__
  lres@gsLineThicknessF = 1.
  mres@gsMarkerThicknessF = 2.
#ENDIF __MEDIUM_RESOLUTION__
#IF __LOW_RESOLUTION__
  lres@gsLineThicknessF = 0.5
  mres@gsMarkerThicknessF = 1.
#ENDIF __LOW_RESOLUTION__
  gsn_polyline(wks,map,p(:,1),p(:,0),lres)
  mres@gsMarkerSizeF = 0.006
  mres@gsMarkerIndex = 6
  do i=0,numpts-1
    mres@gsMarkerColor = floattoint(p(i,2))
    gsn_polymarker(wks,map,p(i,1),p(i,0),mres)
  end do
#IF __HAS_FORECASTS__
  lres@gsLineDashPattern = 2
  gsn_polyline(wks,map,f(:,1),f(:,0),lres)
  gsn_polyline(wks,map,(/ p(numpts-1,1),f(0,1) /),(/ p(numpts-1,0),f(0,0) /),lres)
  mres@gsMarkerSizeF = 0.008
  mres@gsMarkerIndex = 5
  do i=0,numfpts-1
    mres@gsMarkerColor = floattoint(f(i,2))
    gsn_polymarker(wks,map,f(i,1),f(i,0),mres)
  end do
#ENDIF __HAS_FORECASTS__
  tres@txJust="CenterLeft"
  tres@txFontColor=6
  mres@gsMarkerSizeF = 0.006
  mres@gsMarkerIndex = 6
#REPEAT __LEGEND_ENTRY__
  mres@gsMarkerColor = __LEGEND_ENTRY__.MARKER_COLOR
  gsn_polymarker(wks,map,minlon+1.,__LEGEND_ENTRY__.Y,mres)
  gsn_text(wks,map,"__LEGEND_ENTRY__.TEXT",minlon+2.,__LEGEND_ENTRY__.Y,tres)
#ENDREPEAT __LEGEND_ENTRY__
#IF __HAS_FORECASTS__
  mres@gsMarkerColor = 1
  mres@gsMarkerSizeF = 0.008
  mres@gsMarkerIndex = 5
  gsn_polymarker(wks,map,minlon+1.,__Y__,mres)
  gsn_text(wks,map,"Forecast Position",minlon+2.,__Y__,tres)
#ENDIF __HAS_FORECASTS__
  lres@gsLineColor = 0
  lres@gsLineDashPattern = 0
  tres@txFontColor=0
  gsn_text(wks,map,"__FIRST_DATE__",p(0,1)+0.95,p(0,0)-1.55,tres)
  gsn_polyline(wks,map,(/ p(0,1),p(0,1)+0.75 /),(/ p(0,0),p(0,0)-1.45 /),lres)
#IF __HAS_FORECASTS__
  if (f(0,1) .gt. p(numpts-1,1)) then
#ENDIF __HAS_FORECASTS__
#IF __HAS_NO_FORECASTS__
  if (p(numpts-1,1) .lt. p(numpts-2,1)) then
#ENDIF __HAS_NO_FORECASTS__
    tres@txJust="CenterRight"
    gsn_text(wks,map,"__LAST_DATE__",p(numpts-1,1)-0.95,p(numpts-1,0)+1.55,tres)
    gsn_polyline(wks,map,(/ p(numpts-1,1),p(numpts-1,1)-0.75 /),(/ p(numpts-1,0),p(numpts-1,0)+1.45 /),lres)
  else
    gsn_text(wks,map,"__LAST_DATE__",p(numpts-1,1)+0.95,p(numpts-1,0)+1.55,tres)
    gsn_polyline(wks,map,(/ p(numpts-1,1),p(numpts-1,1)+0.75 /),(/ p(numpts-1,0),p(numpts-1,0)+1.45 /),lres)
  end if
end
