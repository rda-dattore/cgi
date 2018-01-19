load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"

begin

  infile = addfile("__INFILE__","r")
  vnames = getfilevarnames(infile)
  var = infile->$vnames(0)$(:,:)
#IF __LAT_LON__
  lat = infile->lat_0
  lon = infile->lon_0
#ENDIF __LAT_LON__
#IF __LAMBERT_CONFORMAL__
  lat = infile->gridlat_0
  lon = infile->gridlon_0
#ENDIF __LAMBERT_CONFORMAL__

  wks_type = "__NCL_IMAGE_TYPE__"
  wks_type@wkWidth = __WIDTH__
  wks_type@wkHeight = __HEIGHT__
  wks = gsn_open_wks(wks_type,"__OUTFILE__")
  gsn_define_colormap(wks,"__COLOR_MAP__")

  res = True

  res@mpDataBaseVersion = "Ncarg4_1"
  res@mpOutlineBoundarySets = "AllBoundaries"
  res@mpFillOn = False
#IF __LAT_LON__
  res@mpProjection = "CylindricalEquidistant"
  res@mpLimitMode = "LatLon"
  res@mpMinLatF = __MIN_LAT__
  res@mpMaxLatF = __MAX_LAT__
  res@mpMinLonF = __MIN_LON__
  res@mpMaxLonF = __MAX_LON__
#ENDIF __LAT_LON__
#IF __LAMBERT_CONFORMAL__
  res@mpProjection = "LambertConformal"
  res@mpLimitMode = "Corners"
  res@mpLambertParallel1F = lat@Latin1
  res@mpLambertParallel2F = lat@Latin2
  res@mpLambertMeridianF = lat@Lov
  res@mpLeftCornerLatF = lat@corners(0)
  res@mpLeftCornerLonF = lon@corners(0)
  res@mpRightCornerLatF = lat@corners(2)
  res@mpRightCornerLonF = lon@corners(2)
  res@tfDoNDCOverlay = True
#ENDIF __LAMBERT_CONFORMAL__

  res@mpShapeMode = "FreeAspect"
  res@vpXF = 0.
  res@vpYF = 1.
  if (wks_type@wkWidth .ge. wks_type@wkHeight) then
    res@vpWidthF = 1.
    res@vpHeightF = tofloat(wks_type@wkHeight)/tofloat(wks_type@wkWidth)
  else
    res@vpHeightF = 1.
    res@vpWidthF = tofloat(wks_type@wkWidth)/tofloat(wks_type@wkHeight)
  end if

  res@tmXBBorderOn = False
  res@tmXBOn = False
  res@tmXTBorderOn = False
  res@tmXTOn = False
  res@tmYLBorderOn = False
  res@tmYLOn = False
  res@tmYRBorderOn = False
  res@tmYROn = False

  res@cnFillOn = True
res@cnFillMode = "RasterFill"
res@cnRasterSmoothingOn = True
  res@cnLinesOn = False
  res@cnLineLabelsOn = False
  res@cnLevelSelectionMode = "ManualLevels"
  res@cnLevelSpacingF = __CONTOUR_INTERVAL__
;  res@cnMinLevelValF = __MIN_VAL__
;  res@cnMaxLevelValF = __MAX_VAL__
  res@cnInfoLabelOn = False

  res@lbLabelBarOn = False

  res@gsnLeftString = ""
  res@gsnRightString = ""

  plot = gsn_csm_contour_map(wks,var,res)

#IF __LAMBERT_CONFORMAL__
; transform Lambert-Conformal
  wks2 = gsn_open_wks(wks_type,"__OUTFILE__")
  gsn_define_colormap(wks2,"__COLOR_MAP__")

  numy = 1 ; make numx an integer
  numx = 1 ; make numy an integer
  d = dimsizes(lat)
  numy = d(0)
  numx = d(1)
  x = res@vpXF
  y = res@vpYF
  w = res@vpWidthF
  h = res@vpHeightF
  ndcx = fspan(x,x+w,numx)
  ndcy = fspan(y-h,y,numy)
; nudge the endpoints to ensure they are inside the boundary
  eps = 5e-7
  ndcx(0) = ndcx(0)+eps
  ndcx(numx-1) = ndcx(numx-1)-eps
  ndcy(0) = ndcy(0)+eps
  ndcy(numy-1) = ndcy(numy-1)-eps
  tndcy = new(numx,float)
  outlat = new((/numy,numx/),float)
  outlon = new((/numy,numx/),float)
  do i = 0,numy-1
    tndcy = ndcy(i)
    ndctodata(plot,ndcx,tndcy,outlon(i,:),outlat(i,:))
  end do

  res2 = res
  res2@mpProjection = "CylindricalEquidistant"
  res2@mpLimitMode = "Corners"
  res2@mpLeftCornerLatF = __MIN_LAT__
  res2@mpLeftCornerLonF = __MIN_LON__
  res2@mpRightCornerLatF = __MAX_LAT__
  res2@mpRightCornerLonF = __MAX_LON__

;  res2@trGridType = "TriangularMesh"
  res2@tfDoNDCOverlay = False

  res2@gsnAddCyclic = False
  res2@sfXArray = outlon
  res2@sfYArray = outlat

  res2@mpFillOn = True
  res2@mpMonoFillColor = True
  res2@mpFillColor = 2
  res2@mpFillBoundarySets = "AllBoundaries"

  plot = gsn_csm_contour_map(wks2,var,res2)
#ENDIF __LAMBERT_CONFORMAL__

end
