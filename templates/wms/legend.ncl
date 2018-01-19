load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"

begin

  wks_type = "png"
  wks = gsn_open_wks(wks_type,"__OUTFILE__")
  gsn_define_colormap(wks,"__COLOR_MAP__")
  colors = read_colormap_file("__COLOR_MAP__");
  dims = dimsizes(colors)
  nboxes = dims(0)
  if (nboxes .gt. 254) then
    nboxes = 254
  end if
  cint = __CONTOUR_INTERVAL__
  fill_colors = ispan(2,nboxes+1,1);
  vals = fspan(__MIN_VAL__,__MIN_VAL__+cint*(nboxes-1),dimsizes(fill_colors))
  lblabels = new(dimsizes(fill_colors),string)
  do i=0,dimsizes(fill_colors)-1
    lblabels(i)=sprintf("%.2f",vals(i))
  end do

  lbres = True
  lbres@lbAutoManage = False
  lbres@vpWidthF = 0.25
  lbres@vpHeightF = 0.05
  lbres@lbRightMarginF = 0.085
  lbres@lbOrientation = "Horizontal"
  lbres@lbFillColors = fill_colors
  lbres@lbPerimOn = True
  lbres@lbBoxLinesOn = False
  lbres@lbMonoFillPattern = True
  lbres@lbLabelAlignment = "InteriorEdges"
  lbres@lbLabelFontHeightF = 0.006
  lbres@lbLabelAngleF = 30.
  lbres@lbLabelOffsetF = 0.02
  lbres@lbTitleString = "__TITLE__"
  lbres@lbTitlePosition = "Bottom"
  lbres@lbTitleDirection = "Across"
  lbres@lbTitleAngleF = 0.
  lbres@lbTitleFontHeightF = 0.0075
  lbres@lbTitleOffsetF = 0.

  gsn_labelbar_ndc(wks,nboxes,lblabels,0.01,0.99,lbres)
  frame(wks)

end
