On AWS, we have fix at:
export BUCKET_URI=s3://noaa-nagape-none-ca-epic

/bucket/global-workflow-shared-data/fix

srcdir=${BUCKET_URI}/global-workflow-shared-data/fix

tardir=/local/fix

for dir in aer/20220805 \
	am/20220805 \
	chem/20220805/fimdata_chem \
	chem/20220805/Emission_data \
	cice/20240416/500 \
	cpl/20230526/aC48o500 \
	datm/20220805/cfsr \
	datm/20220805/gefs \
	datm/20220805/gfs \
	datm/20220805/mom6/100 \
	glwu/20220805 \
	gsi/20240208 \
	lut/20220805 \
	mom6/20240416/500 \
	orog/20240917/C48 \
	raw/orog \
	reg2grb2/20220805 \
	sfc_climo/20230925 \
	ugwd/20240624/C48 \
	verif/20220805 \
	wave/20240105
do
  fullsrcdir=${srcdir}/${dir}
  fulltardir=${tardir}/${dir}

 #Get directory
  directoryname=$(dirname "${fulltardir}")
 #Get localdirname
  localdirname=$(basename "${fulltardir}")
 #Print results
 #echo "fulltardir Path: ${fulltardir}"
 #echo "Directory: ${directoryname}"
 #echo "Filename: ${localdirname}"

  echo "mkdir -p ${directoryname}"
  echo "cd ${directoryname}"
  echo "aws s3 sync ${srcdir}/${dir} ${localdirname}"
done

