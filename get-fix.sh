#!/bin/bash
#On AWS, we have fix at:
#aws --no-sign-request s3 ls  s3://noaa-nws-global-pds/fix

srcdir=s3://noaa-nws-global-pds/fix
tardir=/contrib/global-workflow-shared-data/fix.subset

set -x

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
	orog/20231027/C48 \
	raw/orog \
	reg2grb2/20220805 \
	sfc_climo/20220805 \
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

 #echo "mkdir -p ${directoryname}"
 #echo "cd ${directoryname}"
 #echo "aws --no-sign-request s3 sync ${srcdir}/${dir} ${localdirname}"

  mkdir -p ${directoryname}
  cd ${directoryname}
  aws --no-sign-request s3 sync ${srcdir}/${dir} ${localdirname}
done

for file in ugwd/20240624/ugwp_limb_tau.nc
do
  fullsrcdir=${srcdir}/${file}
  fulltardir=${tardir}/${file}

 #Get directory
  directoryname=$(dirname "${fulltardir}")
 #Get localname
  localname=$(basename "${fulltardir}")
 #Print results
 #echo "fulltardir Path: ${fulltardir}"
 #echo "Directory: ${directoryname}"
 #echo "Filename: ${localname}"

 #echo "mkdir -p ${directoryname}"
 #echo "cd ${directoryname}"
 #echo "aws --no-sign-request s3 sync ${srcdir}/${dir} ${localname}"

  mkdir -p ${directoryname}
  cd ${directoryname}
  aws --no-sign-request s3 cp ${srcdir}/${file} ${localname}
done

