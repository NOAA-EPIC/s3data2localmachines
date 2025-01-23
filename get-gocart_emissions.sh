#!/bin/bash
#On AWS, we have gocart_emissions at:
#aws --no-sign-request s3 ls  s3://noaa-nws-global-pds/fix


 export BUCKET_URI=s3://noaa-nagape-none-ca-epic
 export AWS_ACCESS_KEY_ID='ASIAYJVSFTGORTR5PKR4'
 export AWS_SECRET_ACCESS_KEY=edQWef70de11gamE5qy5IyzeMvqVjW+fNgW015cp
 export AWS_SESSION_TOKEN=FwoGZXIvYXdzEPf//////////wEaDEuVc6VcHGgKk+YclCLRAmUJ1M5qLNuBAIrapfbMg42Qpm5RP7E8G4a9wZsgbQjdFiMX+ZNkr/BL+wQcbpTahDsEkcI06iI2SIUFe2huDQHtzqs2c1ddstJPd3HroHncomk581ixOXQTb+jOCHexp3gE7mFDKVEKuYhsU0zc2zHXcLMAvjWfjnrbMzpY24HCHjIAKHfOWPKGOLNdIces9Jf5kGjy5wTIxtNvLV7kwPVRJ5ZSnWAcUdEpymi4QJ8gnrPb3dXVjLg5JOAYHZ2DaegdLR+AQTxM1DNMa4gm6lxlnVEIVtCtRHNgN+3+tXtUhrML3FIuRDhGmLHqA2Kk6n/ofbPUP9xUGMdMlgg1JisML75LrrkXob5b+5d1fdgwV4Nx3bDANUD8EWlsiTBQGVLaRWl5CLzkKe8ZHdEKjFsf3X1buMJhN8Og7WoKjt3iHx/YywYkZkT2207+5sZvvbkoos7FvAYyKaCxfYOlt3ksLq8uVR7mYTiMh2vHjxASOg3LqYahLsXj6azetSi/2Czt
 export AWS_REGION=us-east-1

srcdir=${BUCKET_URI}//bucket/global-workflow-shared-data/data/gocart_emissions
tardir=/contrib/global-workflow-shared-data/gocart_emissions.subset

set -x

for dir in Dust \
	GBBEPx \
	GEFS_PRODUCTION/CEDS2019/emi_C96 \
	GEFS_PRODUCTION/CEDS2019/fengsha/bsmfv3/C96 \
	MERRA2/sfc \
	monochromatic \
	optics \
	PIESA/L127 \
	PIESA/sfc/HTAP \
	volcanic
do
  fullsrcdir=${srcdir}/${dir}
  fulltardir=${tardir}/${dir}

  if [ ! -d ${fulltardir} ]
  then
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
  else
    echo "Dir ${fulltardir} already exist."
  fi
done

