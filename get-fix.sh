#!/bin/bash
#On AWS, we have fix at:
#aws --no-sign-request s3 ls  s3://noaa-nws-global-pds/fix

while getopts "a:o:v:h" option; do
  case "$option" in
    a) atmgrid="$OPTARG" ;;
    o) ocngrid="$OPTARG" ;;
    v) verbose=true ;;
    h) help=true ;;
    \?) echo "Usage: $0 [-a atmgrid] [-o ocngrid] [-v]" 
      exit -1 ;;
  esac
done

atmgridlist=( "C48" "C96" "C192" "C384" "C768" "C1152" )
ocngridlist=( "500" "100" "050" "025" )

if [ "$help" = true ]; then
  echo "Usage: $0 [-a atmgrid] [-o ocngrid] [-v] [-h]" 
  echo "agrid options are: ${atmgrid[@]}"
  echo "ogrid options are: ${ocngrid[@]}"
  exit 0
fi

if [ "$verbose" = true ]; then
 #echo "Verbose mode enabled."
  set -x
fi

foundatmgrid=false

for grid in "${atmgridlist[@]}"
do
  if [[ "$atmgrid" == "$grid" ]]; then
    foundatmgrid=true
    break
  fi
done

foundocngrid=false

for grid in "${ocngridlist[@]}"
do
  if [[ "$ocngrid" == "$grid" ]]; then
    foundocngrid=true
    break
  fi
done

if [[ "${foundatmgrid}" = false || "${foundocngrid}" = false ]]
then
  echo "Could not find both atmgrid and ocngrid."
  echo "Usage: $0 [-a atmgrid] [-o ocngrid] [-v] [-h]"
  echo "agrid options are: ${atmgridlist[@]}"
  echo "ogrid options are: ${ocngridlist[@]}"
  exit -1
fi

srcdir=s3://noaa-nws-global-pds/fix
tardir=/contrib/global-workflow-shared-data/fix.subset.a${atmgrid}o${ocngrid}

dirlist=( "aer/20220805" "am/20220805" "chem/20220805/fimdata_chem" "chem/20220805/Emission_data" \
 	  "datm/20220805/cfsr" "datm/20220805/gefs" "datm/20220805/gfs" "glwu/20220805" \
	  "gsi/20240208" "lut/20220805" "mom6/20240416/post" "raw/orog" \
	  "reg2grb2/20220805" "sfc_climo/20220805" "verif/20220805 wave/20240105" )

echo "dirlist: ${dirlist[@]}"

subdirlist=( "cice/20240416/${ocngrid}" "cpl/20230526/a${atmgrid}o${ocngrid}" "datm/20220805/mom6/${ocngrid}" "mom6/20240416/${ocngrid}" "orog/20231027/${atmgrid}" "ugwd/20240624/${atmgrid}" )

fulldirlist=( "${dirlist[@]}" "${subdirlist[@]}" )
echo "new dirlist: ${fulldirlist[@]}"

for dir in "${fulldirlist[@]}"
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

for file in ugwd/20240624/ugwp_limb_tau.nc
do
  fullsrcfile=${srcdir}/${file}
  fulltarfile=${tardir}/${file}

  if [ ! -f ${fulltarfile} ]
  then
   #Get directory
    directoryname=$(dirname "${fulltarfile}")
   #Get localname
    localname=$(basename "${fulltarfile}")
   #Print results
   #echo "fulltarfile Path: ${fulltarfile}"
   #echo "Directory: ${directoryname}"
   #echo "Filename: ${localname}"

   #echo "mkdir -p ${directoryname}"
   #echo "cd ${directoryname}"
   #echo "aws --no-sign-request s3 sync ${srcdir}/${file} ${localname}"

    mkdir -p ${directoryname}
    cd ${directoryname}
    aws --no-sign-request s3 cp ${srcdir}/${file} ${localname}
  else
    echo "File ${fulltarfile} already exist."
  fi
done

