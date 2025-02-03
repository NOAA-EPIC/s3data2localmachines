#!/bin/bash
#On AWS, we have fix at:
#aws --no-sign-request s3 ls  s3://noaa-nws-global-pds/fix

while getopts "a:o:g:v:h" option; do
  case "$option" in
    a) atmgrid="$OPTARG" ;;
    o) ocngrid="$OPTARG" ;;
    g) gwhome="$OPTARG" ;;
    v) verbose=true ;;
    h) help=true ;;
    \?) echo "Usage: $0 [-a atmgrid] [-o ocngrid] [-v]" 
      exit -1 ;;
  esac
done

atmgridlist=( "C48" "C96" "C192" "C384" "C768" "C1152" )
ocngridlist=( "500" "100" "050" "025" )

if [ "$help" = true ]; then
  echo "Usage: $0 -a atmgrid -o ocngrid -g gwhome [-v] [-h]" 
  echo "agrid options are: ${atmgridlist[@]}"
  echo "ogrid options are: ${ocngridlist[@]}"
  echo "gwhome is Global-Workflow directory"
  exit 0
fi

if [ -z "${atmgrid+x}" ]; then
  echo "atmgrid is not defined. user mush have run with -a atmgrid."
  echo "run $0 --help for more info."
  exit -1
fi

if [ -z "${ocngrid+x}" ]; then
  echo "ocngrid is not defined. user mush have run with -o ocngrid."
  echo "run $0 --help for more info."
  exit -1
fi

if [ -z "${gwhome+x}" ]; then
  echo "gwhome is not defined. user mush have run with -g gwhome."
  echo "run $0 --help for more info."
  exit -1
else
  if [ ! -d ${gwhome} ]; then
     echo "gwhome is not valid global-workflow-directory."
     echo "run $0 --help for more info."
     exit -1
  else
    source ${gwhome}/versions/fix.ver
  fi
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

#Default fix_ver
#export aer_ver=20220805
#export am_ver=20220805
#export chem_ver=20220805
#export cice_ver=20240416
#export cpl_ver=20230526
#export datm_ver=20220805
#export glwu_ver=20220805
#export gsi_ver=20240208
#export lut_ver=20220805
#export mom6_ver=20240416
#export orog_ver=20231027
#export reg2grb2_ver=20220805
#export sfc_climo_ver=20220805
#export ugwd_ver=20240624
#export verif_ver=20220805
#export wave_ver=20240105

srcdir=s3://noaa-nws-global-pds/fix
tardir=/contrib/global-workflow-shared-data/fix.subset
#tardir=/contrib/global-workflow-shared-data/fix.subset.a${atmgrid}o${ocngrid}

dirlist=( "aer/${aer_ver}" "am/${am_ver}" \
	  "chem/${chem_ver}/fimdata_chem" "chem/${chem_ver}/Emission_data" \
 	  "datm/${datm_ver}/cfsr" "${datm_ver}/20220805/gefs" "datm/${datm_ver}/gfs" \
	  "glwu/${glwu_ver}" "gsi/${gsi_ver}" "lut/${lut_ver}" \
	  "mom6/${mom6_ver}/post" "raw/orog" \
	  "reg2grb2/${reg2grb2_ver}" "sfc_climo/${sfc_climo_ver}" \
	  "verif/${verif_ver}" "wave/${wave_ver}" )

echo "dirlist: ${dirlist[@]}"

subdirlist=( "cice/${cice_ver}/${ocngrid}" "cpl/${cpl_ver}/a${atmgrid}o${ocngrid}" \
	     "datm/${datm_ver}/mom6/${ocngrid}" "mom6/${mom6_ver}/${ocngrid}" \
	     "orog/${orog_ver}/${atmgrid}" "ugwd/${ugwd_ver}/${atmgrid}" )

echo "grid specified dirlist: ${subdirlist[@]}"

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

