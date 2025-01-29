import os
import time
import sys
#import requests
#import json
#import base64
import getopt
import subprocess
from pathlib import Path

#--------------------------------------------------------------------------
class FetchFIXdata():
    def __init__(self, atmgrid='C48', ocngrid='500', localdir=None, verbose=0):
        self.aws_fix_bucket = 's3://noaa-nws-global-pds/fix'
        self.aws_cp = 'aws --no-sign-request s3 cp'
        self.aws_sync = 'aws --no-sign-request s3 sync'

        self.atmgrid = atmgrid
        self.ocngrid = ocngrid
        self.localdir = localdir
        self.verbose = verbose

       #if (os.path.isdir(localdir)):
       #    print('Prepare to download FIX data for %s and %s to %s' %(atmgrid, ocngrid, localdir))
       #else:
       #    print('local dir: <%s> does not exist. Stop' %(localdir))
       #    sys.exit(-1)

        self.ugwd_version = None
        self.mom6_version = None
        self.fix_version = None
        self.gsi_version = None

        self.s3dict = {}
        self.s3dict['raworog'] = 'raw/orog'

        self.targetdir = '%s/fix.subset.a%s.o%s' %(self.localdir, self.atmgrid, self.ocngrid)

        if (self.verbose):
            self.printinfo()

    def set_orog_version(self, orog_version):
        self.orog_version = orog_version
        self.s3dict['orog'] = 'orog/%s/%s' %(self.orog_version, self.atmgrid)

    def set_mom6_version(self, mom6_version):
        self.mom6_version = mom6_version
        self.s3dict['mom6post'] = 'mom6/%s/post' %(self.mom6_version)
        self.s3dict['mom6'] = 'mom6/%s/%s' %(self.mom6_version, self.ocngrid)
        self.s3dict['cice'] = 'cice/%s/%s' %(self.mom6_version, self.ocngrid)

    def set_gsi_version(self, gsi_version):
        self.gsi_version = gsi_version
        self.s3dict['gsi'] = 'gsi/%s' %(self.gsi_version)

    def set_cice_version(self, cice_version):
        self.cice_version = cice_version
        self.s3dict['cice'] = 'cice/%s/%s' %(self.cice_version, self.ocngrid)

    def set_cpl_version(self, cpl_version):
        self.cpl_version = cpl_version
        self.s3dict['cpl'] = 'cpl/%s/a%so%s' %(self.cpl_version, self.atmgrid, self.ocngrid)

    def set_fix_version(self, fix_version):
        self.fix_version = fix_version
        self.s3dict['aer'] = 'aer/%s' %(self.fix_version)
        self.s3dict['am'] = 'am/%s' %(self.fix_version)
        self.s3dict['fimdata_chem'] = 'chem/%s/fimdata_chem' %(self.fix_version)
        self.s3dict['Emission_data'] = 'chem/%s/Emission_data' %(self.fix_version)
        self.s3dict['cfsr'] = 'datm/%s/cfsr' %(self.fix_version)
        self.s3dict['gefs'] = 'chem/%s/gefs' %(self.fix_version)
        self.s3dict['gfs'] = 'chem/%s/gfs' %(self.fix_version)
        self.s3dict['glwu'] = 'glwu/%s' %(self.fix_version)
        self.s3dict['lut'] = 'lut/%s' %(self.fix_version)
        self.s3dict['reg2grb2'] = 'reg2grb2/%s' %(self.fix_version)
        self.s3dict['sfc_climo'] = 'sfc_climo/%s' %(self.fix_version)
        self.s3dict['verif'] = 'verif/%s' %(self.fix_version)
        self.s3dict['wave'] = 'wave/%s' %(self.fix_version)
        self.s3dict['mom6datm'] = 'datm/%s/mom6/%s' %(self.fix_version, self.ocngrid)

    def set_ugwd_version(self, ugwd_version):
        self.ugwd_version = ugwd_version
        self.s3dict['ugwd'] = 'ugwd/%s/%s' %(self.fix_version, self.atmgrid)

    def printinfo(self):
        print('Preparing to fetch ATM grid: %s, ONC grid: %s' %(self.atmgrid, self.ocngrid))
        print('From: %s' %(self.aws_fix_bucket))
        print('To: %s' %(self.targetdir))
        print('fix version:', self.fix_version)
        print('gsi version:', self.gsi_version)
        print('cpl version:', self.cpl_version)
        print('cice version:', self.cice_version)
        print('ugwd version:', self.ugwd_version)
        print('mom6 version:', self.mom6_version)

    def fetchdata(self):
        if (self.verbose):
            print('Create local fix dir: ', self.targetdir)

        path = Path(self.targetdir)
        path.mkdir(parents=True, exist_ok=True)

        self.fetch_ugwp_limb_tau()
        
        for key in self.s3dict.keys():
            self.fetch_dir(self.s3dict[key])

    def fetch_dir(self, dir):
        remotedir = '%s/%s' %(self.aws_fix_bucket, dir)
        localdir = '%s/%s' %(self.targetdir, dir)
        cmd = '%s %s %s'%(self.aws_sync, remotedir, localdir)
        self.download_dir(cmd, localdir)

    def download_dir(self, cmd, localdir):
       #returned_value = os.system(cmd)  # returns the exit code in unix
       #print('returned value:', returned_value)

        if (os.path.isdir(localdir)):
            print('%s already exist. skip' %(localdir))
        else:
            parentdir, dirname = os.path.split(localdir)
            if (self.verbose):
                print('Create local %s dir: ', parentdir)
            path = Path(parentdir)
            path.mkdir(parents=True, exist_ok=True)
            if (self.verbose):
                print(cmd)
            print('Downloading ', localdir)
            returned_value = subprocess.call(cmd, shell=True)  # returns the exit code in unix
            if (self.verbose):
                print('returned value:', returned_value)

    def fetch_ugwp_limb_tau(self):
        ugwp_limb_tau_remotepath = '%s/ugwd/%s/ugwp_limb_tau.nc' %(self.aws_fix_bucket, self.ugwd_version)
        ugwp_limb_tau_localdir = '%s/ugwd/%s' %(self.targetdir, self.ugwd_version)
        filename = '%s/ugwp_limb_tau.nc' %(ugwp_limb_tau_localdir)
        path = Path(ugwp_limb_tau_localdir)
        path.mkdir(parents=True, exist_ok=True)
        cmd = '%s %s %s'%(self.aws_cp, ugwp_limb_tau_remotepath, filename)
        self.download_file(cmd, filename)

    def download_file(self, cmd, filename):
       #returned_value = os.system(cmd)  # returns the exit code in unix
       #print('returned value:', returned_value)

        if (os.path.isfile(filename)):
            print('%s already exist. skip' %(filename))
        else:
            if (self.verbose):
                print(cmd)
            print('Downloading ', filename)
            returned_value = subprocess.call(cmd, shell=True)  # returns the exit code in unix
            if (self.verbose):
                print('returned value:', returned_value)

def print_usage():
    print('Usage: python fetch-fix-data.py \\')
    print('       --atmgrid=AtmospericGrid \\')
    print('       --ocngrid=OceanGrid \\')
    print('       --localdir=Your-local-fix-dir \\')
    print('       [options]')
    print('options are:')
    print('--fix_version=yyyymmdd  default: 20220805')
    print('--gsi_version=yyyymmdd  default: 20240208')
    print('--cpl_version=yyyymmdd  default: 20230526')
    print('--cice_version=yyyymmdd  default: 20240416')
    print('--mom6_version=yyyymmdd  default: 20240416')
    print('--orog_version=yyyymmdd  default: 20231027')
    print('--ugwd_version=yyyymmdd  default: 20240624')

#------------------------------------------------------------------
if __name__ == '__main__':
    atmgridlist = ['C48', 'C96', 'C192', 'C384', 'C768', 'C1152']
    ocngridlist = ['500', '100', '050', '025']

    verbose = 0
    atmgrid = 'C48'
    ocngrid = '500'
    localdir = '/contrib/global-workflow-shared-data'

    ugwd_version = '20240624'
    orog_version = '20231027'
    mom6_version = '20240416'
    cice_version = '20240416'
    cpl_version = '20220805'
    fix_version = '20220805'
    gsi_version = '20240208'

    opts, args = getopt.getopt(sys.argv[1:], '', ['help', 'atmgrid=', 'ocngrid=',
                                                  'verbose=', 'localdir=',
                                                  'fix_version=',
                                                  'gsi_version=',
                                                  'cpl_version=',
                                                  'cice_version=',
                                                  'orog_version=',
                                                  'mom6_version=',
                                                  'ugwd_version='])
    for o, a in opts:
        if o in ['--help']:
            print_usage()
            sys.exit(0)
        elif o in ['--verbose']:
            verbose = 1
        elif o in ['--atmgrid']:
            atmgrid = a
        elif o in ['--ocngrid']:
            ocngrid = a
        elif o in ['--localdir']:
            localdir = a
        elif o in ['--ugwd_version']:
            ugwd_version = a
        elif o in ['--mom6_version']:
            mom6_version = a
        elif o in ['--orog_version']:
            orog_version = a
        elif o in ['--cice_version']:
            cice_version = a
        elif o in ['--cpl_version']:
            cpl_version = a
        elif o in ['--fix_version']:
            fix_version = a
        elif o in ['--gsi_version']:
            gsi_version = a
        else:
            print('o: %s, a: %s' %(o, a))
            assert False, 'unhandled option'

    print('atmgrid: %s, ocngrid: %s' %(atmgrid, ocngrid))

    if (atmgrid not in atmgridlist):
        print('atmgrid: ', atmgrid)
        print('is not in supported grids: ', atmgridlist)
        print_usage()
        sys.exit(-1)

    if (ocngrid not in ocngridlist):
        print('ocngrid: ', ocngrid)
        print('is not in supported grids: ', ocngridlist)
        print_usage()
        sys.exit(-1)

#------------------------------------------------------------------
    ffd = FetchFIXdata(atmgrid=atmgrid, ocngrid=ocngrid,
                       localdir=localdir, verbose=verbose)

    ffd.set_mom6_version(mom6_version)
    ffd.set_orog_version(orog_version)
    ffd.set_ugwd_version(ugwd_version)
    ffd.set_cice_version(cice_version)
    ffd.set_cpl_version(cpl_version)
    ffd.set_fix_version(fix_version)
    ffd.set_gsi_version(gsi_version)

    ffd.fetchdata()
