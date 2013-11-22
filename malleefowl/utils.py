"""
Utility functions for WPS processes.
"""

import json
import pymongo
from netCDF4 import Dataset
from datetime import datetime
from dateutil import parser as date_parser

from pywps import config

import logging

log = logging.getLogger(__file__)


def within_date_range(timesteps, start=None, end=None):
    start_date = None
    if start != None:
        start_date = date_parser.parse(start)
    end_date = None
    if end != None:
        end_date = date_parser.parse(end)
    new_timesteps = []
    for timestep in timesteps:
        candidate = date_parser.parse(timestep)
        # within time range?
        if start_date != None and candidate < start_date:
            continue
        if end_date != None and candidate > end_date:
            break
        new_timesteps.append(timestep)
    return new_timesteps
    
def filter_timesteps(timesteps, aggregation="monthly", start=None, end=None):
    if (timesteps == None or len(timesteps) == 0):
        return []
    timesteps.sort()
    work_timesteps = within_date_range(timesteps, start, end)
    
    new_timesteps = [work_timesteps[0]]
    
    for index in range(1,len(work_timesteps)):
        current = date_parser.parse(new_timesteps[-1])
        candidate = date_parser.parse(work_timesteps[index])
    
        if current.year < candidate.year:
            new_timesteps.append(work_timesteps[index])
        elif current.year == candidate.year:
            if aggregation == "daily":
                if current.timetuple()[7] >= candidate.timetuple()[7]:
                    continue
            if aggregation == "weekly":
                if current.isocalendar()[1] >= candidate.isocalendar()[1]:
                    continue
            if aggregation == "monthly":
                if current.month >= candidate.month:
                    continue
            if aggregation == "yearly":
                if current.year >= candidate.year:
                    continue
            # all checks passed
            new_timesteps.append(work_timesteps[index])
        else:
            continue
    return new_timesteps
    

def database():
    dburi = config.getConfigValue("server", "mongodbUrl")
    conn = pymongo.Connection(dburi)
    return conn.malleefowl_db

def register_process_metadata(identifier, metadata):
    if identifier != None and metadata != None:
        db = database()
        process_metadata = { 'identifier': identifier, 'metadata': json.dumps(metadata) }
        db.metadata.update(
            {'identifier': process_metadata['identifier']},
            process_metadata,
            True)

def retrieve_process_metadata(identifier):
    db = database()
    process_metadata = db.metadata.find_one({'identifier': identifier})
    metadata = {}
    if process_metadata != None:
        metadata = json.loads(process_metadata.get('metadata'))
    return metadata

def nc_copy(source, target, overwrite=True, time_dimname='time', nchunk=10, istart=0, istop=-1, format='NETCDF3_64BIT'):
    """copy netcdf file from opendap to netcdf3 file

     :param overwrite:

          Overwite destination file (default is to raise an error if output file already exists).

     :param format:

          netcdf3 format to use (NETCDF3_64BIT by default, can be set to NETCDF3_CLASSIC)

     :param chunk:

          number of records along unlimited dimension to 
          write at once. Default 10. Ignored if there is no unlimited 
          dimension. chunk=0 means write all the data at once.

     :param istart:

          number of record to start at along unlimited dimension. 
          Default 0.  Ignored if there is no unlimited dimension.
          
     :param istop:

          number of record to stop at along unlimited dimension. 
          Default -1.  Ignored if there is no unlimited dimension.
    """

    nc_in = Dataset(source, 'r')
    nc_out = Dataset(target, 'w', clobber=overwrite, format=format)
    
    # create dimensions. Check for unlimited dim.
    unlimdimname = False
    unlimdim = None

    # create global attributes.
    log.info('copying global attributes ...')
    nc_out.setncatts(nc_in.__dict__) 
    log.info('copying dimensions ...')
    for dimname, dim in nc_in.dimensions.items():
        if dim.isunlimited() or dimname == time_dimname:
            unlimdimname = dimname
            unlimdim = dim
            if istop == -1: istop=len(unlimdim)
            nc_out.createDimension(dimname, istop-istart)
            log.debug('unlimited dimension = %s, length = %d', unlimdimname, len(unlimdim))
        else:
            nc_out.createDimension(dimname, len(dim))

    # create variables.
    for varname, ncvar in nc_in.variables.items():
        log.info('copying variable %s', varname)
        # is there an unlimited dimension?
        if unlimdimname and unlimdimname in ncvar.dimensions:
            hasunlimdim = True
        else:
            hasunlimdim = False
        if hasattr(ncvar, '_FillValue'):
            FillValue = ncvar._FillValue
        else:
            FillValue = None 
        var = nc_out.createVariable(varname, ncvar.dtype, ncvar.dimensions, fill_value=FillValue)
        # fill variable attributes.
        attdict = ncvar.__dict__
        if '_FillValue' in attdict: del attdict['_FillValue']
        var.setncatts(attdict)
        if hasunlimdim: # has an unlim dim, loop over unlim dim index.
            # range to copy
            if nchunk:
                start = istart; stop = istop; step = nchunk
                if step < 1: step = 1
                for n in range(start, stop, step):
                    nmax = n+nchunk
                    if nmax > istop: nmax=istop
                    log.debug('copy chunk [%d:%d]', n, nmax)
                    try:
                        var[n-istart:nmax-istart] = ncvar[n:nmax]
                    except:
                        msg = "n=%d nmax=%d istart=%d istop=%d" % (n, nmax, istart, istop)
                        raise Exception(msg)
            else:
                var[0:istop-istart] = ncvar[:]
        else: # no unlim dim or 1-d variable, just copy all data at once.
            var[:] = ncvar[:]
        nc_out.sync() # flush data to disk
    # close files.
    nc_out.close()
    nc_in.close()
