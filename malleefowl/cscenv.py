##
import ocgis
import icclim
from netCDF4 import Dataset
 
#from malleefowl import wpslogging as logging
import logging
logger = logging.getLogger(__name__)

def indices( ncfile, icclim_SU ):

    SU_file = None
    
    # simple precesses realized by cdo commands:
    if icclim_SU == True :
        
        ncname = namegen(ncfile)
        
        #dt1 = datetime.datetime(2077,01,01)
        #dt2 = datetime.datetime(2078,12,31)
        rd = ocgis.RequestDataset(ncfile, 'tasmax') # time_range=[dt1, dt2]
        group = ['year']
        calc_icclim = [{'func':'icclim_SU','name':'SU'}]
        SU_file = ocgis.OcgOperations(dataset=rd, calc=calc_icclim, calc_grouping=group, prefix=str('SU_'+ ncname), output_format='nc', add_auxiliary_files=False).execute()
        
    return  SU_file

def namegen(ncfile):
    
    ds = Dataset(ncfile)
    if (str(ds.project_id) == 'CMIP5'):
    #day_MPI-ESM-LR_historical_r1i1p1
        #frq = str(ds.frequency)
        gmodel = str(ds.model_id)
        exp = str(ds.experiment_id)
        ens = str(ds.parent_experiment_rip)
        filename = str( gmodel + '_' + exp + '_' + ens )
    elif (str(ds.project_id) == 'CORDEX'):
    #EUR-11_ICHEC-EC-EARTH_historical_r3i1p1_DMI-HIRHAM5_v1_day
        dom = str(ds.CORDEX_domain)
        gmodel = str(ds.driving_model_id)
        exp = str(ds.experiment_id)
        ens = str(ds.driving_model_ensemble_member)
        rmodel = str(ds.model_id)
        ver = str(ds.rcm_version_id)
        #frq = str(ds.frequency)
        filename = str(dom + '_' + gmodel + '_' + exp + '_' + ens + '_' + rmodel + '_' + ver )
    else:
        filename = ncfile
    return filename
    