##
import ocgis
import icclim
from netCDF4 import Dataset
import os
import time 

#from malleefowl import utils, tokenmgr
#from malleefowl import wpslogging as logging
import logging
logger = logging.getLogger(__name__)


def indices( ncfile, SU  ):
    
    outlog = "Starting the indice calculation at: \n"
    
    #userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)
    #ocgis.env.DIR_OUTPUT = os.path.join(self.files_path, userid)
    ocgis.env.OVERWRITE = True
    
    outlog = outlog + "Starting the indice calculation at \n"
    
    # simple precesses realized by cdo commands:
    for nc in ncfile:
        
        outlog = outlog + "Starting to process file:  " + nc + " \n"
        
        ds = Dataset(nc)
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
            filename = nc
        
        outlog = outlog + "Create filename:  " + filename + " \n"
        
        if SU == True :
            SU_file = None
            rd = ocgis.RequestDataset(nc, 'tasmax') # time_range=[dt1, dt2]
            group = ['year']
            calc_icclim = [{'func':'icclim_SU','name':'SU'}]
            SU_file = ocgis.OcgOperations(dataset=rd, calc=calc_icclim, calc_grouping=group, prefix=str('SU_' + filename), output_format='nc', add_auxiliary_files=False).execute()
    
    #if FD == True :
        #FD_file = None
        ##dt1 = datetime.datetime(2077,01,01)
        ##dt2 = datetime.datetime(2078,12,31)
        #rd = ocgis.RequestDataset(ncfile, 'tasmin') # time_range=[dt1, dt2]
        #group = ['year']
        #calc_icclim = [{'func':'icclim_FD','name':'FD'}]
        #FD_file = ocgis.OcgOperations(dataset=rd, calc=calc_icclim, calc_grouping=group, prefix=str('FD_'), output_format='nc', add_auxiliary_files=False).execute()
        
    return outlog ;
    