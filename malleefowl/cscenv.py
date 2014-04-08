##
import ocgis
import icclim
from netCDF4 import Dataset
import os
import time 

import subprocess
from malleefowl.process import WorkerProcess
from malleefowl import utils, tokenmgr

from malleefowl import wpslogging as logging
import logging
logger = logging.getLogger(__name__)


def indices( ncfile, SU ): # TG, TN, TX, SU, DTR, ETR , HI

    self.show_status('indices def call ', 15)
    
    #token = self.token.getValue()
    #outlog = "Starting the indice calculation at: \n"
    
##    self.show_status('starting ECA indices ...', 5)
##    logger.debug('starting ECA indices ... done')
            
    #userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)
    #outdir = os.path.join(self.files_path, userid)
    #utils.mkdir(outdir)
    
#    self.show_status('got token and outdir ...', 5)
    #logger.debug('got token and outdir ... done')

    #userid = tokenmgr.get_userid(tokenmgr.sys_token(), token)
    #ocgis.env.DIR_OUTPUT = os.path.join(self.files_path, userid)
    #logger.debug('outdir ... : '+ outdir )
    
    ocgis.env.OVERWRITE = True
    ocgis.env.DIR_OUTPUT = '/var/lib/pywps/files/nils.hempelmann_hzg.de/' #outdir
    
    #self.show_status('Set ocgis outdir ...', 5)
    #logger.debug('set ocgis outdir ... done')

    outlog = outlog + "Set the output dir \n"
    
    # simple precesses realized by cdo commands:
    for nc in ncfile:

        #self.show_status('in the nc loop ...', 15)
        #logger.debug('in the nc loop ... done')
 
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

        #self.show_status('filename created ...:'+ filename , 15)
        #logger.debug('filename created ...:'+ filename)

        outlog = outlog + "Create filename:  " + filename + " \n"
        
        #if TG == True :
            #TG_file = None
            #rd = ocgis.RequestDataset(nc, 'tas') # time_range=[dt1, dt2]
            #group = ['year']
            #calc_icclim = [{'func':'icclim_TG','name':'TG'}]
            #SU_file = ocgis.OcgOperations(dataset=rd, calc=calc_icclim, calc_grouping=group, prefix=str('TG_' + filename), output_format='nc', add_auxiliary_files=False).execute()

        #if TX == True :
            #TX_file = None
            #rd = ocgis.RequestDataset(nc, 'tasmax') # time_range=[dt1, dt2]
            #group = ['year']
            #calc_icclim = [{'func':'icclim_TX','name':'TX'}]
            #SU_file = ocgis.OcgOperations(dataset=rd, calc=calc_icclim, calc_grouping=group, prefix=str('TX_' + filename), output_format='nc', add_auxiliary_files=False).execute()

        #if TN == True :
            #TN_file = None
            #rd = ocgis.RequestDataset(nc, 'tasmin') # time_range=[dt1, dt2]
            #group = ['year']
            #calc_icclim = [{'func':'icclim_TN','name':'TN'}]
            #SU_file = ocgis.OcgOperations(dataset=rd, calc=calc_icclim, calc_grouping=group, prefix=str('TN_' + filename), output_format='nc', add_auxiliary_files=False).execute()

        if SU == True :
            SU_file = None
            rd = ocgis.RequestDataset(nc, 'tasmax') # time_range=[dt1, dt2]
            group = ['year']
            calc_icclim = [{'func':'icclim_SU','name':'SU'}]
            SU_file = ocgis.OcgOperations(dataset=rd, calc=calc_icclim, calc_grouping=group, prefix=str('SU_' + filename), output_format='nc', add_auxiliary_files=False).execute()
            
            #self.show_status('SU calculated ...:'+ filename , 15)
            #logger.debug('SU calculated ...:'+ filename)

            outlog = outlog + "SU indice processed sucessfully  \n"

            
        #if ETR == True :
            #ETR_file = None
            
            #variables = ['tasmin', 'tasmax']
            #request_datasets = [ocgis.RequestDataset(uri,variable) for uri,variable in zip(uris,variables)]
            #rdc = ocgis.RequestDatasetCollection(request_datasets)
            #group = ['year']
            #calc_icclim = [{'func':'icclim_ETR','name':'ETR','kwds':{'tasmin':'tasmin','tasmax':'tasmax'}}]
            #ETR_file = ocgis.OcgOperations(dataset=rdc, calc=calc_icclim, calc_grouping=group, prefix=str('ETR_'), output_format='nc', add_auxiliary_files=False).execute()

        #if HI == True :
            #HI_file = None
            
            ##variables = ['tasmin', 'tasmax']
            ##request_datasets = [ocgis.RequestDataset(uri,variable) for uri,variable in zip(uris,variables)]
            ##rdc = ocgis.RequestDatasetCollection(request_datasets)
            ##group = ['year']
            ##calc_icclim = [{'func':'icclim_ETR','name':'ETR','kwds':{'tasmin':'tasmin','tasmax':'tasmax'}}]
            ##ETR_file = ocgis.OcgOperations(dataset=rdc, calc=calc_icclim, calc_grouping=group, prefix=str('ETR_'), output_format='nc', add_auxiliary_files=False).execute()


            
        outlog = outlog + "All indices processed sucessfully  \n"
    return outlog ;
    