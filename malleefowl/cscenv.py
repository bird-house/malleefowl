##
import ocgis
import icclim
 
#from malleefowl import wpslogging as logging
import logging
logger = logging.getLogger(__name__)

def indices( files, icclim_SU ):

    SU_file = None
    
    # simple precesses realized by cdo commands:
    if icclim_SU == True :
        #dt1 = datetime.datetime(2077,01,01)
        #dt2 = datetime.datetime(2078,12,31)
        rd = ocgis.RequestDataset(files, 'tasmax') # time_range=[dt1, dt2]
        group = ['year']
        calc_icclim = [{'func':'icclim_SU','name':'SU'}]
        SU_file = ocgis.OcgOperations(dataset=rd, calc=calc_icclim, calc_grouping=group, prefix='my_test_SU', output_format='nc', add_auxiliary_files=False).execute()
        
    return  SU_file