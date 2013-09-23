"""
Processes for Anopheles Gambiae population dynamics 
Author: Nils Hempelmann (nils.hempelmann@hzg)
"""

from datetime import datetime, date
import tempfile
import subprocess

from malleefowl.process import WorkerProcess

class AnophelesProcess(WorkerProcess):
    """This process calculates the evapotranspiration following the Pennan Monteith equation"""

    def __init__(self):
        # definition of this process
        WorkerProcess.__init__(self, 
            identifier = "de.csc.esgf.anopheles",
            title="Population dynamics of Anopheles Gambiae",
            version = "0.1",
            metadata= [
                       {"title": "Climate Service Center", "href": "http://www.climate-service-center.de/"}
                      ],
            abstract="Just testing a nice script to calculate the Population dynamics of Anopheles Gambiae",
            extra_metadata={
                  'esgfilter': 'variable:tas,variable:evspsbl,variable:hurs,variable:pr',  #institute:MPI-M, ,time_frequency:day
                  'esgquery': 'variable:tas AND variable:evspsbl AND variable:hurs AND variable:pr' # institute:MPI-M AND time_frequency:day 
                  },
            )
            
            
        # Literal Input Data
        # ------------------

       
        self.output = self.addComplexOutput(
            identifier="output",
            title="anopheles",
            abstract="Calculated population dynamics of adult Anopheles Gambiae ",
            formats=[{"mimeType":"application/netcdf"}],
            asReference=True,
            )         
            
    def execute(self):
        from Scientific.IO.NetCDF import NetCDFFile
        from os import curdir, path
        import numpy as np
        from cdo import *
        import datetime 
        from math import *
        cdo = Cdo()

        # guess var names of files
        nc_files = self.get_nc_files()
        for nc_file in nc_files: 
            ds = NetCDFFile(nc_file)
            if "tas" in ds.variables.keys():
                nc_tas = NetCDFFile(nc_file , 'r')
            elif "hurs" in ds.variables.keys():
                nc_hurs = NetCDFFile(nc_file , 'r')
            elif "pr" in ds.variables.keys():
                nc_pr = NetCDFFile(nc_file , 'r')
            elif "evspsbl" in ds.variables.keys():
                nc_evspsbl = NetCDFFile(nc_file , 'r')    
            else:
                raise Exception("input netcdf file has not variable tas|hurs|pr|evspsbl")
                        
        nc_n4 = path.join(path.abspath(curdir), "nc_n4.nc")
        script_anophels =  path.join(path.dirname(__file__),"anopheles.R")

        nc_output_file = self.mktempfile(suffix='_n4.nc')


        cdo.selname('pr', input=nc_pr, output=nc_output_file)
        cdo.selname('pr', input='/home/main/data/pr_AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_20060101_20101231.nc', output='/home/main/data/n4_AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_20060101_20101231.nc')




        nc_tas = NetCDFFile('/home/main/data/tas_test_20060101_20101231.nc','r')
        nc_pr = NetCDFFile('/home/main/data/pr_test_20060101_20101231.nc','r')
        nc_hurs = NetCDFFile('/home/main/data/hurs_test_20060101_20101231.nc','r')
        nc_evspsbl = NetCDFFile('/home/main/data/evspsbl_test_20060101_20101231.nc','r')
        #nc_n4 = NetCDFFile('/home/main/data/n4_AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_20060101_20101231.nc' , 'w')

        #change attributes und variable name here 
        #dim.def.nc(nc_n4, "lon", dim_lon)
        #dim.def.nc(nc_n4, "lat", dim_lat)
        #dim.def.nc(nc_n4, "time", unlim=TRUE)
        ###  Create two variables, one as coordinate variable
        #var.def.nc(nc_n4, "time", "NC_INT", "time")
        ## var.def.nc(nc_n4, "lat", "NC_INT", "time")
        ## var.def.nc(nc_n4, "lon", "NC_INT", "time")
        #var.def.nc(nc_n4, "n4", "NC_FLOAT", c(0,1,2))
        #att.put.nc(nc_n4, "n4", "missing_value", "NC_FLOAT", -9e+33)

        ## read in values 

        tas = np.squeeze(nc_tas.variables["tas"])
        pr = np.squeeze(nc_pr.variables["pr"])
        hurs = np.squeeze(nc_hurs.variables["hurs"])
        evspsbl = np.squeeze(nc_evspsbl.variables["evspsbl"])
        n4 = np.zeros([tas.shape[0],tas.shape[1],tas.shape[2]])

        # define some constatnts:
        Increase_Ta = 0
        #Evaporation (> -8)
        Increase_Et = 0
        #Rainfall (> -150)
        Increase_Rt = 0
        #Relative Humidity in (-97,+39)
        Increase_RH = 0
        ## Text
        deltaT = 6.08
        h0 = 97
        AT = 1.79*10**6
        lamb = 1.5
        m = 1000
        De = 37.1
        Te = 7.7
        Nep = 120
        alpha1 = 280.486
        alpha2 = 0.025616
            
        #if (abs(deltaT)<4):
            #b = 0.89
        #else:
        b = 0.88

        for x in range(0,tas.shape[1],1): #tas.shape[1]
            for y in range(0,tas.shape[2],1): #tas.shape[2]

                ## get the appropriate values 
                RH = hurs[:,x,y] * 100
                Ta = tas[:,x,y] -273.15
                Rt = pr[:,x,y] * 86400     
                Et = evspsbl[:,x,y] * -86400  
                Tw = Ta + deltaT

                # create appropriate variabels 
                D = np.zeros(Ta.size)
                Vt = np.zeros(Ta.size)
                Vt[0] = 1000
                beta2 = np.zeros(Ta.size)
                beta1 = np.zeros(Ta.size)
                beta0 = np.zeros(Ta.size)
                
                p4 = ft = Gc_Ta = F4 = N23 = p_DD = np.zeros(Ta.size)
                p_Tw = p_Rt = p_D = G = np.zeros([Ta.size,3])
                P = p = N = d = np.zeros([Ta.size,4])
                N[0,0] = N[0,1] = N[0,2] = N[0,3] = 100
                    
        ## Check for Values out of r
        # Ta = Ta + Increase_Ta
                for t in range(0,tas.shape[0]-1,1): #tas.shape[0]
                    print x, y, t
                    
                    if (Et[t] + Increase_Et >= 0):
                        Et[t] = Et[t] + Increase_Et
                    else:
                        Et[t] = 0
                    if (Rt[t] + Increase_Rt >= 0):
                        Rt[t] = Rt[t] + Increase_Rt
                    else:
                        Et[t] = 0
                    if (RH[t] + Increase_RH >=0 and RH[t] + Increase_RH<=100):
                        RH[t] = RH[t] + Increase_RH
                    elif (RH[t] + Increase_RH >= 100):
                        RH[t] = 100
                    else:
                        RH[t] = 0
                    
                    if(Vt[t] == 0 and Rt[t] == 0):
                        Vt[t+1] = 0
                    else:
                        Vt[t+1] = (Vt[t] + AT*Rt[t]/1000)*(1 - 3*Et[t]/h0*(Vt[t]/(Vt[t]+AT*Rt[t]/1000))**(1/3))
                    if(Vt[t+1] <= 0):
                        Vt[t+1] = 0
                    if (Vt[t+1] == 0):
                        D[t+1] = D[t+1] + 1
                    else:
                        D[t+1] = 0
            
                    beta2[t] = 4*10**(-6)*RH[t]**2 - 1.09*10**(-3)*RH[t] - 0.0255
                    beta1[t] = -2.32*10**(-4)*RH[t]**2 + 0.0515**RH[t] + 1.06
                    beta0[t] = 1.13*10**(-3)*RH[t]**2 - 0.158*RH[t] - 6.61

                    p4[t] = exp(-1/(beta2[t]*Ta[t]**2 + beta1[t]*Ta[t] + beta0[t]))
                    
                    if (Vt[t] > 0):
                        d[t,0] =  1.011 + 20.212*(1 + (Tw[t]/12.096)**4.839)**(-1)
                    else:
                        d[t,0] = 1.011 + 20.212*(1 + (Ta[t]/12.096)**4.839)**(-1)
                    if(Vt[t] > 0):
                        d[t,1] =  8.130 + 13.794*(1 + (Tw[t]/20.742)**8.946)**(-1) - d[t,1]
                    else:
                        d[t,1] = 8.130 + 13.794*(1 + (Ta[t]/20.742)**8.946)**(-1) - d[t,1]
                    if (Vt[t] > 0):
                        d[t,2] = 8.560 + 20.654*(1 + (Tw[t]/19.759)**6.827)**(-1) - d[t,2]
                    else:
                        d[t,2] = 8.560 + 20.654*(1 + (Ta[t]/19.759)**6.827)**(-1) - d[t,2]
                    
                    d[t,3] = -1/log(p4[t])
                    
                    if (Vt[t] != 0):
                        if (Ta[t] >= 14 and Ta[t] <= 40):
                            p_Tw[t,0] = exp(-1/d[t,0])
                        else:
                            p_Tw[t,0] = 0
                    elif (Ta[t] >= 14 and Ta[t] <= 40):
                        p_Tw[t,0] = exp(-1/d[t,0])
                    else:
                        p_Tw[t,0] = 0
                    
                    if (Vt[t] != 0):
                        if (Ta[t] >= 18 and Ta[t] <= 32):
                            p_Tw[t,1] = exp(-1/d[t,1])
                        else:
                            p_Tw[t,1] = 0
                    elif (Ta[t] >= 18 and Ta[t] <= 32):
                        p_Tw[t,1] = exp(-1/d[t,1])
                    else:
                        p_Tw[t,1] = 0
                    
                    if (Vt[t] != 0):
                        if (Ta[t] >= 18 and Ta[t] <= 32):
                            p_Tw[t,2] = exp(-1/d[t,2])
                        else:
                            p_Tw[t,2] = 0
                    elif (Ta[t] >= 18 and Ta[t] <= 32):
                        p_Tw[t,2] = exp(-1/d[t,2])
                    else:
                        p_Tw[t,2] = 0
                    
                    p_Rt[t,0] = exp(-0.0242*Rt[t])
                    p_Rt[t,1] = exp(-0.0127*Rt[t])
                    p_Rt[t,2] = exp(-0.00618*Rt[t])

                    p_D[t,0] = 2*exp(-0.405*D[t])/(1 + exp(-0.405*D[t]))
                    p_D[t,1] = 2*exp(-0.855*D[t])/(1 + exp(-0.855*D[t]))
                    p_D[t,2] = 2*exp(-0.602*D[t])/(1 + exp(-0.602*D[t]))
                    
                    
                    if (Vt[t] != 0):
                        p_DD[t] = (b*m/(1000*(N[t,1]+N[t,2])/Vt[t]))*(1 - lamb**lamb/(lamb +(1000*(N[t,1]+N[t,2])/Vt[t])/m)**lamb)
                    #else:
                        #p_DD[t] = 1
                    
                    p[t,:]=([p_Tw[t,0]*p_Rt[t,0]*p_D[t,0] , p_Tw[t,1]*p_Rt[t,1]*p_D[t,1]*p_DD[t] , p_Tw[t,2]*p_Rt[t,2]*p_D[t,2]*p_DD[t] , p4[t]])
                    
                    j = 0 
                    while(j < 4):
                        P[t,j] = (p[t,j] - p[t,j]**(d[t,j]))/(1 - p[t,j]**d[t,j])
                        j+=1
                    j = 0     
                    while(j < 3):
                        G[t,j] = (1 - p[t,j])/(1 - p[t,j]**d[t,j])*p[t,j]**d[t,j]
                        j+=1
                    
                    ft[t] = 0.518*exp(-6*(N[t,2]/Vt[t] - 0.317)**2) + 0.192
                    Gc_Ta[t] = 1 + De/(Ta[t] - Te)
                    F4[t] = ft[t]*Nep/Gc_Ta[t]

                    N[t+1,:] = [(P[t,0] * N[t,0] + F4[t] * N[t,3]),(P[t,1] * N[t,1] + G[t,0] * N[t,0]),(P[t,2] * N[t,2] + G[t,1] * N[t,1]),(P[t,3] * N[t,3] + G[t,2] * N[t,2])]
                    #N[t+1,1] = (P[t,1] * N[t,1] + G[t,0] * N[t,0])
                    #N[t+1,2] = (P[t,2] * N[t,2] + G[t,1] * N[t,1])
                    #N[t+1,3] = (P[t,3] * N[t,3] + G[t,2] * N[t,2])
                    #N[t+1] = ([P[t,0],G[t,0],0,0],[0,P[t,1],G[t,1],0],[0,0,P[t,2],G[t,2],alpha1*F4[t],0,0,P[t,3]), nrow = 4, ncol = 4) %*% N[i,]
                    
                    #p[dim_time,] = c(p_Tw[dim_time,1]*p_Rt[dim_time,1]*p_D[dim_time,1],p_Tw[dim_time,2]*p_Rt[dim_time,2]*p_D[dim_time,2]*p_DD[dim_time],p_Tw[dim_time,3]*p_Rt[dim_time,3]*p_D[dim_time,3]*p_DD[dim_time],p4[dim_time])

                    n4[t,x,y] = p4[t] # p_D[t,2] #N[t,3]

                    #var.put.nc(nc_n4, "n4" , N[,4], start=c(x,y,1), count=c(1,1,dim_time), na.mode=0, pack=FALSE) # , na.mode=0, pack=FALSE
                    #cat("processed coordinate x: ",x," y: " ,y,"\n")   
                    



        # nc_n4.append(Rt)        
        
        
        
        self.cmd(cmd=["R", "--vanilla", "--args", nc_tas, nc_hurs, nc_pr, nc_evspsbl, nc_anopheles, "<", script_anophels ], stdout=True)
        self.status.set(msg="anopheles done", percentDone=90, propagate=True)
        self.output.setValue( nc_anopheles )
        