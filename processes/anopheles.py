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
        nc_n4 = NetCDFFile(nc_output_file , 'w')
        
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
        
        tas = np.squeeze(tasFile.variables["tas"])
        pr = np.squeeze(prFile.variables["pr"])
        hurs = np.squeeze(tasFile.variables["hurs"])
        evspsbl = np.squeeze(tasFile.variables["evspsbl"])

        ## get the right values 

        #RH <- RH * 100
        #Ta <- Ta -273.15
        #Rt <- Rt * 86400 *6     # factor 6 due to an error in Cordex data
        #Et <- Et * -86400 * 6 

        #Increase_Ta <- 0
        ##Evaporation (> -8)
        #Increase_Et<- 0
        ##Rainfall (> -150)
        #Increase_Rt <- 0
        ##Relative Humidity in (-97,+39)
        #Increase_RH <- 0

        ##Calculating new values
        #Ta <- Ta + Increase_Ta
        #Et <- ifelse(Et + Increase_Et >= 0,Et + Increase_Et,0)
        #Rt <- ifelse(Rt + Increase_Rt >= 0,Rt + Increase_Rt,0)
        #RH <- ifelse(RH + Increase_RH >=0 & RH + Increase_RH<=100,RH + Increase_RH,ifelse(RH + Increase_RH >= 100,100,0))

        ## Text
        #deltaT <- 6.08
        #Tw <- Ta + deltaT
        #Vt <- NULL; Vt[1] <- 1000
        #h0 <- 97
        #AT <- 1.79*10^6
        #D <- NULL; D[1] <- 0
        #b <- ifelse(abs(deltaT)<4, 0.89, 0.88)
        #lambda <- 1.5
        #m <- 1000
        #for(i in 1:(dim_time -1) ){
            #Vt[i+1] <- ifelse(Vt[i] == 0 && Rt[i] == 0, 0, (Vt[i] + AT*Rt[i]/1000)*(1 - 3*Et[i]/h0*(Vt[1]/(Vt[i]+AT*Rt[i]/1000))^(1/3)))
            #Vt[i+1] <- ifelse(Vt[i+1] <= 0, 0, Vt[i+1])
            #if(Vt[i+1] == 0){
            #D[i+1] <- D[i] + 1
            #} else {
            #D[i+1] <- 0
            #}
        #}

        #beta2 = 4*10^(-6)*RH^2 - 1.09*10^(-3)*RH - 0.0255
        #beta1 = -2.32*10^(-4)*RH^2 + 0.0515*RH + 1.06
        #beta0 = 1.13*10^(-3)*RH^2 - 0.158*RH - 6.61

        #p4 <- exp(-1/(beta2*Ta^2 + beta1*Ta + beta0))

        #d <- matrix(0,dim_time,4)
        #d[,1] <- ifelse(Vt != 0, 1.011 + 20.212*(1 + (Tw/12.096)^4.839)^(-1), 1.011 + 20.212*(1 + (Ta/12.096)^4.839)^(-1))
        #d[,2] <- ifelse(Vt != 0, 8.130 + 13.794*(1 + (Tw/20.742)^8.946)^(-1) - d[,1], 8.130 + 13.794*(1 + (Ta/20.742)^8.946)^(-1) - d[,1])
        #d[,3] <- ifelse(Vt != 0, 8.560 + 20.654*(1 + (Tw/19.759)^6.827)^(-1) - d[,2], 8.560 + 20.654*(1 + (Ta/19.759)^6.827)^(-1) - d[,2])
        #d[,4] <- -1/log(p4)

        #p_Tw <- p_Rt <- p_D <- matrix(0,dim_time,3)
        #p_Tw[,1] <- ifelse(Vt != 0,ifelse(Ta >= 14 & Ta <= 40,exp(-1/d[,1]),0),ifelse(Ta >= 14 & Ta <= 40,exp(-1/d[,1]),0))
        #p_Tw[,2] <- ifelse(Vt != 0,ifelse(Tw >= 18 & Tw <= 32,exp(-1/d[,2]),0),ifelse(Tw >= 18 & Tw <= 32,exp(-1/d[,2]),0))
        #p_Tw[,3] <- ifelse(Vt != 0,ifelse(Tw >= 18 & Tw <= 32,exp(-1/d[,3]),0),ifelse(Tw >= 18 & Tw <= 32,exp(-1/d[,3]),0))

        #p_Rt[,1] <- exp(-0.0242*Rt)
        #p_Rt[,2] <- exp(-0.0127*Rt)
        #p_Rt[,3] <- exp(-0.00618*Rt)

        #p_D[,1] <- 2*exp(-0.405*D)/(1 + exp(-0.405*D))
        #p_D[,2] <- 2*exp(-0.855*D)/(1 + exp(-0.855*D))
        #p_D[,3] <- 2*exp(-0.602*D)/(1 + exp(-0.602*D))

        #n1 <- n2 <- n3 <- n4 <- 100
        #P <- p <- N <- matrix(0,dim_time,4); N[1,] <- c(n1, n2, n3, n4)
        #G <- matrix(0,dim_time,3)
        #ft <- Gc_Ta <- F4 <- N23 <- p_DD <- matrix(0,dim_time,1)
        #De <- 37.1; Te <- 7.7; Nep <- 120
        #alpha1 <- 280.486; alpha2<- 0.025616
        #for(i in 1:(dim_time-1)){
            #p_DD[i] <- ifelse(Vt[i] != 0, (b*m/(1000*(N[i,2]+N[i,3])/Vt[i]))*(1 - lambda^lambda/(lambda +(1000*(N[i,2]+N[i,3])/Vt[i])/m)^lambda), 1)
            #p[i,] <- c(p_Tw[i,1]*p_Rt[i,1]*p_D[i,1],p_Tw[i,2]*p_Rt[i,2]*p_D[i,2]*p_DD[i],p_Tw[i,3]*p_Rt[i,3]*p_D[i,3]*p_DD[i],p4[i])
            #for(j in 1:4){
                #P[i,j] <- (p[i,j] - p[i,j]^(d[i,j]))/(1 - p[i,j]^d[i,j])
            #}
            #for(j in 1:3){
                #G[i,j] <- (1 - p[i,j])/(1 - p[i,j]^d[i,j])*p[i,j]^d[i,j]
            #}
            #ft[i] <- 0.518*exp(-6*(N[i,2]/Vt[i] - 0.317)^2) + 0.192
            #Gc_Ta[i] <- 1 + De/(Ta[i] - Te)
            #F4[i] <- ft[i]*Nep/Gc_Ta[i]
            #N[i+1,] <- matrix(c(P[i,1],G[i,1],0,0,0,P[i,2],G[i,2],0,0,0,P[i,3],G[i,3],alpha1*F4[i],0,0,P[i,4]), nrow = 4, ncol = 4) %*% N[i,]
        #}
        #p[dim_time,] <- c(p_Tw[dim_time,1]*p_Rt[dim_time,1]*p_D[dim_time,1],p_Tw[dim_time,2]*p_Rt[dim_time,2]*p_D[dim_time,2]*p_DD[dim_time],p_Tw[dim_time,3]*p_Rt[dim_time,3]*p_D[dim_time,3]*p_DD[dim_time],p4[dim_time])

        #var.put.nc(nc_n4, "n4" , N[,4], start=c(x,y,1), count=c(1,1,dim_time), na.mode=0, pack=FALSE) # , na.mode=0, pack=FALSE

        #cat("processed coordinate x: ",x," y: " ,y,"\n")   


        #}} # end of lat lon loop

        
        
        
        self.cmd(cmd=["R", "--vanilla", "--args", nc_tas, nc_hurs, nc_pr, nc_evspsbl, nc_anopheles, "<", script_anophels ], stdout=True)
        self.status.set(msg="anopheles done", percentDone=90, propagate=True)
        self.output.setValue( nc_anopheles )
        