# to calculate the dynamic of Anopheles Gambiae 

library(sp)
library(raster)
library(ncdf)
library(RNetCDF)

rm(list = ls())

# get the argumets 
# setwd("/home/main/nils/anopheles/")

# args <- commandArgs(trailingOnly = TRUE)



args <- unlist(strsplit(commandArgs(trailingOnly = TRUE), " "))

arguments <- "/home/main/data/tas_AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_20060101_20101231.nc /home/main/data/hurs_AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_20060101_20101231.nc /home/main/data/pr_AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_20060101_20101231.nc /home/main/data/evspsbl_AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_20060101_20101231.nc /home/main/wps_data/n4_AFR-44_MPI-ESM-LR_rcp85_r1i1p1_MPI-RCSM-v2012_v1_day_20060101_20101231.nc"
args <- unlist(strsplit(arguments, " "))

print(args)

# open appropriate files 

nc_tas    <- open.nc(paste(args[1]), write=FALSE)
nc_hurs   <- open.nc(paste(args[2]), write=FALSE)
nc_pr     <- open.nc(paste(args[3]), write=FALSE)
nc_evspsbl<- open.nc(paste(args[4]), write=FALSE)



# get the shape of the domain

dim_time <- dim.inq.nc(nc_tas, "time")$length
dim_lat <- dim.inq.nc(nc_tas, "rlat")$length
dim_lon <- dim.inq.nc(nc_tas, "rlon")$length

dim_lat <- 50
dim_lon <- 50

# generate the N4 output file
# 
nc_n4 <- create.nc(paste(args[5]), clobber=TRUE, large=FALSE, share=FALSE)

dim.def.nc(nc_n4, "lon", dim_lon)
dim.def.nc(nc_n4, "lat", dim_lat)
dim.def.nc(nc_n4, "time", unlim=TRUE)

##  Create two variables, one as coordinate variable
var.def.nc(nc_n4, "time", "NC_INT", "time")
# var.def.nc(nc_n4, "lat", "NC_INT", "time")
# var.def.nc(nc_n4, "lon", "NC_INT", "time")
var.def.nc(nc_n4, "n4", "NC_FLOAT", c(0,1,2))

att.put.nc(nc_n4, "n4", "missing_value", "NC_FLOAT", -9e+33)

# read in values 

for(x in 1:dim_lat){
  for(y in 1:dim_lon){
    
Et <- var.get.nc(nc_evspsbl, variable='evspsbl', start=c(x,y,1), count=c(1,1,dim_time), na.mode=0, collapse=TRUE, unpack=FALSE)
Rt <- var.get.nc(nc_pr, variable='pr', start=c(x,y,1), count=c(1,1,dim_time), na.mode=0, collapse=TRUE, unpack=FALSE)
Ta <- var.get.nc(nc_tas, variable='tas', start=c(x,y,1,1), count=c(1,1,1,dim_time), na.mode=0, collapse=TRUE, unpack=FALSE)
RH <- var.get.nc(nc_hurs, variable='hurs', start=c(x,y,1), count=c(1,1,dim_time), na.mode=0, collapse=TRUE, unpack=FALSE)

# get the right values 

RH <- RH * 100
Ta <- Ta -273.15
Rt <- Rt * 86400 *6     # factor 6 due to an error in Cordex data
Et <- Et * -86400 * 6 

Increase_Ta <- 0
#Evaporation (> -8)
Increase_Et<- 0
#Rainfall (> -150)
Increase_Rt <- 0
#Relative Humidity in (-97,+39)
Increase_RH <- 0

#Calculating new values
Ta <- Ta + Increase_Ta
Et <- ifelse(Et + Increase_Et >= 0,Et + Increase_Et,0)
Rt <- ifelse(Rt + Increase_Rt >= 0,Rt + Increase_Rt,0)
RH <- ifelse(RH + Increase_RH >=0 & RH + Increase_RH<=100,RH + Increase_RH,ifelse(RH + Increase_RH >= 100,100,0))

# Text
deltaT <- 6.08
Tw <- Ta + deltaT
Vt <- NULL; Vt[1] <- 1000
h0 <- 97
AT <- 1.79*10^6
D <- NULL; D[1] <- 0
b <- ifelse(abs(deltaT)<4, 0.89, 0.88)
lambda <- 1.5
m <- 1000
for(i in 1:(dim_time -1) ){
	Vt[i+1] <- ifelse(Vt[i] == 0 && Rt[i] == 0, 0, (Vt[i] + AT*Rt[i]/1000)*(1 - 3*Et[i]/h0*(Vt[1]/(Vt[i]+AT*Rt[i]/1000))^(1/3)))
	Vt[i+1] <- ifelse(Vt[i+1] <= 0, 0, Vt[i+1])
	if(Vt[i+1] == 0){
	  D[i+1] <- D[i] + 1
	} else {
	  D[i+1] <- 0
	}
}

beta2 = 4*10^(-6)*RH^2 - 1.09*10^(-3)*RH - 0.0255
beta1 = -2.32*10^(-4)*RH^2 + 0.0515*RH + 1.06
beta0 = 1.13*10^(-3)*RH^2 - 0.158*RH - 6.61

p4 <- exp(-1/(beta2*Ta^2 + beta1*Ta + beta0))

d <- matrix(0,dim_time,4)
d[,1] <- ifelse(Vt != 0, 1.011 + 20.212*(1 + (Tw/12.096)^4.839)^(-1), 1.011 + 20.212*(1 + (Ta/12.096)^4.839)^(-1))
d[,2] <- ifelse(Vt != 0, 8.130 + 13.794*(1 + (Tw/20.742)^8.946)^(-1) - d[,1], 8.130 + 13.794*(1 + (Ta/20.742)^8.946)^(-1) - d[,1])
d[,3] <- ifelse(Vt != 0, 8.560 + 20.654*(1 + (Tw/19.759)^6.827)^(-1) - d[,2], 8.560 + 20.654*(1 + (Ta/19.759)^6.827)^(-1) - d[,2])
d[,4] <- -1/log(p4)

p_Tw <- p_Rt <- p_D <- matrix(0,dim_time,3)
p_Tw[,1] <- ifelse(Vt != 0,ifelse(Ta >= 14 & Ta <= 40,exp(-1/d[,1]),0),ifelse(Ta >= 14 & Ta <= 40,exp(-1/d[,1]),0))
p_Tw[,2] <- ifelse(Vt != 0,ifelse(Tw >= 18 & Tw <= 32,exp(-1/d[,2]),0),ifelse(Tw >= 18 & Tw <= 32,exp(-1/d[,2]),0))
p_Tw[,3] <- ifelse(Vt != 0,ifelse(Tw >= 18 & Tw <= 32,exp(-1/d[,3]),0),ifelse(Tw >= 18 & Tw <= 32,exp(-1/d[,3]),0))

p_Rt[,1] <- exp(-0.0242*Rt)
p_Rt[,2] <- exp(-0.0127*Rt)
p_Rt[,3] <- exp(-0.00618*Rt)

p_D[,1] <- 2*exp(-0.405*D)/(1 + exp(-0.405*D))
p_D[,2] <- 2*exp(-0.855*D)/(1 + exp(-0.855*D))
p_D[,3] <- 2*exp(-0.602*D)/(1 + exp(-0.602*D))

n1 <- n2 <- n3 <- n4 <- 100
P <- p <- N <- matrix(0,dim_time,4); N[1,] <- c(n1, n2, n3, n4)
G <- matrix(0,dim_time,3)
ft <- Gc_Ta <- F4 <- N23 <- p_DD <- matrix(0,dim_time,1)
De <- 37.1; Te <- 7.7; Nep <- 120
alpha1 <- 280.486; alpha2<- 0.025616
for(i in 1:(dim_time-1)){
	p_DD[i] <- ifelse(Vt[i] != 0, (b*m/(1000*(N[i,2]+N[i,3])/Vt[i]))*(1 - lambda^lambda/(lambda +(1000*(N[i,2]+N[i,3])/Vt[i])/m)^lambda), 1)
	p[i,] <- c(p_Tw[i,1]*p_Rt[i,1]*p_D[i,1],p_Tw[i,2]*p_Rt[i,2]*p_D[i,2]*p_DD[i],p_Tw[i,3]*p_Rt[i,3]*p_D[i,3]*p_DD[i],p4[i])
	for(j in 1:4){
		P[i,j] <- (p[i,j] - p[i,j]^(d[i,j]))/(1 - p[i,j]^d[i,j])
	}
	for(j in 1:3){
		G[i,j] <- (1 - p[i,j])/(1 - p[i,j]^d[i,j])*p[i,j]^d[i,j]
	}
	ft[i] <- 0.518*exp(-6*(N[i,2]/Vt[i] - 0.317)^2) + 0.192
	Gc_Ta[i] <- 1 + De/(Ta[i] - Te)
	F4[i] <- ft[i]*Nep/Gc_Ta[i]
	N[i+1,] <- matrix(c(P[i,1],G[i,1],0,0,0,P[i,2],G[i,2],0,0,0,P[i,3],G[i,3],alpha1*F4[i],0,0,P[i,4]), nrow = 4, ncol = 4) %*% N[i,]
}
p[dim_time,] <- c(p_Tw[dim_time,1]*p_Rt[dim_time,1]*p_D[dim_time,1],p_Tw[dim_time,2]*p_Rt[dim_time,2]*p_D[dim_time,2]*p_DD[dim_time],p_Tw[dim_time,3]*p_Rt[dim_time,3]*p_D[dim_time,3]*p_DD[dim_time],p4[dim_time])

var.put.nc(nc_n4, "n4" , N[,4], start=c(x,y,1), count=c(1,1,dim_time), na.mode=0, pack=FALSE) # , na.mode=0, pack=FALSE

cat("processed coordinate x: ",x," y: " ,y,"\n")   


}} # end of lat lon loop

close.nc(nc_n4)
close.nc(nc_evspsbl)
close.nc(nc_tas)
close.nc(nc_pr)
close.nc(nc_hurs)


# 
# ##Saving Plots
# workingdir <- "./plots"
# data_value <- paste('dTa=',Increase_Ta,',dEt=',Increase_Et,',dRt=',Increase_Rt,',dRH=',Increase_RH)
# dir.create(file.path(workingdir,data_value))
# setwd(file.path(workingdir,data_value))
# 
# mosquitoplot <- function(DATA,TITLE,NAME){
# 	jpeg(NAME, width = 1920, height = 1080)
# 	par(mfrow=c(2,2))
# 	plot(DATA[,1],type="l")
# 	title(main='EGGS')
# 	plot(DATA[,2],type="l")
# 	title(main='LARVAE')
# 	plot(DATA[,3],type="l")
# 	title(main='PUPAE')
# 	plot(DATA[,4],type="l")
# 	title(main='ADULTS')
# 	title(main=paste(TITLE,data_value),outer=TRUE,line=-1)
# 	dev.off()
# }
# 
# 
# jpeg('data_plot.jpg', width = 1920, height = 1080)
# par(mfrow=c(2,2))
# plot(Ta,type="l")
# title(main='AIR TEMPERATURE')
# plot(Et,type="l")
# title(main='EVAPORATION')
# plot(Rt,type="l")
# title(main='RAINFALL')
# plot(RH,type="l")
# title(main='RELATIVE HUMIDITY')
# title(main=paste('DATA',data_value),outer=TRUE,line=-1)
# dev.off()
# 
# jpeg('Vt_plot.jpg', width = 1920, height = 1080)
# plot(Vt,type="l")
# title(main=paste('BREEDING SITE VOLUME',data_value),outer=TRUE,line=-1)
# dev.off()
# 
# mosquitoplot(log(N),'MOSQUITO POPULATION','N_plot.jpg')
# mosquitoplot(P,'PROPORTION OF VECTORS SURVIVING AND REMAINING','PR_plot.jpg')
# mosquitoplot(p,'DAILY SURVIVAL PROBABILITY','p_plot.jpg')
# mosquitoplot(d,'AVERAGE TIME SPENT IN STAGE','d_plot.jpg')
# mosquitoplot(cbind(G,F4),'PROPORTION SURVIVING AND PROGRESSING + AVERAGE NUMBER OF EGGS LAID PER DAY PER FEMALE ADULT','G_plot.jpg')

