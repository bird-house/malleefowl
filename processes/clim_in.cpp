/*
 * CLIMIN.cpp
 *
 *  Created on  : 01.09.2012
 *      Author  : Nils Hempelmann
 * 		Version : 1.0
 *      INFO    : based on TREESD
 * 				: Input parameter: 	air_temperature 2m mean, 
 * 									precipitation, 
 * 									land area fraction
 * 		History : 	01.09.2012: Test for C3-Grid-INAD cluster use
 * 			  		05.09.2012: Add FR_LAND as possible land_area_fraction variable name
 * 					01.10.2012: Adding the ID code "1,0,0,1,0,0,1 ....,1" as an argument 
 * 								reducing the output to yearly values (climatic mean is calculated external)
 * 								deleting obsolete code 
 * 								18 varables (+ two dummies)
 * 					04.10.2012: write correct time values 
 *
 * 
 * compile with: g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"CLIMIN.d" -MT"CLIMIN.d" -o"CLIMIN.o" "CLIMIN.cpp" -I//usr/include/netcdf-3/
 * 				g++  -o"CLIMIN"  ./CLIMIN.o   -lnetcdf_c++ -lnetcdf
 * 
 * call 		: ./CLIMIN infile outfile vegthres VarID 
 * 
 */

#include <iostream>
#include <netcdf.h>
#include <netcdfcpp.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>

#include <stdio.h> // for printf
// #include <algorithm> // for sort 
#include <vector>
#include <sstream> // for indicator code 

static const int NC_ERR = 2;
using namespace std;

struct dims{
	long int nlat;
	long int nlon;
	long int ntime;
	long int timestp; 
}domain;

class meteo2d
{
public:
     meteo2d();
	~meteo2d();
	 float **param;
private:
	float **Allocate2DArray();
	void Free2DArray(float** Array);
};

/*****************************************************************************/
meteo2d::meteo2d() {
     this->param = Allocate2DArray();
}
/*****************************************************************************/
meteo2d::~meteo2d()
{
	Free2DArray(param);
}
/*****************************************************************************/
float **meteo2d::Allocate2DArray()
{
	float ** arr;
	int i;
   arr = new float*[domain.nlat];
   arr[0] = new float[domain.nlat*domain.nlon];
   for (i = 1; i< domain.nlat; i++){
     arr[i] = arr[i-1] + domain.nlon;
   }
    return arr;
}
/*****************************************************************************/
void meteo2d::Free2DArray(float** Array)
{
    delete [] *Array;
    delete [] Array;
}

class meteo3d
{
public:
     meteo3d();
	~meteo3d();
	 float ***param;
private:
	float ***Allocate3DArray();
	void Free3DArray(float*** Array);
};

/*****************************************************************************/
meteo3d::meteo3d() {
     this->param = Allocate3DArray();
}
/*****************************************************************************/
meteo3d::~meteo3d()
{
	Free3DArray(param);
}
/*****************************************************************************/
float ***meteo3d::Allocate3DArray()
{
	float *** arr;
	int j;
	
   arr = new float**[domain.timestp];
   arr[0] = new float*[domain.timestp*domain.nlat];
   arr[0][0] = new float[domain.timestp*domain.nlat*domain.nlon];
   
   for (int i = 0; i < domain.timestp; i++)
	{
		if (i> 0)
		{
		arr[i] = arr[i-1] + domain.nlat; 
		arr[i][0] = arr[i-1][0] + (domain.nlat * domain.nlon); 
		}
		
		for (j = 1; j < domain.nlat ; j++)
		{ 
		arr[i][j] = arr[i][j-1] + domain.nlon; 
		}
	}
    return arr;
}

/*****************************************************************************/
void meteo3d::Free3DArray(float*** Array)
{
    delete [] **Array;
    delete [] *Array;
    delete [] Array;
}

// #########################################
// Start main program
// #########################################
 
int main(int argc, char *argv[]) {

// Program call CLIMIN infile outfile Tveg 1_0indexcode


	int rec;         // timestep of begin moving Average container        
	int ts = 0;      // timestep of current Values to read 
	int i = 0;
	int j = 0;
	int k = 0;
	
	int count = 0;
	int Tveg = atoi(argv[3]); // Threshold for Temp to start Vegetation period

	// read in the ID code ("1,0,0,1,0,0,1 ....,1")
	
        std::string str = argv[4];
        std::vector<int> climin;
        std::stringstream ss(str);
        
        while (ss >> i)
        {
        climin.push_back(i);
        if (ss.peek() == ',')
                ss.ignore();
        }
	
	NcError ncError(NcError::silent_nonfatal);
	NcFile dataFile(argv[1], NcFile::ReadOnly);
	    // Check to see if the file exist.
	if(!dataFile.is_valid())
	{std::cout << "Input file is not valid"<< std::endl;
    return 0;
     }
     
	if (argc == 5 ){
		std::cout << "Input file    :"<< argv[1]<< std::endl;
		std::cout << "OUTput file   :"<< argv[2]<< std::endl;
		std::cout << "Veg Thhold    :"<< argv[3]<< std::endl;
		std::cout << "CLIMIN code   :"<< argv[4]<< std::endl;
	}
	else {
	std::cout << "Wrong Program call: ./TREESD input outfile VegeThreshold CliminCode "<< std::endl;
	return 0;
	}

	//  read the dimension size

	  NcDim *latDim, *lonDim, *timeDim;

	  if ((dataFile.get_dim("lat")))
	  latDim = dataFile.get_dim("lat");
	  else if (dataFile.get_dim("latitude"))
	  latDim = dataFile.get_dim("latitude");
	  else if (dataFile.get_dim("rlat"))
	  latDim = dataFile.get_dim("rlat");
	  else  std::cout << "*** Latitude ID not found **** " << std::endl;

	  if ((dataFile.get_dim("lon")))
	  lonDim = dataFile.get_dim("lon");
	  else if (dataFile.get_dim("longitude"))
	  lonDim = dataFile.get_dim("longitude");
	  else if (dataFile.get_dim("rlat"))
	  lonDim = dataFile.get_dim("rlat");
	  else  std::cout << "*** Longitude ID not found **** " << std::endl;

	if (!(timeDim = dataFile.get_dim("time")))
	   return NC_ERR;
		
	domain.nlat =  latDim->size();
	domain.nlon =  lonDim->size();
	domain.ntime = timeDim->size();
	domain.timestp = 11;  // number of the stored timesteps for running agerage
     
	std::cout << "*** Lat Dimension  :"<< domain.nlat << std::endl;
	std::cout << "*** Lon Dimension  :"<< domain.nlon << std::endl;
	std::cout << "*** Time Dimension :"<< domain.ntime << std::endl;

	// #########################################
	// read the standard values
    // #########################################
    
	double* latval= NULL, * lonval= NULL;  //Values
	latval= new double[domain.nlat];
	lonval= new double[domain.nlon];

	time_t *timeval = NULL;
	timeval= new time_t[domain.ntime];

    NcVar *latVar, *lonVar, *timeVar;

	   if (dataFile.get_var("lat"))
	     latVar = dataFile.get_var("lat");
	   if (dataFile.get_var("latitude"))
	     latVar = dataFile.get_var("latitude");
	   if (dataFile.get_var("rlat"))
	     latVar = dataFile.get_var("rlat");

	   if (dataFile.get_var("lon"))
	     lonVar = dataFile.get_var("lon");
	   if (dataFile.get_var("longitude"))
	     lonVar = dataFile.get_var("longitude");
	   if (dataFile.get_var("rlon"))
	     lonVar = dataFile.get_var("rlon");

	   if (!(timeVar = dataFile.get_var("time")))
	      return NC_ERR;

	   // Get the lat/lon data from the file.
	   if (!latVar->get(latval, domain.nlat))
	      return NC_ERR;
	   if (!lonVar->get(lonval, domain.nlon))
	      return NC_ERR;
	   if (!timeVar->get(timeval, domain.ntime))
	      return NC_ERR;

 	   std::cout << "Latitude Value :" << latval[5] << std::endl;
   	   std::cout << "Longitude Value:" << lonval[5] << std::endl;

	 //##############################################################
	 // Preparing the output file
	 //##############################################################

     NcFile outFile(argv[2], NcFile::Replace);
       	    // Check to see if the file exist.
       	 if(!outFile.is_valid())
       	    return NC_ERR;
            std::cout << "**** Success creating out file " << std::endl;

	 if (!(latDim = outFile.add_dim("lat", domain.nlat)))
	  return NC_ERR;
	 if (!(lonDim = outFile.add_dim("lon", domain.nlon)))
	  return NC_ERR;
	 if (!(timeDim = outFile.add_dim("time"))) // unlimited dimension
	  return NC_ERR;

	 	 std::cout << "**** Success write dimension " << std::endl;

	 // #### define standard values ##################################

	 if (!(latVar = outFile.add_var("lat", ncDouble, latDim)))
		    return NC_ERR;
	 if (!(lonVar = outFile.add_var("lon", ncDouble, lonDim)))
		    return NC_ERR;
	 if (!(timeVar = outFile.add_var("time", ncDouble, timeDim)))
	 		    return NC_ERR;
 
    if (!timeVar->add_att("standard_name", "time"))
	    return NC_ERR;
    if (!timeVar->add_att("units", "seconds since 1970-01-01 00:00:00"))
	    return NC_ERR;	    
    if (!timeVar->add_att("calendar", "standard"))
	    return NC_ERR;		    
 
	 // #### write lat lon values  ##################################
	 		    
	 if (!latVar->put(&latval[0], domain.nlat))
	      return NC_ERR;
	 if (!lonVar->put(&lonval[0], domain.nlon))
	      return NC_ERR;

	 std::cout << "**** Success write standard values " << std::endl;

	 // ##### define the meteorological attributes #########################################

	 NcVar *sftls_in,  *pr_in,  *tas_in; 
	 NcAtt  *pr_sc,  *tas_sc; 
													
	 // get the pointers to the variables

	 // Precipitation
	 if (dataFile.get_var("pr"))
	 {pr_in = dataFile.get_var("pr");
	 std::cout << "****Variable Precipitation found "<< std::endl;}
	 else if (dataFile.get_var("PRECIP_TOT"))
	 {pr_in = dataFile.get_var("PRECIP_TOT");
	 std::cout << "****Variable Precipitation found "<< std::endl;}
	 else if (dataFile.get_var("var260"))
	 {pr_in = dataFile.get_var("var260");
	 std::cout << "****Variable Precipitation found "<< std::endl;
	 }
	 else if (dataFile.get_var("rr"))
	 {pr_in = dataFile.get_var("rr");
	 pr_sc = pr_in->get_att("scale_factor");
	 std::cout << "****Variable Precipitation found "<< std::endl;
	 std::cout << "****Precipitation scale factor: "<<  pr_sc->as_double(0) << std::endl;
	 }
	 else
	 {
		 std::cout << "****Variable Precipitation NOT found !!! "<< std::endl;
		 return 0;
	 }

	 // Temperature MEAN air 2m
	 if (dataFile.get_var("tas"))
	 {tas_in = dataFile.get_var("tas");
	 std::cout << "****Variable Tmean found "<< std::endl;
	 }
	 else if (dataFile.get_var("T_2M_AV"))
	 {tas_in = dataFile.get_var("T_2M_AV");
	 std::cout << "****Variable Tmean found "<< std::endl;
	 }
	 else if (dataFile.get_var("var167"))
	 {tas_in = dataFile.get_var("var167");
	 std::cout << "****Variable Tmean found "<< std::endl;
	 }
	 else if (dataFile.get_var("tg"))
	 {tas_in = dataFile.get_var("tg");
	 tas_sc = tas_in->get_att("scale_factor");
	 std::cout << "****Variable Tmean found "<< std::endl;
	 }
	 else
	 {
	std::cout << "****Variable Tmean NOT found !!!"<< std::endl;
	 return 0;
	 }

	 // Land surface
	 if (dataFile.get_var("sftls"))
	 sftls_in = dataFile.get_var("sftls");
	 if (dataFile.get_var("FR_LAND"))
	 sftls_in = dataFile.get_var("FR_LAND");
         std::cout << "****Land surface found !!!"<< std::endl;
   
	//##############################################################
	//  create the variables in the outfile 
	//##############################################################
	
	NcVar 	*ts6to8,  	// ID - 1
			*ts5to9,	// ID - 2
			*pr6to8,	// ID - 3 
			*pr5to9,	// ID - 4
			*tgsd,		// ID - 5
			*tgsst,		// ID - 6
			*tstgsd,	// ID - 7
			*tmindorm,  // ID - 8
			*prtgsd,    // ID - 9
			*tstgs0to30,// ID - 10
			*prtgs0to30,// ID - 11
			*hdtgs,    	// ID - 12
			*ddtgs,    	// ID - 13
			*dptgs,    	// ID - 14
			*lfdtgs,    // ID - 15
			*glt200,    // ID - 16
			*gltyear,   // ID - 17
			*prsn,    	// ID - 18
			*dummy01,   // ID - 19
			*dummy02;	// ID - 20
											        
     if (climin[0] > 0){									
     if (!(ts6to8 = outFile.add_var("ts6to8", ncFloat, timeDim, latDim, lonDim)))
        return NC_ERR;}
          
     if (climin[1] > 0){   
     if (!(ts5to9 = outFile.add_var("ts5to9", ncFloat, timeDim, latDim, lonDim)))
		return NC_ERR;}
	  
     if (climin[2] > 0){ 
     if (!(pr6to8 = outFile.add_var("pr6to8", ncFloat, timeDim, latDim, lonDim)))
        return NC_ERR;}
        
     if (climin[3] > 0){ 
     if (!(pr5to9 = outFile.add_var("pr5to9", ncFloat, timeDim, latDim, lonDim)))
        return NC_ERR;}
        
     if (climin[4] > 0){ 
     if (!(tgsd = outFile.add_var("tgsd", ncFloat, timeDim, latDim, lonDim)))
        return NC_ERR;}
        
     if (climin[5] > 0){									
     if (!(tgsst = outFile.add_var("tgsst", ncFloat, timeDim, latDim, lonDim)))
        return NC_ERR;}
        
     if (climin[6] > 0){   
     if (!(tstgsd = outFile.add_var("tstgsd", ncFloat, timeDim, latDim, lonDim)))
		return NC_ERR;}
	  
     if (climin[7] > 0){ 
     if (!(tmindorm = outFile.add_var("tmindorm", ncFloat, timeDim, latDim, lonDim)))
        return NC_ERR;}
        
     if (climin[8] > 0){ 
     if (!(prtgsd = outFile.add_var("prtgsd", ncFloat, timeDim, latDim, lonDim)))
        return NC_ERR;}
        
     if (climin[9] > 0){ 
     if (!(tstgs0to30 = outFile.add_var("tstgs0to30", ncFloat, timeDim, latDim, lonDim)))
        return NC_ERR;}
        
     if (climin[10] > 0){									
     if (!(prtgs0to30 = outFile.add_var("prtgs0to30", ncFloat, timeDim, latDim, lonDim)))
        return NC_ERR;}
        
     if (climin[11] > 0){   
     if (!(hdtgs = outFile.add_var("hdtgs", ncFloat, timeDim, latDim, lonDim)))
		return NC_ERR;}
		
     if (climin[12] > 0){ 
     if (!(ddtgs = outFile.add_var("ddtgs", ncFloat, timeDim, latDim, lonDim)))
        return NC_ERR;}
        
     if (climin[13] > 0){ 
     if (!(dptgs = outFile.add_var("dptgs", ncFloat, timeDim, latDim, lonDim)))
        return NC_ERR;}
        
     if (climin[14] > 0){ 
     if (!(lfdtgs = outFile.add_var("lfdtgs", ncFloat, timeDim, latDim, lonDim)))
        return NC_ERR;}
                  
	 if (climin[15] > 0){									
     if (!(glt200 = outFile.add_var("glt200", ncFloat, timeDim, latDim, lonDim)))
        return NC_ERR;}
        
     if (climin[16] > 0){   
     if (!(gltyear = outFile.add_var("gltyear", ncFloat, timeDim, latDim, lonDim)))
		return NC_ERR;}
		
     if (climin[17] > 0){ 
     if (!(prsn = outFile.add_var("prsn", ncFloat, timeDim, latDim, lonDim)))
         return NC_ERR;}
         
     if (climin[18] > 0){ 
     if (!(dummy01 = outFile.add_var("dummy01", ncFloat, timeDim, latDim, lonDim)))
         return NC_ERR;}
         
     if (climin[19] > 0){ 
     if (!(dummy02 = outFile.add_var("dummy02", ncFloat, timeDim, latDim, lonDim)))
         return NC_ERR;} 


	// Define units attributes for variables.
    if (climin[0] > 0){
    if (!ts6to8->add_att("longname", "temperature sum may to september"))
	    return NC_ERR;
    if (!ts6to8->add_att("standard_name", "air_temperature"))
	    return NC_ERR;
    if (!ts6to8->add_att("units", "C"))
	    return NC_ERR;}
	    
    if (climin[1] > 0){
    if (!ts5to9->add_att("longname", "Precipitation in vegetation period"))
	    return NC_ERR;
    if (!ts5to9->add_att("standard_name", "precipitation_amount"))
	    return NC_ERR;
    if (!ts5to9->add_att("units", "mm"))
	    return NC_ERR;}
	    
    if (climin[2] > 0){
    if (!pr6to8->add_att("longname", "Temperature min in dormance phase"))
	    return NC_ERR;
    if (!pr6to8->add_att("standard_name", "air_temperature"))
	    return NC_ERR;
    if (!pr6to8->add_att("units", "C"))
	    return NC_ERR;}

    if (climin[3] > 0){
    if (!pr5to9->add_att("longname", "Temperature sum in vegetation period"))
	    return NC_ERR;
    if (!pr5to9->add_att("standard_name", "air_temperature"))
	    return NC_ERR;
    if (!pr5to9->add_att("units", "C"))
	    return NC_ERR;}
	    
    if (climin[4] > 0){
    if (!tgsd->add_att("longname", "Precipitation in vegetation period"))
	    return NC_ERR;
    if (!tgsd->add_att("standard_name", "precipitation_amount"))
	    return NC_ERR;
    if (!tgsd->add_att("units", "mm"))
	    return NC_ERR;}
	    
    if (climin[5] > 0){
    if (!tgsst->add_att("longname", "Temperature min in dormance phase"))
	    return NC_ERR;
    if (!tgsst->add_att("standard_name", "air_temperature"))
	    return NC_ERR;
    if (!tgsst->add_att("units", "C"))
	    return NC_ERR;}


    if (climin[6] > 0){
    if (!tstgsd->add_att("longname", "Temperature sum in vegetation period"))
	    return NC_ERR;
    if (!tstgsd->add_att("standard_name", "air_temperature"))
	    return NC_ERR;
    if (!tstgsd->add_att("units", "C"))
	    return NC_ERR;}
	    
    if (climin[7] > 0){
    if (!tmindorm->add_att("longname", "Precipitation in vegetation period"))
	    return NC_ERR;
    if (!tmindorm->add_att("standard_name", "precipitation_amount"))
	    return NC_ERR;
    if (!tmindorm->add_att("units", "mm"))
	    return NC_ERR;}
	    
    if (climin[8] > 0){
    if (!prtgsd->add_att("longname", "Temperature min in dormance phase"))
	    return NC_ERR;
    if (!prtgsd->add_att("standard_name", "air_temperature"))
	    return NC_ERR;
    if (!prtgsd->add_att("units", "C"))
	    return NC_ERR;}

    if (climin[9] > 0){
    if (!tstgs0to30->add_att("longname", "Temperature sum in vegetation period"))
	    return NC_ERR;
    if (!tstgs0to30->add_att("standard_name", "air_temperature"))
	    return NC_ERR;
    if (!tstgs0to30->add_att("units", "C"))
	    return NC_ERR;}
	    
    if (climin[10] > 0){
    if (!prtgs0to30->add_att("longname", "Precipitation in vegetation period"))
	    return NC_ERR;
    if (!prtgs0to30->add_att("standard_name", "precipitation_amount"))
	    return NC_ERR;
    if (!prtgs0to30->add_att("units", "mm"))
	    return NC_ERR;}
	    
    if (climin[11] > 0){
    if (!hdtgs->add_att("longname", "Temperature min in dormance phase"))
	    return NC_ERR;
    if (!hdtgs->add_att("standard_name", "air_temperature"))
	    return NC_ERR;
    if (!hdtgs->add_att("units", "C"))
	    return NC_ERR;}

    if (climin[12] > 0){
    if (!ddtgs->add_att("longname", "Temperature sum in vegetation period"))
	    return NC_ERR;
    if (!ddtgs->add_att("standard_name", "air_temperature"))
	    return NC_ERR;
    if (!ddtgs->add_att("units", "C"))
	    return NC_ERR;}
	    
    if (climin[13] > 0){
    if (!dptgs->add_att("longname", "Precipitation in vegetation period"))
	    return NC_ERR;
    if (!dptgs->add_att("standard_name", "precipitation_amount"))
	    return NC_ERR;
    if (!dptgs->add_att("units", "mm"))
	    return NC_ERR;}
	    
    if (climin[14] > 0){
    if (!lfdtgs->add_att("longname", "Temperature min in dormance phase"))
	    return NC_ERR;
    if (!lfdtgs->add_att("standard_name", "air_temperature"))
	    return NC_ERR;
    if (!lfdtgs->add_att("units", "C"))
	    return NC_ERR;}

    if (climin[15] > 0){
    if (!glt200->add_att("longname", "Temperature sum in vegetation period"))
	    return NC_ERR;
    if (!glt200->add_att("standard_name", "air_temperature"))
	    return NC_ERR;
    if (!glt200->add_att("units", "C"))
	    return NC_ERR;}
	    
    if (climin[16] > 0){
    if (!gltyear->add_att("longname", "Precipitation in vegetation period"))
	    return NC_ERR;
    if (!gltyear->add_att("standard_name", "precipitation_amount"))
	    return NC_ERR;
    if (!gltyear->add_att("units", "mm"))
	    return NC_ERR;}
	    
    if (climin[17] > 0){
    if (!prsn->add_att("longname", "Temperature min in dormance phase"))
	    return NC_ERR;
    if (!prsn->add_att("standard_name", "air_temperature"))
	    return NC_ERR;
    if (!prsn->add_att("units", "C"))
	    return NC_ERR;}

    if (climin[18] > 0){
    if (!dummy01->add_att("longname", "Temperature sum in vegetation period"))
	    return NC_ERR;
    if (!dummy01->add_att("standard_name", "air_temperature"))
	    return NC_ERR;
    if (!dummy01->add_att("units", "C"))
	    return NC_ERR;}
	    
    if (climin[19] > 0){
    if (!dummy02->add_att("longname", "Precipitation in vegetation period"))
	    return NC_ERR;
    if (!dummy02->add_att("standard_name", "precipitation_amount"))
	    return NC_ERR;
    if (!dummy02->add_att("units", "mm"))
	    return NC_ERR;}
	    

std::cout << "**** Success creating Variables in the tmp file " << std::endl;
 

	//#################################################################
	// defining the meteorological 2D and 3D Container to store values 
	//#################################################################

	meteo3d Tmean;   // Temperature container

	meteo2d Prec;    // input values
	meteo2d sftls;
	meteo2d T;       // Temperature values 

	meteo2d Tmove;
	
// ### values container

	meteo2d var_ts6to8;  	// ID - 1
	meteo2d var_ts5to9;		// ID - 2
	meteo2d var_pr6to8;		// ID - 3 
	meteo2d var_pr5to9;		// ID - 4
	meteo2d var_tgsd;		// ID - 5
	meteo2d var_tgsst;		// ID - 6
	meteo2d var_tstgsd;		// ID - 7
	meteo2d var_tmindorm;  	// ID - 8
	meteo2d var_prtgsd;    	// ID - 9
	meteo2d var_tstgs0to30;	// ID - 10
	meteo2d var_prtgs0to30;	// ID - 11
	meteo2d var_hdtgs;    	// ID - 12
	meteo2d var_ddtgs;    	// ID - 13
	meteo2d var_dptgs;    	// ID - 14
	meteo2d var_lfdtgs;    	// ID - 15
	meteo2d var_glt200;    	// ID - 16
	meteo2d var_gltyear;   	// ID - 17
	meteo2d var_prsn;    	// ID - 18
	meteo2d var_dummy01;   	// ID - 19
	meteo2d var_dummy02;	// ID - 20

	

// ### counter
	meteo2d cy  ;     // counter years
	meteo2d v01;      // check vegetation start
	meteo2d c5d;      // five day counter
	meteo2d cvb;      // consecutive days over temperature threthold
	meteo2d cvs;      // consecutive days under temperature threthold 
	meteo2d count_dptgs;

 std::cout << "**** Success alocating space for containers " << std::endl;
 
 	//################################################
	// read in the land/sea mask 
	//################################################
	
  if (sftls_in->num_dims() == 2)
	 {sftls_in->get(&sftls.param[0][0], domain.nlat, domain.nlon);
	 std::cout << "Sucess read in 2D Land Surface " << std::endl;}
  else if (sftls_in->num_dims() == 3 )
	  {sftls_in->get(&sftls.param[0][0], 1, domain.nlat, domain.nlon);
  	  std::cout << "Sucess read in 3D Land Surface " << std::endl;}
  else
	  {std::cout << "Land Surface not readable " << std::endl;
	  return 0;}

  std::cout << "**** surface Value " <<  sftls.param[50][150]<< std::endl ;

   //################################################
   // Start reading the values per time step
   //################################################


for (rec = 0; rec < domain.ntime ; rec++)
{
ts = rec - (domain.timestp - 1) ;  // current timestep for P and T values 

	// get 2D matrix per timestep 
   
   if(ts >= 0 )
	{	
	if(pr_in->num_dims() == 3){	
	pr_in->set_cur(ts,0,0);
	if (!pr_in->get(&Prec.param[0][0], 1, domain.nlat, domain.nlon))
	return NC_ERR;}
    if(pr_in->num_dims() == 4){	
	pr_in->set_cur(ts,0,0,0);
	if (!pr_in->get(&Prec.param[0][0], 1,1, domain.nlat, domain.nlon))
	return NC_ERR;}
    }
  
// get 3D matrix per timestep (plus timesteps used for moving average) 
    if(tas_in->num_dims() == 3)
	  {
	 tas_in->set_cur(rec,0,0);
	  if (!tas_in->get(&T.param[0][0], 1, domain.nlat, domain.nlon))
	 return NC_ERR;
      }
     if(tas_in->num_dims() == 4)
	  {
	 tas_in->set_cur(rec,0,0,0);
	  if (!tas_in->get(&T.param[0][0], 1, 1,  domain.nlat, domain.nlon))
	 return NC_ERR;
      }
 
// std::cout << " ++ get pr and tas ++ " << std::endl;  
 
   //################################################
   // Start reading the 2D grid of one timestep
   //################################################

for( j= 0; j< domain.nlon; j++)
   {
for( i= 0; i< domain.nlat; i++) // loop grid box wise
   {
	   
// initialize the var_tmindorm array
if (rec == 0)
{var_tmindorm.param[i][j] = 100; 
}

if (!(sftls.param[i][j] < 0.5 || sftls.param[i][j] > 1 ))  // land surface mask sftls.param[i][j]<=1.1 &&
	{
	if (Prec.param[i][j] < 0)
	{Prec.param[i][j] = 0;
	// sftls.param[i][j] = 0.0/0.0;
	}

	// Check Temperature Values
    if(dataFile.get_var("tas") || dataFile.get_var("var167") || dataFile.get_var("T_2M_AV"))
		T.param[i][j] = T.param[i][j] -273.15 ;
	if(dataFile.get_var("tg"))
		T.param[i][j]  = T.param[i][j] * (tas_sc->as_double(0));
	   
	if(T.param[i][j] < -60 || T.param[i][j] > 60 )
	{ T.param[i][j] = 0.0/0.0;
	  sftls.param[i][j] = 0.0/0.0;
	}
		
// fill the container for moving average

for (k=0; k< domain.timestp -1; ++k){
Tmean.param[k][i][j] =  Tmean.param[k+1][i][j];
}
Tmean.param[domain.timestp -1][i][j] =  T.param[i][j]; 

   //################################################
   // Start the calculation operations 
   //################################################

// only start operations, when the container for moving average is filled 
// calculate the moving average 	
for (k=0; k < domain.timestp; k++){	
Tmove.param[i][j]+= Tmean.param[k][i][j]; 
}
Tmove.param[i][j] = Tmove.param[i][j] / domain.timestp; 

// if (i== 100 && j == 200)
// 	{
// std::cout << "T  :"<< Tmean.param[domain.timestp -1][i][j]<< "  Pr   :" << Prec.param[i][j] <<" Psum  :"<< var_prtgsd.param[i][j] << "  " <<  ((gmtime(&timeval[rec]))->tm_yday) <<std::endl;	
//	}

	//******************************
	// calculation of standard variables
	//******************************

    // var_ts6to8;		// ID - 1
	// var_ts5to9;		// ID - 2
	// var_pr6to8;		// ID - 3 
	// var_pr5to9;		// ID - 4

if(ts >= 0 )
{		
	if (gmtime(&timeval[ts])->tm_mon >= 4 && gmtime(&timeval[ts])->tm_mon <= 8 )
	{
	var_ts5to9.param[i][j]+= Tmean.param[0][i][j] - Tveg;	
	var_pr5to9.param[i][j]+= Prec.param[i][j];	
	if (gmtime(&timeval[ts])->tm_mon >= 5 && gmtime(&timeval[ts])->tm_mon <= 7 )
	{
	var_ts6to8.param[i][j]+= Tmean.param[0][i][j] - Tveg;	
	var_pr6to8.param[i][j]+= Prec.param[i][j];
	}	
	}

   // var_gltyear;   	// ID - 17
	
/*
 Es werden ab Jahresbeginn alle
positiven mittleren Tagesmittel erfasst und im Januar mit dem Faktor 0,5 multipliziert. Im Februar wird mit dem Faktor 0,75 multipliziert und ab März geht
dann der "volle" Tageswert (mal Faktor 1) in die Rechnung ein.
*/

if (gmtime(&timeval[ts])->tm_mon == 0 && Tmean.param[0][i][j] >= 0)
	var_gltyear.param[i][j]+= (Tmean.param[0][i][j] * 0.5);	
if (gmtime(&timeval[ts])->tm_mon == 1 && Tmean.param[0][i][j] >= 0)
	var_gltyear.param[i][j]+= (Tmean.param[0][i][j] * 0.75);	
if (gmtime(&timeval[ts])->tm_mon >= 2 && Tmean.param[0][i][j] >= 0)
	var_gltyear.param[i][j]+=  Tmean.param[0][i][j];	

  // var_glt200;    	// ID - 16
 
if (var_gltyear.param[i][j] >= 200 && var_glt200.param[i][j] == 0 )
var_glt200.param[i][j] = ((gmtime(&timeval[ts]))->tm_yday + 1); 
	
	
	// var_prsn;    	// ID - 18
if (Tmean.param[0][i][j] < 0 ) 
var_prsn.param[i][j]+= Prec.param[i][j]; 
	
}	
	//******************************
	// dedection of the growing season start and end
	//******************************

	
 // Calculation of Vegetation period  (five day over Temp threshold arg[4] in Tmove)	
if (Tmove.param[i][j] >= Tveg && c5d.param[i][j] == 1  )
cvb.param[i][j]++; 	    // counter vegetation begin condition
else
cvb.param[i][j]= 0;
if (Tmove.param[i][j] <= Tveg && c5d.param[i][j] == 0 && ((gmtime(&timeval[rec]))->tm_mon) > 5 )

cvs.param[i][j]++; 	    // counter vegetation end  condition
else
cvs.param[i][j]= 0;

if (Tmove.param[i][j] >= Tveg)
c5d.param[i][j] = 1;  	// yesterday memory
else
c5d.param[i][j] = 0;	
	
	//******************************
	// calculation of growing season variables 
	//******************************
	
  if ( ts >= 0 &&                                 // exclude the first ten days of the whole timeseries 
   cvb.param[i][j] >= 5 && cvs.param[i][j] <= 5  // five day over temperature threthold
   && ((gmtime(&timeval[ts]))->tm_yday) > 61  	 // Exclude Januar / Februar 
   && ((gmtime(&timeval[ts]))->tm_yday) < 306 )  // Exclude November/December
		{
		// var_tgsd;		// ID - 5
		var_tgsd.param[i][j]++; 		
		
		// var_tgsst;		// ID - 6
		if ( v01.param[i][j] == 0)
		{
		var_tgsst.param[i][j] = ((gmtime(&timeval[ts]))->tm_yday + 1 );
		v01.param[i][j] = 1; 
		}
		
		// var_tstgsd;		// ID - 7
		if ((Tmean.param[0][i][j] - Tveg) >= 0)   // negative values are not taken into account
		var_tstgsd.param[i][j]+= Tmean.param[0][i][j] - Tveg;		
		
		// var_prtgsd;    	// ID - 9
		var_prtgsd.param[i][j]+= Prec.param[i][j];
		
		// var_tstgs0to30;	// ID - 10
		// var_prtgs0to30;	// ID - 11
		if(var_tgsd.param[i][j] <=30)
		{
 		var_tstgs0to30.param[i][j]+= Tmean.param[0][i][j] - Tveg;
		var_prtgs0to30.param[i][j]+= Prec.param[i][j];}
		
		// var_hdtgs;    	// ID - 12
		if (Tmean.param[0][i][j] >= 30)
		var_hdtgs.param[i][j]++; 
		
		// var_ddtgs;    	// ID - 13
		// var_dptgs;    	// ID - 14
		if (Prec.param[i][j] <= 0.1 && Tmean.param[0][i][j] >= 10)
		{   var_ddtgs.param[i][j]++; 
			count_dptgs.param[i][j]++;
				if (count_dptgs.param[i][j]> var_dptgs.param[i][j])
				var_dptgs.param[i][j]= count_dptgs.param[i][j];
		}
		else count_dptgs.param[i][j] = 0;
		
		
		// var_lfdtgs;    	// ID - 15
		if (Tmean.param[0][i][j] <= 0 && ((gmtime(&timeval[ts]))->tm_mon) <=6)  // including July 
		var_lfdtgs.param[i][j]++; 
		
		
		} // end of vegetation loop 
		else // start domance loop
		{
		// var_tmindorm;  	// ID - 8
		if (!(var_tmindorm.param[i][j] < Tmove.param[i][j]))
			{
			var_tmindorm.param[i][j] =  Tmove.param[i][j];   // calculation of absolute min temperature
			}
		} // end of dormance loop

	//################################################
	// Calculation yearly values and saveing to multi year container
	//################################################

//if (!(((gmtime(&timeval[rec]))->tm_year) == ((gmtime(&timeval[rec+1]))->tm_year)))
//{
//			    cy.param[i][j]++;        // year counter		    	       
//}   // end of Calculation yearly values and saveing to multi year container		
}     // end of land surface mask
else  // cut out sea areas
{
	
	 var_ts6to8.param[i][j]    = 0.0/0.0;  	// ID - 1
	 var_ts5to9.param[i][j]    = 0.0/0.0;	// ID - 2
	 var_pr6to8.param[i][j]    = 0.0/0.0;	// ID - 3 
	 var_pr5to9.param[i][j]    = 0.0/0.0;	// ID - 4
	 var_tgsd.param[i][j]      = 0.0/0.0;	// ID - 5
	 var_tgsst.param[i][j]     = 0.0/0.0;	// ID - 6
	 var_tstgsd.param[i][j]    = 0.0/0.0;	// ID - 7
	 var_tmindorm.param[i][j]  = 0.0/0.0;  	// ID - 8
	 var_prtgsd.param[i][j]    = 0.0/0.0;   // ID - 9
	 var_tstgs0to30.param[i][j]  = 0.0/0.0;	// ID - 10
	 var_prtgs0to30.param[i][j]  = 0.0/0.0;	// ID - 11
	 var_hdtgs.param[i][j]    = 0.0/0.0;    // ID - 12
	 var_ddtgs.param[i][j]    = 0.0/0.0;    // ID - 13
	 var_dptgs.param[i][j]    = 0.0/0.0;    // ID - 14
	 var_lfdtgs.param[i][j]   = 0.0/0.0;    // ID - 15
	 var_glt200.param[i][j]   = 0.0/0.0;    // ID - 16
	 var_gltyear.param[i][j]  = 0.0/0.0;   	// ID - 17
	 var_prsn.param[i][j]     = 0.0/0.0;    // ID - 18
	 var_dummy01.param[i][j]  = 0.0/0.0;   	// ID - 19
	 var_dummy02.param[i][j]  = 0.0/0.0;	// ID - 20
}
}}// end of lat lon box

	//################################################
	// write the yearly values to out file 
	//################################################

if (!((gmtime(&timeval[rec]))->tm_year == gmtime(&timeval[rec+1])->tm_year))
	{
			
	timeVar->set_cur(count);
	timeVar->put(&timeval[rec], 1);
	

	
	if (climin[0] > 0){
	     ts6to8->set_cur(count,0,0);
	if (!ts6to8 ->put(&var_ts6to8.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}
		
	if (climin[1] > 0){
	     ts5to9->set_cur(count,0,0);
	if (!ts5to9->put(&var_ts5to9.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}
				
	if (climin[2] > 0){
	     pr6to8->set_cur(count,0,0);	
	if (!pr6to8->put(&var_pr6to8.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}
		
	if (climin[3] > 0){
	  pr5to9->set_cur(count,0,0);
	if (!pr5to9->put(&var_pr5to9.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}
		
	if (climin[4] > 0){
	  tgsd->set_cur(count,0,0);
	if (!tgsd->put(&var_tgsd.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}

	if (climin[5] > 0){
	     tgsst->set_cur(count,0,0);
	if (!tgsst ->put(&var_tgsst.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}
		
	if (climin[6] > 0){
	     tstgsd->set_cur(count,0,0);
	if (!tstgsd->put(&var_tstgsd.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}
				
	if (climin[7] > 0){
	     tmindorm->set_cur(count,0,0);	
	if (!tmindorm->put(&var_tmindorm.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}
		
	if (climin[8] > 0){
	  prtgsd->set_cur(count,0,0);
	if (!prtgsd->put(&var_prtgsd.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}
		
	if (climin[9] > 0){
	  tstgs0to30->set_cur(count,0,0);
	if (!tstgs0to30->put(&var_tstgs0to30.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}

	if (climin[10] > 0){
	     prtgs0to30->set_cur(count,0,0);
	if (!prtgs0to30 ->put(&var_prtgs0to30.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}
		
	if (climin[11] > 0){
	     hdtgs->set_cur(count,0,0);
	if (!hdtgs->put(&var_hdtgs.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}
				
	if (climin[12] > 0){
	     ddtgs->set_cur(count,0,0);	
	if (!ddtgs->put(&var_ddtgs.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}
		
	if (climin[13] > 0){
	  dptgs->set_cur(count,0,0);
	if (!dptgs->put(&var_dptgs.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}
		
	if (climin[14] > 0){
	  lfdtgs->set_cur(count,0,0);
	if (!lfdtgs->put(&var_lfdtgs.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}

	if (climin[15] > 0){
	     glt200->set_cur(count,0,0);
	if (!glt200 ->put(&var_glt200.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}
		
	if (climin[16] > 0){
	     gltyear->set_cur(count,0,0);
	if (!gltyear->put(&var_gltyear.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}
				
	if (climin[17] > 0){
	     prsn->set_cur(count,0,0);	
	if (!prsn->put(&var_prsn.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}
		
	if (climin[18] > 0){
	  dummy01->set_cur(count,0,0);
	if (!dummy01->put(&var_dummy01.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}
		
	if (climin[19] > 0){
	  dummy02->set_cur(count,0,0);
	if (!dummy02->put(&var_dummy02.param[0][0], 1, domain.nlat, domain.nlon))
		return NC_ERR;}

	count++;

	//################################################
	// Print out check values and set containers back
	//################################################

for( j= 0; j< domain.nlon; j++)
   {
for( i= 0; i< domain.nlat; i++) // loop grid box wise
   {
			if (i== 100 && j == 200)
			{ 
				
			std::cout << "************************************"       <<"                "<< std::endl;				
			std::cout << "****** Saving the Year : " << ((gmtime(&timeval[rec]))->tm_year) + 1900 << "*********" << std::endl;
		//	std::cout << "****** Year as int : " << year << "*********" << std::endl;
			std::cout << "**** " <<((gmtime(&timeval[rec]))->tm_yday) + 1 << " *******" << cy.param[i][j]  << std::endl;
			std::cout << "************************************"       <<"                "<< std::endl;
			std::cout << "vegetation lenght   :" << var_tgsd.param[i][j]  <<"                "<< std::endl;
			std::cout << "vegetation start    :" << var_tgsst.param[i][j]   <<"                "<< std::endl;
			std::cout << "P                   :" << Prec.param[i][j]  <<"                "<< std::endl;
			std::cout << "P sum               :" << var_prtgsd.param[i][j] <<"                "<< std::endl;
			std::cout << "T                   :" << T.param[i][j]     <<"                "<< std::endl;
			std::cout << "var_tstgsd          :" << var_tstgsd.param[i][j]<<"                "<< std::endl;
			std::cout << "var_tmindorm        :" << var_tmindorm.param[i][j] <<"                "<< std::endl;
			std::cout << "************************************"       <<"                "<< std::endl;			
			}
			
	var_ts6to8.param[i][j]     = 0; // ID - 1
	var_ts5to9.param[i][j]     = 0;	// ID - 2
	var_pr6to8.param[i][j]     = 0;	// ID - 3 
	var_pr5to9.param[i][j]     = 0;	// ID - 4
	var_tgsd.param[i][j]       = 0;	// ID - 5
	var_tgsst.param[i][j]      = 0;	// ID - 6
	var_tstgsd.param[i][j]     = 0;	// ID - 7
	var_tmindorm.param[i][j]   = 88888; // ID - 8
	var_prtgsd.param[i][j]     = 0; // ID - 9
	var_tstgs0to30.param[i][j] = 0;	// ID - 10
	var_prtgs0to30.param[i][j] = 0;	// ID - 11
	var_hdtgs.param[i][j]      = 0; // ID - 12
	var_ddtgs.param[i][j]      = 0; // ID - 13
	var_dptgs.param[i][j]      = 0; // ID - 14
	var_lfdtgs.param[i][j]     = 0; // ID - 15
	var_glt200.param[i][j]     = 0; // ID - 16
	var_gltyear.param[i][j]    = 0; // ID - 17
	var_prsn.param[i][j]       = 0; // ID - 18
	var_dummy01.param[i][j]    = 0; // ID - 19
	var_dummy02.param[i][j]    = 0;	// ID - 20			
	v01.param[i][j]          = 0;
				
}} // end print out check values    
}  // end write the yearly values to tmp file 
}  // end of timesteps

	     std::cout << "*** Saving the file  ***"<< std::endl;
     	 std::cout << "***********************"<< std::endl;
		 std::cout << "******** FINE *********"<< std::endl;
	     std::cout << "***********************"<< std::endl;
 return 0;
}
