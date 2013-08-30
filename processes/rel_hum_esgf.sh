#!/bin/bash

# calculation of relative humidity for WPS 
# prepared for COREDEX variables 

# call: ./rel_hum.sh path experiment_name bbox start_date end_date


nc_tas=$1
nc_huss=$2
nc_ps=$3
nc_hurs=$4

filename="test.nc"
mkdir ./temp$$
temp=./temp$$

# calculation of vapour pressure
# cdo  mul ../in/ps_$filename -divc,62.2 ../in/huss_$filename ../out/e_$filename

cdo -O merge $nc_ps $nc_huss  $temp/merge_$filename
cdo expr,'e=((ps*huss)/62.2)' $temp/merge_$filename $temp/e_$filename

# partial vapour pressure using Magnus-Formula over water
# cdo expr,'es=6.1078*10^(7.5*(tas-273.16)/(237.3+(tas-273.16)))' ../in/tas_$filename  ../out/es_$filename
cdo expr,'es=6.1078*exp(17.08085*(tas-273.16)/(234.175+(tas-273.16)))' $nc_tas $temp/es_$filename

# calculate relative humidity

cdo -div $temp/e_$filename  $temp/es_$filename $nc_hurs

#setname
# ncatted -O -a units,hurs,o,c,1 $outpath'hurs_'$filename 
# ncatted -O -a standard_name,hurs,o,c,"relative_humidity" $outpath'hurs_'$filename

# cdo -setreftime,1949-12-01,00:00:00,day  $path'pr_'$filename   $outpath'pr_'$filename
# cdo -setreftime,1949-12-01,00:00:00,day  $path'tas_'$filename   $outpath'tas_'$filename
# cdo -setreftime,1949-12-01,00:00:00,day  $path'evspsbl_'$filename   $outpath'evspsbl_'$filename


rm -r /temp$$


exit 0

# 
#        Thorsten comandes:
#        Code: 
#        c167 = tas 
#        c168 = dew point temperature 
#        c157 = relative humidity
#        c416 = 2m specific humidity

#        vapour pressure using Magnus-Formula over water
#        cdo mulc,610.78 -exp -div -mulc,17.5 -subc,273.16 ${filename}_c168_\${year}\${month} -subc,35.86 ${filename}_c168_\${year}\${month} EDDEW_e_tsdx.ieg
#        cdo mulc,610.78 -exp -div -mulc,17.5 -subc,273.16 ${filename}_c167_\${year}\${month} -subc,35.86 ${filename}_c167_\${year}\${month} EDT2M_e_tsdx.ieg

#        echo "...calculate RELHUM2..."
#        cdo chcode,168,157 -mulc,100 -div EDDEW_e_tsdx.ieg EDT2M_e_tsdx.ieg ${filename}_c157_\${year}\${month}

#        echo "...calculate 2m specific humidity..."
#        cdo chcode,168,416 -div -mulc,0.622 EDDEW_e_tsdx.ieg -sub ${filename}_c134_\${year}\${month} -mulc,0.378 EDDEW_e_tsdx.ieg ${filename}_c416_\${year}\${month}