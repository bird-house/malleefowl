bind = 'unix://${prefix}/var/run/adagucserver.socket'
workers = 3

# environment
raw_env = [
    "ADAGUC_CONFIG=${prefix}/etc/adagucserver/autowms.xml",
    "ADAGUC_LOGFILE=${prefix}/var/log/adaguc.log",
    "ADAGUC_ERRORFILE=${prefix}/var/log/adaguc_error.log",
    "ADAGUC_DATARESTRICTION=SHOW_QUERYINFO|ALLOW_GFI|ALLOW_METADATA",
    "PATH=${prefix}/bin:/usr/bin:/bin:/usr/local/bin",
    "GDAL_DATA=${prefix}/share/gdal",
    ]                                                                                                              

# logging
errorlog = '-'
accesslog = '-'
