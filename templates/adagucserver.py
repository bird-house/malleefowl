import os

def parse_output(output):
    lines = output.split('\n')
    if 'Content-Type' in lines[0]:
        content_type = lines[0].split(':')[1]
        content_type = content_type.strip()
    data = '\n'.join(lines[2:-1])
    return content_type,data

def app(environ, start_response):
    from subprocess import check_output, STDOUT
    data = None
    try:
        # fake cgi parameters
        """
        QUERY_STRING       
        REQUEST_METHOD     
        CONTENT_TYPE       
        CONTENT_LENGTH     

        SCRIPT_FILENAME 
        SCRIPT_NAME     
        REQUEST_URI        
        DOCUMENT_URI       
        DOCUMENT_ROOT      
        SERVER_PROTOCOL    

        GATEWAY_INTERFACE  CGI/1.1;
        SERVER_SOFTWARE    

        REMOTE_ADDR        
        REMOTE_PORT        
        SERVER_ADDR        
        SERVER_PORT        
        SERVER_NAME        
        """
        params = [
            'QUERY_STRING',
            'REQUEST_METHOD',
            'SERVER_PROTOCOL',
            'SERVER_SOFTWARE',
            'REMOTE_ADDR',
            'SERVER_PORT',
            'SERVER_NAME'
            ]
        
        for param in params:
            os.environ[param] = environ[param]
        os.environ['GATEWAY_INTERFACE'] = 'CGI/1.1'

        # adaguc parameters
        os.environ['ADAGUC_CONFIG'] = '${prefix}/etc/adagucserver/autowms.xml'
        os.environ['ADAGUC_LOGFILE']= '${prefix}/var/log/adaguc.log'
        os.environ['ADAGUC_ERRORFILE'] = '${prefix}/var/log/adaguc_error.log'
        os.environ['ADAGUC_DATARESTRICTION'] = "SHOW_QUERYINFO|ALLOW_GFI|ALLOW_METADATA"
        output = check_output(['adagucserver'], stderr=STDOUT)
        content_type, data = parse_output(output)
        #raise Exception(str(data))
        start_response("200 OK", [
            ("Content-Type", content_type),
            #("Content-Length", str(len(data)))
            ])
    except Exception as e:
        data = "Message:<br/>"
        data += e.message

        # TODO: returncode 143 = application was killed
            
        start_response("200 OK", [
            ("Content-Type", "text/html"),
            ("Content-Length", str(len(data)))
            ])
    return iter([data])
