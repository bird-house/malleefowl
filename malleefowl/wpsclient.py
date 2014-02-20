def _execute(service, identifier, inputs=[], outputs=[], openid=None, password=None, file_identifiers=None):
    import urllib2
    from owslib.wps import WebProcessingService, monitorExecution

    wps = WebProcessingService(service, verbose=False)
    if openid != None:
        inputs.append( ("openid", openid) )
    if password != None:
        inputs.append( ("password", password) )
    if file_identifiers != None:
        for file_identifier in file_identifiers.split(','):
            inputs.append( ("file_identifier", file_identifier) )
    execution = wps.execute(identifier, inputs=inputs, output=outputs)
    monitorExecution(execution, sleepSecs=1)
    status = execution.status
    print status
    print execution.processOutputs
    for out in execution.processOutputs:
        print out.reference
    #output = execution.processOutputs[0].reference

def main():
    import optparse

    parser = optparse.OptionParser()
    parser.add_option('-v', '--verbose',
                      dest="verbose",
                      default=False,
                      action="store_true",
                      help="verbose mode"
                      )
    parser.add_option('-s', '--service',
                      dest="service",
                      default="http://localhost:8090/wps",
                      action="store")
    parser.add_option('-i', '--identifier',
                      dest="identifier",
                      action="store")
    parser.add_option('--input',
                      dest="inputs",
                      action="append",
                      default=[])
    parser.add_option('--output',
                      dest="outputs",
                      action="append",
                      default=[])

    options, remainder = parser.parse_args()
    
    if options.verbose:
        print "SERVICE    = ", options.service
        print "IDENTIFIER = ", options.identifier
        print "INPUTS     = ", options.inputs
        print "OUTPUTS    = ", options.outputs

    inputs = []
    for param in options.inputs:
        key,value = param.split('=')
        inputs.append( (key, value) )

    outputs = []
    for param in options.outputs:
        outputs.append( (param, True) )

    if options.verbose:
        print "SERVICE    = ", options.service
        print "IDENTIFIER = ", options.identifier
        print "INPUTS     = ", inputs
        print "OUTPUTS    = ", outputs

    _execute(service = options.service,
             identifier = options.identifier,
             inputs = inputs,
             outputs = outputs)

    
