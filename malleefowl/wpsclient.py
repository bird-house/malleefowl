from owslib.wps import WebProcessingService, monitorExecution

def get_caps(service, verbose=False):
    wps = WebProcessingService(service, verbose=verbose)
    wps.getcapabilities()
    for process in wps.processes:
        print "Title      = ", process.title
        print "Identifier = ", process.identifier
        print "Abstract   = ", process.abstract
        print "********************************************"
    print "Number of processes:", len(wps.processes)
        

def describe_process(service, identifer, verbose=False):
    raise NotImplementedError('to be done ...')

def execute(service, identifier, inputs=[], outputs=[], openid=None, password=None, file_identifiers=None, verbose=False):
    wps = WebProcessingService(service, verbose=verbose)
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

    parser = optparse.OptionParser(usage='%prog execute|describe|caps [options]',
                                   version='0.1')
    parser.add_option('-v', '--verbose',
                      dest="verbose",
                      default=False,
                      action="store_true",
                      help="verbose mode"
                      )
    parser.add_option('-s', '--service',
                      dest="service",
                      default="http://localhost:8090/wps",
                      action="store",
                      help="WPS service url (default: http://localhost:8090/wps)")
    parser.add_option('-i', '--identifier',
                      dest="identifier",
                      action="store",
                      help="WPS process identifier")

    execute_opts = optparse.OptionGroup(
        parser, 'Execute Options',
        'Options for exection command')
    execute_opts.add_option('--input',
                            dest="inputs",
                            action="append",
                            default=[],
                            help="zero or more input params: key=value")
    execute_opts.add_option('--output',
                            dest="outputs",
                            action="append",
                            default=[],
                            help="one or more output params")
    parser.add_option_group(execute_opts)

    options, remainder = parser.parse_args()
    
    if options.verbose:
        print "SERVICE    = ", options.service
        print "IDENTIFIER = ", options.identifier
        print "INPUTS     = ", options.inputs
        print "OUTPUTS    = ", options.outputs
        print "REMAINDER  = ", remainder

    inputs = []
    for param in options.inputs:
        key,value = param.split('=')
        inputs.append( (key, value) )

    outputs = []
    for param in options.outputs:
        outputs.append( (param, True) )

    if 'caps' in remainder:
        get_caps(service=options.service, verbose=options.verbose)
    elif 'info' in remainder:
        describe_process(service=options.service, identifier=options.identifier, verbose=options.verbose)
    elif 'run' in remainder:
        execute(service = options.service,
                identifier = options.identifier,
                inputs = inputs,
                outputs = outputs,
                verbose=options.verbose)
    else:
        print "Unknown command", remainder
        exit(1)

    
