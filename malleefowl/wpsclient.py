##
## docs
## http://bip.weizmann.ac.il/course/python/PyMOTW/PyMOTW/docs/optparse/index.html#module-optparse
## http://pymotw.com/2/json/
## http://groovy.codehaus.org/Executing+External+Processes+From+Groovy


import sys
import json

class MyEncoder(json.JSONEncoder):
    
    def default(self, obj):
        #print 'default(', repr(obj), ')'
        # Convert objects to a dictionary of their representation
        d = {}
        try:
            d.update(obj.__dict__)
        except:
            pass
        return d

from owslib.wps import WebProcessingService, monitorExecution

def get_caps(service, verbose=False):
    wps = WebProcessingService(service, verbose=verbose)
    wps.getcapabilities()

    count = 0
    for process in wps.processes:
        count = count + 1
        print "%3d. %s [%s]" % (count, process.title, process.identifier)

    return wps.processes

def describe_process(service, identifier, verbose=False):
    wps = WebProcessingService(service, verbose=verbose)
    process = wps.describeprocess(identifier)

    print "Title            = ", process.title
    print "Identifier       = ", process.identifier
    print "Abstract         = ", process.abstract
    print "Store Supported  = ", process.storeSupported
    print "Status Supported = ", process.statusSupported
    print "Data Inputs      = ", reduce(lambda x,y: x + ', ' + y.identifier, process.dataInputs, '')
    print "Process Outputs  = ", reduce(lambda x,y: x + ', ' + y.identifier, process.processOutputs, '')

    return process

def execute(service, identifier, inputs=[], outputs=[], sleep_secs=1, verbose=False):
    wps = WebProcessingService(service, verbose=verbose)
    execution = wps.execute(identifier, inputs=inputs, output=outputs)
    monitorExecution(execution, sleepSecs=sleep_secs)

    return execution.processOutputs

def message(msg=None):
    print msg

def write_result(outfile, result, format='json'):
    try:
        fp = outfile
        if not type(outfile) is file:
            fp = open(outfile, 'w')
        chunk = MyEncoder(sort_keys=True, indent=2).encode(result)
        fp.write(chunk)
    finally:
        fp.close()

def main():
    import optparse

    parser = optparse.OptionParser(usage='%prog caps|describe|execute [options]',
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
    parser.add_option('-f', '--format',
                      dest="format",
                      type = "choice",
                      choices = ['json'],
                      default="json",
                      action="store",
                      help="format for result (default: json)")
    parser.add_option('-o', '--outfile',
                      dest="outfile",
                      default=sys.stdout,
                      action="store",
                      help="output file for results (default: stdout)")

    execute_opts = optparse.OptionGroup(
        parser, 'Execute Options',
        'Options for exection command')
    execute_opts.add_option('--input',
                            dest = "inputs",
                            action = "append",
                            default = [],
                            help = "zero or more input params: key=value")
    execute_opts.add_option('--output',
                            dest = "outputs",
                            action = "append",
                            default = [],
                            help = "one or more output params")
    execute_opts.add_option('--sleep',
                            dest = "sleep_secs",
                            action = "store",
                            default = 1,
                            type="int",
                            help = "sleep interval in seconds when checking process status")
    parser.add_option_group(execute_opts)

    options, remainder = parser.parse_args()
    
    if options.verbose:
        message("SERVICE    = %s" % ( options.service ))
        message("IDENTIFIER = %s" % ( options.identifier ))
        message("INPUTS     = %s" % ( options.inputs ))
        message("OUTPUTS    = %s" % ( options.outputs ))
        message("SLEEP      = %s" % ( options.sleep_secs ))
        message("FORMAT     = %s" % ( options.format ))
        message("COMMAND    = %s" % ( remainder ))

    inputs = []
    for param in options.inputs:
        key,value = param.split('=')
        inputs.append( (key, value) )

    outputs = []
    for param in options.outputs:
        outputs.append( (param, True) )

    result = None
    if 'caps' in remainder:
        result = get_caps(
            service = options.service,
            verbose = options.verbose)
    elif 'describe' in remainder:
        result = describe_process(
            service = options.service,
            identifier = options.identifier,
            verbose = options.verbose)
    elif 'execute' in remainder:
        result = execute(
            service = options.service,
            identifier = options.identifier,
            inputs = inputs,
            outputs = outputs,
            sleep_secs = options.sleep_secs,
            verbose = options.verbose)
    else:
        print "Unknown command", remainder
        exit(1)

    
    write_result(options.outfile, result, format=options.format)
