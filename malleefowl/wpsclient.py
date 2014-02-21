##
## docs
## http://bip.weizmann.ac.il/course/python/PyMOTW/PyMOTW/docs/optparse/index.html#module-optparse
## http://pymotw.com/2/json/
## http://groovy.codehaus.org/Executing+External+Processes+From+Groovy


import sys
import json

import logging
logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
    return wps.processes

def describe_process(service, identifier, verbose=False):
    wps = WebProcessingService(service, verbose=verbose)
    process = wps.describeprocess(identifier)
    return process

def execute(service, identifier, inputs=[], outputs=[], sleep_secs=1, verbose=False):
    wps = WebProcessingService(service, verbose=verbose)
    execution = wps.execute(identifier, inputs=inputs, output=outputs)
    monitorExecution(execution, sleepSecs=sleep_secs)
    return execution.processOutputs

def _write_json(fp, result):
    chunk = MyEncoder(sort_keys=True, indent=2).encode(result)
    fp.write(chunk)

def _write_caps(fp, processes, format='text'):
    count = 0
    for process in processes:
        count = count + 1
        fp.write("%3d. %s [%s]\n" % (count, process.title, process.identifier))

def _write_process(fp, processes, format='text'):
    fp.write("Title            = %s\n", process.title)
    fp.write("Identifier       = %s\n", process.identifier)
    fp.write("Abstract         = %s\n", process.abstract)
    fp.write("Store Supported  = %s\n", process.storeSupported)
    fp.write("Status Supported = %s\n", process.statusSupported)
    fp.write("Data Inputs      = %s\n", reduce(lambda x,y: x + ', ' + y.identifier, process.dataInputs, ''))
    fp.write("Process Outputs  = %s\n", reduce(lambda x,y: x + ', ' + y.identifier, process.processOutputs, ''))

def _write_execute(fp, processes, format='text'):
    pass

def write_result(outfile, command, result, format='text'):
    try:
        fp = outfile
        if not type(outfile) is file:
            fp = open(outfile, 'w')

        if format=='json':
            _write_json(fp, result)
        elif 'caps' in command:
            _write_caps(fp, result, format)
        elif 'describe' in command:
            _write_describe(fp, result, format)
        elif 'execute' in command:
            _write_execute(fp, result, format)
        else:
            logger.error('unknown command: %s', command)
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
                      choices = ['json', 'text'],
                      default="text",
                      action="store",
                      help="format for result [json, text] (default: text)")
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

    options, command = parser.parse_args()
    
    if options.verbose:
        logger.setLevel(logging.DEBUG)
        
    logger.debug("SERVICE    = %s", options.service)
    logger.debug("IDENTIFIER = %s", options.identifier)
    logger.debug("INPUTS     = %s", options.inputs)
    logger.debug("OUTPUTS    = %s", options.outputs)
    logger.debug("SLEEP      = %s", options.sleep_secs)
    logger.debug("FORMAT     = %s", options.format)
    logger.debug("COMMAND    = %s", command)

    inputs = []
    for param in options.inputs:
        key,value = param.split('=')
        inputs.append( (key, value) )

    outputs = []
    for param in options.outputs:
        outputs.append( (param, True) )

    result = None
    if 'caps' in command:
        result = get_caps(
            service = options.service,
            verbose = options.verbose)
    elif 'describe' in command:
        result = describe_process(
            service = options.service,
            identifier = options.identifier,
            verbose = options.verbose)
    elif 'execute' in command:
        result = execute(
            service = options.service,
            identifier = options.identifier,
            inputs = inputs,
            outputs = outputs,
            sleep_secs = options.sleep_secs,
            verbose = options.verbose)
    else:
        logger.error("Unknown command %s", command)
        exit(1)

    
    write_result(options.outfile, command, result, format=options.format)
