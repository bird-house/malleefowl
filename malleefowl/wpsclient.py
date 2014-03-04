##
## Simple script to call wps service. Using wps module from owslib.
##
## Results are written as json to output file.


import sys
import json

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class MyEncoder(json.JSONEncoder):
    
    def default(self, obj):
        #print 'default(', repr(obj), ')'
        # Convert objects to a dictionary of their representation
        d = {}
        try:
            d.update(obj.__dict__)
            d.pop('_root', None)
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
        logger.info("%3d. %s [%s]", count, process.title, process.identifier)

    return to_json(wps.processes)

def describe_process(service, identifier, verbose=False):
    wps = WebProcessingService(service, verbose=verbose)
    process = wps.describeprocess(identifier)

    logger.info("Title            = %s", process.title)
    logger.info("Identifier       = %s", process.identifier)
    logger.info("Abstract         = %s", process.abstract)
    logger.info("Store Supported  = %s", process.storeSupported)
    logger.info("Status Supported = %s", process.statusSupported)
    logger.info("Data Inputs      = %s", reduce(lambda x,y: x + ', ' + y, map(lambda x: x.identifier, process.dataInputs)))
    logger.info("Process Outputs  = %s", reduce(lambda x,y: x + ', ' + y, map(lambda x: x.identifier, process.processOutputs)))
    
    return to_json(process)

def execute(service, identifier, inputs=[], outputs=[], sleep_secs=1, verbose=False):
    logger.debug("inputs %s", inputs)
    logger.debug("outputs %s", outputs)
    
    wps = WebProcessingService(service, verbose=verbose)
    execution = wps.execute(identifier, inputs=inputs, output=outputs)
    monitorExecution(execution, sleepSecs=sleep_secs)

    logger.info("process %s results", identifier)
    for item in execution.processOutputs:
        logger.info("%s: %s", item.identifier, item.reference)

    return to_json(execution.processOutputs)
    
def to_json(result):
    # TODO fix json encoding (unicode, ascii)
    import yaml
    result_json = yaml.load( MyEncoder().encode(result) )
    logger.debug("result as json: %s", result_json)
    return result_json

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
    parser.add_option('-o', '--outfile',
                      dest="outfile",
                      action="store",
                      help="output file for results")
    parser.add_option('--status',
                      dest="statusfile",
                      action="store",
                      help="output file for status")

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
    
    #if options.verbose:
    #    logger.setLevel(logging.DEBUG)
        
    logger.debug("SERVICE    = %s", options.service)
    logger.debug("IDENTIFIER = %s", options.identifier)
    logger.debug("INPUTS     = %s", options.inputs)
    logger.debug("OUTPUTS    = %s", options.outputs)
    logger.debug("SLEEP      = %s", options.sleep_secs)
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

    if options.outfile:
        logger.debug('writing result %s', options.outfile)
        with open(options.outfile, "w") as fp:
            json.dump(result, fp, sort_keys=True, indent=2)
            

    logger.info('done')
