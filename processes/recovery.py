import types
from malleefowl.process import WPSProcess
from malleefowl import wpsclient
import threading
import time

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class RecoveryProcess(WPSProcess):

    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier="recovery", 
            title="Recovery",
            version = "1.0",
            metadata = [],
            abstract="Allows to run any WPS process in recovery mode.",
            )

        self.process_identifier = self.addLiteralInput(
            identifier = "process_identifier",
            title = "Process identifier",
            type = types.StringType,
            )

        self.wps_url = self.addLiteralInput(
            identifier = "wps_url",
            title = "WPS url",
            type = types.StringType,
            default = self.service_url,
            abstract = "The url of the WPS for the given identifier"
            )

        self.process_inputs = self.addLiteralInput(
            identifier = "process_inputs",
            title = "Process inputs",
            type = types.StringType,
            minOccurs = 0,
            maxOccurs = 200000,
            abstract = "The inputs like in the normal WPS call. Syntax: inputname=value",
            )
        
        self.process_outputs = self.addLiteralInput(
            identifier = "process_outputs",
            title = "Process outputs",
            type = types.StringType,
            minOccurs = 0,
            maxOccurs = 200000,
            abstract = ("The selected output names and if they are returned as reference. " + 
                       "use True or False for asReference. Syntax: outputname=asReference")
            )

        self.timeout = self.addLiteralInput(
            identifier = "timeout",
            title = "Timelimit per try",
            type = types.FloatType,
            abstract = "The time in seconds when a process is considered as locked.",
            #The is no ultimate default value.
            )

        self.retry_delays = self.addLiteralInput(
            identifier = "retry_delays",
            title = "Delays between retries",
            type = types.StringType,
            default = "1, 1",
            abstract = ("The time to wait before retrying after a failure. Use a comma " +
                        "separated list of numbers. The number of elements " + 
                        "is the number of retries. The number itself is the delay in seconds.")
            )

                                           
        self.result = self.addLiteralOutput(
            identifier = "result",
            title = "Result",
            type = types.StringType)

        self.process_log = self.addComplexOutput(
            identifier = "process_log",
            title = "Log of this process",
            metadata = [],
            formats = [{"mimeType":"text/plain"}],
            asReference = True,
            )


    def execute(self):
        process_log = open(self.mktempfile(suffix=".txt"),"w")
        process_log.write("Loading input parameters\n")
        wps_url = self.wps_url.getValue()
        inputlist = self.process_inputs.getValue()
        if inputlist in [None, "<colander.null>"]:
            inputlist = []
        inputs = []
        for inp in inputlist:
            if inp in [None, "<colander.null>", ""] or "<colander._drop" in str(inp):
                continue#ignore empty fields.
            if "=" not in inp:
                raise Exception(inp + " does not follow the inputname=value syntax")
            inputname, value = inp.split("=",1)
            if len(inputname) == 0:
                raise Exception("The inputname is empty")
            inputs.append((inputname, value))
        outputlist = self.process_outputs.getValue()
        if outputlist in [None, "<colander.null>"]:
            outputlist = []
        #for the wps call
        outputs = []
        #better suited for the evaluation of the result
        outputref = {}
        for outp in outputlist:
            #In the case an empty box is submitted the value of outp is something like
            #<colander._drop object at 0x28a8690> 
            if str(outp) in ["None", "<colander.null>", ""] or "<colander._drop" in str(outp):
                continue#ignore empty fields.
            if "=" not in outp:
                raise Exception(outp + " does not follow the outputname=asReference")
            outputname, asReference = outp.split("=",1)
            if len(outputname) == 0:
                raise Exception("The inputname is empty")
            asref = asReference.lower()=="true"
            outputs.append((outputname, asref))
            outputref[outputname] = asref
        
        timeout = self.timeout.getValue()
        retry_delays = self.retry_delays.getValue()
        ret = retry_delays.split(",")
        retries = [x.strip() for x in ret if x.strip().isdigit()]
        identifier = self.process_identifier.getValue()
        process_log.write("Calling recovery for " + identifier + "\n")
        def method(resdict):
            result = wpsclient.execute(
                service = wps_url,
                identifier = identifier,
                inputs = inputs, 
                outputs = outputs,
                )
            resdict["result"] = result
            #Find the defined outputs in the returned list of dicts result
            for entry in result:
                eid = entry["identifier"]
                if eid in outputref: 
                   if outputref[eid]:#if as reference is used
                       val = entry["reference"]
                   else:#else return the plain data
                       val = entry["data"][0]
                   resdict[eid] = val
        resdict = {}
        try:
            recovery(method, resdict = resdict, timeout = timeout, retries = retries,
                     status = self.show_status) 
        except Exception as e:
            raise Exception("Trying to run " + identifier + " failed even though " + 
                            str(len(retries)) + " retries were done. The exception thrown by " +
                            "recovery was " + str(e))
        self.result.setValue(resdict["result"])
        for key, val in resdict.items():
            process_log.write("\n" + 60*"*" + "\n" + str(key) + ":\n" + str(val) + 2*"\n")
        self.process_log.setValue(process_log)


#For now the method is assumed to be parameterless. 
def recovery(method, resdict, timeout=300.0, timeout_step=1.0, retries=[1.0]*10, status=lambda x,y:None):
    retries = [0]+retries#to reduce the number of lines the first delay is set to 0.
    for delay in retries:
        time.sleep(float(delay))#wait for the delay and then try to run it.
        finished = run(method, resdict=resdict, timeout = timeout ,
                       timeout_step = timeout_step, status=status)
        if finished:
            return
    #if not finished:
    raise Exception("Did not finish correctly after the given number of retries.")
            

def run(method, resdict,  timeout=10.0, timeout_step=1.0, status = lambda x,y:None):
    """
    :param method: The to execute method. No parameters for now.
    :param timeout: Time limit in seconds.
    :param timeout_step: Time between time limit checks in seconds.
    """
    met = threading.Thread(target=method, kwargs={"resdict":resdict})
    met.daemon = True
    met.start()
    timer = 0.0
    while met.is_alive():
        if timer > timeout:
            return False# Exception("Time limit reached.")
        timer += timeout_step
        time.sleep(timeout_step)
    return True
