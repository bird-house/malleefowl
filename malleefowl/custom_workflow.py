"""
Run custom workflow without prior knowledge of the underlying component except the fact that they are WPS
The workflow must have the following structure:
Dict of 2 elements :
  - name : Workflow name
  - tasks : Array of workflow task, each describe by a dict :
    - name : Unique name given to each workflow task
    - url : Url of the WPS provider
    - identifier : Identifier of a WPS process
    - inputs : Array of static input required by the WPS process, each describe by a 2 elements array :
      - Name of the input
      - Value of the input
    - linked_inputs : Array of dynamic input required by the WPS process and obtained by the output of other tasks,
                      each describe by a 2 elements array :
      - Name of the input
      - Provenance of the input, describe by a dict :
        - task : Name of the task from which this input must come from
        - output : Name of the task output that will be linked
        - as_reference : Specify the required form of the input** [True: Expect an URL to the input,
                                                                   False: Expect the data directly]
    - progress_range : 2 elements array defining the overall progress range of this task :
      - Start
      - End

** The workflow executor is able obviously to assign a reference output to an expected reference input and
   a data output to an expected data input but will also be able to read the value of a reference output to send the
   expected data input. However, a data output linked to an expected reference input will yield to an exception.

Exemple:
{
        "name": "Subsetting workflow",
        "tasks": [
            {
                "name": "Downloading",
                "url": "http://localhost:8091/wps",
                "identifier": "thredds_download",
                "inputs": [["url", "http://localhost:8083/thredds/catalog/birdhouse/catalog.xml"]],
                "progress_range": [0, 50]
            },
            {
                "name": "Subsetting",
                "url": "http://localhost:8093/wps",
                "identifier": "subset_WFS",
                "inputs": [["typename", "ADMINBOUNDARIES:canada_admin_boundaries"],
                           ["featureids", "canada_admin_boundaries.5"],
                           ["mosaic", "False"]],
                "linked_inputs": [["resource", { "task": "Downloading",
                                                 "output": "output",
                                                 "as_reference": False}],],
                "progress_range": [50, 100]
            },
        ]
    }
"""

import sys
import json
import urllib2
from cStringIO import StringIO
from time import sleep

from owslib.wps import WebProcessingService
from owslib.wps import ComplexDataInput
from owslib.wps import printInputOutput

from dispel4py.workflow_graph import WorkflowGraph
from dispel4py import simple_process
from dispel4py.core import GenericPE

from malleefowl.workflow import run as run_basic_workflow

# For check_status function
from owslib.wps import WPSExecuteReader
from owslib.etree import etree

import logging
logger = logging.getLogger("PYWPS")


# If the xml document is unavailable after 5 attempts consider that the process has failed
XML_DOC_READING_MAX_ATTEMPT = 5


class ProxyPE(GenericPE):
    """
    Pass-through PE that let connect 2 PEs by multiple connections using this proxy
    Connecting A to B twice is not allowed but connecting A to B then A to ProxyPE and ProxyPe to B
    is functionality equivalent and let the goal to be reached
    """
    def __init__(self, input_name, output_name):
        GenericPE.__init__(self)

        self.input_name = input_name
        self.output_name = output_name
        self._add_input(input_name)
        self._add_output(output_name)

    def process(self, inputs):
        """
        Simply feed the input as output
        """
        return {self.output_name: inputs[self.input_name]}


class MonitorPE(GenericPE):
    """
    Augment GenericPE with a functionality to scale the current progress to a specific progress range
    """
    def __init__(self):
        GenericPE.__init__(self)

        self._monitor = None
        self._pstart = 0
        self._pend = 100

    def set_monitor(self, monitor, start_progress=0, end_progress=100):
        self._monitor = monitor
        self._pstart = start_progress
        self._pend = end_progress

    def progress(self, execution):
        return int(self._pstart + ((self._pend - self._pstart) / 100.0 * execution.percentCompleted))


class GenericWPS(MonitorPE):
    """
    Wrap the execution of a WPS process into a dispel4py PE
    """
    STATUS_NAME = 'status'
    STATUS_LOCATION_NAME = 'status_location'
    DUMMY_INPUT_NAME = 'None'

    def __init__(self, name, url, identifier, inputs=[], linked_inputs=[], headers=None, **kwargs):
        MonitorPE.__init__(self)

        def log(message, progress):
            """
            Dispatch the message and progress to the monitor if available or to the logger if not
            """
            if self._monitor:
                self._monitor("{0}: {1}".format(
                    self.identifier, message),
                    progress)
            else:
                logger.info('STATUS ({0}: {2}/100) - {1}'.format(
                    self.identifier, message, progress))

        self.monitor = log

        self._add_output(self.STATUS_NAME)
        self._add_output(self.STATUS_LOCATION_NAME)

        self.name = name
        self.identifier = identifier
        self.wps = WebProcessingService(url=url, skip_caps=True, verify=False, headers=headers)
        self.proc_desc = self.wps.describeprocess(identifier)

        # Validate that given inputs exist in the wps
        valid_inputs = [wps_input.identifier for wps_input in self.proc_desc.dataInputs]
        # Allow a process not requiring any input to be linked to a previous one by using a dummy ("None") input name
        valid_inputs.append(self.DUMMY_INPUT_NAME)
        for submitted_input in inputs + linked_inputs:
            if submitted_input[0] not in valid_inputs:
                raise Exception('Invalid workflow : Input "{input}" of process "{proc}" is unknown.'.format(
                    input=submitted_input[0],
                    proc=identifier))

        # These are the static inputs
        # (linked inputs will be appended to wps_inputs just before execution by the _set_inputs function)
        self.wps_inputs = [(input_tuple[0], input_tuple[1]) for input_tuple in inputs]

        # Will be filled as PE are connected to us (by the require_output function)
        self.wps_outputs = []

        self.linked_inputs = {}
        for linked_input in linked_inputs:
            # Here we add PE input that will need to be connected
            self._add_input(linked_input[0])
            self.linked_inputs[linked_input[0]] = linked_input[1]

    def require_output(self, output, as_reference):
        """
        Call this function to signal to this PE that its "output" will be required
        :param output: output name as returned by the WPS describe process
        :param as_reference: Boolean requesting the output to be a reference or the real value
                             (as requested by the workflow)
        :return: True if the given output is available, False otherwise
        """
        # Validate that the required output exists in the wps
        valid_outputs = [wps_output.identifier for wps_output in self.proc_desc.processOutputs]
        if output not in valid_outputs:
            return False

        # Here we add a PE output that is required and about to be connected
        self._add_output(output)

        self.wps_outputs.append((output, as_reference))
        return True

    def process(self, inputs):
        """
        Callback of dispel4py when this PE is ready to be executed
        This function is called multiple time if more than one input must be set
        :param inputs: One of the linked input
        :return: Result or None if not ready to be executed (need more inputs)
        """
        try:
            # Assign the input internally
            self._set_inputs(inputs)

            # Check that all required inputs have been set
            # if not wait (by returning None) for a subsequent process call that will set other inputs
            if self._is_ready():
                result = self._execute()
                if result is not None:
                    return result
        except Exception:
            logger.exception("process failed!")
            raise
        return None

    @staticmethod
    def check_status(execution):
        """
        Try to read the xml status of the underlying WPS process, raise Exception if the url cannot be read properly
        """
        reader = WPSExecuteReader(verbose=execution.verbose)
        # override status location
        logger.info('\nChecking execution status... (location=%s)' % execution.statusLocation)
        try:
            response = reader.readFromUrl(
                execution.statusLocation,
                username=execution.username,
                password=execution.password,
                verify=execution.verify,
                headers=execution.headers)
            response = etree.tostring(response)
        except:
            logger.error("Could not read status document.")
            raise
        else:
            execution.checkStatus(response=response, sleepSecs=3)

    def monitor_execution(self, execution):
        """
        Monitor the execution of the underlying WPS and return only when the process end (successfully or not)
        """
        progress = self.progress(execution)
        self.monitor("status_location={0.statusLocation}".format(execution), progress)

        xml_doc_read_failure = 0
        while execution.isNotComplete():
            try:
                # Check the status of the wps execution
                self.check_status(execution)
            except:
                # Try XML_DOC_READING_MAX_ATTEMPT time before raising an exception
                logger.exception("Could not read status xml document.")
                xml_doc_read_failure += 1
                if xml_doc_read_failure > XML_DOC_READING_MAX_ATTEMPT:
                    raise
                else:
                    # Sleep 5 seconds to give a chance
                    sleep(5)
            else:
                progress = self.progress(execution)
                self.monitor(execution.statusMessage, progress)

        # In case of success log all output value
        if execution.isSucceded():
            for output in execution.processOutputs:
                if output.reference is not None:
                    self.monitor(
                        '{0.identifier}={0.reference} ({0.mimeType})'.
                        format(output),
                        progress)
                else:
                    self.monitor(
                        '{0}={1}'.
                        format(output.identifier, ", ".join(output.data)),
                        progress)

        # Of log the errors
        else:
            self.monitor('\n'.join(
                ['ERROR: {0.text} code={0.code} locator={0.locator})'.
                    format(ex) for ex in execution.errors]), progress)

    @staticmethod
    def _print_input_output(value):
        """
        Get a complete description of the WPS input or output value
        """
        sys.stdout = mystdout = StringIO()

        # print to stdout which we capture
        printInputOutput(value)

        sys.stdout = sys.__stdout__
        return mystdout.getvalue()

    def _get_exception(self, wps_output, wps_input, as_ref):
        """
        Produce a detailed exception description when 2 tasks cannot agree on the datatype
        """
        details = "Upstream task '{out}' output doesn't produce a compatible format for '{input}' input of '{task}'.".\
            format(out=wps_output.identifier,
                   input=wps_input.identifier,
                   task=self.name)

        more = 'Output :\n{out}\nInput (is reference : {as_ref}) :\n{input}'.format(
            out=self._print_input_output(wps_output),
            input=self._print_input_output(wps_input),
            as_ref=as_ref)
        msg = 'Workflow datatype incompatibility error : {details}\n{more}'.format(details=details, more=more)
        return Exception(msg)

    @staticmethod
    def _read_reference(reference):
        """
        Read a WPS reference and return the content
        """
        try:
            return urllib2.urlopen(reference).read()
        except urllib2.URLError:
            # Don't raise exceptions coming from that.
            return None

    def _adapt(self, wps_output, wps_input):
        """
        Try to fit the wps output data to the needs of the wps input requirements
        This can involve returning the data or its reference, parsing the reference to return its content or even
        loading a json structure (or a reference ot it) and returning its content
        The function will raise an exception if the output data cannot met the input requirements
        :param wps_output: wps output data as returned by the upstream WPS process
        :param wps_input: wps input description as declared by the downstream WPS process
        :return: The data in the required form
        """
        as_reference = self.linked_inputs[wps_input.identifier]['as_reference']
        output_datatype = [wps_output.dataType, ]

        # Downstream process wants a reference, so consider the reference as the data from this point
        if as_reference:
            output_data = wps_output.reference

            # If the downstream process want a string it is ok too (no mimetype check in that case)
            output_datatype.append('string')

        # Downstream process wants the data directly, but we only have the reference: Extract the data!
        elif not wps_output.data and wps_output.reference:
            output_data = self._read_reference(wps_output.reference)

        # process output data are append into a list so extract the first value here
        elif isinstance(wps_output.data, list) and len(wps_output.data) == 1:
            output_data = wps_output.data[0]

        # Is it possible to have more than one output?
        else:
            raise self._get_exception(wps_output, wps_input, as_reference)

        # At this point raise an exception if we don't have data in wps_output.data
        if not output_data:
            raise self._get_exception(wps_output, wps_input, as_reference)

        # Consider the validation completed if the dataType match for non-complex data or
        # if the mimetype match for complex data
        is_complex = wps_input.dataType == 'ComplexData'
        supported_mimetypes = [value.mimeType for value in wps_input.supportedValues] if is_complex else []
        if wps_input.dataType in output_datatype and (not is_complex or wps_output.mimeType in supported_mimetypes):
            return [output_data, ]

        # Remain cases are either datatypes or complex data mimetypes mismatching...
        # Before raising an exception we will check for a specific case that we can handle:
        # json array that could be fed into the downstream wps wanting an array of data.
        # If this specific case is detected we will simply send the json content to the downstream wps without further
        # validation since the json content type cannot be verified.
        take_array = wps_input.maxOccurs > 1
        if take_array and 'ComplexData' in output_datatype and wps_output.mimeType == 'application/json':
            # If the json data was still referenced read it now
            if as_reference:
                output_data = self._read_reference(wps_output.reference)

            json_data = json.loads(output_data)
            if isinstance(json_data, list):
                array = []
                for value in json_data:
                    array.append(ComplexDataInput(value) if is_complex else value)

                return array

        # Cannot do anything else
        raise self._get_exception(wps_output, wps_input, as_reference)

    def _is_ready(self):
        """
        Check if all the required wps inputs had been set
        """
        ready_wps_inputs = [wps_input[0] for wps_input in self.wps_inputs]
        # Consider the dummy input to be always ready!
        ready_wps_inputs.append(self.DUMMY_INPUT_NAME)

        for linked_input in self.linked_inputs.keys():
            if linked_input not in ready_wps_inputs:
                return False
        return True

    def _set_inputs(self, inputs):
        """
        Take the inputs coming from the dispel4py upstream PE and assign them to the wps process inputs
        """
        wps_inputs = {wps_input.identifier: wps_input for wps_input in self.proc_desc.dataInputs}

        for key in inputs.keys():
            if key in wps_inputs:
                # This is the upstream wps output object
                wps_output = inputs[key]

                # This is the current wps input object
                wps_input = wps_inputs[key]

                # Now we try to do most of the conversion job between these two datatypes with the knowledge we have
                # and append the new wps input into the list
                self.wps_inputs += [(key, value) for value in self._adapt(wps_output, wps_input)]

    def _execute(self):
        """
        This is the function doing the actual WPS process call, monitoring its execution and parsing the output.
        :return: Return the data that is send to the downstream PE
        """
        logger.debug("execute with inputs=%s to get outputs=%s", self.wps_inputs, self.wps_outputs)
        execution = self.wps.execute(
            identifier=self.identifier,
            inputs=self.wps_inputs,
            output=self.wps_outputs,
            lineage=True)
        self.monitor_execution(execution)

        result = {self.STATUS_NAME: execution.status,
                  self.STATUS_LOCATION_NAME: execution.statusLocation}

        if execution.isSucceded():
            # NOTE: only set workflow output if specific output was needed
            for wps_output in self.wps_outputs:
                for output in execution.processOutputs:
                    if wps_output[0] == output.identifier:
                        # outputs from execution structure do not always carry the dataType
                        # so find it from the process description because the _adapt fct needs it
                        if not output.dataType:
                            for desc_wps_output in self.proc_desc.processOutputs:
                                if desc_wps_output.identifier == output.identifier:
                                    output.dataType = desc_wps_output.dataType
                        # Also the datatype is not always strip correctly so do the job here
                        else:
                            output.dataType = output.dataType.split(':')[-1]

                        # Send directly the wps output object to the downstream PE
                        # Note: output.data is always an array since a wps output is append to processOutputs[x].data
                        result[wps_output[0]] = output
                        break
            return result
        else:
            failure_msg = '\n'.join(['{0.text}'.
                                    format(ex) for ex in execution.errors])
            raise Exception(failure_msg)


def connect(graph, from_node, from_connection, to_node, to_connection):
    """
    Make the dispel4py graph connection between 2 nodes.
    Dispel doesn't support multiple connections between 2 nodes so if required we add a proxy node between them for
    additional connections
    :param graph: Target dispel4py grapg
    :param from_node: Upstream node
    :param from_connection: Output name of the upstream node
    :param to_node: Downstream node
    :param to_connection: Input name of the downstream node
    """
    try:
        # Detect if an edge already exist between these 2 nodes (Will throw an exception if the nodes are still unknown)
        is_connected = graph.graph.has_edge(graph.objToNode[from_node], graph.objToNode[to_node])
    except KeyError:
        # If the nodes are unknown we know for sure that they are not connected
        is_connected = False

    if is_connected:
        # Insert a proxy node between the 2 nodes if an edge already exist keeping the maximum number of connections
        # between any 2 nodes to 1
        proxy_node = ProxyPE(from_connection, to_connection)
        graph.connect(from_node, from_connection, proxy_node, from_connection)
        graph.connect(proxy_node, to_connection, to_node, to_connection)
    else:
        # Connect as usual for the 1st connection
        graph.connect(from_node, from_connection, to_node, to_connection)


def run(workflow, monitor=None, headers=None):
    """
    Run the given workflow
    :param workflow: json structure describinf the workflow
    :param monitor: monitor callback to receive messages and progress
    :param headers: Headers to use when making a request to the WPS
    :return: A summary of the execution which is a list of all task's xml status
    """

    # Back compatibility with the basic workflow detected by looking at the new tasks array required for custom workflow
    if 'tasks' not in workflow:
        run_basic_workflow(workflow, monitor, headers)

    graph = WorkflowGraph()

    # Create WPS processes and append them in a task array
    wps_tasks = []
    for task in workflow['tasks']:
        progress_range = task.get('progress_range', [0, 100])
        wps_task = GenericWPS(headers=headers, **task)
        wps_task.set_monitor(monitor,
                             progress_range[0],
                             progress_range[1])
        wps_tasks.append(wps_task)

    # Connect each task PE in the dispel graph using the linked inputs information
    # (raise an exception if some connection cannot be done)
    for wps_task in wps_tasks:
        # Try to find the referenced PE for each of the linked inputs
        for input_name, linked_input in wps_task.linked_inputs.iteritems():
            found_linked_input = False

            # Loop in the task array searching for the linked task
            for source_wps_task in wps_tasks:
                if source_wps_task.name == linked_input['task']:
                    # The linked process has been found
                    # The require_output function will validate the existence of the required output and prepare it
                    # which involve creating the graph PE output and the requested WPS output as reference or not
                    if source_wps_task.require_output(output=linked_input['output'],
                                                      as_reference=linked_input['as_reference']):
                        # The required output has been found we can now safely connect both PE in the graph
                        connect(graph, source_wps_task, linked_input['output'], wps_task, input_name)
                        found_linked_input = True
                    break
            # Unfortunately the linked input has not been resolved, we must raise an exception for that
            if not found_linked_input:
                raise Exception(
                    'Cannot build workflow graph : Task "{task}" has an unknown linked input : '
                    'task: {linked_task}, output: {output}'.format(
                        task=wps_task.name,
                        linked_task=linked_input['task'],
                        output=linked_input['output']))

    # Search for the 'source' PEs (which have no inputs)
    source_pe = {}
    for wps_task in wps_tasks:
        if not wps_task.linked_inputs:
            source_pe[wps_task] = [{}]

    # Run the graph
    try:
        result = simple_process.process(graph, inputs=source_pe)
    except Exception as e:
        # Augment the exception message but conserve the full exception stack
        e.args = ('Cannot run the workflow graph : {0}'.format(str(e)),)
        raise

    summary = {}
    for wps_task in wps_tasks:
        summary[wps_task.name] =\
            {'status_location': result.get((wps_task.id, wps_task.STATUS_LOCATION_NAME))[0],
             'status': result.get((wps_task.id, wps_task.STATUS_NAME))[0]
             }
    return summary
