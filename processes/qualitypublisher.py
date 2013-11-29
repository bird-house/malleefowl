"""
The QualityPublisher reads a file with one filename per line and publishes the local
data to network.

Date: 20.11.2013
Author: Tobias Kipp (kipp@dkrz.de)
"""
import malleefowl.process 
import types
import os
import urllib2

class QualityPublisherProcess(malleefowl.process.WPSProcess):
    def __init__(self):
        abstract_ml =("Read trough a file containing one filename per line an publish it.")
        malleefowl.process.WPSProcess.__init__(self,
            identifier = "QualityPublisher", 
            title="Publish QualityControl results.",
            version = "0.1",
            metadata=[],
            abstract=abstract_ml)
            
        self.ssh_host= self.addLiteralInput(
            identifier="ssh_name",
            title="ssh_name",
            abstract="The ssh_name is a shortform for a ssh connection defined in .ssh/config. ",
            default="esgf-dev",
            type=types.StringType,
            )
           
        #self.server_path = self.addLiteralInput(
        #    identifier = "server_path",
        #    title = "The path on the server to publish to.",
        #    default = "/usr/local/tomcat/webapps/ROOT/qc_docs",
        #    type=types.StringType,
        #    )
        self.SERVER_PATH = "/usr/local/tomcat/webapps/ROOT/qc_docs/"

        self.filename_from_qualitycontrol = self.addLiteralInput(
            identifier = "filename_from_qualitycontrol",
            title = "The file containing the generated quality results.",
            default = "",
            type=types.StringType,
            )

        self.system_calls = self.addComplexOutput(
            identifier = "system_calls",
            title = "Used system calls",
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )



    def execute(self):
        ssh_host = self.ssh_host.getValue()
        qc_filename = self.filename_from_qualitycontrol.getValue()
        qc_file = urllib2.urlopen(qc_filename)
        text = qc_file.read()
        lines = text.split("\n")
        logname = self.mktempfile(suffix=".log")
        system_calls = open(logname,'w')
        for line in lines:
            #cut off the path to use the rest as part of the server filename
            filename_without_path = line.split("/")[-1]
            if(filename_without_path[0:3]=="QC-"):#make sure it is a QC file
                server_location = self.SERVER_PATH+filename_without_path
                os.system("scp "+line+" "+ssh_host+":"+server_location)
                system_calls.write("scp "+line+" "+ssh_host+":"+server_location+"\n")
        system_calls.close()
        self.system_calls.setValue(system_calls)



        
