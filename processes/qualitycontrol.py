
"""
Test Process kept simple to learn the WPS structure

Author: Tobias Kipp (kipp@dkrz.de)
"""
import types
from pywps.Process import WPSProcess                                
class Process(WPSProcess):
     def __init__(self):
          # init process
         abstractMultiLine = ("The Test process is a simple process to learn how to use"+
                               "the WPS structure. The process will accept 2 input numbers"+
                               "and will calculate the product of both.")
         

         WPSProcess.__init__(self,
              identifier = "QualityControl", 
              title="TestTitle",
              version = "0.1",
              storeSupported = "true",
              statusSupported = "true",
              abstract=abstractMultiLine,                    
              grassLocation =False)
              
         self.Input1 = self.addLiteralInput(identifier = "int",
                                            title = "Input1 number", 
                                            default=100,
                                            type=type(1),
                                            minOccurs = 1,#required
                                            maxOccurs = 1,
                                            )
         self.Input2 = self.addLiteralInput(identifier="string", 
                                            title="Some text", 
                                            #tefault="A B C 350",
                                            type=types.StringType,
                                            minOccurs = 1,#required
                                            maxOccurs = 1,
                                            )
         self.Output= self.addLiteralOutput(identifier="output", 
                                            title="Output is an integer-string product.",
                                            default = None,
                                            )
     def execute(self):
         
         self.Output.setValue(self.Input1.getValue()*str(self.Input2.getValue()))
         return #If execute() returns anything but null, it is considered as error and exception is called
