"""
Created on 14.11.2013

:author: tobiaskipp
"""

import lapis
import lapis.infra.infrastructure
import lapis.infra.handleinfrastructure

from lapis.model.doset import DigitalObjectSet
#from lapis.model.do import DigitalObject

class DOHandler():
    """ DOHandler accessess the handle system to create new digital objects.
       
    Digital Object will be abbreviated as DO.
    The simple link just maps an identifier to a location.
    For collections the collection_do method is given to create a collection digital object.
    With add_to_collection digitial objects can be added to the collection. It contains a duplicate
    entry protection for identifiers.
    """

    def __init__(self):
        """ Set the server parameters and create an HandleInfrastructure object. """
        self.host = "handleoracle.dkrz.de"
        self.path = "/handle/"
        self.port = 8090
        self.prefix = "10876"
        self.additional_identifier_element ="SQL-CORDEX-"
        self.infra = lapis.infra.handleinfrastructure.HandleInfrastructure(self.host,self.port,
                      self.path,self.prefix,self.additional_identifier_element)


    def link(self,location,identifier=None):
        """Creates a digital object referencing to a given link.
        
        :param location: The target location which the digital object should reference.
        :param identifier: A PID identifier string. If not given a random identifier will be generated.
        :returns: (dourl,realidentifier). The url to the DO and the DO identifier string.  
        """
        do=None
        if(identifier is not None):#check if the digital object exists
            do = self.infra.lookup_pid(identifier)
        if do is None:
            do = self.infra.create_do(identifier)
        do.resource_location=location
        #do.identifier lacks the additional_identifier_element part. The lookup_pid solves it.
        realidentifier = str(self.infra.lookup_pid(do.identifier))
        dourl = self.host+":"+str(self.port)+self.path+realidentifier
        return dourl,realidentifier

    def delete_do(self,identifier):
        """ Delete a DO by entering its identifier.
         
        :param identifier: The string identifier of the DO.
        """
        self.infra.delete_do(identifier)

    def collection_do(self,identifier = None):
        """ Makes a collection digital object, its identifier and url accessible.
        
        It looks for an existing object with the given identifier. If it does not exist
        it creates a new digitial object with a collection class.

        :param identifier: DO identifier string. If None a random identifier will be generated.
        :returns: The DO its string identifier and the url to the DO.
        """
        do = None
        if identifier is not None:
            do = self.infra.lookup_pid(identifier)
        if do is None:
            do = self.infra.create_do(identifier,DigitalObjectSet)
        identifier = str(self.infra.lookup_pid(do.identifier))
        dourl = self.host+":"+str(self.port)+self.path+identifier
        return do,identifier,dourl

    def add_to_collection(self,do_collection_identifier,do_ids):
        """ Add a list of DO identfiers to the collection DO.
        
        If an identifier is found in the collection, it is not added again to prevent duplicates.

        :param do_collection_identifier: The string identifier of the DO representing a collection.
        :param do_ids: The list of DO identifiers that should be added to the collection.  
          It may contain duplicates.
        """ 
        do = self.infra.lookup_pid(do_collection_identifier)
        for identifier in do_ids:
            existingInCollection = do.contains_do(self.infra.lookup_pid(identifier))
            if not existingInCollection:
                do.add_do(self.infra.lookup_pid(identifier))
