"""
Created on 14.11.2013

:author: tobiaskipp
"""

import processes.qc.dohandler as dohandler
import processes.qc.splitqc as splitqc
import processes.qc.sqlitepid as sqlitepid

class PidGenerator():
    """ The PidGenerator is used to access or create a database and add PIDs to it.

    By supplying a path to the create_pids method, the path is searched for folders containing
    NetCDF files. It generates a PID for each file and one PID for each collection, which in 
    this case is equivalent to a path.
    Currently only tested with a SQLite3 database.
    """
    def __init__(self,database_location,data_node="ipcc-ar5.dkrz.de"):
        """ Load or create the database and initialize tables if needed.
        
        :param database_location: The sqlite database file reference.
        """
        self.sqlpid = sqlitepid.SqlitePid(database_location) 
        self.sqlpid.first_run()
        self.do_handler = dohandler.DOHandler()
        self.errors = []
        self.data_node = data_node

    def create_pids(self,search_path):
        """ Search through the paths structure and create PID for every file and collection.
        Add the location and PID url and identifier to the database.

        If search_path's structure is not valid error messages are generated and stored in errors.
        
        :param search_path: The root of the folders to be checked. Should be equivalent to 
          QC_PROJECT_DATA
        :returns: True if search_path has a valid structure else False 
        """
        sqc = splitqc.SplitQc()
        #Gather information about the tree structure
        valid = sqc.search(search_path)
        if(valid):
            #Create a list for each path/dataset to store string identifiers of digitial objects
            #TODO: Find a better solution than modifing the location strings to match it to their
            #real target location after the QC is finished.
            identifiers_by_path = dict()
            for path in sqc.datasets:
                path2 = path.lstrip(search_path).lstrip("CORDEX/")                
                path = self.data_node+"/thredds/fileServer/cordex/"+path2
                identifiers_by_path[path]=[]
            #for each file found in the search check if it already exists in the database
            for filename in sqc.full_path_files:
                filename = filename.lstrip(search_path).lstrip("CORDEX/")
                filename = self.data_node+"/thredds/fileServer/cordex/"+filename
                dbentry = self.sqlpid.get_by_key_value("location",filename)
                path = "/".join(filename.split("/")[:-1])
                #if it does not exist in the database create a digital object in the handle system,
                #add the identifier to the dataset's list and store the do information in the database.
                if(len(dbentry)==0):
                    url,identifier = self.do_handler.link(filename)
                    identifiers_by_path[path].append(identifier)
                    self.sqlpid.add_do(filename,identifier,url)
                #if it exists add the digital object identifier to the dataset list.
                else:
                    #dbentry[0] is the first found result [1] is for the identifier
                    identifiers_by_path[path].append(str(dbentry[0][1]))
            #Similar to the file it is searched for the existance of path/dataset in the database
            #The identifiers for the files are added to the collection.
            for path in sqc.datasets:
                path2 = path.lstrip(search_path).lstrip("CORDEX/")                
                path = self.data_node+"/thredds/fileServer/cordex/"+path2
                dbentry = self.sqlpid.get_by_key_value("location",path)
                collection_id=""
                #if the collection does not exist create it
                if(len(dbentry)==0):
                    docollection,collection_id,url = self.do_handler.collection_do()
                    self.sqlpid.add_do(path,collection_id,url)
                else:
                    collection_id = str(dbentry[0][1])
                #It is assumed that add_to_collection prevents the creation of an digital object if the 
                #reference already exists in the collection.
                self.do_handler.add_to_collection(collection_id,identifiers_by_path[path]) 
        else:
            self.errors+=sqc.errors
        return valid
    

