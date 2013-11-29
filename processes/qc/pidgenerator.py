"""
Created on 14.11.2013

:author: tobiaskipp
"""

import processes.qc.dohandler as dohandler
import processes.qc.splitqc as splitqc
import processes.qc.sqlitepid as sqlitepid

import urllib2

class PidGenerator():
    """ The PidGenerator is used to access or create a database and add PIDs to it.

    By supplying a path to the create_pids method, the path is searched for folders containing
    NetCDF files. It generates a PID for each file and one PID for each collection, which in 
    this case is equivalent to a path.
    Currently only tested with a SQLite3 database.
    """
    def __init__(self,database_location,data_node):
        """ Load or create the database and initialize tables if needed.
        
        :param database_location: The sqlite database file reference.
        """
        self.sqlpid = sqlitepid.SqlitePid(database_location) 
        self.sqlpid.first_run()
        self.do_handler = dohandler.DOHandler()
        self.errors = []
        self.data_node = data_node
        self.SERVERDIR = "/thredds/fileServer/cordex"
        self.HANDLESERVER = "http://handleoracle.dkrz.de:8090/handle/"

    def _to_server_name(self,local_name,search_path):
        """Create the server path name from the local path name.

        :param local_name: The local name (path or file).
        :param search_path: The root of the search path
        :returns: The name on the server
        """
        #remove the search_path and CORDEX/
        stripped_name = local_name.lstrip(search_path).lstrip("CORDEX/")                
        #create the server path from the stripped path 
        server_name = self.data_node+self.SERVERDIR+stripped_name
        return server_name


    def create_pids(self,search_path):
        """ Search through the paths structure. Create a PID for every unknown file. If the 
        elements in the collection is change create a new PID for the new collection. If 
        the same data as last time is used make no changes.
        Add the location and identifier for every new information to the database.

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
            id_by_path = dict()#map a path to its contained identifiers
            for path in sqc.datasets:
                server_path = self._to_server_name(path,search_path)
                id_by_path[server_path]=[]
            #for each file found in the search check if it already exists in the database
            for filename in sqc.full_path_files:
                server_filename = self._to_server_name(filename,search_path)
                dbentry = self.sqlpid.get_by_key_value("location",server_filename)
                server_path = "/".join(server_filename.split("/")[:-1])
                #if it does not exist in the database create a digital object in the handle system,
                #add the identifier to the dataset's list and store the do information in the database.
                if(len(dbentry)==0):
                    identifier = self.do_handler.link(server_filename)
                    self.sqlpid.add_do(server_filename,identifier)
                #if it exists add the digital object identifier to the dataset list.
                else:
                    #dbentry[0] is the first found result [1] is for the identifier
                    identifier = str(dbentry[0][1])
                id_by_path[server_path].append(identifier)
            #Similar to the file it is searched for the existance of path/dataset in the database
            #The identifiers for the files are added to the collection.
            #To avoid duplicate content digitial objects the current collection is compared
            #with the new collection entries. At any difference a new DO is generated.
            for path in sqc.datasets:
                server_path = self._to_server_name(path,search_path)
                dbentry = self.sqlpid.get_by_key_value("location",server_path)
                collection_id=""
                create_new = True

                if(len(dbentry)!=0):
                    collection_id = str(dbentry[0][1])
                    #find the PIDs in the collection
                    qc_file = urllib2.urlopen(self.HANDLESERVER+collection_id)
                    text = qc_file.read()
                    prefix = "10876/SQL-CORDEX-" 
                    textsplit = text.split('"')
                    pids = [k for k in textsplit if k[:len(prefix)]==prefix]
                    #each pid occurs twice in the handle file therefore duplicates are eliminated
                    pids = list(set(pids))
                    pids.sort()
                    new_pids = id_by_path[server_path]
                    new_pids.sort()
                    #compare the collections local and server. If they are equal no new DO is created.
                    if(new_pids == pids):
                        create_new = False

                if(create_new):
                    docollection,collection_id = self.do_handler.collection_do()
                    self.sqlpid.add_do(server_path,collection_id)
                #It is assumed that add_to_collection prevents the creation of an digital object if the 
                #reference already exists in the collection.
                self.do_handler.add_to_collection(collection_id,id_by_path[server_path]) 
        else:
            self.errors+=sqc.errors
        return valid
    

