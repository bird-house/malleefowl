"""
Created on 14.11.2013

:author: tobiaskipp
"""

import os

class SplitQc():
    """The class is used to search for folders that contain .nc files.
    
    For a valid result it is required that there is no further folder once the nc files are found.
    In additon the only files allowed are those ending with .nc.
    If the structure matches the above expectations the search method returns True.
    errors stores a list of error messages. It might be used to identify problems.
    """

    def __init__(self):
        """Initialize the storage variables and the valid flag."""
        self.datasets = []
        self.all_ok = True
        self.total_file_count = 0
        self.full_path_files = []
        self.errors = []

    def _error(self,text):
        self.errors.append(text)

    def _path_type(self,dir_list,path):
        """ Determine the type of the current folder and store additional information.
        
        For file it is required that all files end with .nc. 
        If the type is file the filename with full path is stored in full_path_files.
        If the type is not file or folder the valid flag will be set false.

        :param dir_list: The list of items in the current folder.
        :param path: The search starts at the given path. It should be equal to QC_PROJECT_DATA.
        :returns: Type of the folder referenced by path. Can be ["mixed", "folder", "file", ""]
        """
        pt = ""
        is_folder = False
        is_file = False
        for element in dir_list:
            fullname = os.path.join(path,element)
            if(os.path.isdir(fullname)):
                is_folder=True
            else:
                is_file=True
                if(element[-3:]!=".nc"):
                    self._error("Filename is not *.nc:%s"%fullname)
                    self.all_ok = False
                    pt = "Bad Name"
        if(pt == ""):
            if(is_folder and is_file):
                self._error("Path %s is of mixed type."%path)
                pt = "mixed"
                self.all_ok=False
            elif(is_folder):
                pt = "folder"
            elif(is_file):
                pt = "file"
                self.total_file_count+=len(dir_list)
                for filename in dir_list:
                    self.full_path_files.append("/".join((path,filename)))
        return pt
    
               
    def search(self,path):
        """ Searches recursivley for files in the directory tree starting at the given path.
        
        In the case the search was not valid and it should no further processing 
        be done. The class variable errors contains the error messages.

        :param path: The path to start searching at.
        :returns: If the search found a valid directory tree.
        """
        dir_list=os.listdir(path)
        pt =self._path_type(dir_list,path)
        if(pt=="folder"):
            for element in dir_list:
                fullname = os.path.join(path,element)
                self.search(fullname)
        elif(pt=="file"):
            self.datasets.append(path)
        return self.all_ok
