"""

Date: 15.11.2013
Author: Tobias Kipp (kipp@dkrz.de)
"""
import yaml
import os
import datetime

import processes.qc.sqlitepid as sqlitepid 
from pyesgf.search import SearchConnection

HTTPEXTENSION = "|application/netcdf|HTTPServer"

class Yaml2Xml():
    def __init__(self,data_node,index_node,access, xml_output_path,replica,latest,
                 metadata_format,sqlite_database,addpath):
        ADDPATH = addpath
        self.PATHLIST_NAMES=["domain","institute","driving_model","experiment","ensemble","model",
                       "version", "time_frequency","variable"]
        self.xml_output_path = xml_output_path
        #make sure that xml_output_path ends with /. 
        if(self.xml_output_path[-1]!="/"):
            self.xml_output_path+="/"
        self.project_data_path = ""
        self.VARIABLEMAP = self._import_variable_map(ADDPATH+"variableMap.csv")
        self.DEG44 = self._import_variable_map(ADDPATH+"c44.csv")
        self.DEG44I = self._import_variable_map(ADDPATH+"c44i.csv")
        self.EXPERIMENTS = self._import_variable_map(ADDPATH+"experimentFamily.csv")
        self.data_node=data_node 
        self.index_node =index_node
        self.latest=latest
        self.replica=replica
        self.access=access
        self.metadata_format=metadata_format
        self.sqlite_database = sqlite_database
        self.sqlitepid = sqlitepid.SqlitePid(self.sqlite_database)
        self.FILETYPES = ["QC-File","QC-Dataset","File","Dataset"]
        self.allow_esg_search = False #Significantly increases the processing time if True
        self.clear()
        #self.createdLinks = []
        self.global_count_by_checkresult = {"fail":0,"omit":0,"pass":0,"fixed":0}
        self.dataset_name_to_path = dict()
        self._add_dataset_cache = dict()
        self.QCDOCSERVERPATH="esgf-dev.dkrz.de/qc_docs/"
        self.METADATAVIEW = "http://esgf-dev.dkrz.de/esgf-web-fe/metadataview/"
        self.DATANODETHREDDS = "https://"+self.data_node+"/thredds/fileServer/cordex/"
        self.HANDLESERVER = "http://handleoracle.dkrz.de:8090/handle/"
        self.qc_filenames = []

    def clear(self):
        """ Empties the storage for the current log file.

        The method clears the stored variables per log file. It has to be called
        before the new logfile is loaded with the load_file method.
        """
        self.errors = []
        self.to_write_files= []
        self.not_write_files = []
        self.store = None
        self.dataset_contained_ids=dict()#maps a dataset_id to a list of file ids
        #parameters[filetype][identifier][parameter_name] => parameter_value
        #Is used to store the parameters as name value pairs for the differnt file types.
        self.parameters = dict()
        self.esgfinfo_by_masterid=dict()

        #server_xml_filenames[filetype][identifier] => global location of the xml file
        #xml_filenames[filetype][identifier] => local location of the xml file
        self.xml_filenames=dict()
        self.server_xml_filenames=dict() 
        for filetype in self.FILETYPES:
            self.xml_filenames[filetype]=dict()
            self.server_xml_filenames[filetype]=dict()

    def run(self):
        """Collect information, arrange it and create files.

        It gathers the file information found in the YAML file. The items in the yaml file 
        contain a file property if there is no serious issue. The file identifier is added
        to the files that are allowed to be generated and the dataset it belongs to. In addition
        the check results and events are stored. 
        If the item has no file property it has to be atomic. In that case no file can be
        generated and the erros are logged. If it is neither it is a serious issue that needs
        to be handled by the QC team. 

        After handling each file the dataset information is generated.
        A name generator creates a name for each file.
        This name is used in the creation of all files that are allowed to be written.
        """
        #find the PROJECT_DATA variable in the YAML file
        self.project_data_path=self._get_project_data_path()
        #Collect the information for each file described in the YAML file
        length=len(self._get_by_keys(['items']))
        for i in range(0,length):
            file_parameters=self._gather_file_parameters(i)
            if(file_parameters is not None):
                identifier = file_parameters["title"]
                self._add_to_parameters("File",identifier,file_parameters)
                self.to_write_files.append(identifier)#unless events finds an error allow to write.
                dataset_id = file_parameters["dataset_id"]
                if(not(dataset_id in self.not_write_files or dataset_id in self.to_write_files)):
                    self.to_write_files.append(dataset_id)
                events = self._gather_events(i,file_parameters)
                self._add_to_parameters("QC-File-Events",identifier,events)
                checks = self._gather_checks(i)
                self._add_to_parameters("QC-File-Checks",identifier,checks)

                self._add_to_dataset(dataset_id,identifier)
            else:
                atomic = self._get_by_keys(["items",i,"atomic"])
                if(atomic is None):
                    self._add_error("There is a serious problem, as neither file nor atomic can be "+
                    "found in the quality control log file for the entry with number "+str(i)+".")
                else:
                    self._add_error("Atomic set "+atomic+" has the tags:")
                    self.not_write_files.append(atomic)
                events =  self._get_by_keys(["items",i,"events"])
                if events is not None:
                   for eventlist in events:
                       self._add_error(eventlist["event"]["tag"])

        for dataset_id in self.dataset_contained_ids:
            dataset_parameters = self._gather_dataset_parameters(dataset_id)
            self._add_to_parameters("Dataset",dataset_id,dataset_parameters)

        self._create_names()
        self._create_xml()

    def _add_error(self,message):
        """ Adds an error message to a temporal storage.
        
        :param message: String of the error. 
        """
        self.errors.append(message)

    def show_all_errors(self):
        """Glues together all found errors in this cycle. 
           
           The clear method will erase all errors. Use this method calling clear method 
           to store the errors elsewhere.

           :returns: A string containing all errors in this cycle.
        """
        out=""
        for error in self.errors:
            out+=error+"\n"
        if len(self.not_write_files) > 0:
            out+="Write not allowed for:\n"
            for wna in self.not_write_files:
                out+=wna+"\n"
        return out

    def _add_to_parameters(self,filetype,identifier,parameters):
        """Stores the map containing the parameters for the XML files.

        :param filetype: The type of the to generate file. ["File","Dataset","QC-File","QC-Dataset"]
        :param identifier: An identifier for the current file. It is shared for Metadata and QC.
        :param parameters: A map from a keyword to its value. e.g. parameters["size"] => 200
        """
        if filetype not in self.parameters:
           self.parameters[filetype]=dict()
        self.parameters[filetype][identifier]=parameters


    def esg_search(self,master_id):
        """Uses the external esg search. It has some delay involved therfore it is not used by default.
          
        It searches by the master_id and stores the information found. To avoid duplicate searches
        with the delay it is checked if the master_id was already used. 
          
        :param master_id: The master_id is a facet that allows to find the dataset.
        """
        if(master_id not in self.esgfinfo_by_masterid):
            self.esgfinfo_by_masterid[master_id]= dict()
            sc = SearchConnection('http://esgf-data.dkrz.de/esg-search', distrib=True)
            ctx = sc.new_context(project='CORDEX', master_id=master_id)
            if(ctx.hit_count>0):
                dataset=ctx.search()[0]
                self.esgfinfo_by_masterid[master_id]["dataseturls"]=dataset.urls
                file_context=dataset.file_context()
                #look for the first file and remove the filename from the url
                for file_i in file_context.search():
                    furl = file_i.download_url
                    fs = furl.split("/")
                    url = fs[0]+"/"+"/".join(fs[1:-1])+"/"
                    self.esgfinfo_by_masterid[master_id]["server_dir"]=url
                    break#just look at the first file

    def load_file(self, filename):
        """Load the log file in YAML format into the memory.

        :param filename: The name of the logfile. 
        """
        self.store = yaml.safe_load(file(filename, 'r'))

    def _get_by_keys(self,keylist,base=None):
        """Searches through a nested dictionary. If there is a key error None is returned.

        :param keylist: The keys in the order they are to be applied to the nested dictionary.
        :param base: The root dictionary. If not given the root of the YAML file dictionary is used.
        :returns: The found entry at the given keys or None.
        """
        if base is None:
            base = self.store
        out = None
        current = base
        valid = True
        for key in keylist:
            try:#if its a valid access method
                current = current[key]
            except:#else return none
                valid = False
                break
        if valid:
           out = current
        return out

    def _import_variable_map(self,filename,separator=","):
        """Load a csv-style file and store it as map. 

        The first line contains the header. The following lines start with the key
        followed by the values of the respective keyin the header.

        example file content:
        variable,variable_long                
        tas,Near-Surface Air Temperature

        The header lines defines the key "variable_long" and the first non-header line 
        defines the key "tas". map["tas"]["variable_long"] will return Near-Surface Air Temperature
        """
        variable_map = dict()
        f = open(filename,"r")
        header = f.readline()
        header = header.rstrip("\n")
        hs = header.split(",")
        for line in f.readlines():
            line= line.rstrip("\n")
            ls = line.split(",")
            variable_map[ls[0]]=dict()
            for i in range(1,len(hs)):
                variable_map[ls[0]][hs[i]]=ls[i]
        return variable_map

    def _add_to_dataset(self,dataset_id,file_id):
        """Add a file identifier to its dataset. 

        :param dataset_id: The dataset the file belongs to. If the dataset is not known it is added.
        :param file_id: The file to be added to the dataset.
        """
        if dataset_id not in self.dataset_contained_ids:
           self.dataset_contained_ids[dataset_id]=[]
        self.dataset_contained_ids[dataset_id].append(file_id)


    def _field_name_line(self,field_name,value):
        """Generates the default XML line for parameters.
        
        :param file_name: The name of the facet
        :param value: The value of the facet.
        """
        return ("    <field name=\""+str(field_name)+"\">"+str(value)+"</field>")
     
    def _sorted_field_name_lines(self,facets,regexplist=None):
        """Generates a sorted list of XML lines. The sorting can be set by a limited regular expression.

        Due to mixing of letters and numbers the default sorting might not be good enough. In that 
        case a regular expression of alternating letters and numbers, however with a fixed structure,
        can be used. 

        :param facets: The parameters that should be used to generate the XML lines.
        :param regexplist: A list of strings containing only "(\D+)" and "(\d+)". Use it to get the 
            sort working right for mixed type keys.
        """
        outlines = []
        sortkeys = []
        for key in facets:
            sortkeys.append(key)
        if regexplist is not None:
           import re
           regular = "".join(["^"]+regexplist+["$"])
           key_pat = re.compile(regular)
           def key( item ):
               m= key_pat.match( item )
               groups = m.groups()
               groupl = []
               for i in range(1,len(regexplist)):
                   if(regexplist[i]=="(\d+)"):
                       groupl.append(int(groups[i]))
                   else:
                       groupl.append(groups[i])
               return groupl
           sortkeys.sort(key=key)
        else:
           sortkeys.sort()
        for key in sortkeys: 
            outlines.append(self._field_name_line(key,facets[key]))
        return outlines


    #def _create_name_shared(self,filetype,identifier):
    #    """Create names for the files that will be generated. For now the names 
    #    are very similar vor the differnt filetypes.

    #    :param filetype: The type of the XML file. ["File","Dataset","QC-File","QC-Dataset"]
    #    :param identifier: The identifier of the file or dataset.
    #    """
    #    filename = self.xml_output_path+filetype+"-"+identifier+".xml"
    #    self.xml_filenames[filetype][identifier]= filename
    #    if(filetype=="Dataset"):
    #        self.server_xml_filenames[filetype][identifier]=self.METADATAVIEW+filename
    #    if(filetype[:2]=="QC"):
    #        self.server_xml_filenames[filetype][identifier]=self.QCDOCSERVERPATH+filename
    #    else:#if File
    #        self.server_xml_filenames[filetype][identifier]=self.DATANODETHREDDS+filename

    def _create_name_file(self,identifier):
        """Create a name for a "File" type """ 
        filetype="File"
        version = self.parameters["File"][identifier]["version"]
        name = filetype+"-"+identifier+".v"+version+".xml"
        self.xml_filenames[filetype][identifier]= self.xml_output_path+name
        dataset_id = self.parameters["File"][identifier]["dataset_id"]
        dataset_name = ".".join(dataset_id.split("|")[0].split(".")[:-1])
        path = self.dataset_name_to_path[dataset_name]
        noprefixpath = path.lstrip(self.project_data_path)
        self.server_xml_filenames[filetype][identifier] =self.DATANODETHREDDS+noprefixpath+"/"+identifier

    def _create_name_qc_file(self,identifier):
        """Create a name for a "QC-File" type """ 
        filetype="QC-File"
        version = self.parameters["File"][identifier]["version"]
        name = filetype+"-"+identifier+".v"+version+".xml"
        self.xml_filenames[filetype][identifier]= self.xml_output_path+name
        self.server_xml_filenames[filetype][identifier] = self.QCDOCSERVERPATH+name

    def _create_name_dataset(self,identifier):
        """Create a name for a "Dataset" type
        
        For Dataset the node should not be part of the filename.
        instance_id is the id without the node
        """ 
        filetype = "Dataset"
        nodeless_identifier = self.parameters["Dataset"][identifier]["instance_id"]
        name = filetype+"-"+nodeless_identifier+".xml"
        self.xml_filenames[filetype][identifier]= self.xml_output_path+name
        #The file in server_xml_filenames will not be used directly. The name could be better.
        self.server_xml_filenames[filetype][identifier] = self.METADATAVIEW+identifier+".html"

    def _create_name_qc_dataset(self,identifier):
        """Create a name for a "QC-Dataset" type 

        For Dataset the node should not be part of the filename.
        """ 
        filetype = "QC-Dataset"
        nodeless_identifier = self.parameters["Dataset"][identifier]["instance_id"]
        name = filetype+"-"+nodeless_identifier+".xml"
        self.xml_filenames[filetype][identifier]= self.xml_output_path+name
        self.server_xml_filenames[filetype][identifier] = self.QCDOCSERVERPATH+name

    def _create_xml_shared(self,filename,lines):
        """The _create_xml methods share the write of a list of lines to a file.

        :param filename: The target files name
        :param lines: The list of lines to be written.
        """
        f = open(filename,"w")
        for line in lines:
            f.write(line+"\n")
        f.close()
          

    def _create_xml_file(self,identifier):
        """Uses the information found earlier to create an XML file for the "File" type.

        In addition the metadata the checks and the events are added.

        :param identifier: The identifier of the file.
        """
        filename = self.xml_filenames["File"][identifier] 
        #Meta info
        fieldlines=self._sorted_field_name_lines(self.parameters["File"][identifier])
        fieldlines.append(self._field_name_line("experiment_family","All"))
        qualityurl = self.server_xml_filenames["QC-File"][identifier]+"|application/xml|QCDoc"
        fieldlines.append(self._field_name_line("url",qualityurl))
        fieldlines.sort()
        #QC info
        checkmap = self.parameters["QC-File-Checks"][identifier]
        checks = self._sorted_field_name_lines(self._rename_map(checkmap,"checks_"))
        eventmap = self.parameters["QC-File-Events"][identifier]
        events = self._sorted_field_name_lines(eventmap)

        lines=[]
        lines.append("<doc schema=\"esgf\">")
        lines+=fieldlines
        lines+=checks#QC
        lines+=events#QC
        lines.append("</doc>")
        self._create_xml_shared(filename,lines)

    def _create_xml_dataset(self,identifier):
        """Uses the information found earlier to create an XML file for the "Dataset" type

        In addition the metadata the checks and the events are added.

        :param identifier: The identifier of the dataset.
        """
        filename = self.xml_filenames["Dataset"][identifier]
        #Meta info
        fieldlines = self._sorted_field_name_lines(
            self.parameters["Dataset"][identifier])
        qualityurl = self.server_xml_filenames["QC-Dataset"][identifier]
        fieldlines.append(self._field_name_line("experiment_family","All"))
        fieldlines.append(self._field_name_line("quality_url",qualityurl))
        fieldlines.sort()
        #QC info
        file_ids = self.dataset_contained_ids[identifier]
        k=0
        events = dict()
        files = dict()
        found_tags = []
        count_by_checkresult = {"fail":0,"omit":0,"pass":0,"fixed":0}
        for fileid in file_ids:
            files["file_"+str(k)] = fileid
            checkmap = self.parameters["QC-File-Checks"][fileid]
            for check in checkmap:
                result = checkmap[check]
                count_by_checkresult[result.lower()]+=1
            eventmap = self.parameters["QC-File-Events"][fileid]
            for key in eventmap:
                events["file_"+str(k)+"_"+key]=eventmap[key]
                found_tags.append(eventmap[key].split(":::")[0])
            k+=1

        additional_lines = []
        dataset_checks_okay = False
        if((count_by_checkresult["fail"]+count_by_checkresult["omit"])==0):
            dataset_checks_okay = True
        additional_lines.append(self._field_name_line("QC_Pass",str(dataset_checks_okay)))
        additional_lines.append(self._field_name_line("QC_Status","pid"))
        found_tags = list(set(found_tags))#eliminate duplicates
        for tag in found_tags:
            additional_lines.append(self._field_name_line("QC_Tag",tag))
  
        qc_dataset_parameters = dict()
        for key in count_by_checkresult:
            qc_dataset_parameters["checks_"+key] = count_by_checkresult[key]


        lines=[]
        lines.append("<doc schema=\"esgf\">")
        lines+=fieldlines
        lines.append("")#QC information following
        lines+=additional_lines
        lines+=self._sorted_field_name_lines(events)
        lines+=self._sorted_field_name_lines(qc_dataset_parameters)
        if(len(events) > 0):
            lines+=self._sorted_field_name_lines(files,["(\D+)","(\d+)"])
        lines.append("</doc>")
        self._create_xml_shared(filename,lines)

    def _rename_map(self,Map,Prefix="",Suffix=""):
        """ Allows to add a prefix and suffix to all keys in a dictionary """
        map2=dict()
        for key in Map:
            map2[Prefix+key+Suffix]=Map[key]
        return map2

    def _create_xml_qc_file(self,identifier):
        """Uses the information found earlier to create an XML file for the "QC-File" type

        Next to check results and events, references to the metadata are given.

        :param identifier: The identifier of the file.
        """
        filename = self.xml_filenames["QC-File"][identifier]
        qc_dataset_parameters = dict()
        file_parameters = self.parameters["File"][identifier]
        qc_dataset_parameters["file_id"]=file_parameters["master_id"]
        if("url" in file_parameters):
            qc_dataset_parameters["file_url"] = file_parameters["url"][:-len(HTTPEXTENSION)]
        qc_dataset_parameters["version"]=file_parameters["version"]

        #qc_dataset_parameters["file_metadata_url"]=self.server_xml_filenames["File"][identifier]
        checkmap = self.parameters["QC-File-Checks"][identifier]
        checks = self._sorted_field_name_lines(self._rename_map(checkmap,"checks_"))
        eventmap = self.parameters["QC-File-Events"][identifier]
        events=self._sorted_field_name_lines(eventmap)
        lines=[]
        lines.append("<doc schema=\"QC-File\">")
        lines+=checks
        lines+=events
        lines+=self._sorted_field_name_lines(qc_dataset_parameters)
        lines.append("</doc>")
        self.qc_filenames.append(filename)
        self._create_xml_shared(filename,lines)

    def _create_xml_qc_dataset(self,identifier):
        """Uses the information found earlier to create an XML file for the "QC-Dataset" type

        Next to check results and events, references to the metadata are given.

        :param identifier: The identifier of the dataset.
        """
        filename = self.xml_filenames["QC-Dataset"][identifier]
        file_ids = self.dataset_contained_ids[identifier]
        k=0
        events = dict()
        files = dict()
        count_by_checkresult = {"fail":0,"omit":0,"pass":0,"fixed":0}
        for fileid in file_ids:
            files["file_"+str(k)] = fileid
            checkmap = self.parameters["QC-File-Checks"][fileid]
            for check in checkmap:
                result = checkmap[check]
                count_by_checkresult[result.lower()]+=1
                self.global_count_by_checkresult[result.lower()]+=1
            eventmap = self.parameters["QC-File-Events"][fileid]
            for key in eventmap:
                events["file_"+str(k)+"_"+key]=eventmap[key]
            k+=1
  
        qc_dataset_parameters = dict()
        for key in count_by_checkresult:
            qc_dataset_parameters["checks_"+key] = count_by_checkresult[key]
        qc_dataset_parameters["number_of_files"]=len(files)
        qc_dataset_parameters["metadata_url"]=self.server_xml_filenames["Dataset"][identifier]
        lines=[]
        lines.append("<doc schema=\"QC-Dataset\">")
        lines+=self._sorted_field_name_lines(events)
        lines+=self._sorted_field_name_lines(qc_dataset_parameters)
        lines+=self._sorted_field_name_lines(files,["(\D+)","(\d+)"])
        lines.append("</doc>")
        self.qc_filenames.append(filename)
        self._create_xml_shared(filename,lines)

    def _concatenate(self,dict_or_list,keys,separator,prefix=""):
        """The methods helps to avoid using the name of the dictonary or list again and a again.

        :param dict_or_list: A dictionary or list whichs name should not be written again and again.
        :param keys: The keys according to the type used.
        :param separator: The text between each element.
        :param prefix: Add something in front of the the generated text.
        :returns: The concatenated text.
        """
        out = prefix
        for key in keys:
            out+=dict_or_list[key]+separator
        out=out.rstrip(separator)
        return out

    def _add_dataset(self,path):
        """ Gathers information from the path, creates a dataset_id and remembers the path for it.
        
        To prevent multiple generations of the same data the results are cached.
        :param path: The path of the dataset
        :returns: The dataset_id and path_info containing parameters used for facets.
        """

        if(path not in self._add_dataset_cache):
            path_info = dict()
            path_line = path.lstrip(self.project_data_path)
            path_list= path_line.split("/")
            for j in range(len(path_list)):
                path_info[self.PATHLIST_NAMES[j]] = path_list[j]
            path_info["full"] = path_line
            #The model in the example is RCA4-v1. Therfore the institute- part has to be removed
            #and the version in the path has to be appended
            path_info["metaModel"]= (path_info["model"].lstrip(path_info["institute"]+"-")+
                "-"+path_info["version"])
            dataset_id = self._concatenate(path_info,["domain","institute","driving_model",
                              "experiment","ensemble","metaModel","time_frequency","variable"],
                              ".",prefix="cordex.")
            self.dataset_name_to_path[dataset_id]=path
            self._add_dataset_cache[path] = dataset_id,path_info
        return self._add_dataset_cache[path]

    def _gather_file_parameters(self,file_index):
        """Searches through the file information with the given index for factes.

        In addition to the facets that can be found directly in the YAML file, 
        the variables that can be derived are created. 
        When the esg search is enabled the version and data_node are set accordingly. 

        :param file_index: The index of the file according to the YAML file.
        :returns: The facets for the file.
        """
         
        filename = self._get_by_keys(["items",file_index,"file"])
        if(filename==None):
            return None
        file_parameters = dict()
        path_line = self._get_by_keys(["items",file_index,"data_path"])
        ds_master_id,path_info = self._add_dataset(path_line)
        file_parameters["title"] = filename
        #version : as the date in YYYYMMDD format.
        versiondate = self._get_by_keys(["start","date"])
        file_parameters["version"]=str(versiondate).replace("-","")[:8]
        file_parameters["data_node"]=self.data_node
        file_parameters["index_node"] =self.index_node
        file_parameters["latest"]=self.latest
        file_parameters["replica"]=self.replica
        file_parameters["access"]=self.access
        file_parameters["metadata_format"]=self.metadata_format
        file_parameters["type"]="File"
        file_parameters["model"] = path_info["metaModel"]
        kwl=["ensemble","experiment","institute","time_frequency","variable","domain","driving_model"]
        for facet in kwl:
            file_parameters[facet]= path_info[facet]

        domain = file_parameters["domain"]
        deg_dict = None
        if domain in self.DEG44:
            deg_dict=self.DEG44
        elif domain in self.DEG44I:
            deg_dict=self.DEG44I
        if(deg_dict is not None):
            for direction in ["west","east","north","south"]:
                name = direction+"_degrees"
                file_parameters[name]=deg_dict[domain][name]
        experiment = file_parameters["experiment"]
        if experiment in self.EXPERIMENTS:
            file_parameters["experiment_family"]=self.EXPERIMENTS[experiment]["experiment_family"]
        else:
            self._add_error("Unknown experiment_family for experiment "+experiment)
        file_parameters["project"] = self._get_by_keys(["configuration","options","PROJECT"])
        period = self._get_by_keys(['items',file_index,'period'])
        if(period!=None):
            begin = period["begin"]
            if(isinstance(begin,datetime.datetime)):
                #begin = "T".join(str(begin).split(" "))+"Z"
                begin = str(begin).replace(" ","T")+"Z"
            end = period["end"]
            if(isinstance(end,datetime.datetime)):
                #end = "T".join(str(end).split(" "))+"Z"
                end = str(end).replace(" ","T")+"Z"
            file_parameters["datetime_start"]=str(begin)
            file_parameters["datetime_stop"]=str(end)
        #generate long variable name from variable if known. Else set it to unknown.
        variable = file_parameters["variable"]
        for keyword in ["variable_long_name","variable_units","cf_standard_name"]:
            value = self._get_by_keys([variable,keyword],self.VARIABLEMAP)
            if(value!=None):
                file_parameters[keyword]=value

        if(self.allow_esg_search):
            #try to find the data in the search. Overwrite data_node and version if it exists.
            self.esg_search(ds_master_id)
            if("server_dir" in self.esgfinfo_by_masterid[ds_master_id]):
                server_dir = self.esgfinfo_by_masterid[ds_master_id]["server_dir"]
                file_parameters["url"]=server_dir+file_parameters["title"]+HTTPEXTENSION
                file_parameters["data_node"]=server_dir.lstrip("http://").split("/")[0]
                #the file exists therefore grab the existing version number.
                a = self.esgfinfo_by_masterid[ds_master_id]["dataseturls"]["Catalog"][0][0]
                b = a.split(".")[-1]
                c = b.split("|")[0].lstrip("v")
                file_parameters["version"]=c
        else:
            server_dir = "http://"+file_parameters["data_node"]+"/thredds/fileServer/cordex/"
            #file_parameters["url"] = server_dir+path_info['full']+"/"+filename+ HTTPEXTENSION
            results = self.sqlitepid.get_like_location(filename)
            #results contains a list of results. It is expected to contain 1 result. An error
            #message will be generated if it does not match with this. If there are more results
            #The first one will be used in the file generation.
            #The tuple is (location,identifier,url)
            url = ""
            reslen = len(results)
            if(reslen > 0):
                pid = results[0][1]
                url = self.HANDLESERVER+pid
                file_parameters["pid"]=pid
                if(reslen != 1):
                    self._add_error("There are too many results for the file "+filename+
                        ". Please check if the duplicates are intended.")
                    for result in results:
                        self._add_error(str(result[0]))#location
                    self._add_error("\n")
            else:
                self._add_error("Did not find an url for the file "+filename)
            file_parameters["url"] = url+HTTPEXTENSION

        file_parameters["id"]=(ds_master_id+".v"+file_parameters["version"]+"."+filename+"|"+
            file_parameters["data_node"])
        file_parameters["dataset_id"] = (ds_master_id+".v"+file_parameters["version"]+"|"+
            file_parameters["data_node"])
        file_parameters["instance_id"]= ds_master_id+".v"+file_parameters["version"]+"."+filename
        file_parameters["master_id"]= ds_master_id+"."+filename

        fullname =path_line+"/"+filename
        try:#to get the size if allowed. Works only for local files, like the QC.
            file_parameters["size"] = str(os.stat(fullname).st_size)
        except:
            pass
        return file_parameters

    def _get_project_data_path(self):
        """ Searches for the PROJECT_DATA variable in the YAML file. 

        The PROJECT_DATAV may be a list. In that case the first element is used.

        :returns: The PROJECT_DATA value.
        """
        project_data_path=self._get_by_keys(['configuration','options','PROJECT_DATAV'])
        if(isinstance(project_data_path,dict)):
            if(len(project_data_path) > 0):
                project_data_path = project_data_path[0]
        elif(not isinstance(project_data_path,str)):#should not occur
            print("Please check project data path. Type:"+str(type(project_data_path)))
        return project_data_path

    def _gather_events(self,file_index,file_parameters):
        """Finds the events in the YAML file for the given file_index

        The file_parameters are only used when a serious event occurs.

        :param file_index: The index of the item in the YAML file
        :param file_parameters: The gathered facets of the file.
        :returns: The events (as dictionary)
        """
        events_dict = self._get_by_keys(["items",file_index,"events"])
        events = dict()
        if(events_dict != None):
            def xml_replace(line):
                xmlmap = {"\"":"&quot;","<":"&lt;",">":"&gt;","&":"&amp;"}
                for key in xmlmap:
                    line=line.replace(key,xmlmap[key])
                return line

            k = 0 # event index
            for eventlist in events_dict:
                event = eventlist['event']
                #It is assmued that event always has "tag","impact" and "caption"
                out = str(event["tag"])+":::"+str(event["impact"])+":::"+str(event["caption"])
                if "text" in event:
                    out+=":::"
                    for line in event["text"]:
                        out+=line+","
                out=xml_replace(out[:-1])
                events["event_"+str(k)]=out
                if(event["tag"]=="M5"):
                    self._add_error("Tag M5. The files are not allowed to be uploaded.")
                    self.not_write_files.append(file_parameters["title"])
                    self.to_write_files.remove(file_parameters["title"])
                    self.not_write_files.append(file_parameters["dataset_id"])
                    self.to_write_files.remove(file_parameters["dataset_id"])
                k+=1
        return events

    def _gather_checks(self,file_index):
        """Finds the checks for the given file_index.
        
        :param file_index: The item index according to the YAML file.
        :returns: The found checks.
        """
        checks = dict()
        check_dict = self._get_by_keys(["items",file_index,"check"])
        if(check_dict!=None):
            for key in ["meta_data","time_values","data"]:
                checks[key] = check_dict[key]
        return checks

    def _gather_dataset_parameters(self,dataset_id):
        """Using the data gathered about files the metadata for the dataset is created.

        :param dataset_id: The identifier of the dataset. (Similar to a path)
        :returns: The facets of the dataset.
        """

        dataset_parameters = dict()
        file_count = len(self.dataset_contained_ids[dataset_id])
        identifiers = self.dataset_contained_ids[dataset_id]
        #TODO: Is the date format always valid.
        """ Due to the fact that datetime objects are used to generate the date
            strings it has always the format %Y-%m-%d %H:%M:%S,
            where %Y is the year in 4 digits year, the year 201 would be "0201"
            It is limited from year 1 to 9999. Due to this format it is also possible
            to compare the strings, which will search from start till a character is
            found that differs. 
        """
        start_times = []
        end_times = []
        size = 0

        for identifier in identifiers:
            file_parameters = self.parameters["File"][identifier]
            if("datetime_start" in file_parameters):#If datetime_start exists datetime_stop should too.
                start_times.append(file_parameters["datetime_start"])
                end_times.append(file_parameters["datetime_stop"])
            if("size" in file_parameters):
                size+=int(file_parameters["size"])

        if("datetime_start" in file_parameters):
            dataset_parameters["datetime_start"]=str(min(start_times))
            dataset_parameters["datetime_stop"]=str(max(end_times))
        if(size!=0):
            dataset_parameters["size"]=str(size)
        else:
            dataset_parameters["size"]="Not available"

        sharedValues = ["data_node","domain","driving_model","ensemble","experiment","index_node",
                        "institute",  "latest","project","model","replica","time_frequency",
                        "variable","variable_long_name","version","variable_units",
                        "cf_standard_name","experiment_family","west_degrees","east_degrees",
                        "south_degrees","north_degrees","metadata_format"]
        id0= identifiers[0]
        file_parameters = self.parameters["File"][id0]
        for field in sharedValues: 
            if(field in file_parameters):#to prevent errors for non existent fields
                dataset_parameters[field] = file_parameters[field]

        dataset_parameters["id"] = dataset_id
        #instance_id = id without |data_node
        instanceid = dataset_id.split("|")[0]
        dataset_parameters["instance_id"]=instanceid
        #master_id = instanceid without .v<version>
        masterid = instanceid.rstrip(".v"+dataset_parameters["version"])
        dataset_parameters["master_id"]=masterid
        dataset_parameters["title"]=masterid

        dataset_parameters["type"]="Dataset"
        #TODO:url
        try:
            dsurls = self.esgfinfo_by_masterid[masterid]["dataseturls"]["Catalog"]
            dataset_parameters["url"]="|".join(dsurls[0])               
        except:
            pass#if it does not exist skip it

        dataset_parameters["number_of_files"]=str(file_count)
        dataset_parameters["dataset_id_template_"]=("cordex.%(domain)s.%(institute)s."+
            "%(driving_model)s.%(experiment)s.%(ensemble)s."+dataset_parameters["model"]+
            ".%(time_frequency)s.%(variable)s")

        #dataset_name is dataset_id without the version and data_node
        dataset_name = ".".join(dataset_id.split("|")[0].split(".")[:-1])
        results = self.sqlitepid.get_like_location(self.dataset_name_to_path[dataset_name])
        #results contains a list of results. It is expected to contain 1 result. An error
        #message will be generated if it does not match with this. If there are more results
        #The first one will be used in the file generation.
        #The tuple is (location,identifier,url)
        url = ""
        reslen = len(results)
        if(reslen > 0):
            pid = results[0][1]
            url = self.HANDLESERVER+url
            dataset_parameters["pid"]=pid
            dataset_parameters["pid_url"] = url
            if(reslen != 1):
                self._add_error("There are too many results for the dataset "+dataset_id)
                self._add_error(str(results))
        else:
            self._add_error("Did not find an url for the dataset "+dataset_id)
        return dataset_parameters

    def _create_shared(self,m1,m2,m3,m4):
        """The name creation and the xml creation share the same pattern with the file generation
        protection.

        For files the dataset must be allowed to write as well.

        :param m1: The first method for files
        :param m2: The second method for files
        :param m3: The first method for datasets
        :param m4: The second method for datasets
        """
        for identifier in self.parameters["File"]:
            if(identifier in self.to_write_files):
                dsid= self.parameters["File"][identifier]["dataset_id"]
                if(dsid not in self.not_write_files):
                    m1(identifier)
                    m2(identifier)
        for identifier in self.parameters["Dataset"]:
            if(identifier in self.to_write_files):
                m3(identifier)
                m4(identifier)

    def _create_names(self):
        """Create the names for the XML files"""
        self._create_shared(self._create_name_file,self._create_name_qc_file,
                          self._create_name_dataset,self._create_name_qc_dataset)

    def _create_xml(self):
        """Create the XML files after they received their names with the _create_names method."""
        self._create_shared(self._create_xml_file,self._create_xml_qc_file,
                          self._create_xml_dataset,self._create_xml_qc_dataset)

        
        
