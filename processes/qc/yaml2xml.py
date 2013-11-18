"""

Date: 15.11.2013
Author: Tobias Kipp (kipp@dkrz.de)
"""
import yaml
import os

import processes.qc.sqlitepid as sqlitepid 
from pyesgf.search import SearchConnection

ADDPATH = "/home/tk/sandbox/climdaps/src/Malleefowl/processes/qc/"
HTTPEXTENSION = "|application/netcdf|HTTPServer"

class Yaml2Xml():
    def __init__(self,data_node="ipcc-ar5.dkrz.de",index_node="esgf-data.dkrz.de",access="HTTPServer",
                 xml_output_path ="/home/tk/sandbox/xmlresults/",replica="false",latest="true",
                 metadata_format = "THREDDS",sqlite_database=""):
        self.PATHLIST_NAMES=["domain","institute","driving_model","experiment","ensemble","model",
                       "version", "time_frequency","variable"]
        self.xml_output_path = xml_output_path
        #make sure that xml_output_path ends with /. 
        if(self.xml_output_path[-1]!="/"):
            self.xml_output_path+="/"
        self.project_data_path = ""
        self.VARIABLEMAP = self.import_variable_map(ADDPATH+"variableMap.csv")
        self.DEG44 = self.import_variable_map(ADDPATH+"c44.csv")
        self.DEG44I = self.import_variable_map(ADDPATH+"c44i.csv")
        self.EXPERIMENTS = self.import_variable_map(ADDPATH+"experimentFamily.csv")
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

    def clear(self):
        self.errors = []
        self.to_write_files= []
        self.not_write_files = []
        self.dataset_contained_ids=dict()#maps a dataset_id to a list of file ids
        #file_parameters_collection[filetype][identifier][parameter_name] => parameter_value
        #Is used to store the parameters as name value pairs for the differnt file types.
        self.file_parameters_collection = dict()
        self.esgfinfo_by_masterid=dict()
        #TODO: The handle system is no longer used for QC data. For now self.xml_filenames is used.
        #self.XMLURLS=dict() 

        #xml_filenames[filetype][identifier] => location of the xml file
        self.xml_filenames=dict()
        for key in self.FILETYPES:
            #self.XMLURLS[key]=dict()
            self.xml_filenames[key]=dict()

    """ Collect all files in the yaml and create their xml and from the collection of files
        with the same dataset_id a dataset xml is created."""
    def run(self):
        #Clear data from previous runs
        self._clear_dataset()
        #find the PROJECT_DATA variable in the YAML file
        self.project_data_path=self._get_project_data_path()
        #Collect the information for each file described in the YAML file
        length=len(self._get_by_keylist(['items']))
        for i in range(0,length):
            file_parameters=self._gather_file_parameters(i)
            if(file_parameters is not None):
                identifier = file_parameters["title"]
                self.add_to_file_parameters_collection("File",identifier,file_parameters)
                self.to_write_files.append(identifier)#unless events finds an error allow to write.
                dataset_id = file_parameters["dataset_id"]
                if(not(dataset_id in self.to_write_files or dataset_id in self.to_write_files)):
                    self.to_write_files.append(dataset_id)
                events = self._gather_events(i,file_parameters)#file_parameters is passed for the case of errors.
                self.add_to_file_parameters_collection("QC-File-Events",identifier,events)
                checks = self._gather_checks(i)
                self.add_to_file_parameters_collection("QC-File-Checks",identifier,checks)

                self.add_to_dataset(dataset_id,identifier)
            else:
                atomic = self._get_by_keylist(["items",i,"atomic"])
                if(atomic is None):
                    self._add_error("There is a serious problem, as neither file nor atomic can be "+
                    "found in the quality control log file for the entry with number "+str(i)+".")
                else:
                    self._add_error("Atomic set "+atomic+" has the tags:")
                    self.not_write_files.append(atomic)
                events =  self._get_by_keylist(["items",i,"events"])
                if events is not None:
                   for eventlist in events:
                       self._add_error(eventlist["event"]["tag"])

        for dataset_id in self.dataset_contained_ids:
            dataset_parameters = self._gather_dataset_parameters(dataset_id)
            self.add_to_file_parameters_collection("Dataset",dataset_id,dataset_parameters)

        self._create_names()
        self._create_xml()
        #self.show_all_errors()

    def _add_error(self,message):
        self.errors.append(message)

    def show_all_errors(self):
        out=""
        for error in self.errors:
            out+=error+"\n"
        if len(self.not_write_files) > 0:
            out+="Write not allowed for:\n"
            for wna in self.not_write_files:
                out+=wna+"\n"
        return out

    def add_to_file_parameters_collection(self,filetype,identifier,MAP):
        if filetype not in self.file_parameters_collection:
           self.file_parameters_collection[filetype]=dict()
        self.file_parameters_collection[filetype][identifier]=MAP


    def esg_search(self,master_id):
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
        self.store = yaml.safe_load(file(filename, 'r'))

    """ Some entries might not exist.(e.g. period does not exist for fx frequency). In that case
        None is returned. To avoid the program from crashing the try statement is used."""
    def _get_by_keylist(self,keylist,base=None):
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

    """ Load a csv-style file with a given separator. The first line contains the header.
        The following lines start with the key followed by the values of the respective key
        in the header.
        e.g.
        variable,variable_long                
        tas,Near-Surface Air Temperature
        The header lines defines the key "variable_long" and the first non-header line 
        defines the key "tas". MAP["tas"]["variable_long"] will return Near-Surface Air Temperature"""
    def import_variable_map(self,filename,separator=","):
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

    def add_to_dataset(self,dataset_id,metaid):
        if dataset_id not in self.dataset_contained_ids:
           self.dataset_contained_ids[dataset_id]=[]
        self.dataset_contained_ids[dataset_id].append(metaid)

    def _clear_dataset(self):
        self.dataset_contained_ids=dict()

    """The output format is similar to xml, however it allows the use of duplicate keys.
       Which keys are duplicate depends on the type. Therfore the shared methods will collect
       all key value pairs from a dictionary and sort them. The outer block (<doc schema="esgf">)
       is handled by eachs method to allow for adding the duplicate keys."""
    def _field_name_line(self,field_name,value):
        return ("    <field name=\""+str(field_name)+"\">"+str(value)+"</field>")
     
    """ For now regexplist may only contain (\D+) for text and (\d+) numbers.
        This is due to the conversion to an integer for (\d+) groups."""
    def _sorted_field_name_lines(self,Dict,regexplist=None):
        outlines = []
        sortkeys = []
        for key in Dict:
            sortkeys.append(key)
        if regexplist is not None:
           import re
           regular = "".join(["^"]+regexplist+["$"])
           key_pat = re.compile(regular)# r"^(\D+)(\d+)$" )
           def key( item ):
               m= key_pat.match( item )
               groups = m.groups()
               groupl = []
               for i in range(1,len(regexplist)):
                   if(regexplist[i]=="(\d+)"):
                       groupl.append(int(groups[i]))
                   else:
                       groupl.append(groups[i])
               return groupl# m.group(1), int(m.group(2))
           sortkeys.sort(key=key)
        else:
           sortkeys.sort()
        for key in sortkeys: 
            outlines.append(self._field_name_line(key,Dict[key]))
        return outlines

    #def link(self,identifier,location):
    #    return self.doHandler.link(identifier,location)

    # For now the name creation is the same for all processes, however it should be 
    # flexible enough to allow idividual implementations.
    def _create_names_shared(self,filetype,identifier):
        filename = self.xml_output_path+filetype+"-"+identifier+".xml"
        self.xml_filenames[filetype][identifier]= filename
        #self.XMLURLS[filetype][identifier] =self.link(None,filename)
        #self.createdLinks.append((self.XMLURLS[filetype][identifier],filename))
         
    def _create_names_file(self,identifier):
        self._create_names_shared("File",identifier)

    def _create_names_qc_file(self,identifier):
        self._create_names_shared("QC-File",identifier)

    def _create_names_dataset(self,identifier):
        self._create_names_shared("Dataset",identifier)

    def _create_names_qc_dataset(self,identifier):
        self._create_names_shared("QC-Dataset",identifier)

    def _create_xml_shared(self,filename,lines):
        f = open(filename,"w")
        for line in lines:
            f.write(line+"\n")
        f.close()
          

    def _create_xml_file(self,identifier):
        filename = self.xml_filenames["File"][identifier] 
        #Meta info
        fieldlines=self._sorted_field_name_lines(self.file_parameters_collection["File"][identifier])
        fieldlines.append(self._field_name_line("experiment_family","All"))
        qualityurl = self.xml_filenames["QC-File"][identifier]+"|application/xml|QCDoc"
        fieldlines.append(self._field_name_line("url",qualityurl))
        fieldlines.sort()
        #QC info
        checkmap = self.file_parameters_collection["QC-File-Checks"][identifier]
        checks = self._sorted_field_name_lines(self._rename_map(checkmap,"checks_"))
        eventmap = self.file_parameters_collection["QC-File-Events"][identifier]
        events = self._sorted_field_name_lines(eventmap)

        lines=[]
        lines.append("<doc schema=\"esgf\">")
        lines+=fieldlines
        lines+=checks#QC
        lines+=events#QC
        lines.append("</doc>")
        self._create_xml_shared(filename,lines)

    def _create_xml_dataset(self,identifier):
        filename = self.xml_filenames["Dataset"][identifier]
        #Meta info
        fieldlines = self._sorted_field_name_lines(
            self.file_parameters_collection["Dataset"][identifier])
        qualityurl = self.xml_filenames["QC-Dataset"][identifier]
        fieldlines.append(self._field_name_line("experiment_family","All"))
        fieldlines.append(self._field_name_line("quality_url",qualityurl))
        fieldlines.sort()
        #QC info
        file_ids = self.dataset_contained_ids[identifier]
        k=0
        events = dict()
        files = dict()
        count_by_checkresult = {"fail":0,"omit":0,"pass":0,"fixed":0}
        for fileid in file_ids:
            files["file_"+str(k)] = fileid
            checkmap = self.file_parameters_collection["QC-File-Checks"][fileid]
            for check in checkmap:
                result = checkmap[check]
                count_by_checkresult[result.lower()]+=1
            eventmap = self.file_parameters_collection["QC-File-Events"][fileid]
            for key in eventmap:
                events["file_"+str(k)+"_"+key]=eventmap[key]
            k+=1
  
        qc_dataset_parameters = dict()
        for key in count_by_checkresult:
            qc_dataset_parameters["checks_"+key] = count_by_checkresult[key]


        lines=[]
        lines.append("<doc schema=\"esgf\">")
        lines+=fieldlines
        lines.append("")#QC information following
        lines+=self._sorted_field_name_lines(events)
        lines+=self._sorted_field_name_lines(qc_dataset_parameters)
        if(len(events) > 0):
            lines+=self._sorted_field_name_lines(files,["(\D+)","(\d+)"])
        lines.append("</doc>")
        self._create_xml_shared(filename,lines)

    def _rename_map(self,Map,Prefix="",Suffix=""):
        map2=dict()
        for key in Map:
            map2[Prefix+key+Suffix]=Map[key]
        return map2

    def _create_xml_qc_file(self,identifier):
        filename = self.xml_filenames["QC-File"][identifier]
        qc_dataset_parameters = dict()
        file_parameters = self.file_parameters_collection["File"][identifier]
        qc_dataset_parameters["file_id"]=file_parameters["master_id"]
        if("url" in file_parameters):
            qc_dataset_parameters["file_url"] = file_parameters["url"][:-len(HTTPEXTENSION)]
        qc_dataset_parameters["version"]=file_parameters["version"]
        qc_dataset_parameters["file_metadata_url"]=self.xml_filenames["File"][identifier]
        checkmap = self.file_parameters_collection["QC-File-Checks"][identifier]
        checks = self._sorted_field_name_lines(self._rename_map(checkmap,"checks_"))
        eventmap = self.file_parameters_collection["QC-File-Events"][identifier]
        events=self._sorted_field_name_lines(eventmap)
        lines=[]
        lines.append("<doc schema=\"QC-File\">")
        lines+=checks
        lines+=events
        lines+=self._sorted_field_name_lines(qc_dataset_parameters)
        lines.append("</doc>")
        self._create_xml_shared(filename,lines)

    def create_xml_qc_dataset(self,identifier):
        filename = self.xml_filenames["QC-Dataset"][identifier]
        file_ids = self.dataset_contained_ids[identifier]
        k=0
        events = dict()
        files = dict()
        count_by_checkresult = {"fail":0,"omit":0,"pass":0,"fixed":0}
        for fileid in file_ids:
            files["file_"+str(k)] = fileid
            checkmap = self.file_parameters_collection["QC-File-Checks"][fileid]
            for check in checkmap:
                result = checkmap[check]
                count_by_checkresult[result.lower()]+=1
                self.global_count_by_checkresult[result.lower()]+=1
            eventmap = self.file_parameters_collection["QC-File-Events"][fileid]
            for key in eventmap:
                events["file_"+str(k)+"_"+key]=eventmap[key]
            k+=1
  
        qc_dataset_parameters = dict()
        for key in count_by_checkresult:
            qc_dataset_parameters["checks_"+key] = count_by_checkresult[key]
        qc_dataset_parameters["number_of_files"]=len(files)
        qc_dataset_parameters["metadata_url"]=self.xml_filenames["Dataset"][identifier]
        lines=[]
        lines.append("<doc schema=\"QC-Dataset\">")
        lines+=self._sorted_field_name_lines(events)
        lines+=self._sorted_field_name_lines(qc_dataset_parameters)
        lines+=self._sorted_field_name_lines(files,["(\D+)","(\d+)"])
        lines.append("</doc>")
        self._create_xml_shared(filename,lines)

    """ To avoid having to write the dictionary name again and again, a concatenation method
        is defined. It connects the elements of a dictionary by inserting the separator.
        The last element has no separator following it."""
    def concatenate(self,DictOrList,Keys,separator,prefix=""):
        out = prefix
        for key in Keys:
            out+=DictOrList[key]+separator
        out=out.rstrip(separator)
        return out

    def _gather_file_parameters(self,fileIndex):
        filename = self._get_by_keylist(["items",fileIndex,"file"])
        path_info = dict()
        if(filename==None):
            print("filename is a required variable. Stopping XML generation.")
            return None
        file_parameters = dict()
        path_line = self._get_by_keylist(["items",fileIndex,"data_path"]).lstrip(self.project_data_path)
        path_list= path_line.split("/")
        path_info["full"] = path_line
        for j in range(len(path_list)):
            path_info[self.PATHLIST_NAMES[j]] = path_list[j]
        file_parameters["title"] = filename
        #version : as the date in YYYYMMDD format.
        versiondate = self._get_by_keylist(["start","date"])
        file_parameters["version"]=str(versiondate).replace("-","")[:8]
        file_parameters["data_node"]=self.data_node
        file_parameters["index_node"] =self.index_node
        file_parameters["latest"]=self.latest
        file_parameters["replica"]=self.replica
        file_parameters["access"]=self.access
        file_parameters["metadata_format"]=self.metadata_format
        file_parameters["type"]="File"
        #The model in the example is RCA4-v1. Therfore the institute- part has to be removed
        #and the version in the path has to be appended
        file_parameters["model"] =(path_info["model"].lstrip(path_info["institute"]+"-")+
            "-"+path_info["version"])
        kwl=["ensemble","experiment","institute","time_frequency","variable","domain","driving_model"]
        for facet in kwl:
            file_parameters[facet]= path_info[facet]

        #Metadata dependent on found Metadata
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
        file_parameters["project"] = self._get_by_keylist(["configuration","options","PROJECT"])
        period = self._get_by_keylist(['items',fileIndex,'period'])
        if(period!=None):
            file_parameters["datetime_start"]=str(period["begin"])
            file_parameters["datetime_stop"]=str(period["end"])
        #generate long variable name from variable if known. Else set it to unknown.
        variable = file_parameters["variable"]
        for keyword in ["variable_long_name","variable_units","cf_standard_name"]:
            value = self._get_by_keylist([variable,keyword],self.VARIABLEMAP)
            if(value!=None):
                file_parameters[keyword]=value

        path_info["metaModel"]= file_parameters["model"]
        ds_master_id = self.concatenate(path_info,["domain","institute","driving_model",
                          "experiment","ensemble","metaModel","time_frequency","variable"],
                          ".",prefix="cordex.")
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
                url = results[0][2]
                pid = results[0][1]
                file_parameters["pid"]=pid
                if(reslen != 1):
                    self._add_error("There are too many results for the file "+filename)
            else:
                self._add_error("Did not find an url for the file "+filename)
            file_parameters["url"] = url+HTTPEXTENSION

        file_parameters["id"]=(ds_master_id+".v"+file_parameters["version"]+"."+filename+"|"+
            file_parameters["data_node"])
        file_parameters["dataset_id"] = (ds_master_id+".v"+file_parameters["version"]+"|"+
            file_parameters["data_node"])
        file_parameters["instance_id"]= ds_master_id+".v"+file_parameters["version"]+"."+filename
        file_parameters["master_id"]= ds_master_id+"."+filename

        fullname =self.project_data_path+"/"+path_line+"/"+filename
        try:#to get the size if allowed. Works only for local files, like the QC.
            file_parameters["size"] = str(os.stat(fullname).st_size)
        except:
            pass
        return file_parameters

    """ If the PROJECT_DATAV variable is a list select the first element as project_data_path"""
    def _get_project_data_path(self):
        project_data_path=self._get_by_keylist(['configuration','options','PROJECT_DATAV'])
        if(isinstance(project_data_path,dict)):
            if(len(project_data_path) > 0):
                project_data_path = project_data_path[0]
        elif(not isinstance(project_data_path,str)):#should not occur
            print("Please check project data path. Type:"+str(type(project_data_path)))
        return project_data_path

        
    """ In the case something is not conform with the standard, there will be events
        in the YAML file.
        The file_parameters variable is only to be used in the case of errors."""
    def _gather_events(self,fileIndex,file_parameters):
        events_dict = self._get_by_keylist(["items",fileIndex,"events"])
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

    def _gather_checks(self,fileIndex):
        """Checks are not part of the metadata"""
        checks = dict()
        check_dict = self._get_by_keylist(["items",fileIndex,"check"])
        if(check_dict!=None):
            for key in ["meta_data","time_values","data"]:
                checks[key] = check_dict[key]
        return checks

    """Generate dataset files from the information of the files that have the same dataset_id
    """
    def _gather_dataset_parameters(self,key):
        dataset_parameters = dict()
        file_count = len(self.dataset_contained_ids[key])
        identifiers = self.dataset_contained_ids[key]
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
            file_parameters = self.file_parameters_collection["File"][identifier]
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
        file_parameters = self.file_parameters_collection["File"][id0]
        for field in sharedValues: 
            if(field in file_parameters):#to prevent errors for non existent fields
                dataset_parameters[field] = file_parameters[field]

        dataset_parameters["id"] = key
        #instance_id = id without |data_node
        instanceid = key.split("|")[0]
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
        return dataset_parameters

    def _create_shared(self,m1,m2,m3,m4):
        for identifier in self.file_parameters_collection["File"]:
            if(identifier in self.to_write_files):
                dsid= self.file_parameters_collection["File"][identifier]["dataset_id"]
                if(dsid not in self.not_write_files):
                    m1(identifier)
                    m2(identifier)
        for identifier in self.file_parameters_collection["Dataset"]:
            if(identifier in self.to_write_files):
                m3(identifier)
                m4(identifier)

    def _create_names(self):
        self._create_shared(self._create_names_file,self._create_names_qc_file,
                          self._create_names_dataset,self._create_names_qc_dataset)

    def _create_xml(self):
        self._create_shared(self._create_xml_file,self._create_xml_qc_file,
                          self._create_xml_dataset,self.create_xml_qc_dataset)

    #def getCreatedFilenames(self):
    #    out="<html>\n<body>"
    #    out+="Created "+str(len(self.createdLinks))+" handles:\n"
    #    
    #    for tupl in self.createdLinks:
    #        x="file://"
    #        y="file://"
    #        if(tupl[0][0]!="/"):
    #            x="http://"
    #        if(tupl[1][0]!="/"):
    #            y ="http://"
    #        out+="<a href=\""+x+tupl[0]+"\">"+tupl[0]+"</a>  =>"
    #        out+="<a href=\""+y+tupl[1]+"\">"+tupl[1]+"</a><br>"
    #    out+="</body>\n</html>"
    #    return out
        

