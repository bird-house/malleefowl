import yaml
import os

from pyesgf.search import SearchConnection

import lapis
import lapis.infra.infrastructure
import lapis.infra.handleinfrastructure

addpath = "/home/tk/sandbox/climdaps/src/Malleefowl/processes/qc/"
class DOHandler():
    def __init__(self):
        self.host = "handleoracle.dkrz.de"
        self.path = "/handle/"
        self.port = 8090
        self.prefix = "10876"
        self.additional_identifier_element ="CORDEX-" 
        self.handle = lapis.infra.handleinfrastructure.HandleInfrastructure(self.host,self.port,
                      self.path,self.prefix,self.additional_identifier_element)

    def createDO(self,identifier=None):
        if(identifier is not None):
            pid = self.handle.lookup_pid(identifier) 
            if(pid is not None):
                return pid
        do = self.handle.create_do(identifier)
        return do

    def setResourceLocation(self,identifier,location):
        do = self.handle.lookup_pid(identifier)
        do.resource_localtion=location

    #Combines the two above. If the DO with the identifier does not exist it will be created
    def link(self,identifier,location):
        pid=None
        if(identifier is not None):
            pid = self.handle.lookup_pid(identifier)
        if pid is None:
            pid = self.handle.create_do(identifier)
        pid.resource_location=location
        pidurl = (self.host+":"+str(self.port)+self.path+self.prefix+"/"+
                  self.additional_identifier_element+str(pid).split("/")[-1])
        return pidurl


class Yaml2Xml():
    def __init__(self,data_node="ipcc-ar5.dkrz.de",index_node="esgf-data.dkrz.de",
                 xmlOutputPath ="/home/tk/sandbox/xmlresults/"):
        self.doHandler = DOHandler()
        #self.handlePrefix="handleoracle.dkrz.de:8090/handle/"
        self.pathListNames=["domain","institute","driving_model","experiment","ensemble","model",
                       "version", "time_frequency","variable"]
        self.xmlOutputPath = xmlOutputPath
        """TIM: type identifier map, is used to store the key and values for the given 
           types (file,dataset,qc-file,qc-dataset) and the specific instance. It allows to handle
           each type in its own fashion. The id should be equal for matching file/dataset and its qc.
           e.g. TIM["QC-File"]["id"] = MAP or eventlist = TIM["QC-File"]["id"]["events"]"""
        self.projectDataPath = ""
        self.VMAP = self.importVariableMap(addpath+"variableMap.csv")
        self.DEG44 = self.importVariableMap(addpath+"c44.csv")
        self.DEG44I = self.importVariableMap(addpath+"c44i.csv")
        self.EXPERIMENTS = self.importVariableMap(addpath+"experimentFamily.csv")
        self.FIXEDMETA = dict()
        self.FIXEDMETA["data_node"]=data_node 
        self.FIXEDMETA["index_node"] =index_node
        self.FIXEDMETA["latest"]="true"
        self.FIXEDMETA["replica"]="false"
        self.FIXEDMETA["access"]="HTTPServer"
        self.FIXEDMETA["metadata_format"]="THREDDS"
        self.filetypes = ["QC-File","QC-Dataset","File","Dataset"]
        self.allow_esg_search = False #Significantly increases the processing time if True
        self.clear()
        self.createdLinks = []

    def clear(self):
        self.ErrorLog = []
        self.writeAllowed= []
        self.writeNotAllowed = []
        self.DATASET=dict()#maps a dataset_id to a list of file ids
        self.TIM = dict()
        self.ESGS=dict()
        self.XMLURLS=dict()
        self.FILENAMES=dict()
        for key in self.filetypes:
            self.XMLURLS[key]=dict()
            self.FILENAMES[key]=dict()

    """ Collect all files in the yaml and create their xml and from the collection of files
        with the same dataset_id a dataset xml is created."""
    def toXML(self):
        self.clearDataset()
        self.projectDataPath=self.getProjectDataPath()
        self.CREATEFILES = dict()

        length=len(self.getDicEntry(['items']))

        """ Collect the information for each file described in the YAML file"""
        for i in range(0,length):
            META=self.createFileMeta(i)
            if(META is not None):
                identifier = META["title"]
                self.addToTIM("File",identifier,META)
                self.writeAllowed.append(identifier)#unless events finds an error allow to write.
                ds = META["dataset_id"]
                if(not(ds in self.writeAllowed or ds in self.writeAllowed)):
                    self.writeAllowed.append(ds)
                EVENTS = self.createEvents(i,META)#META is passed for the case of errors.
                self.addToTIM("QC-File-Events",identifier,EVENTS)
                CHECKS = self.createChecks(i)
                self.addToTIM("QC-File-Checks",identifier,CHECKS)

                self.addToDataset(ds,identifier)
            else:
                atomic = self.getDicEntry(["items",i,"atomic"])
                if(atomic is None):
                    self.addError("There is a serious problem, as neither file nor atomic can be "+
                    "found in the quality control log file for the entry with number "+str(i)+".")
                else:
                    self.addError("Atomic set "+atomic+" has the tags:")
                    self.writeNotAllowed.append(atomic)
                events =  self.getDicEntry(["items",i,"events"])
                if events is not None:
                   for eventlist in events:
                       self.addError(eventlist["event"]["tag"])

        for key in self.DATASET:
            DS = self.createDatasetsMeta(key)
            self.addToTIM("Dataset",key,DS)

        self.createNames()
        self.createFiles()
        #self.showAllErrors()

    def addError(self,message):
        self.ErrorLog.append(message)

    def showAllErrors(self):
        out=""
        for error in self.ErrorLog:
            out+=error+"\n"
        if len(self.writeNotAllowed) > 0:
            out+="Write not allowed for:\n"
            for wna in self.writeNotAllowed:
                out+=wna+"\n"
        return out

    def addToTIM(self,filetype,identifier,MAP):
        if filetype not in self.TIM:
           self.TIM[filetype]=dict()
        self.TIM[filetype][identifier]=MAP


    def esg_search(self,master_id):#,version,replica):
        #self.ESGS[master_id]=dict()
        #return#disable the online search for debugging 
        if(master_id not in self.ESGS):
            self.ESGS[master_id]= dict()
            sc = SearchConnection('http://esgf-data.dkrz.de/esg-search', distrib=True)
            ctx = sc.new_context(project='CORDEX', master_id=master_id)#, version=version, replica=replica)
            if(ctx.hit_count>0):
                dataset=ctx.search()[0]
                self.ESGS[master_id]["dataseturls"]=dataset.urls
                file_context=dataset.file_context()
                #look for the first file and remove the filename from the url
                for file in file_context.search():
                    furl = file.download_url
                    fs = furl.split("/")
                    url = fs[0]+"/"+"/".join(fs[1:-1])+"/"
                    self.ESGS[master_id]["serverDir"]=url
                    break#just look at the first file

    def loadFile(self, filename):
        self.store = yaml.safe_load(file(filename, 'r'))

    """ Some entries might not exist.(e.g. period does not exist for fx frequency). In that case
        None is returned. To avoid the program from crashing the try statement is used."""
    def getDicEntry(self,List,base=None):
        if base is None:
            base = self.store
        out = None
        current = base
        valid = True
        for key in List:
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
    def importVariableMap(self,filename,separator=","):
        MAP = dict()
        f = open(filename,"r")
        header = f.readline()
        header = header.rstrip("\n")
        hs = header.split(",")
        for line in f.readlines():
            line= line.rstrip("\n")
            ls = line.split(",")
            MAP[ls[0]]=dict()
            for i in range(1,len(hs)):
                MAP[ls[0]][hs[i]]=ls[i]
        return MAP

    #""" self.DATASET is used to create the dataset xml files for files with equal dataset_id.
    #    The add method creates the dictionary entry if it does not exist.
    #    The clear method allows for resetting the stored data if the program is used for
    #    multiple YAML files.
    #    """
    def addToDataset(self,identifier,metaid):#meta,events,checks):
        if identifier not in self.DATASET:
           self.DATASET[identifier]=[]
        self.DATASET[identifier].append(metaid)

    def clearDataset(self):
        self.DATASET=dict()

    """The output format is similar to xml, however it allows the use of duplicate keys.
       Which keys are duplicate depends on the type. Therfore the shared methods will collect
       all key value pairs from a dictionary and sort them. The outer block (<doc schema="esgf">)
       is handled by eachs method to allow for adding the duplicate keys."""
    def fieldnameline(self,key,value):
        return ("    <field name=\""+str(key)+"\">"+str(value)+"</field>")
     
    """ For now regexplist may only contain (\D+) for text and (\d+) numbers.
        This is due to the conversion to an integer for (\d+) groups."""
    def makeFieldnameValueStrings(self,Dict,regexplist=None):
        outlines = []
        sortkeys = []
        for key in Dict:
            sortkeys.append(key)
        if regexplist is not None:
           import re
           key_pat = re.compile( r"^(\D+)(\d+)$" )
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
            outlines.append(self.fieldnameline(key,Dict[key]))
        return outlines

    def link(self,identifier,location):
        return self.doHandler.link(identifier,location)

    # For now the name creation is the same for all processes, however it should be 
    # flexible enough to allow idividual implementations.
    def createNamesShared(self,filetype,identifier):
        filename = self.xmlOutputPath+filetype+"-"+identifier+".xml"
        self.FILENAMES[filetype][identifier]= filename
        self.XMLURLS[filetype][identifier] =self.link(None,filename)
        self.createdLinks.append((self.XMLURLS[filetype][identifier],filename))
         
    def createMetaFileNames(self,identifier):
        self.createNamesShared("File",identifier)

    def createQCFileNames(self,identifier):
        self.createNamesShared("QC-File",identifier)

    def createDatasetFileNames(self,identifier):
        self.createNamesShared("Dataset",identifier)

    def createQCDatasetNames(self,identifier):
        self.createNamesShared("QC-Dataset",identifier)

    def makeMetaXml(self,filename,Dict):
        f = open(filename,"w")
        f.write(self.makeMetaXmlString(Dict))
        f.close()

    def makeFileMetaXml(self,identifier):
        filename = self.FILENAMES["File"][identifier] 
        fieldlines=self.makeFieldnameValueStrings(self.TIM["File"][identifier])
        fieldlines.append(self.fieldnameline("experiment_family","All"))
        #qualityurl = ("http://en.wikipedia.org/wiki/File:Malleefowl-camouflage.JPG|application/xml"+
        #              "|QCDoc")
        qualityurl = self.XMLURLS["QC-File"][identifier]+"|application/xml|QCDoc"
        fieldlines.append(self.fieldnameline("url",qualityurl))
        fieldlines.sort()
        lines=[]
        lines.append("<doc schema=\"esgf\">")
        lines+=fieldlines
        lines.append("</doc>")
        f = open(filename,"w")
        for line in lines:
            f.write(line+"\n")
        f.close()

    def makeDatasetMetaXml(self,identifier):
        filename = self.FILENAMES["Dataset"][identifier]
        fieldlines = self.makeFieldnameValueStrings(self.TIM["Dataset"][identifier])
        qualityurl = self.XMLURLS["QC-Dataset"][identifier]
        fieldlines.append(self.fieldnameline("experiment_family","All"))
        fieldlines.append(self.fieldnameline("quality_url",qualityurl))
        fieldlines.sort()
        lines=[]
        lines.append("<doc schema=\"esgf\">")
        lines+=fieldlines
        lines.append("</doc>")
        f = open(filename,"w")
        for line in lines:
            f.write(line+"\n")
        f.close()

    def renameMap(self,Map,Prefix="",Suffix=""):
        map2=dict()
        for key in Map:
            map2[Prefix+key+Suffix]=Map[key]
        return map2

    def makeFileQualityXml(self,identifier):
        filename = self.FILENAMES["QC-File"][identifier]
        QC = dict()
        META = self.TIM["File"][identifier]
        QC["file_id"]=META["master_id"]
        if("url" in META):
            QC["file_url"] = META["url"][:-len("|application/netcdf|HTTPServer")]
        QC["version"]=META["version"]
        QC["file_metadata_url"]=self.XMLURLS["File"][identifier]
        checkmap = self.TIM["QC-File-Checks"][identifier]
        checks = self.makeFieldnameValueStrings(self.renameMap(checkmap,"checks_"))
        eventmap = self.TIM["QC-File-Events"][identifier]
        events=self.makeFieldnameValueStrings(eventmap)#self.renameMap(eventmap,"events_"))
        lines=[]
        lines.append("<doc schema=\"QC-File\">")
        lines+=checks
        lines+=events
        lines+=self.makeFieldnameValueStrings(QC)
        lines.append("</doc>")
        f = open(filename,"w")
        for line in lines:
            f.write(line+"\n")
        f.close()

    def makeDatasetQualityXml(self,identifier):
        filename = self.FILENAMES["QC-Dataset"][identifier]
        files = self.DATASET[identifier]
        k=0
        EVENTS = dict()
        FILES = dict()
        CHECKSSUMMARY = {"fail":0,"omit":0,"pass":0,"fixed":0}
        for fileid in files:
            FILES["file_"+str(k)] = fileid
            checkmap = self.TIM["QC-File-Checks"][fileid]
            for check in checkmap:
                result = checkmap[check]
                CHECKSSUMMARY[result.lower()]+=1
            eventmap = self.TIM["QC-File-Events"][fileid]
            for key in eventmap:
                EVENTS["file_"+str(k)+"_"+key]=eventmap[key]
            k+=1
  
        QC = dict()
        for key in CHECKSSUMMARY:
            QC["checks_"+key] = CHECKSSUMMARY[key]
        #META = self.TIM["Dataset"][identifier]
        QC["number_of_files"]=len(files)
        #QC["metalink"]="http://esgf-data.dkrz.de/esgf-web-fe/metadataview/"+META["id"]+".html"
        QC["metadata_url"]=self.XMLURLS["Dataset"][identifier]
        lines=[]
        lines.append("<doc schema=\"QC-Dataset\">")
        lines+=self.makeFieldnameValueStrings(EVENTS)
        lines+=self.makeFieldnameValueStrings(QC)
        lines+=self.makeFieldnameValueStrings(FILES,["(\D+)","(\d+)"])
        lines.append("</doc>")
        f = open(filename,"w")
        for line in lines:
            f.write(line+"\n")
        f.close()

    """ To avoid having to write the dictionary name again and again, a concatenation method
        is defined. It connects the elements of a dictionary by inserting the separator.
        The last element has no separator following it."""
    def concatenate(self,DictOrList,Keys,separator,prefix=""):
        out = prefix
        for key in Keys:
            out+=DictOrList[key]+separator
        out=out.rstrip(separator)
        return out

    def createFileMeta(self,fileIndex):
        filename = self.getDicEntry(["items",fileIndex,"file"])
        PATH = dict()
        if(filename==None):
            print("filename is a required variable. Stopping XML generation.")
            return None
        META = dict()
        pathLine = self.getDicEntry(["items",fileIndex,"data_path"]).lstrip(self.projectDataPath)
        pathList= pathLine.split("/")
        PATH["full"] = pathLine
        for j in range(len(pathList)):
            PATH[self.pathListNames[j]] = pathList[j]
        META["title"] = filename
        #version : as the date in YYYYMMDD format.
        versiondate = self.getDicEntry(["start","date"])
        META["version"]=str(versiondate).replace("-","")[:8]
        #TODO:HARDCODED META START
        META["data_node"]=self.FIXEDMETA["data_node"]#"ipcc-ar5.dkrz.de" 
        META["index_node"] =self.FIXEDMETA["index_node"]#"esgf-data.dkrz.de"
        META["latest"]=self.FIXEDMETA["latest"]#"true"
        META["replica"]=self.FIXEDMETA["replica"]#"false"
        META["access"]=self.FIXEDMETA["access"]#"HTTPServer"
        META["metadata_format"]=self.FIXEDMETA["metadata_format"]#"THREDDS"
        #HARD CODED META END
        META["type"]="File"
        META["model"] = PATH["model"].lstrip(PATH["institute"]+"-")+"-"+PATH["version"]
        kwl=["ensemble","experiment","institute","time_frequency","variable","domain","driving_model"]
        for keyword in kwl:
            META[keyword]= PATH[keyword]

        #Metadata dependent on found Metadata
        domain = META["domain"]
        DEGDIC = None
        if domain in self.DEG44:
            DEGDIC=self.DEG44
        elif domain in self.DEG44I:
            DEGDIC=self.DEG44I
        if(DEGDIC is not None):
            for direction in ["west","east","north","south"]:
                name = direction+"_degrees"
                META[name]=DEGDIC[domain][name]
        #META["experiment_family"]="All"
        experiment = META["experiment"]
        if experiment in self.EXPERIMENTS:
            META["experiment_family"]=self.EXPERIMENTS[experiment]["experiment_family"]
        else:
            self.addError("Unknown experiment_family for experiment "+experiment)
        #The model in the example is RCA4-v1. Therfore the institute- part has to be removed
        #and the version in the path has to be appended
        META["project"] = self.getDicEntry(["configuration","options","PROJECT"])
        period = self.getDicEntry(['items',fileIndex,'period'])
        if(period!=None):
            META["datetime_start"]=str(period["begin"])
            META["datetime_stop"]=str(period["end"])
        #generate long variable name from variable if known. Else set it to unknown.
        variable = META["variable"]
        for keyword in ["variable_long_name","variable_units","cf_standard_name"]:
            value = self.getDicEntry([variable,keyword],self.VMAP)
            if(value!=None):
                META[keyword]=value

        PATH["metaModel"]= META["model"]
        ds_master_id = self.concatenate(PATH,["domain","institute","driving_model",
                          "experiment","ensemble","metaModel","time_frequency","variable"],
                          ".",prefix="cordex.")
        if(self.allow_esg_search):
            #try to find the data in the search. Overwrite data_node and version if it exists.
            self.esg_search(ds_master_id)
            if("serverDir" in self.ESGS[ds_master_id]):
                serverDir = self.ESGS[ds_master_id]["serverDir"]
                META["url"]=serverDir+META["title"]+"|application/netcdf|HTTPServer"
                META["data_node"]=serverDir.lstrip("http://").split("/")[0]
                #the file exists therefore grab the existing version number
                a = self.ESGS[ds_master_id]["dataseturls"]["Catalog"][0][0].split(".")[-1]
                b = a.split("|")[0].lstrip("v")
                META["version"]=b
        else:
            serverDir = "http://"+META["data_node"]+"/thredds/fileServer/cordex/"
            META["url"] = serverDir+PATH['full']+"/"+filename+"|application/netcdf|HTTPServer"

        META["id"]=ds_master_id+".v"+META["version"]+"."+filename+"|"+META["data_node"]
        META["dataset_id"] = ds_master_id+".v"+META["version"]+"|"+META["data_node"]
        META["instance_id"]= ds_master_id+".v"+META["version"]+"."+filename
        META["master_id"]= ds_master_id+"."+filename

        fullname =(self.projectDataPath+"/"+pathLine+"/"+filename)
        try:#to get the size if allowed. Works only for local files, like the QC.
            META["size"] = str(os.stat(fullname).st_size)
        except:
            pass
        return META

    """ If the PROJECT_DATAV variable is a list select the first element as projectDataPath"""
    def getProjectDataPath(self):
        projectDataPath=self.getDicEntry(['configuration','options','PROJECT_DATAV'])
        if(isinstance(projectDataPath,dict)):
            if(len(projectDataPath) > 0):
                projectDataPath = projectDataPath[0]
        elif(not isinstance(projectDataPath,str)):#should not occur
            print("Please check project data path. Type:"+str(type(projectDataPath)))
        return projectDataPath

    #""" Generate a filename and store the data to create the XML files later on."""
    #def addToCreatefiles(self,dic,filename=None):
    #    if filename is None:
    #        filename = self.xmlOutputPath+dic["type"]+"."+dic["id"].split("|")[0]+".xml"
    #    self.CREATEFILES[filename] = dic
        
    """ In the case something is not conform with the standard, there will be events
        in the YAML file.
        The META variable is only to be used in the case of errors."""
    def createEvents(self,fileIndex,META):
        events = self.getDicEntry(["items",fileIndex,"events"])
        #print(events)
        EVENTS = dict()
        if(events != None):
            def xmlReplace(line):
                xmlmap = {"\"":"&quot;","<":"&lt;",">":"&gt;","&":"&amp;"}
                for key in xmlmap:
                    line=line.replace(key,xmlmap[key])
                return line

            k = 0 # event index
            for eventlist in events:
                event = eventlist['event']
                #It is assmued that event always has "tag","impact" and "caption"
                out = str(event["tag"])+":::"+str(event["impact"])+":::"+str(event["caption"])
                if "text" in event:
                    out+=":::"
                    for line in event["text"]:
                        out+=line+","
                out=xmlReplace(out[:-1])
                EVENTS["event_"+str(k)]=out
                if(event["tag"]=="M5"):
                    self.addError("Tag M5. The files are not allowed to be uploaded.")
                    self.writeNotAllowed.append(META["title"])
                    self.writeAllowed.remove(META["title"])
                    self.writeNotAllowed.append(META["dataset_id"])
                    self.writeAllowed.remove(META["dataset_id"])
                k+=1
        return EVENTS

    def createChecks(self,fileIndex):
        """Checks are not part of the metadata"""
        CHECKS = dict()
        check = self.getDicEntry(["items",fileIndex,"check"])
        if(check!=None):
            CHECKS["meta_data"]= check["meta_data"]
            CHECKS["time_values"]=check["time_values"]
            CHECKS["data"]=check["data"]
        return CHECKS

    """Generate dataset files from the information of the files that have the same dataset_id
    """
    def createDatasetsMeta(self,key):
        DS = dict()
        fileCount = len(self.DATASET[key])
        identifiers = self.DATASET[key]
        #TODO: Is the date format always valid.
        """ Due to the fact that datetime objects are used to generate the date
            strings it has always the format %Y-%m-%d %H:%M:%S,
            where %Y is the year in 4 digits year, the year 201 would be "0201"
            It is limited from year 1 to 9999. Due to this format it is also possible
            to compare the strings, which will search from start till a character is
            found that differs. 
        """
        startTimes = []
        endTimes = []
        size = 0

        for identifier in identifiers:
            META = self.TIM["File"][identifier]
            if("datetime_start" in META):#If datetime_start exist datetime_stop should exist as well.
                startTimes.append(META["datetime_start"])
                endTimes.append(META["datetime_stop"])
            if("size" in META):
                size+=int(META["size"])

        if("datetime_start" in META):
            DS["datetime_start"]=str(min(startTimes))
            DS["datetime_stop"]=str(max(endTimes))
        if(size!=0):
            DS["size"]=str(size)
        else:
            DS["size"]="Not available"

        sharedValues = ["data_node","domain","driving_model","ensemble","experiment","index_node",
                        "institute",  "latest","project","model","replica","time_frequency",
                        "variable","variable_long_name","version","variable_units",
                        "cf_standard_name","experiment_family","west_degrees","east_degrees",
                        "south_degrees","north_degrees","metadata_format"]
        id0= identifiers[0]
        META = self.TIM["File"][id0]
        for field in sharedValues: 
            if(field in META):#to prevent errors for non existent fields
                DS[field] = META[field]

        DS["id"] = key
        #instance_id = id without |data_node
        instanceid = key.split("|")[0]
        DS["instance_id"]=instanceid
        #master_id = instanceid without .v<version>
        masterid = instanceid.rstrip(".v"+DS["version"])
        DS["master_id"]=masterid
        DS["title"]=masterid

        DS["type"]="Dataset"
        #TODO:url
        try:
            dsurls = self.ESGS[masterid]["dataseturls"]["Catalog"]
            DS["url"]="|".join(dsurls[0])               
        except:
            pass#if it does not exist skip it

        DS["number_of_files"]=str(fileCount)
        DS["dataset_id_template_"]=("cordex.%(domain)s.%(institute)s.%(driving_model)s."+
                   "%(experiment)s.%(ensemble)s."+DS["model"]+".%(time_frequency)s.%(variable)s")
        return DS
        #self.addToCreatefiles(DS)
        #self.createQualityXML(DS,EVENTS,CHECKS)
    


    def createShared(self,m1,m2,m3,m4):
        for identifier in self.TIM["File"]:
            if(identifier in self.writeAllowed):
                dsid= self.TIM["File"][identifier]["dataset_id"]
                if(dsid not in self.writeNotAllowed):
                    m1(identifier)
                    m2(identifier)
        for identifier in self.TIM["Dataset"]:
            if(identifier in self.writeAllowed):
                m3(identifier)
                m4(identifier)

    def createNames(self):
        self.createShared(self.createMetaFileNames,self.createQCFileNames,
                          self.createDatasetFileNames,self.createQCDatasetNames)
        #for identifier in self.TIM["file"]:
        #    if(identifier in self.writeAllowed):
        #        dsid= self.TIM["File"][identifier]["dataset_id"]
        #        if(dsid not in self.writeNotAllowed):
        #            self.createMetaFileNames(identifier)
        #            self.createQCFileNames(identifier)
        #for identifier in self.TIM["Dataset"]:
        #    if(identifier in self.writeAllowed):
        #        self.createDatasetFileNames(identifier)
        #        self.createQCDatasetNames(identifier)

    def createFiles(self):
        #create FileMetadata
        for identifier in self.TIM["File"]:
            #if both file and dataset are allowed for writing, create the file meta and qc.
            if(identifier in self.writeAllowed):
                dsid= self.TIM["File"][identifier]["dataset_id"]
                if(dsid not in self.writeNotAllowed):
                    self.makeFileMetaXml(identifier)#,self.TIM["File"][identifier])
                    self.makeFileQualityXml(identifier)
        for identifier in self.TIM["Dataset"]:
            if(identifier in self.writeAllowed):
                self.makeDatasetMetaXml(identifier)
                self.makeDatasetQualityXml(identifier)


    def getCreatedFilenames(self):
        out=""
        for tupl in self.createdLinks:
            out+=tupl[0]+" => "+tupl[1]+"\n"
        return out
        

#if __name__ == "__main__":
#    try:
#        mY2 = MyYaml()
#        #mY2.loadFile("event.log")
#        #mY2.loadFile("data3-2.log")
#        #mY2.loadFile("data3-3.log")
#        mY2.loadFile("dlerror.log")
#        #mY2.loadFile("2file.log")
#        mY2.toXML()
#    except yaml.YAMLError, exc:
#        print "Error in configuration file:", exc
#
