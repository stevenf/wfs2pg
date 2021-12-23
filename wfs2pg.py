#!/usr/bin/python3


import psycopg2, requests, subprocess, codecs
#temp directory to write downloaden GML to.
TEMP_DIR = "c:/temp"
from xml.etree import ElementTree
import xml.etree


class WFS2PG:
    def __init__(self, host, port, db, user, password, schema, prefixtable, base_url, **typenames):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.schema = schema
        self.prefixtable = prefixtable
        self.base_url= base_url
        self.url_capabilities = f"{base_url}&request=GetCapabilities"
        self.typename=typenames
        

        self.conn = psycopg2.connect("dbname={} user={} host={} password={}".format (db,user,host,password))
        
        self.checkSchema()
            

    
    def checkSchema(self):
        sql = f"CREATE SCHEMA IF NOT EXISTS {self.schema} AUTHORIZATION dsr_usr_admin;" 
        
        cur = self.conn.cursor()
        cur.execute(sql)
        cur.close()
        self.conn.commit()
  
    def dropTableIfExists(self, tablename):
  
        sql = f"DROP TABLE IF EXISTS {self.schema}.{tablename}" 
        
        cur = self.conn.cursor()
        cur.execute(sql)
        cur.close()
        self.conn.commit()
  
  
  
    def tableExists (self, table):
        sql = f"SELECT COUNT(*) = 1 as exists FROM pg_tables a WHERE a.schemaname = '{self.schema}' and a.tablename = '{table}';" 
        
        cur = self.conn.cursor()
        cur.execute(sql)
        exists = cur.fetchone()[0]
        cur.close()
  
        return exists
    
    def findNumber(self, txt, stofind):
        i1 = txt.find (stofind)
        i2 = txt.find ('"', i1+len(stofind))
        
        return (txt[i1+len(stofind):i2])
        
        
    def getReturnedAndMatched (self, res):
        txt = res.text
        imatched = self.findNumber(txt, 'numberMatched="')
        ireturned = self.findNumber(txt, 'numberReturned="')
        
        return (imatched, ireturned)
  
    def loadDB (self, filename, tablename):
         cmd = f'ogr2ogr -skipfailures -f "PostgreSQL" PG:"host={self.host} port={self.port} dbname={self.db} user={self.user} password={self.password}"  "{TEMP_DIR}/{filename}" -nln {self.schema}.{tablename} -progress -lco ENCODING=UTF-8 -lco OVERWRITE=No'
        print (cmd)
        subprocess.call(cmd)
    
    def countRecords(self, tablename):
        sql = f"SELECT COUNT(*) FROM {self.schema}.{tablename};" 
        
        cur = self.conn.cursor()
        cur.execute(sql)
        count = cur.fetchone()[0]
        cur.close()
  
        return count
 
 
    def initLoadWFS (self, bbox, featuretype):
        tablename = f"{self.prefixtable}{featuretype}".replace (":", "_").replace (" ", "_")
        self.dropTableIfExists(tablename)
        print(f"tablename: {tablename}")
        self.count = 0
        self.loadWFS (bbox, featuretype, tablename)
        
        #check number of records in database
        count_db = self.countRecords (tablename)
        
        return (self.count, count_db) 
    
    def loadWFS (self, bbox, featuretype, tablename):
        #
        (xmin, ymin, xmax, ymax) = bbox
        print (xmin, ymin, xmax, ymax)
        
        url = f"{self.base_url}&request=GetFeature&typenames={featuretype}&BBOX={xmin},{ymin},{xmax},{ymax}"
        
        res = requests.get(url)
      
        #retrieve numberReturned and numberMatched
        (matched, returned) = self.getReturnedAndMatched(res)
        print (returned, matched)
        
        if (int(returned) < int(matched)):
            #start using bounidng box
            mx = (xmax+xmin)/2.0
            my = (ymax+ymin)/2.0
            
            self.loadWFS ( (xmin,ymin, mx, my), featuretype, tablename)
            self.loadWFS ( (xmin,my,mx,ymax), featuretype, tablename)
            self.loadWFS ( (mx, ymin, xmax, my), featuretype, tablename)
            self.loadWFS ( (mx, my, xmax, ymax), featuretype, tablename)
        elif int(returned) > 0: #store in database
            #save response in temp dir
            post_fix=f"{xmin}-{ymin}-{xmax}-{ymax}".replace(".","_")
            filename = f"{featuretype}-{post_fix}.gml".replace (":","_").replace (" ", "_")
            
            #if there are spaces in the FeatureTypeName invalid XML is generated. Try to fix it by replacing spaces in the tags with _.
            if (" " in featuretype):
                text = res.text.replace (featuretype+" ", featuretype.replace(" ","_")+" ")
                text = text.replace(featuretype+">", featuretype.replace(" ","_")+">")
            else:
                text = res.text
                
            print(filename)
            
            f = codecs.open(f"{TEMP_DIR}/{filename}", "w", "utf-8") 
            f.write(text)
            f.close()
        
            self.count += int(returned)
            #now load in database
            self.loadDB (filename, tablename)
            
    def generateID(self):
        return uuid.uuid4().hex
    
    def getFeatureTypes(self):
        url = f"{self.base_url}&request=GetCapabilities"
        
        res = requests.get(url)
        
        root = ElementTree.fromstring(res.text)
        
        featuretypes = []
        for c in root:
            if "FeatureTypeList" in c.tag:
                for d in c:
                    if "Feature" in d.tag:
                        for e in d:
                            if "Name" in e.tag:
                                featuretypes.append (e.text)
        return featuretypes

if __name__ == "__main__":
    base_url = "<<server>>?service=WFS&version=2.0.0"
    
    pg2wfs = WFS2PG ("db host", "db port", "db database", "db user", "db password", "prefix for table", "", base_url)
    #initial bounding box
    start_bbox = (0, 310000, 277000, 637000) #extent of NL in RD
    
    #test bbox amstertdam-dam
    #bbox_dam = (121000, 487000, 122000, 488000)
    bbox_dam = (120000, 486000, 123000, 489000)
    
    donefeatures = []
    
    #1 Per WFS
    #GetCapabilities to retrieve FeatureTypes 
    featuretypes = pg2wfs.getFeatureTypes()
    for featuretypename in featuretypes:
        (count_wfs, count_db) = pg2wfs.initLoadWFS( bbox_dam, featuretypename)
        donefeatures.append ( (featuretypename, count_wfs, count_db) )
        
    for (featuretypename, count_wfs, count_db) in donefeatures:
        print (f"{featuretypename} - number of features in WFS: {count_wfs} - number of records in database: {count_db}")
        
    '''
    #2. Per FeatureType 
    (count_wfs, count_db) = pg2wfs.initLoadWFS( bbox_dam, "ODNZKG:BOORPUNT")
    #check number of features vs number of records in DB: 
    print (f"number of features in WFS: {count_wfs} - number of records in database: {count_db}")
    '''