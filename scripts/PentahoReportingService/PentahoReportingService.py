__author__ = 'Sajeetharan'
__version__ = '1.0.0'

import sys,os
 #code added by sajee on 12/27/2015
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
import subprocess
from subprocess import Popen, PIPE
import modules.BigQueryHandler as BQ
import modules.SQLQueryHandler as mssql
import modules.PostgresHandler as PG
import scripts.DigINCacheEngine.CacheController as CC
import modules.CommonMessageGenerator as cmg
from multiprocessing import Process
from xml.dom import minidom
import json
import operator
import ast
import logging
import datetime
import configs.ConfigHandler as conf

Report_cnf = conf.get_conf('DatasourceConfig.ini','Reports')
Reports_path = Report_cnf['Path']

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/PentahoReportService.log'
handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info('--------------------------------------  LogicImplementer  -----------------------------------------------')
logger.info('Starting log')

def get_queries(params):
        reportname = params.Reportname
        fields = ast.literal_eval(params.fieldnames)  # 'fieldnames' {1 : 'agents', 2:'direction'}
        xmlpath = conf.get_conf('DatasourceConfig.ini', 'Reports')
        f = []
        # Directory = xmlpath["Path"] + "\\" + reportname + "\\" + "datasources\\"
        Directory = 'C:\\Reports\\'  + reportname + "\\" + "datasources\\"
        #files = os.listdir(Directory)
        dicts = []
        for field in fields:
            #xmldoc = minidom.parse(Directory + "\\" + "sql-ds.xml")
            xmldoc = minidom.parse(Directory +  "sql-ds.xml")
            attributes = xmldoc.getElementsByTagName("data:query")
            d = {}
            for fielde in attributes:
                if fielde._attrs[u'name'].firstChild.data == fields[field]:
                    queryobtained = json.dumps(fielde.getElementsByTagName("data:static-query")[0].childNodes[0].data)
                    query = queryobtained.replace("\\n", " ")
                    query = query.replace("\"", "")

                    fieldresult = {'Fieldname': fields[field],
                                   'Query': query}
                    d.update(fieldresult)
            dicts.append(d)
        return json.dumps(dicts)
        # elif authResult.reason == 'Unauthorized':
        #       return comm.format_response(False,authResult.reason,"Check the custom message",exception=None)


def get_layout(params):

        reportname = params.Reportname

        f = []
        #Directory = "C:\Reports" + "\\" + reportname + "\\"
        #Directory = Reports_path + "\\" + reportname + "\\"
        Directory = 'C:\\Reports\\'  + reportname + "\\"
        files = os.listdir(Directory)
        for file in files:
            if file == 'datadefinition.xml':
                #xmldoc = minidom.parse(Directory + "\\" + "datadefinition.xml")
                xmldoc = minidom.parse(Directory  + "datadefinition.xml")
                itemlist = xmldoc.getElementsByTagName("plain-parameter")
                itemlist2 = xmldoc.getElementsByTagName("list-parameter")
                dicts = []
                result = []
                i = 0
                for plainparameter in itemlist:

                    attributes = plainparameter.getElementsByTagName("attribute")
                    print attributes
                    d = {}
                    fieldtype = {}
                    for attribute in attributes:
                        sid = attribute.getAttribute("name")
                        fieldtype = {sid: attribute.firstChild.nodeValue}
                        d.update(fieldtype)
                    dicts.append(d)
                for plainparameter in itemlist2:

                    attributes = plainparameter.getElementsByTagName("attribute")
                    print attributes
                    d = {}
                    fieldtype = {}
                    for attribute in attributes:
                        sid = plainparameter.getAttribute("query")
                        sid2 = attribute.getAttribute("name")
                        if(sid2 == "label"):
                           fieldtype = {"label": sid}

                        else:
                           fieldtype = {sid2: attribute.firstChild.nodeValue}
                        d.update(fieldtype)
                    dicts.append(d)

        #DirectoryQuery = Reports_path + "\\" + reportname + "\\" + "datasources\\"
        DirectoryQuery = 'C:\\Reports\\'  + reportname + "\\"  + "datasources\\"
        #files = os.listdir(DirectoryQuery)
        newDicts =[]
        #xmldoc = minidom.parse(DirectoryQuery + "\\" + "sql-ds.xml")
        xmldoc = minidom.parse(DirectoryQuery  + "sql-ds.xml")
        attributes = xmldoc.getElementsByTagName("data:query")
        for field in dicts:
            if(field["hidden"] == "false") :
                # for fielde in attributes:
                        #if(field["hidden"] == "false") :
                             fieldresult = {'Fieldname': field['label'],
                                               'parameter-render-type':field['parameter-render-type'],
                                               'Query': ""}
                             newDicts.append(fieldresult)
        print json.dumps(newDicts)

        for param in newDicts:
            for fielde in attributes:
                if fielde._attrs[u'name'].firstChild.data == param['Fieldname']:
                        queryobtained = json.dumps(fielde.getElementsByTagName("data:static-query")[0].childNodes[0].data)
                        query = queryobtained.replace("\\n", " ")
                        query = query.replace("\"", "")
                        query = query.replace("\\t", "")
                        param['Query']= query

        return json.dumps(newDicts)


def get_report_names(params):
        names =  [name for name in os.listdir(Reports_path)]
       # print [name for name in os.listdir(Reports_path) if os.path.isdir(name)] #Reports_path
        result = cmg.format_response(True,names,'Data successfully processed!',None)
        return result


def executeKTR(params):
      eportName = params.ReportName
      paramaeters = ast.literal_eval(params.parameters)
      strJSON = json.dumps(paramaeters)
      reportName = 'C:\\Reports\\' + eportName + "\\" +eportName
      if(os.path.isfile(reportName+'.html')):
          os.remove(reportName+'.html')
      renderedReport = 'http://104.131.48.155/reports/' +eportName + '/' + eportName
      args = ['ktrjob.jar',reportName,strJSON]
      result = cmg.format_response(True,jarWrapper(*args),renderedReport +'.html',None)
      return result


def jarWrapper(*args):
    process = Popen(['java', '-jar']+list(args), stdout=PIPE, stderr=PIPE)
    ret = []
    while process.poll() is None:
        line = process.stdout.readline()
        print line
        if line != '' and line.endswith('\n'):
            ret.append(line[:-1])
    stdout, stderr = process.communicate()
    ret += stdout.split('\n')
    if stderr != '':
        ret += stderr.split('\n')
    ret.remove('')
    return ret
