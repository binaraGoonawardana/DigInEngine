__author__ = 'Jeganathan Thivatharan'
__version__ = '1.0.0'

import os
import zipfile
from xml.dom import minidom
import logging
import ast
import configs.ConfigHandler as conf



Report_cnf = conf.get_conf('FilePathConfig.ini','User Files')
User_Reports_path = Report_cnf['Path']

ReportName_cnf = conf.get_conf('PentahoReportConfig.ini','Default Reports')
FilesNames = ast.literal_eval(ReportName_cnf['Reports_names'])


path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/PentahoReportingService.log'

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

logger.info('Starting log')

def prptConfig(database, user_id, domain):
    for filename in FilesNames:

        xmldoc = minidom.parse(User_Reports_path+'/ReportSourceFile/'+filename+'/datasources/sql-ds.xml')
        ConUrl = xmldoc.getElementsByTagName("data:url")[0]
        ConUrl.firstChild.nodeValue = 'jdbc:mysql://104.197.32.159:3306/'+ database
        DbNames = xmldoc.getElementsByTagName("data:property")[4]
        DbNames.firstChild.nodeValue = database
        with open(User_Reports_path+'/ReportSourceFile/'+filename+'/datasources/sql-ds.xml', "wb") as f:
            xmldoc.writexml(f)
        logger.info('* DB name Connection URL changed')

        if os.path.exists(User_Reports_path+'/digin_user_data/'+user_id+'/'+domain+'/prpt_files/'+filename+'/'+filename+'.prpt'):
            os.remove(User_Reports_path+'/digin_user_data/'+user_id+'/'+domain+'/prpt_files/'+filename+'/'+filename+'.prpt')

        prpt_path = User_Reports_path+'/digin_user_data/'+user_id+'/'+domain+'/prpt_files/'+filename
        try:
            os.makedirs(prpt_path)
        except OSError:
            if not os.path.isdir(prpt_path):
                raise

        _zip = zipfile.ZipFile(prpt_path+'/'+filename+'.prpt', 'a')
        _zip.write(User_Reports_path+'/ReportSourceFile/'+filename+'/translations.properties','translations.properties',zipfile.ZIP_DEFLATED)
        _zip.write(User_Reports_path+'/ReportSourceFile/'+filename+'/styles.xml','styles.xml',zipfile.ZIP_DEFLATED)
        _zip.write(User_Reports_path+'/ReportSourceFile/'+filename+'/settings.xml','settings.xml',zipfile.ZIP_DEFLATED)
        _zip.write(User_Reports_path+'/ReportSourceFile/'+filename+'/mimetype','mimetype',zipfile.ZIP_DEFLATED)
        _zip.write(User_Reports_path+'/ReportSourceFile/'+filename+'/meta.xml','meta.xml',zipfile.ZIP_DEFLATED)
        _zip.write(User_Reports_path+'/ReportSourceFile/'+filename+'/dataschema.xml','dataschema.xml',zipfile.ZIP_DEFLATED)
        _zip.write(User_Reports_path+'/ReportSourceFile/'+filename+'/datadefinition.xml','datadefinition.xml',zipfile.ZIP_DEFLATED)
        _zip.write(User_Reports_path+'/ReportSourceFile/'+filename+'/content.xml','content.xml',zipfile.ZIP_DEFLATED)
        _zip.write(User_Reports_path+'/ReportSourceFile/'+filename+'/layout.xml','layout.xml',zipfile.ZIP_DEFLATED)
        _zip.write(User_Reports_path+'/ReportSourceFile/'+filename+'/datasources/compound-ds.xml','datasources/compound-ds.xml',zipfile.ZIP_DEFLATED)
        _zip.write(User_Reports_path+'/ReportSourceFile/'+filename+'/datasources/sql-ds.xml','datasources/sql-ds.xml',zipfile.ZIP_DEFLATED)
        _zip.write(User_Reports_path+'/ReportSourceFile/'+filename+'/META-INF/manifest.xml','META-INF/manifest.xml',zipfile.ZIP_DEFLATED)
        _zip.close()

        zip_ref = zipfile.ZipFile(User_Reports_path+'/digin_user_data/'+user_id+'/'+domain+'/prpt_files/'+filename+'/'+filename+'.prpt', 'r')
        zip_ref.extractall(User_Reports_path+'/digin_user_data/'+user_id+'/'+domain+'/prpt_files/'+filename+'/')
        zip_ref.close()
    logger.info('* new Report default folders created for user :'+user_id)
    return True

def ktrConfig( user_id, domain):

    for filename in FilesNames:

        xmldoc = minidom.parse(User_Reports_path+'/ReportSourceFile/'+filename+'/'+filename+'.ktr')

        ReportDF = xmldoc.getElementsByTagName("item")[0]
        ReportDF.firstChild.nodeValue = User_Reports_path+'/digin_user_data/'+user_id+'/'+domain+'/prpt_files/'+filename+'/'+filename+'.prpt'
        OutPut = xmldoc.getElementsByTagName("item")[1]
        OutPut.firstChild.nodeValue = User_Reports_path+'/digin_user_data/'+user_id+'/'+domain+'/prpt_files/'+filename+'/'+filename+'.pdf'

        with open(User_Reports_path+'/digin_user_data/'+user_id+'/'+domain+'/prpt_files/'+filename+'/'+filename+'.ktr', "wb") as f:
            xmldoc.writexml(f)
    logger.info('* Ktr file path changed for user :'+user_id)
    logger.info('***************sucessfuly done Pentaho Report Configration Service***************')
    return True