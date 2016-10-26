# -*- coding: utf-8 -*-
__author__ = 'Jeganathan Thivatharan'
__version__ = '3.0.0.0.2'

import configs.ConfigHandler as conf
import os
import time
def get_folder_names(params,user_id,domain):


    if params.folder_type == 'data_source_folder':
        if params.folder_name == '' or params.folder_name == None:
            file_path = conf.get_conf('FilePathConfig.ini', 'User Files')[
                          'Path'] + '/digin_user_data/' + user_id + '/' + domain + '/data_sources'
            try:
                if not os.path.isdir(file_path):
                    print "no folder"
                    return []
                else:
                    directory = file_path
                    # root, dirs, files = os.walk(file_path).next()
                    # print dirs
                    # return dirs
                    file_list = []
                    for i in os.listdir(directory):
                        if os.path.isdir(os.path.join(directory, i)):
                            a = os.stat(os.path.join(directory, i))
                            file = {'file' : i,
                                    'created_date': time.ctime(a.st_ctime),
                                    'created_user': user_id
                            }
                            file_list.append(file)  # [file,user_id,created]
                    return file_list
            except OSError, err:
                print err
                if not os.path.isdir(file_path):
                    return 'No data_source_folder'
            except Exception, err:
                print err


        else:
            file_path = conf.get_conf('FilePathConfig.ini', 'User Files')[
                            'Path'] + '/digin_user_data/' + user_id + '/' + domain + '/data_sources/'+params.folder_name
            try:
                if not os.path.isdir(file_path):
                    print "no folder"
                    return []
                else:
                    filenames = os.listdir(file_path)
                    return [filename for filename in filenames if filename.endswith(".csv")]

            except OSError, err:
                print err
                if not os.path.isdir(file_path):
                    return 'No data_source_folder'
            except Exception, err:
                print err
