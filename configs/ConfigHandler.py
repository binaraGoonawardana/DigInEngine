__author__ = 'Marlon Abeykoon'

import ConfigParser
import os
import sys
#code added by sajee on 12/27/2015
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))

def get_conf(filename,section):

    Config = ConfigParser.ConfigParser()
    Config.optionxform = str   #This makes configparser not to lowercase the keys
    
    Config.read(filename)
    print  currDir 
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                print("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    print dict1
    return dict1