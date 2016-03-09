__author__ = 'Marlon Abeykoon'

import ConfigParser
import os

def get_conf(filename,section):

    Config = ConfigParser.ConfigParser()
    Config.optionxform = str   #This makes configparser not to lowercase the keys
    Config.read(os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))), 'configs', filename))

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