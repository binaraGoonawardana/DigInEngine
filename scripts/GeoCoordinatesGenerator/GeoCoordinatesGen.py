__author__ = 'Marlon'

import web
import geoFileReader
import MapsGeoCordingAPIauth



urls = (
    '/coordinates/(.*)', 'get_coords'
)

app = web.application(urls, globals())

class get_coords:
    def GET(self, path):
        #output = 'coordinates:[';
        #http://www.dreamsyssoft.com/python-scripting-tutorial/create-simple-rest-web-service-with-python.php
        # for child in root:
        #         print 'child', child.tag, child.attrib
        #         output += str(child.attrib) + ','
        data = []
        data = geoFileReader.open_file(path)
      #  print "data: " + str(data)
        coords = MapsGeoCordingAPIauth.getCoords(data)

        print "coords: " + str(coords)
        return coords



if  __name__ == "__main__":
    app.run()