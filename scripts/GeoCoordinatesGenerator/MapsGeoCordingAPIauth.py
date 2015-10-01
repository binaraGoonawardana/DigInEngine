__author__ = 'Marlon'
import googlemaps
import json


gmaps = googlemaps.Client(key='AIzaSyAt0kbTQstry1tjqSQiMhkAJuMMd-K7Epo')


def getCoords(address):
    geocode_result = []

    for x in address:

        #print "x: " + x
        geocode_result.append(json.dumps(gmaps.geocode(x)))
        print geocode_result

    return json.dumps(geocode_result)
# Look up an address with reverse geocoding
#reverse_geocode_result = gmaps.reverse_geocode((40.714224, -73.961452))
#print reverse_geocode_result

# Request directions via public transit
#now = datetime.now()
#directions_result = gmaps.directions("Sydney Town Hall",
                                  #   "Parramatta, NSW",
                                   #  mode="transit",
                                    # departure_time=now)
