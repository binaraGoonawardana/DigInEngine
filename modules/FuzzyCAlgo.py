__author__ = 'Jeganathan Thivatharan'
__version__ = '1.0.0.0'

import pandas as pd
import skfuzzy as fuzz
import numpy as np
#http://localhost:8080/fuzzyc_calculation?data=[{%27demo_duosoftware_com.iris%27:[%27Sepal_Length%27,%27Petal_Length%27]}]&dbtype=bigquery&SecurityToken=ab46f8451d401be58d12eb5081660e80&Domain=duosoftware.com
def FuzzyC_algo(alldata):
    OutPut =[]
    Y = alldata.as_matrix()  # convert the data frame to narray
    InputData = Y.T
    ncenters=5
    cntr, u, u0, d, jm, p, fpc = fuzz.cluster.cmeans(InputData, ncenters, 2, error=0.005, maxiter=1000, init=None)
    #fpcs.append(fpc)  # Store fpc values for later
    cluster_membership = np.argmax(u, axis=0)
    #CM = pd.DataFrame(cluster_membership)
    #DataSet = pd.concat([alldata, CM], axis=1,join='inner')
    DataSet = alldata
    #key = range(0,ncenters)
    #Centers = dict(zip(key, cntr))
    DataSet['clusterPoints'] = cluster_membership
    result ={'DataSet': DataSet.to_dict('records'),  # converting the dataframe to dictionary
            'Centers': str(cntr).splitlines(),  # converting the array to string and removing the now row'\n'
             'ncenters': ncenters
            }
    print "result['Centers']: ", result['Centers']
    print cntr
    #OutPut.append(result)
    #print result
    return result







