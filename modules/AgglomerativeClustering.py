__author__ = 'Jeganathan Thivatharan'
__version__ = '1.0.0.0'

'''
A Python implementation of the AgglomerativeClustering algorithm. by sklearn.cluster-AgglomerativeClustering
'''

print(__doc__)
#from time import time
import pandas as pd
#import numpy as np
#from matplotlib import pyplot as plt

from sklearn import manifold


def  AgglomerativeClustering(x):
# 2D embedding of the digits dataset
    print("Computing embedding")
    X_red = manifold.SpectralEmbedding(n_components=2).fit_transform(x)
    print("Done.")

    from sklearn.cluster import AgglomerativeClustering
    AllData =[]
    for linkage in ('ward', 'average', 'complete'):
        clustering = AgglomerativeClustering(linkage=linkage, n_clusters=10)
        #t0 = time()
        clustering.fit(X_red)
        # print("%s : %.2fs" % (linkage, time() - t0))
        # print X_red
        # print 'cl',clustering.labels_
        # print 'linkage',linkage
        z = pd.DataFrame(clustering.labels_)
        Y = pd.DataFrame(X_red)
        z.columns = ['cluster_labels']
        DataSet = pd.concat([x,Y, z], axis=1,join='inner')
        print type(DataSet.to_dict('records')) # change the dataframe to dictionary
        result ={'linkage':linkage,
                'Data':DataSet.to_dict('records')
                }
        AllData.append(result)
    return AllData


#-------------------------------------------------------------------------------------
# Visualize the clustering
# plot_clustering(X_red, X, clustering.labels_, "%s linkage" % linkage)
#
# def plot_clustering(X_red, X, labels, title=None):
#     x_min, x_max = np.min(X_red, axis=0), np.max(X_red, axis=0)
#     X_red = (X_red - x_min) / (x_max - x_min)
#
#     plt.figure(figsize=(6, 4))
#     for i in range(X_red.shape[0]):
#         plt.text(X_red[i, 0], X_red[i, 1], str(X[i,]),
#                  color=plt.cm.spectral(labels[i] / 10.),
#                  fontdict={'weight': 'bold', 'size': 9})
#
#     plt.xticks([])
#     plt.yticks([])
#     if title is not None:
#         plt.title(title, size=17)
#     plt.axis('off')
#     plt.tight_layout()
#
#


#http://localhost:8080/fuzzyc_calculation?data=[{%27demo_duosoftware_com.iris%27:[%27Sepal_Length%27,%27Petal_Length%27]}]&dbtype=bigquery&SecurityToken=ab46f8451d401be58d12eb5081660e80&Domain=duosoftware.com&iddd=1
# output=ca.AgglomerativeClustering(df) make change in algorithom processor