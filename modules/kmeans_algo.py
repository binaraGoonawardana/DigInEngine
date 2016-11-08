__author__ = 'Manura Omal Bhagya'
__version__ = '1.0.0.0'

#from sklearn import datasets
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_samples
import numpy as np
import os,sys
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
import logging
import configs.ConfigHandler as conf

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/Kmeans.log'
handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info('--------------------------------------  KMEANS  ------------------------------------------------------')
logger.info('Starting log')

# iris = datasets.load_iris()
# X = pd.DataFrame(iris.data)

range_n_clusters = [2, 3, 4, 5, 6, 7, 8, 9, 10]

def optimum_clusters(range_n_clusters,y):

    logger.info('Start deciding Optimum Number of Clusters')

    dic = {}

    for n_clusters in range_n_clusters:

        s_cluster = []
        cluster = KMeans(n_clusters=n_clusters, random_state=10)    # # Create a subplot with 1 row and 2 columns

        cluster_labels = cluster.fit_predict(y)

        # The silhouette_score gives the average value for all the samples.
        # This gives a perspective into the density and separation of the formed
        # clusters
        #silhouette_avg = silhouette_score(y, cluster_labels)
        #print("For n_clusters =", n_clusters, "The average silhouette_score is :", silhouette_avg)

        # centers = cluster.cluster_centers_
        # print (centers)

        sample_silhouette_values = silhouette_samples(y, cluster_labels)
        for i in range(n_clusters):

            ith_cluster_silhouette_values = \
                sample_silhouette_values[cluster_labels == i]
            ith_cluster_silhouette_values.sort()

            size_cluster_i = ith_cluster_silhouette_values.shape[0]

            s_cluster.append(size_cluster_i)

        mean_s = np.mean(s_cluster)
        mse = 0
        for j in s_cluster:
            mse = mse+(s_cluster[j] - mean_s)**2

        dic[n_clusters] = mse
    #print (dic)

    min_d = min(dic, key=dic.get)
    logger.info('Optimum number of Clusters Decided')
    return (min_d)

def kmeans_algo(y):

    logger.info('Start executing kmeans algorithm')

    n_clusters = optimum_clusters(range_n_clusters,y)
    cluster = KMeans(n_clusters = n_clusters)
    cluster_labels = cluster.fit_predict(y)

    logger.info('Data points has Clustered')

    centers = cluster.cluster_centers_
    centers = centers.tolist()

    logger.info('Centers of each cluster decided')

    z = pd.DataFrame(cluster_labels)

    z.columns = ['cluster']
    result = pd.concat([y, z], axis=1,join='inner')
    result = result.to_dic('records')
    dic = {'kmeans': result, 'centers': centers}

    return dic

#print (kmeans_algo(X))
