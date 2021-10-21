from sklearn.cluster import KMeans
from kneed import KneeLocator
import matplotlib.pyplot as plt
from file_operations import file_methods


class KMeansCluster:

    def __init__(self,file_object,logger_object):
        self.file_object=file_object
        self.logger=logger_object

    def elbow_plot(self,data):
        self.logger.log(self.file_object,'Clustering using K Means Started')
        wcss=[]
        try:
            for i in range(1,11):
                kmeans=KMeans(n_clusters=i,init='k-means++',random_state=40)
                kmeans.fit(data)
                wcss.append(kmeans.inertia_)

            plt.plot(range(1,11),wcss)
            plt.title('Elbow Method')
            plt.xlabel('Number of clusters')
            plt.ylabel('WCSS')
            plt.savefig('preprocessing_data/K-Means_Elbow.PNG')

            self.kn=KneeLocator(range(1,11),wcss,curve='convex',direction='decreasing')
            self.logger.log(self.file_object,'Clustering using KneeLocator ended')
            return self.kn

        except Exception as e:
            self.logger.log(self.file_object,'An Exception Occured{}'.format(e))
            raise e


    def create_clusters(self,data,number_of_clusters):
        self.logger.log(self.file_object,'creating Clusters Function Begins')
        self.data=data

        try:
            self.kmeans=KMeans(nclusters=number_of_clusters,init='k-means++',random_state=40)
            self.y_kmeans=self.kmeans.fit_predict(data)

            self.file_op=file_methods.FileOperation(self.file_object,self.logger)
            self.save_model=self.file_op.save_model(self.kmeans,'KMeans')


            self.data['Cluster']=self.y_kmeans
            self.logger.log(self.file_object,'Successfully Created K means Clusters')
            return self.data

        except Exception as e:
            self.logger.log(self.file_object,'An Exception Occured{}'.format(e))
            raise e
