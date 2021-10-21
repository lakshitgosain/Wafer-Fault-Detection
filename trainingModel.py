from sklearn.model_selection import train_test_split
from application_logging.logger import App_logger
from data_ingestion import data_loader
from Data_Preprocessing import preprocessor
from Data_Preprocessing import clustering
from best_model_finder import tuner
from file_operations import file_methods


class trainModel:
    def __init__(self):
        self.logger=App_logger()
        self.file_object = open('Training_Logs/ModelTraining.txt', 'a+')

    def trainingModel(self):
        self.logger.log(self.file_object,'Starting Training Model')
        try:
            data_getter=data_loader.Data_Getter()
            data=data_getter.get_data()

            preprocess=preprocessor.Preprocessor(self.file_object,self.logger)
            data=preprocess.remove_columns(data,['Wafer'])


            #Create Features and labels
            X,y=preprocess.separate_label_output(data,label_output_name='Output')

            #Checking if Null Values are present in the columns
            is_null_present=preprocess.is_null_present(X)

            if (is_null_present):
                X=preprocess.impute_data(X)

            #Checking Columns with 0 Std Deviation which means that the columns have a same value and not different Values in it.
            cols_to_drop=preprocess.get_cols_with_zero_std_deviation(X)
            #Now removing the columns with zero Std Deviation
            X=preprocess.remove_columns(X,cols_to_drop)

            cluster=clustering.KMeansCluster(self.file_object,self.logger)
            number_of_clusters=cluster.elbow_plot(X)

            X=cluster.create_clusters(X,number_of_clusters)

            X['Labels']=y

            list_of_clusters=X['Cluster'].unique()

            for i in list_of_clusters:
                cluster_data=X[X['Cluster']]==i#Filtering the Data for one cluster

                cluster_features=cluster_data.drop(['Labels','Cluster']) # We do not need cluster and Labels data in the Dataset, hence dropping it.
                cluster_label=cluster_data['Labels']

                X_train,X_test,y_train,y_test=train_test_split(cluster_features,cluster_label,random_state=0,test_size=0.20)


                model_finder=tuner.Model_finder(self.file_object,self.logger)

                best_model_name,best_model=model_finder.get_best_model(X_train,X_test,y_train,y_test)

                file_op=file_methods.FileOperation(self.file_object,self.logger)
                save_model=file_op.save_model(best_model,best_model_name+str(i))

            self.logger.log(self.file_object,'Successful End of Training!')
            self.file_object.close()

        except Exception as e:
            self.logger.log('Exception Occured while Training the model{}'.format(e))
            raise e












