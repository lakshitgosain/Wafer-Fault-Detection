import pandas as pd
import numpy as np
import os
import datetime

from sklearn.impute import KNNImputer

class Preprocessor:
    def __init__(self,file_object,logger_object):
        self.file_object=file_object
        self.logger=logger_object


    def remove_columns(self,data,columns):
        self.logger.log(self.file_object,'Starting the Preprocessing to remove the columns provided')
        self.data=data
        self.columns=columns

        try:
            self.useful_data=self.data.drop([self.columns],axis=1)
            self.logger.log(self.file_object,'Removed the provided column')

            return self.useful_data

        except Exception as e:
            self.logger.log(self.file_object,'Unable to remove the column, error {} occured',e)
            raise e



    def separate_label_output(self,data,label_output_name):

        try:
            self.logger.log(self.file_object,'Separating labels and Outputs')
            self.X=data.drop([label_output_name],axis=1)
            self.y=data[label_output_name]
            return self.X,self.y

        except Exception as e:
            self.logger.log(self.file_object,'Error received while separating columns{}',e)
            raise e


    def is_null_present(self,X):
        self.logger.log(self.file_object,'Entered the Check NULL Stage')
        self.null_present=False

        try:
            self.null_counts=X.isna.sum()
            for i in self.null_counts:
                if i >0:
                    self.null_present=True
                    break

            if(self.null_present):
                dataframe_with_null=pd.DataFrame()
                dataframe_with_null['columns']=X.columns
                dataframe_with_null['missing_Value_count']=np.asarray(X.isna.sum())
                dataframe_with_null.to_csv('Data_Preprocessing/dataframe_with_null.csv')
            self.logger.log(self.file_object,'Finding Missing Values Function Successfully executed!')
            return self.null_present

        except Exception as e:
            self.logger.log(self.file_object,e)
            raise e


    def impute_data(self,data):
        self.data=data
        self.logger.log(self.file_object,'Imputer process Started')
        try:

            imputer=KNNImputer(n_neighbours=3,weights='uniform',missing_values=np.nan)
            X=imputer.fit_transform(self.data)
            self.new_data=pd.DataFrame(X,columns=self.data.columns)
            self.logger.log(self.file_object, 'Imputation Successful!!!')
            return self.new_data

        except Exception as e:

            self.logger.log(self.file_object,'Imputation was unsuccessful!{}'.format(e))
            raise e



    def get_cols_with_zero_std_deviation(self,data):
        self.logger.log(self.file_object,'Zero Std Deviation Check Started!')
        self.columns=data.columns
        self.data_n=data.describe()
        self.col_to_drop=[]

        try:
            for x in self.columns:
                if (self.data_n[x]['std']==0):
                    self.col_to_drop.append(x)
            self.logger.log(self.file_object,'Zero Deviation Check completed!')
            return self.col_to_drop

        except Exception as e:
            self.logger.log('Error Occured',e)

            raise e


