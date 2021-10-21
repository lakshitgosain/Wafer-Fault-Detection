from datetime import datetime
from application_logging.logger import App_logger
import os
import pandas as pd

class DataTransform:
    def __init__(self):
        self.logger=App_logger()
        self.GoodDataPath='Training_Raw_Files_Validated/Good_raw'

    def replaceMissingValuesWithNULL(self):

        log_file=open('Training_Logs/Data_Transformation_Log.txt','a+')
        try:
            all_files=[file for file in os.listdir(self.GoodDataPath)]
            for file in all_files:
                csv=pd.read_csv(self.GoodDataPath+'/'+file)
                csv.fillna('NULL',inplace=True)
                csv['Wafer']=csv['Wafer'].str[6:]
                csv.to_csv(self.goodDataPath+ "/" + file, index=None, header=True)
                self.logger.log(log_file,'Data Transformation Completed for {}!!'.format(file))

        except Exception as e:
            self.logger.log(log_file,e)
            log_file.close()
            raise e
        log_file.close()