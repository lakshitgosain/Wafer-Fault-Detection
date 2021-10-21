import pandas as pd
from application_logging.logger import App_logger
class Data_Getter:

    def __init__(self):
        self.logger = App_logger()
        self.file = open('Training_Logs/DataGetter.txt','a+')
        self.training_file = 'Training_FileFromDB/InputFile.csv'


    def get_data(self):
        try:
            self.logger.log(self.file, 'Starting to fetch the Data')
            self.data = pd.read_csv(self.training_file)
            self.logger.log(self.file, 'Data Load Successful!!')
            self.file.close()
            return self.data

        except Exception as e:
            self.logger.log(self.file, 'Exception Occured {}'.format(e))
            self.file.close()
            raise e
