from datetime import datetime
from Training_Raw_data_validation.rawValidation import Raw_Data_validation
from application_logging import logger
from DataTransformation_Training.DataTransformation import DataTransform
from DataType_Validation_Insertion_Training.DataTypeValidation import DBOperation
from flask import Flask,request,render_template
#path=path=request.json['folderpath']


class train_validation:
    def __init__(self, path):
        self.raw_data = Raw_Data_validation(path)
        self.dataTransform = DataTransform()
        self.DBOperation = DBOperation()
        self.file_object = open("Training_Logs/Training_Main_Log.txt", 'a+')
        self.log_writer = logger.App_logger()

    def train_validation(self):
        try:
            # This is the message we are giving to the logger object.
            self.log_writer.log(self.file_object,'Start of Validation on Files')
            # extracting values for prediction
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_name,noofcolumns =self.raw_data.ValuesFromSchema()
            # Getting the RegEX defined to validate filename.
            regex=self.raw_data.manualRegexCreation()
            self.raw_data.ValidationFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)
            # Validate Column length in the File
            self.raw_data.ValidateColumnLength(noofcolumns)
            self.raw_data.ValidateMissingValuesInAllColumns()
            self.log_writer.log(self.file_object,'Raw Validation of the FIle Completed!!!')

            self.dataTransform.replaceMissingValuesWithNULL()

            self.log_writer.log(self.file_object,'Data Transformation Completed!')

            self.log_writer.log(self.file_object,'Creation of DB and tables on the basis of given Schema Started!!')
            self.DBOperation.CreateTableDB('Training',column_name)


            self.DBOperation.InsertIntoTableGoodData('Training')
            self.log_writer.log(self.file_object,'Insertion into the Good Raw Data Table Completed!!!!!')


            self.log_writer.log(self.file_object,'Moving Bad Files to Archives and Deleting Them!!')
            # Move bad files to Archive Folder

            self.raw_data.moveToArchive()

            self.DBOperation.selectingDataFromTableIntocsv('Training')

            self.file_object.close()

        except Exception as e:
            raise e


train1=train_validation(r'C:\Users\LaKgos01\Desktop\Data science Practice\Wafer Fault Detection\Prediction_Batch_files')
train1.train_validation()







