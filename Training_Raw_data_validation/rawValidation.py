import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_logger

class Raw_Data_validation:
    def __init__(self,path):
        self.Batch_directory=path
        self.schema_path='schema_training.json'
        self.logger=App_logger()

    def ValuesFromSchema(self):
        try:
            with open(self.schema_path,'r') as f:
                dic=json.load(f)
                f.close()

            pattern=dic['SampleFileName']
            LengthOfDateStampInFile=dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile=dic['LengthOfTimeStampInFile']
            column_names=dic['ColName']
            NumberOfColumns=dic['NumberofColumns']

            file=open('Training_Logs/valuesFromSchemaValidationLog.txt','a+')
            message='LengthOfDateTimeStamp{}\tLengthOfDateTimeStamp{}\tNumberOfColumns{}'.format(LengthOfDateStampInFile,LengthOfTimeStampInFile,NumberOfColumns)
            self.logger.log(file,message)

            file.close()

        except ValueError:
            file=open('Training_logs/valuesFromSchemaValidation.txt','a+')
            self.logger.log(file,'ValueError:Value not found in schema_training.json File')
            file.close()
            raise ValueError

        except KeyError:
            file=open('Training_logs/ValuesFromSchemaValidation.txt','a+')
            self.logger.log(file,'Key Error: Key not found in schema_training.json File')
            file.close()
            raise KeyError

        except Exception as e:
            file=open('Training_logs/ValuesFromSchemaValidation.txt','a+')
            self.logger.log(file,str(e))
            file.close()
            raise e

        return LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,NumberOfColumns



    def manualRegexCreation(self):
        regex="['wafer']+['\_'']+['\d_]+['\d]+\.csv"
        return regex

    def ValidationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        #Delete the folders created after an unsuccessful run of the Validation of the file.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()


        self.createDirectoryForGoodBadRawData()

        files=[f for f in listdir(self.Batch_directory)]

        try:
            f=open('Training_Logs/nameValidationLog.txt','a+')
            for filename in files:
                if re.match(regex,filename):
                    splitatdot=re.split('.csv',filename)
                    splitatunderscore=re.split('_',splitatdot[0])

                    if len(splitatunderscore[1])==LengthOfDateStampInFile:
                        if len(splitatunderscore[2])==LengthOfTimeStampInFile:
                            shutil.copy('Prediction_Batch_Files/'+str(filename),"Training_Raw_files_validated/Good_Raw")
                            self.logger.log(f,'Valid File name! Moving the file to good data folder')

                        else:
                            shutil.copy('Prediction_Batch_Files/'+filename,'Training_Raw_Files_Validated/Bad_Raw')
                            self.logger.log(f,'File has been rejected and moved to Bad Raw Folder')
                    else:
                        shutil.copy('Prediction_Batch_Files/' + filename, 'Training_Raw_Files_Validated/Bad_Raw')
                        self.logger.log(f, 'File has been rejected and moved to Bad Raw Folder')
                else:
                    shutil.copy('Prediction_Batch_Files/' + filename, 'Training_Raw_Files_Validated/Bad_Raw')
                    self.logger.log(f, 'File has been rejected and moved to Bad Raw Folder')
            f.close()

        except Exception as e:
            f=open('Training_Logs.txt','a+')
            self.logger.log(f,'Error Occured{}'.format(e))
            raise e
            f.close()


    def ValidateColumnLength(self,noofcolumns) :
        try:
            f=open('Training_Logs/ColumnValidationLog.txt','a+')
            self.logger.log(f,'Validation of column length started')

            for file in listdir('Training_Raw_files_validated/Good_Raw'):
                csv=pd.read_csv('Training_Raw_files_validated/Good_Raw/' + file)
                if csv.shape[1]==noofcolumns:
                    pass
                else:
                    shutil.move('Training_Raw_files_validated/Good_Raw/' + file
                                , 'Training_Raw_files_validated/Bad_Raw')

                self.logger.log(f,'File did not have the required number of columns{}\t{}, Movied file to Bad Data Folder'.format(csv.shape[1],noofcolumns))
        except OSError as e:
            f=open('Training_Logs/ColumnValidationLog.txt','a+')
            self.logger.log(f,'Error while moving the file to different Directory{}'.format(e))
            f.close()
            raise OSError

        except Exception as e:
            f=open('Training_Logs/ColumnValidationLog.txt','a+')
            self.logger.log(f,e)
            f.close()
            raise e



    def ValidateMissingValuesInAllColumns(self):
        try:

            f=open('Training_Logs/MissingValueValidationLog.txt','a+')
            self.logger.log(f,'Starting Missing Values Validation')
            for files in listdir('Training_Raw_files_validated/Good_Raw'):
                csv=pd.read_csv('Training_Raw_files_validated/Good_Raw/'+files)
                csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                if csv.isna.sum()==False:
                    pass
                else:
                    shutil.move("Training_Raw_files_validated/Good_Raw/" + files,"Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f,'Invalid Column Blank Data , Moving file to Bad_raw')
                    csv.to_csv("Training_Raw_files_validated/Good_Raw/" +files,header=True)

        except OSError:
            f=open('Training_Logs/MissingValueValidationLog.txt','a+')
            self.logger.log(f,'error occured while moving file')
            f.close()
            raise OSError

        except Exception as e:
            f=open('Training_Logs/MissingValueValidationLog.txt','a+')
            self.logger.log(f, e)
            f.close()
            raise e

        f.close()

    def createDirectoryForGoodBadRawData(self):
        try:

            path=os.path.join("Training_Raw_files_validated/",'Good_Raw/')
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Training_Raw_files_validated/", 'Bad_Raw/')
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file=open('Training_Logs/GeneralLog.txt','a+')
            self.logger.log(file,'Error while creating Directory{}'.format(ex))
            file.close()
            raise OSError


    def deleteExistingGoodDataTrainingFolder(self):
        try:
            path='Training_Raw_Files_Validated/'
            if os.path.isdir(path+'Good_raw/'):
                shutil.rmtree(path+'Good_raw/')
                file=open('Training_Logs/General_Log.txt','a+')
                self.logger.log(file,'Good_Raw Directory Deleted!!')
                file.close()

        except OSError as s:
            file=open('Training_Logs/General_Log.txt','a+')
            self.logger.log(file, 'Error Received{}'.format(s))
            file.close()
            raise OSError

    def deleteExistingBadDataTrainingFolder(self):
        try:
            path = 'Training_Raw_Files_Validated/'
            if os.path.isdir(path + 'Bad_raw/'):
                shutil.rmtree(path + 'Bad_raw/')
                file = open('Training_Logs/General_Log.txt', 'a+')
                self.logger.log(file, 'Bad_Raw Directory Deleted!!')
                file.close()

        except OSError as s:
            file = open('Training_Logs/General_Log.txt', 'a+')
            self.logger.log(file, 'Error Received{}'.format(s))
            file.close()
            raise OSError





    def moveToArchive(self):
        try:
            now=datetime.now()
            date=now.date()
            time=now.time()

            src='Training_Raw_Files_validated/Bad_Raw/'
            if os.path.isdir(src):
                path='TrainingArchiveBadData'
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest='TrainingArchiveBadData/BadData_'+str(date)#+'_'+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)

                files=os.listdir(src)
                #files=[file for file in os.listdir(dest)]

                for f in files :
                    if f not in os.listdir(dest):
                        shutil.move(src+f,dest)

                file=open('Training_Logs/MoveToArchive.txt','a+')
                self.logger.log(file,'Files moved to Archives!')

                path='Training_Raw_Files_Validated/'
                if os.path.isdir(path+'Bad_Raw'):
                    shutil.rmtree(path+'Bad_Raw')

                self.logger.log(file,'Bad Raw Files Folder Removed')

                file.close()


        except Exception as e:
            file=open('Training_Logs/GeneralLogs.txt','a+')
            self.logger.log(file,'Error Moing the Files{}'.format(e))
            file.close()
            raise e







