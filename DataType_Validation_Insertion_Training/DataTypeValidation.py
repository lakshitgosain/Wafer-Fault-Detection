import os
from datetime import datetime
import sqlite3
from application_logging.logger import App_logger
import shutil
import csv

class DBOperation:
    def __init__(self):

        self.path='Training_Database/'
        self.BadFilePath='Training_Raw_Files_Validated/Bad_Raw/'
        self.GoodFilePath='Training_Raw_Files_Validated/Good_Raw/'
        self.logger=App_logger()

    def dBConnector(self,DBName):
        try:
            conn=sqlite3.connect(self.path+DBName+'.db')
            file=open('Training_Logs/DBConnectionLogs.txt','a+')
            self.logger.log(file,'Connection With Database Successful!')
            file.close()


        except Exception as e:
            file=open('Training_Logs/DBConnectionLogs.txt','a+')
            self.logger.log(file,e)
            file.close()
            raise e

        except ConnectionError:
            file = open('Training_Logs/DBConnectionLogs.txt', 'a+')
            self.logger.log(file, "Connection to Database Failed")
            file.close()
            raise ConnectionError

        return conn





    def CreateTableDB(self,DBname,column_name):
        try:
            file=open('Training_Logs/CreateTable_Logs.txt','a+')
            self.logger.log(file,'Creation Of DB Started')

            conn=self.dBConnector(DBName)
            c=conn.cursor()
            c.execute("Select count(name) from sqlite_master where type='table' and name='Good_Raw_Data")
            if c.fetchone()[0]==1:
                conn.close()
                file=open('Training_Logs/DBCreateLogs.txt','a+')
                self.logger.log(file,'Database Tables Created Successfully!')
                file.close()
            else:

                for key in column_name.keys():
                    type=column_name[key]

                    try:
                        conn.execute('ALTER TABLE Good_Raw_Data ADD Column {}{}'.format(column_name,type))
                    except:
                        conn.execute('CREATE Table Good_Raw_Data({}{})'.format(column_name,type))


                conn.close()
                file=open('Training_Logs/DBTableCreateLog.txt','a+')
                self.logger.log(file,'Tables Created Successfully')
                file.close()

        except Exception as e :
            file=open('Training_Logs/DbTableCreateLogs.txt','a+')
            self.logger.log(file,'Error while creating Tables')
            file.close()




    def InsertIntoTableGoodData(self,Database):
        conn=self.dBConnector(Database)
        goodfilepath=self.GoodFilePath
        badfilepath=self.BadFilePath
        onlyfiles=[f for f in os.listdir(goodfilepath)]
        file=open('Training_Logs/InsertionLogs.txt','a+')

        for file in onlyfiles:
            try:
                with open(goodfilepath+'/'+file,'r') as f:
                    next(f)
                    reader=csv.reader(f,delimiter='\n')
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('Insert Into Good_Raw_Data values({})'.format(list_))
                                self.logger.log(file,'Insertion into DB Successful!!')
                                conn.commit()
                            except Exception as e:
                                raise e

            except Exception as e:
                raise e

        conn.close()
        file.close()


    def selectingDataFromTableIntocsv(self,Database):

        self.fileFromDB='TrainingFileFromDB'
        self.Inputfile='InputFile.csv'
        log_file=open('Training_Logs/Select_Data_From_DB.txt','a+')

        try:
            conn=self.dBConnector(Database)
            sqlStmt='Select * from Good_Raw_Data'
            cursor=conn.cursor()

            cursor.execute(sqlStmt)

            results=cursor.fetchall()

            headers=[i[0] for i in cursor.description]

            if not os.path.isdir(self.fileFromDB):
                os.mkdirs(self.fileFromDB)

            csvfile=csv.writer(open(self.fileFromDB+self.filename,'w',newline=''),delimiter=',',lineterminator='\r\n',quoting=csv.QUOTE_ALL,escapechar='\\')

            csvfile.writerow(headers)
            csvfile.writerow(results)

            self.logger.log(log_file,'File Exported Successfully!')
            log_file.close()

        except Exception as e:
            self.logger.log(log_file,'Export To CSV Failed')
            log_file.close()
            raise e







