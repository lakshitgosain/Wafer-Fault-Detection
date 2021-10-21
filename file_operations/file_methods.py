import os
import pickle
import shutil

class FileOperation:

    def __init__(self,file_object,logger):
        self.file_object=file_object
        self.logger=logger
        self.model_directory='models/'

    def save_model(self,model,filename):
        self.logger.log(self.file_object,'Started to save the model!')

        try:

            path=os.path.join(self.model_directory,filename)
            if os.path.isdir(path):
                shutil.rmtree(self.model_directory)
                os.makedirs(path)
            else:
                os.makedirs(path)
            with open(path + '/'+ filename+'.sav','wb') as f:
                pickle.dump(model,f)
            self.logger.log(self.file_object,'Model Saved!!!')
            return 'sucess'


        except Exception as e:
            self.logger.log(self.file_object,'Exception Occured{}'.format(e))
            raise e




