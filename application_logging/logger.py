from datetime import datetime

class App_logger:

    def __init__(self):
        pass

    def log(self, file_object, log_message):
        self.now=datetime.now()
        self.date=self.now.date()
        self.current_time=self.now.strftime('%H:%M:%S')
        file_object.write(
            str(self.date)+ "/"+ str(self.current_time)+"\t"+str(log_message)+"\n")
