from flask import Flask,request,render_template
import numpy as np
from wsgiref import simple_server
import os
from training_validation_Insertion import train_validation

from flask_cors import CORS,cross_origin
from flask import Response
import json
app=Flask(__name__)
CORS(app)

@app.route('/',methods=['GET'])
@cross_origin()
#def home():
    #return render_template((index.html))

@app.route('/predict',methods=['POST'])
@cross_origin()
def predictRouteClient():
    try :
        if request.json is not None:
            path=request.json['filepath']
            pred_val=pred_validation(path) #Initializing Object
            pred_val.prediction_validation() #Calling the pred_validation Function

            pred=prediction(path)



@app.route(('/train',methods=['POST']))
@cross_origin()
def trainRouteClient():
    try:
        if request.json['folderpath'] is not None:
            path=request.json['folderpath']

            train_valObj=train_validation(path)

            train_valObj.train_validation() #calling the training validation function. train_valobj is the object initialized with the class train_validation

            trainModelObj=trainModel()
            trainModelObj.trainingModel()

