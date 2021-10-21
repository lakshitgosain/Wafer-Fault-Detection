from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import auc,roc_auc_score,accuracy_score
from xgboost import XGBClassifier
from sklearn.model_selection import GridSearchCV,RandomizedSearchCV


class Model_finder:


    def __init__(self,file_object,logger):
        self.file_object=file_object
        self.logger=logger
        self.rndclf=RandomForestClassifier()
        self.xgbclf=XGBClassifier(objective="binary:logistic")

    def get_best_params_for_random_forest(self,X_train,y_train):
        self.logger.log(self.file_object,'Getting Best Params')
        try:
            self.param_grid={'n_estimators':[10,20,30,50,100],
                             'criterion':['gini','entropy'],
                             'max_depth':range(2,4,1),
                             'max_features':['auto','log2']}

            self.grid=GridSearchCV(estimator=self.rndclf,param_grid=self.param_grid,n_jobs=-1,cv=5,verbose=3)

            self.grid.fit(X_train,y_train)

            self.criterion=self.grid.best_params_['criterion']
            self.max_depth=self.grid.best_params_['max_depth']
            self.max_features=self.grid.best_params_['max_features']
            self.max_nestimators=self.grid.best_params_['n_estimators']

            self.rndclf=RandomForestClassifier(n_estimators=self.max_nestimators,max_depth=self.max_depth,criterion=self.criterion,max_features=self.max_features)
            self.rndclf.fit(X_train,y_train)

            self.logger.log(self.file_object,'Training using Random Forrest Classifier Completed')

            return self.rndclf

        except Exception as e:
            self.logger.log(self.file_object,'and Exception occured while training Random Forest Classifier{}'.format(e))
            raise e


    def get_best_params_for_xgb(self,X_train,y_train):
        self.logger.log(self.file_object,'Starting the Get Best Params function for XGBoost Classifier')
        try:
            self.param_grid_xgboost={'learning_rate':[0.5,0.1,0.01,0.001],
                                     'max_depth':[3,5,10,20],
                                     'n_estimators':[10,50,100,200]}

            self.grid=GridSearchCV(estimator=self.xgbclf,param_grid=self.param_grid_xgboost,n_jobs=-1,verbose=3)
            self.grid.fit(X_train,y_train)


            self.learning_rate=self.grid.best_params_['learning_rate']
            self.max_depth=self.grid.best_params_['max_depth']
            self.n_estimators=self.grid.best_params_['n_estimators']

            self.xgb=XGBClassifier(learning_rate=self.learning_rate,max_depth=self.max_depth,n_estimators=self.n_estimators)

            self.xgb.fit(X_train,y_train)

            self.logger.log(self.file_object,'Getting Best Params for XGB Classifier completed!!')

            return self.xgb

        except Exception as e:
            self.logger.log(self.file_object,'an Exception occured while training the XGB Classifier {}'.format(e))
            raise e



    def get_best_model(self,X_train,X_test,y_train,y_test):

        self.logger.log(self.file_object,)

        try:
            self.xgboost=self.get_best_params_for_xgb(X_train,y_train)
            self.prediction_xgboost=self.xgboost.predict(X_test,y_test)

            if len(y_test.unique())==1:
                self.xgboost_score=accuracy_score(y_test,self.prediction_xgboost)
                self.logger.log(self.file_object,'Accuracy for XG Boost Classifier is{}'.format(self.xgboost_score))
            else:
                self.xgboost_score=roc_auc_score(y_test,self.prediction_xgboost)
                self.logger.log(self.file_object,'Roc_Auc Score for XG Boost Classifier is{}'.format(self.xgboost_score))

            self.random_forest=self.get_best_params_for_random_forest(X_train,y_train)
            self.prediction_random_forest=self.random_forest.predict(X_test,y_test)

            if len(y_test.unique()) == 1:
                self.random_forest_score = accuracy_score(y_test, self.prediction_random_forest)
                self.logger.log(self.file_object, 'Accuracy for Random Forest Classifier is{}'.format(self.random_forest_score))
            else:
                self.random_forest_score = roc_auc_score(y_test, self.prediction_random_forest)
                self.logger.log(self.file_object, 'Roc_Auc Score for Random Forest Classifier is{}'.format(self.random_forest_score))

            if (self.random_forest_score>self.xgboost_score):
                return 'Random Forest',self.random_forest

            else:
                return 'XGBoost',self.xgboost

        except Exception as e:
            self.logger.log(self.file_object,'an Exception Occured{}'.format(e))
            raise e














