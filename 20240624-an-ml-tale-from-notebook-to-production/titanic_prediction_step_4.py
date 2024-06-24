import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from model_base_pipeline import model_base_pipeline
from model_histgradientboosting import model_histgradientboosting

# Training
dataset = pd.read_csv('data/train.csv')
dependent_variable = 'Survived'
excluded_fields = ['PassengerId','Ticket', 'Name']
category_fields = ['Sex', 'Cabin', 'Embarked']

test_dataset = pd.read_csv('data/test.csv')

# first model to test
print('XGBoost Results:')
titanic_model_xgboost = model_base_pipeline(dependent_variable, excluded_fields, category_fields)
titanic_model_xgboost.train(dataset)

titanic_model_xgboost_predict = model_base_pipeline(dependent_variable, excluded_fields, category_fields)
pd.DataFrame(
    {
        'PassengerId': test_dataset.iloc[:, 0],
        'Survived': titanic_model_xgboost_predict.predict_dataset(test_dataset)['y']
    }
).to_csv('predictions/submission_4_1.csv', index=False)

# second model to test
print('HistGradientBoostingClassifier Results:')
titanic_model_histgradient = model_histgradientboosting(dependent_variable, excluded_fields, category_fields)
titanic_model_histgradient.train(dataset)

# Prediction
titanic_model_histgradient_predict = model_histgradientboosting(dependent_variable, excluded_fields, category_fields)
pd.DataFrame(
    {
        'PassengerId': test_dataset.iloc[:, 0],
        'Survived': titanic_model_histgradient_predict.predict_dataset(test_dataset)['y']
    }
).to_csv('predictions/submission_4_2.csv', index=False)

