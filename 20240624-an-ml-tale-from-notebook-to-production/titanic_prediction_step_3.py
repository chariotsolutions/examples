import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from model_base_pipeline import model_base_pipeline

# Training
dataset = pd.read_csv('data/train.csv')
dependent_variable = 'Survived'
excluded_fields = ['PassengerId','Ticket', 'Name']
category_fields = ['Sex', 'Cabin', 'Embarked']
titanic_model = model_base_pipeline(dependent_variable, excluded_fields, category_fields)
titanic_model.train(dataset)

# Prediction
test_dataset = pd.read_csv('data/test.csv')
titanic_model_predict = model_base_pipeline(dependent_variable, excluded_fields, category_fields)
pd.DataFrame(
    {
        'PassengerId': test_dataset.iloc[:, 0],
        'Survived': titanic_model_predict.predict_dataset(test_dataset)['y']
    }
).to_csv('predictions/submission_3.csv', index=False)
