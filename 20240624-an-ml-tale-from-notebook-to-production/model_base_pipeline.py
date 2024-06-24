import os
import pandas as pd
import json
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import LabelEncoder
from sklearn.pipeline import Pipeline
import joblib

import custom_transformers

class model_base_pipeline():
    def __init__(self, dependent_variable: str = None, excluded_fields: list[str] = [], category_fields: list[str] = []):
        self.dependent_field = dependent_variable
        self.excluded_fields = excluded_fields
        self.category_fields = category_fields        
        
        self.pipeline = Pipeline(steps=[])
    
    def training_prepare(self, dataset: pd.DataFrame) -> bool:
        if self.dependent_field is None:
            return False
        self.dataset = {}
        self.dataset['X'] = dataset.loc[:, ~dataset.columns.isin([self.dependent_field])]
        self.dataset['y'] = dataset[[self.dependent_field]]
    
    def split_training_and_test(self, ratio: int = 0.2) -> None:
        self.training_set = {}
        self.test_set = {}
        self.training_set['X'], self.test_set['X'], self.training_set['y'], self.test_set['y'] = train_test_split(self.dataset['X'], self.dataset['y'], test_size = ratio, random_state = 0)

    def add_exclusion(self) -> None:
        self.pipeline.steps.append(('exclusion', custom_transformers.ExcludeFields(fields_to_exclude=self.excluded_fields)))

    def add_encoding(self) -> None:
        preprocessor = ColumnTransformer(
            transformers=[
                ('cat', OneHotEncoder(handle_unknown='ignore'), self.category_fields)
            ],
            remainder='passthrough'  # Keep the rest of the features as is
        )
        self.pipeline.steps.append(('preprocessor', preprocessor))

    def add_model(self) -> None:
        self.pipeline.steps.append(('classifier', XGBClassifier(n_estimators=500, learning_rate=0.01)))

    def predict(self, test_set:list = None) -> list:
        self.predictions = {}
        if test_set is None:
            test_set = self.dataset['X']
        if hasattr(self, 'pipeline') is False:
            raise Exception('No pipeline has been loaded')
        self.predictions['y'] = self.pipeline.predict(test_set).tolist()

    def analytics(self) -> None:
        accuracies = cross_val_score(estimator = self.pipeline, X = self.test_set['X'], y = self.test_set['y'], cv = 10)
        print("Accuracy: {:.2f} %".format(accuracies.mean()*100))
        print("Standard Deviation: {:.2f} %".format(accuracies.std()*100))

    def train(self, dataset: pd.DataFrame) -> None:
        self.training_prepare(dataset)
        self.add_exclusion()
        self.add_encoding()
        self.add_model()
        self.split_training_and_test()
        self.pipeline.fit(self.training_set['X'], self.training_set['y'])
        self.analytics()
        self.save_model()

    def predict_dataset(self, dataset: pd.DataFrame) -> list:
        self.load_model()
        self.dataset = {'X': dataset}
        self.predict()
        return self.predictions
    
    def output_config(self) -> dict:
        return {
            'steps': [f'{step[0]}: {str(step[1])}' for step in self.pipeline.steps],
            'columns': self.pipeline.named_steps['exclusion'].get_feature_names_out()
        }

    def save_model(self) -> None:
        os.makedirs('models', exist_ok=True)
        joblib.dump(self.pipeline, 'models/pipeline.pkl.compressed', compress=True)
        with open('models/pipeline.config.json', 'w') as file:
            json.dump(self.output_config(), file, indent=4)

    def load_model(self) -> None:
        self.pipeline = joblib.load('models/pipeline.pkl.compressed')

