import os
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import LabelEncoder
import joblib

class model_base():
    def __init__(self, dependent_variable: str = None, excluded_fields: list[str] = [], category_fields: list[str] = []):
        self.dependent_field = dependent_variable
        self.excluded_fields = excluded_fields
        self.category_fields = category_fields

    def training_prepare(self, dataset: pd.DataFrame) -> bool:
        if self.dependent_field is None:
            return False
        self.dataset = {}
        self.dataset['X'] = dataset.loc[:, ~dataset.columns.isin([self.dependent_field])]
        self.dataset['y'] = dataset[[self.dependent_field]]

    def exclude_fields(self) -> None:
        self.dataset['X'] = self.dataset['X'].loc[:, ~self.dataset['X'].columns.isin(self.excluded_fields)]

    def split_training_and_test(self, ratio: int = 0.2) -> None:
        self.training_set = {}
        self.test_set = {}
        self.training_set['X'], self.test_set['X'], self.training_set['y'], self.test_set['y'] = train_test_split(self.transformed_set['X'], self.transformed_set['y'], test_size = ratio, random_state = 0)
    
    def transform(self) -> None:
        self.transformed_set = {}
        if hasattr(self, 'onehot_encoder') is False:
            self.onehot_encoder = ColumnTransformer(transformers=[('encoder', OneHotEncoder(handle_unknown='ignore'), self.category_fields)], remainder='passthrough')
            self.transformed_set['X'] = self.onehot_encoder.fit_transform(self.dataset['X']).toarray()
        else: # need to use the original encoder without fitting
            self.transformed_set['X'] = self.onehot_encoder.transform(self.dataset['X']).toarray()

        # do not run on predictions since dependent variable is not there
        if 'y' in self.dataset:
            # placeholder if encoding is ever needed
            self.transformed_set['y'] = self.dataset['y'].values

    def fit(self) -> None:
        if hasattr(self, 'model') is False:
            self.model = XGBClassifier(n_estimators=500, learning_rate=0.01)
        self.model.fit(X=self.training_set['X'], y=self.training_set['y'])

    def predict(self, test_set:list = None) -> list:
        self.predictions = {}
        if test_set is None:
            test_set = self.transformed_set['X']
        if hasattr(self, 'model') is False:
            raise Exception('No model has been loaded')
        self.predictions['y'] = self.model.predict(test_set).tolist()

    def analytics(self) -> None:
        accuracies = cross_val_score(estimator = self.model, X = self.test_set['X'], y = self.test_set['y'], cv = 10)
        print("Accuracy: {:.2f} %".format(accuracies.mean()*100))
        print("Standard Deviation: {:.2f} %".format(accuracies.std()*100))

    def train(self, dataset: pd.DataFrame) -> None:
        self.training_prepare(dataset)
        self.exclude_fields()
        self.transform()
        self.split_training_and_test()
        self.fit()
        self.analytics()
        self.save_model()

    def predict_dataset(self, dataset: pd.DataFrame) -> list:
        self.load_model()
        self.dataset = {'X': dataset}
        self.exclude_fields()
        self.transform()
        self.predict()
        return self.predictions

    def save_model(self) -> None:
        os.makedirs('models', exist_ok=True)
        joblib.dump(self.model, 'models/model.pkl.compressed', compress=True)
        joblib.dump(self.onehot_encoder, 'models/onehotencoder.pkl.compressed', compress=True)

    def load_model(self) -> None:
        self.model = joblib.load('models/model.pkl.compressed')
        self.onehot_encoder = joblib.load('models/onehotencoder.pkl.compressed')
