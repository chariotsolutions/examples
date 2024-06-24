from sklearn.base import BaseEstimator, TransformerMixin

# Custom Transformers
class ExcludeFields(BaseEstimator, TransformerMixin):
    def __init__(self, fields_to_exclude):
        self.fields_to_exclude = fields_to_exclude
    
    def fit(self, X, y=None):
        self.feature_names_in_ = X.columns.tolist()
        self.feature_names_out_ = [col for col in self.feature_names_in_ if col not in self.fields_to_exclude]
        return self
    
    def transform(self, X):
        return X.drop(columns=self.fields_to_exclude)
    
    def get_feature_names_out(self, input_features=None):
        return self.feature_names_out_
    
# Custom transformer for debugging
class DebugTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, message):
        self.message = message
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        print(f"{self.message} - Shape: {X.shape}")
        print(X.head())  # Print the first few rows for a quick check
        return X

class ArrayTransformer(TransformerMixin):

    def fit(self, X, y=None, **fit_params):
        return self

    def transform(self, X, y=None, **fit_params):
        return X.toarray()