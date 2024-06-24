from sklearn.ensemble import HistGradientBoostingClassifier
from model_base_pipeline import model_base_pipeline
import custom_transformers

class model_histgradientboosting(model_base_pipeline):
    def add_model(self):
        self.pipeline.steps.append(('denser', custom_transformers.ArrayTransformer()))
        self.pipeline.steps.append(('classifier', HistGradientBoostingClassifier()))