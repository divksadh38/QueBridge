import pandas as pd
import joblib
import xgboost as xgb
from sklearn.model_selection import train_test_split

# Load dataset
class ModelTrainer:
    def __init__(self,pre,sec1,sec2):
        self.pre = pd.read_pickle(pre)
        self.sec1 = pd.read_pickle(sec1)
        sefl.sec2 = pd.read_pickle(sec2)
        
# Train and save Model 3 (Post-Sector 2)
    def model1(self):
        dataset = self.pre
        target = dataset['Laptime']
        # Train and save Model 1 (Pre-Lap)
        X=dataset.drop(columns=['Laptime'])
        X_train, X_test, y_train, y_test = train_test_split(X, target, test_size=0.4, random_state=42, shuffle=True)
        model = xgb.XGBRegressor(objective="reg:squarederror",
                                 n_estimators=100,max_depth=6,
                                 learning_rate=0.1,subsample=0.8,
                                 colsample_bytree=0.8,
                                 random_state=42,
                                 enable_categorical=True)

                                 
        model.fit(X_train, y_train)
        joblib.dump(model1, "models/model_pre.pkl")
        

# Train and save Model 3 (Post-Sector 2)
    def model2(self):
            dataset = self.sec1
            target = dataset['Laptime']
            # Train and save Model 1 (Pre-Lap)
            X=dataset.drop(columns=['Laptime'])
            X_train, X_test, y_train, y_test = train_test_split(X, target, test_size=0.4, random_state=42, shuffle=True)
            model = xgb.XGBRegressor(objective="reg:squarederror",
                                             n_estimators=100,max_depth=6,
                                             learning_rate=0.1,subsample=0.8,
                                             colsample_bytree=0.8,
                                             random_state=42,
                                             enable_categorical=True)    
            model.fit(X_train, y_train)
            joblib.dump(model2, "models/model_sec1.pkl")

# Train and save Model 3 (Post-Sector 2)
    def model3(self):
            dataset = self.sec2
            target = dataset['Laptime']
            # Train and save Model 1 (Pre-Lap)
            X=dataset.drop(columns=['Laptime'])
            X_train, X_test, y_train, y_test = train_test_split(X, target, test_size=0.4, random_state=42, shuffle=True)
            model = xgb.XGBRegressor(objective="reg:squarederror",
                                     n_estimators=100,max_depth=6,
                                     learning_rate=0.1,subsample=0.8,
                                     colsample_bytree=0.8,
                                     random_state=42,
                                     enable_categorical=True)
            model.fit(X_train, y_train)
            joblib.dump(model3, "models/model_sec2.pkl")

    def train_all_models(self):
    """Train and save all three models in one command."""
        print("Training all models...")
        self.train_model_pre()
        self.train_model_sec1()
        self.train_model_sec2()
        print("✅ All 3 models trained and saved!")

