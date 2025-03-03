import pandas as pd
from ydata_profiling import ProfileReport
from sklearn.preprocessing import LabelEncoder


class eda:
    def __init__(self, df):
        self.df = df  # Store the DataFrame passed during initialization

    def profile_data(self):
        profile = ProfileReport(self.df, title="EDA Report")
        profile.to_notebook_iframe()
        profile.to_file("EDA.html")

    def drop_columns(self, cols=None):
        if cols is None:
            cols = ['Time', 'DriverNumber', 'Deleted', 'FreshTyre','SpeedFL','SpeedI1', 'SpeedI2', 'SpeedST', 'Sector1Time', 'Sector2Time','Sector3Time','IsPersonalBest']
        self.df.drop(columns=cols, errors="ignore", inplace=True)

    def fill_nulls(self):
        LDF = self.df
        cols = LDF.select_dtypes(include=['number']).columns  # Only process numeric columns
    
        for col in cols:
            # Check if the column has more than 9 null values
            if LDF[col].isnull().sum() > 9:
                # Apply grouped interpolation with polynomial order=2
                LDF[col] = LDF.groupby('Driver')[col].transform(
                    lambda x: x.interpolate(method='polynomial', order=2) 
                              if len(x.dropna()) >= 3 
                              else x.fillna(x.mean())
                )
    
                # If there are still nulls (e.g., entire group is null), fill with the overall column mean
                if LDF[col].isnull().sum() > 0:
                    LDF[col] = LDF[col].fillna(LDF[col].mean())
    
            else:
                # For columns with <= 9 nulls, fill with the overall column mean
                LDF[col] = LDF[col].fillna(LDF[col].mean())
    
        # Update the DataFrame in the object
            self.df = LDF

    def compound_checker(self):
        valid_compounds = ['SOFT', 'MEDIUM', 'HARD', 'INTERMEDIATE']
        self.df.loc[~self.df['Compound'].isin(valid_compounds), 'Compound'] = 'SOFT'

    def labelencoder(self):
        df=self.df
        from sklearn.preprocessing import LabelEncoder
        # Initialize label encoders
        team_encoder = LabelEncoder()
        driver_encoder = LabelEncoder()
        race_encoder = LabelEncoder()
        tyre_encoder=LabelEncoder()
        ispersonalbest_encoder= LabelEncoder()
        rainfall=LabelEncoder()
        df["Driver"] = driver_encoder.fit_transform(df["Driver"])
        df["Compound"] = tyre_encoder.fit_transform(df["Compound"])
        df["Team"] = team_encoder.fit_transform(df["Team"])
        df["gp_name"] = race_encoder.fit_transform(df["gp_name"])
        df["Rainfall"] = rainfall.fit_transform(df["Rainfall"])

        df=self.df

    def process(self):
        self.drop_columns()  # Drop specific columns
        self.fill_nulls()  # Fill missing values
        self.compound_checker()  # Check and fix compound values
        self.labelencoder()
        return self.df


