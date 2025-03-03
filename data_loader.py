import pandas as pd
import fastf1 as f1
import matplotlib.pyplot as plt
import numpy as np

# Loading The Data
class F1dataloader:
    def __init__(self, start_year, stop_year):
        self.start_year = start_year
        self.stop_year = stop_year
        self.data = None
        self.laps = None
        self.weather = pd.DataFrame()

    def load_data(self, year, race_name, session_type):
        
        try:
            session = f1.get_session(year, race_name,session_type)
            session.load(weather=True)
            results = session.results
            results['year'] = year
            results['gp_name'] = f"{race_name}_{year}"  # Append year for other races
        
        # Loading weather and laps
            weather = session.weather_data
            WDF = pd.DataFrame(weather)
            laps = session.laps
            LDF = pd.DataFrame(laps)

        # Add gp_name to the laps DataFrame (so we can merge it later)
            LDF['gp_name'] = race_name

        # Debugging prints to check if laps and weather data are being loaded
            print(f"Laps data for {race_name} {year}:")
            print(LDF.head())
        
            print(f"Weather data for {race_name} {year}:")
            print(WDF.head())

        # Store laps and weather data as attributes
            self.laps = LDF
            self.weather = WDF

            return LDF
        except Exception as e:
            print(f"Failed to load {race_name} {year}: {e}")
            return None

    def process_data(self, year, race_name):
        # Load data for the race
        RDF=self.load_data(year, race_name,'Q')
        WDF=self.weather.copy()
        LDF=self.laps.copy()

        if RDF is None or LDF is None or WDF is None:
            print(f"Skipping {race_name} {year} due to missing data.")
            return None

        # Validate data
        if RDF.empty or LDF.empty or WDF.empty:
            print(f"No data available for {race_name} {year}.")
            return Nones

        LDF.drop(columns=['LapNumber', 'Stint','PitOutTime', 'PitInTime', 'LapStartTime', 'LapStartDate', 'TrackStatus',
                              'Sector1SessionTime', 'Sector2SessionTime', 'Sector3SessionTime',
                              'DeletedReason', 'FastF1Generated', 'IsAccurate','Position', 'DriverNumber', 'Deleted', 'FreshTyre',
                              'SpeedFL','SpeedI1', 'SpeedI2', 'SpeedST', 
                              'Sector1Time', 'Sector2Time','Sector3Time','IsPersonalBest'], inplace=True)

        # Process lap data
        LDF['LapTime'] = LDF['LapTime'].dt.total_seconds().astype(float).round(3)
        LDF = LDF[LDF['LapTime'].notna()]
        print(LDF['LapTime'])
        # Drop duplicates
        LDF.drop_duplicates(subset=['Time'], keep='first', inplace=True)

        # Debugging: Check intermediate results
        print("LDF after merging lap data:", LDF.shape)


        # Drop rows with null Time values

        if LDF.empty or WDF.empty:
            print(f"No valid rows after dropping nulls for {race_name} {year}.")
            return None

        # Sort by Time for merge_asof
        LDF = LDF.sort_values(by='Time')
        WDF = WDF.sort_values(by='Time')
        GDF = pd.read_csv('gp_name.csv')

        # Merge RDF with weather data (WDF) based on Time
        lapthresh=LDF.groupby('Driver')['LapTime'].quantile(0.75)
        LDF = LDF[LDF.apply(lambda x:x['LapTime'] <= lapthresh[x['Driver']], axis=1)]
        LDF = pd.merge_asof(LDF,WDF,on='Time',direction='nearest')
        LDF = pd.merge(LDF,GDF, on = 'gp_name', how = 'left')
        LDF = LDF.sort_values(by='LapTime')
        LDF['gp_name']=pd.Categorical(LDF['gp_name'], categories=LDF['gp_name'].unique(), ordered=True)
        LDF = LDF.sort_values(by=['gp_name','DriverNumber','LapTime']).reset_index(drop=True)

        # Debugging: Check final result
        print("Final RDF after merging weather data:", LDF.shape)

        return LDF

    def load_year_data(self):
        all_data = []
        for year in range(self.start_year, self.stop_year + 1):
            schedule = f1.get_event_schedule(year)
            race_names = [name for name in schedule['EventName'] if "Pre-Season Testing" not in name]
            for race in race_names:                   
                LDF = self.process_data(year, race)
                if LDF is not None:
                    all_data.append(LDF)
                    print(f"Loaded qualifying data for {race} {year}")
                else:
                    print(f"No valid data for {race} {year}. Skipping.")

        # Concatenate all valid data
        self.data = pd.concat(all_data, ignore_index=True)
        
        if self.data is not None:
            print("Final merged data:")
            print(self.data.head())
        else:
            print("No data available")

        return self.data

    def get_clean_data(self):
        if self.data is None:
            print('No data loaded yet.')
            return None
        else:
            return self.data

    def get_weather_data(self):
        if self.data is None:
            print('No data loaded yet.')
            return None
        else:
            return self.weather

    def get_results_data(self):
        if self.results is None:
            print('No data loaded yet.')
            return None
        else:
            return self.laps

    def save_data(self):
        if self.data is None:
            print('No data loaded yet.')
            return None
        else:
            self.data.to_pickle("dataset.pkl")
            print("SAVED")