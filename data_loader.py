import pandas as pd
import fastf1 as f1
import matplotlib.pyplot as plt
import numpy as np

class F1dataloader:
    def __init__(self, start_year, stop_year):
        self.start_year = start_year
        self.stop_year = stop_year
        self.data = None
        self.laps = None
        self.weather = pd.DataFrame()
        self.D1 = None
        self.D2 = None
        self.D3 = None

    def load_data(self, year, race_name, session_type):
        """Load race data for a specific year, race, and session type."""
        try:
            session = f1.get_session(year, race_name, session_type)
            session.load(weather=True)
            results = session.results
            results['year'] = year

            # Load weather and laps data
            weather = session.weather_data
            WDF = pd.DataFrame(weather)
            laps = session.laps
            LDF = pd.DataFrame(laps)
            LDF['Year'] = year  # Append year for uniqueness

            # Debugging prints
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
        """Process qualifying data for a specific race."""
        RDF = self.load_data(year, race_name, 'Q')
        WDF = self.weather.copy()
        LDF = self.laps.copy()

        if RDF is None or LDF is None or WDF.empty:
            print(f"Skipping {race_name} {year} due to missing data.")
            return None

        # Skip races with "season" in gp_name

        if RDF['gp_name'].str.contains("season", case=False, na=False).any():
            return None

        # Validate data
        if RDF.empty or LDF.empty or WDF.empty:
            print(f"No data available for {race_name} {year}.")
            return None

        # Drop unnecessary columns
        drop_columns = [
            'LapNumber', 'Stint', 'PitOutTime', 'PitInTime', 'LapStartTime',
            'LapStartDate', 'TrackStatus', 'Sector1SessionTime', 'Sector2SessionTime',
            'Sector3SessionTime', 'DeletedReason', 'FastF1Generated', 'IsAccurate',
            'Position', 'DriverNumber', 'Deleted', 'FreshTyre', 'SpeedFL', 'SpeedI1',
            'SpeedI2', 'SpeedST', 'IsPersonalBest'
        ]

        # Create three datasets with different sector time configurations
        D1 = LDF.drop(columns=drop_columns + ['Sector1Time', 'Sector2Time', 'Sector3Time'])
        D2 = LDF.drop(columns=drop_columns + ['Sector2Time', 'Sector3Time'])
        D3 = LDF.drop(columns=drop_columns + ['Sector3Time'])


       # Process each dataset

        processed_datasets = []
        for df in [D1, D2, D3]:
            # Ensure LapTime is in timedelta format
            df['LapTime'] = pd.to_timedelta(df['LapTime'], errors='coerce')
            df = df[df['LapTime'].notna()]  # Drop rows with NaT in LapTime

            # Convert LapTime to total seconds

            df['LapTime'] = df['LapTime'].dt.total_seconds().astype(float).round(3)
            # Drop duplicates
            df.drop_duplicates(subset=['Time'], keep='first', inplace=True)
            # Merge with weather data
            df = df.sort_values(by='Time')
            WDF = WDF.sort_values(by='Time')
            try:
             GDF= pd.read_csv('gp_name,.csv')  # Ensure this  exists
             df = pd.merge(df,GDF, on='gp_name', how='left')
             df = pd.merge_asof(df,WDF, on='Time', direction='nearest')
            except FileNotFoundError:
                print("GP name mapping file not found. Skipping merge.")
            # Final sorting and categorization
            df['gp_name'] = pd.Categorical(df['gp_name'], categories=df['gp_name'].unique(), ordered=True)
            df = df.sort_values(by=['gp_name', 'Driver', 'LapTime']).reset_index(drop=True)
            # Debugging: Check intermediate results
            print(f"Processed DataFrame shape: {df.shape}")
            # Append processed DataFrame to the list
            processed_datasets.append(df)
        # Apply 75th percentile lap time filter to all datasets
        filtered_datasets = []
        for df in processed_datasets:
            lapthresh = df.groupby('Driver')['LapTime'].quantile(0.75)
            df = df[df['LapTime'] <= df['Driver'].map(lapthresh)]
            filtered_datasets.append(df)
        # Debugging: Check final results
        print(f"Final shapes after filtering: {[df.shape for df in filtered_datasets]}")
        return filtered_datasets
    def load_year_data(self):
        """Load and process data for all races in the specified year range."""
        Pre, sec1, sec2 = [], [], []  # Lists to store D1, D2, and D3 datasets
        for year in range(self.start_year, self.stop_year + 1):
            schedule = f1.get_event_schedule(year)
            race_names = [name for name in schedule['EventName'] if "Pre-Season Testing" not in name]
            for race in race_names:
                datasets = self.process_data(year, race)  # Get datasets for the race
                # Check if datasets is valid and contains exactly 3 elements
                if datasets is not None and len(datasets) == 3:
                    D1, D2, D3 = datasets  # Unpack datasets
                    Pre.append(D1)   # Append D1 to Pre list
                    sec1.append(D2)  # Append D2 to sec1 list
                    sec2.append(D3)  # Append D3 to sec2 list
                    print(f"Loaded qualifying data for {race} {year}")
                else:
                    print(f"No valid data for {race} {year}. Skipping.")

        # Concatenate all datasets for each type, if the lists are not empty
        self.D1 = pd.concat(Pre, ignore_index=True) if Pre else None
        self.D2 = pd.concat(sec1, ignore_index=True) if sec1 else None
        self.D3 = pd.concat(sec2, ignore_index=True) if sec2 else None
        # Print final merged data
        if self.D1 is not None:
            print("Final merged data for D1:")
            print(self.D1.head())
        if self.D2 is not None:
            print("Final merged data for D2:")
            print(self.D2.head())
        if self.D3 is not None:
            print("Final merged data for D3:")
            print(self.D3.head())

        # Save datasets to pickle files
        if self.D1 is not None:
            self.D1.to_pickle('Pre.pkl')
        if self.D2 is not None:
            self.D2.to_pickle('sec1.pkl')
        if self.D3 is not None:
            self.D3.to_pickle('sec2.pkl')
        if self.D1 is None and self.D2 is None and self.D3 is None:
            print("No data available")

        return self.D1, self.D2, self.D3

    def get_clean_data(self):
        """Return the processed datasets."""
        if self.D1 is None or self.D2 is None or self.D3 is None:
            print("No data loaded yet.")
            return None
        return self.D1, self.D2, self.D3

    def get_weather_data(self):
        if self.weather.empty:
            print('No weather data loaded yet.')
            return None
        return self.weather

    def get_results_data(self):
        if self.laps is None:
            print('No laps data loaded yet.')
            return None
        return self.laps
