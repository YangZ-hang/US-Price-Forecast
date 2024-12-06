import os
import pdb
import json
import boto3
import datetime
import numpy as np
import pandas as pd

from GetData import GetData

class DataFetcher:
    def __init__(self, config_path):
        with open(config_path, "r") as file:
            self.config = json.load(file)

        self.now = datetime.datetime.now()
        self.n_days = self.config["settings"]["n_days"]
        self.forecast_creation_hour = self.config["settings"]["forecast_creation_hour"]
        self.bucket = self.config["s3"]["bucket"]
        self.directory = self.config["s3"]["directory"]
        self.batteries = self.config["batteries"]
        self.driver = self.config["dremio"]["driver"]
        self.host = self.config["dremio"]["host"]
        self.port = self.config["dremio"]["port"]
        self.uid = os.environ["USERNAME"].upper()
        self.pwd = self.config["dremio"]["token"]

        self.gd = GetData()
        session = boto3.Session(profile_name="653768265138_App_FullDBA")
        self.s3_session = session.resource("s3")
        self.s3_bucket = self.s3_session.Bucket(self.bucket)

    def get_data(self):
        print(f"reading forecasts")
        forecast = dict()
        for battery in self.batteries:
            print(f" {battery}")
            timezone = self.config["timezones"][battery]
            function = self.config["forecast"][battery]["function"]
            patterns = self.config["forecast"][battery]["patterns"]
            index, dates = self.gd.get_index_and_dates(self.now, timezone, self.n_days)
            df = pd.DataFrame(index=index, columns=["value"], data=np.nan).astype(np.float64)
            func = getattr(self.gd, function)
            for date in dates:
                df1 = func(self.s3_bucket, self.directory, patterns, date, timezone, self.forecast_creation_hour)
                df.update(df1)
                del df1
            df.ffill(inplace=True)
            forecast[battery] = df
            del df

        print(f"reading rtlmp actuals")
        rtlmp = dict()
        for battery in self.batteries:
            print(f" {battery}")
            timezone = self.config["timezones"][battery]
            function = self.config["rtlmp"][battery]["function"]
            table = self.config["rtlmp"][battery]["table"]
            objectid = self.config["rtlmp"][battery]["objectid"]
            index, _ = self.gd.get_index_and_dates(self.now, timezone, self.n_days)
            sdatetime = index[0].strftime("%Y-%m-%d %H:%M:%S")
            edatetime = index[-1].strftime("%Y-%m-%d %H:%M:%S")
            df = pd.DataFrame(index=index, columns=["value"], data=np.nan).astype(np.float64)
            func = getattr(self.gd, function)
            df1 = func(self.driver, self.host, self.port, self.uid, self.pwd, timezone, table, objectid, sdatetime, edatetime)
            df.update(df1)
            del df1
            rtlmp[battery] = df
            del df

        print(f"reading dalmp actuals")
        dalmp = dict()
        for battery in self.batteries:
            print(f" {battery}")
            timezone = self.config["timezones"][battery]
            function = self.config["dalmp"][battery]["function"]
            table = self.config["rtlmp"][battery]["table"]
            objectid = self.config["dalmp"][battery]["objectid"]
            index, _ = self.gd.get_index_and_dates(self.now, timezone, self.n_days)
            sdatetime = index[0].strftime("%Y-%m-%d %H:%M:%S")
            edatetime = index[-1].strftime("%Y-%m-%d %H:%M:%S")
            df = pd.DataFrame(index=index, columns=["value"], data=np.nan).astype(np.float64)
            func = getattr(self.gd, function)
            df1 = func(self.driver, self.host, self.port, self.uid, self.pwd, timezone, table, objectid, sdatetime, edatetime)
            df.update(df1)
            del df1
            dalmp[battery] = df
            del df

        print(f"concatenating forecasts and actuals")
        data = dict()
        for battery in self.batteries:
            timezone = self.config["timezones"][battery]
            index, _ = self.gd.get_index_and_dates(self.now, timezone, self.n_days)
            df = pd.DataFrame(index=index, columns=["forecast", "rtlmp", "dalmp"], data=np.nan).astype(np.float64)
            df.update(forecast[battery].rename(columns={"value": "forecast"}))
            df.update(rtlmp[battery].rename(columns={"value": "rtlmp"}))
            df.update(dalmp[battery].rename(columns={"value": "dalmp"}))
            data[battery] = df
            del df

        return data
    
