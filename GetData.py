import datetime
import io
import os
import pdb
import sys
import pytz
import pyodbc
import numpy as np
import pandas as pd


class GetData:

 def __init__(self):
  pass

 def get_difference_to_utc(self, timezone):
  timezones = {
   "CST": "-06:00",
   "CDT": "-05:00",
   "PST": "-08:00",
   "PDT": "-07:00"
  }
  return timezones[timezone]

 def get_index_and_dates(self, now, timezone, n_days):
  dsls = (now.weekday()+1) % 7
  hours = (n_days * 24) - 1
  now = now.astimezone(pytz.timezone(timezone))
  now = pd.to_datetime(now)
  sdatetime = now.normalize() - pd.Timedelta(dsls+n_days-1, unit="d")
  edatetime = sdatetime + pd.Timedelta(hours, unit="h")
  index = pd.date_range(start=sdatetime, end=edatetime, freq="1h", inclusive="both")
  dates = np.unique(index.date).tolist()
  return index, dates

 def get_enertel_forecast_for_ERCOT_from_s3(self, s3_bucket, directory, patterns, date, timezone, creation_hour):
  year = date.year.__str__().zfill(4)
  month = date.month.__str__().zfill(2)
  day = date.day.__str__().zfill(2)
  start = datetime.datetime.combine(date, datetime.datetime.min.time())
  end = start + datetime.timedelta(days=1) - datetime.timedelta(hours=1)
  index = pd.date_range(start=start, end=end, freq="h", inclusive="both", tz=timezone)
  df = pd.DataFrame(index=index, columns=["value"], data=np.nan).astype(np.float64)
  for pattern in patterns:
   prefix = f"{directory}/{year}/{month}/{day}/{pattern}"
   files = s3_bucket.objects.filter(Prefix=prefix)
   file = [file.key for file in files if file.last_modified.astimezone(pytz.timezone(timezone)).hour == creation_hour]
   if not file: continue
   file = file[-1]
   response = s3_bucket.Object(file).get()
   content = response["Body"].read().decode("utf-8")
   df1 = pd.read_csv(io.StringIO(content))
   df1.sort_values(by=["timestamp", "batch_id"], inplace=True)
   df1.drop_duplicates(subset=["timestamp"], keep="last", inplace=True)
   df1.index = pd.to_datetime(df1["timestamp"])
   df1.index.name = None
   df1 = df1.loc[:, ["p50"]].copy()
   df.update(df1.rename(columns={"p50": "value"}))
   del df1
  return df

 def get_enertel_forecast_for_CAISO_from_s3(self, s3_bucket, directory, patterns, date, timezone, creation_hour):
  year = date.year.__str__().zfill(4)
  month = date.month.__str__().zfill(2)
  day = date.day.__str__().zfill(2)
  start = datetime.datetime.combine(date, datetime.datetime.min.time())
  end = start + datetime.timedelta(days=1) - datetime.timedelta(hours=1)
  index = pd.date_range(start=start, end=end, freq="h", inclusive="both", tz=timezone)
  df = pd.DataFrame(index=index, columns=["value"], data=np.nan).astype(np.float64)
  for pattern in patterns:
   prefix = f"{directory}/{year}/{month}/{day}/{pattern}"
   files = s3_bucket.objects.filter(Prefix=prefix)
   file = [file.key for file in files if file.last_modified.astimezone(pytz.timezone(timezone)).hour == creation_hour]
   if not file: continue
   file = file[-1]
   response = s3_bucket.Object(file).get()
   content = response["Body"].read().decode("utf-8")
   df1 = pd.read_csv(io.StringIO(content))
   df1.sort_values(by=["timestamp", "batch_id"], inplace=True)
   df1.drop_duplicates(subset=["timestamp"], keep="last", inplace=True)
   df1.index = pd.to_datetime(df1["timestamp"])
   df1.index.name = None
   df1 = df1.loc[:, ["p50"]].copy()
   df.update(df1.rename(columns={"p50": "value"}))
   del df1
  return df

 def get_actual_rtlmp_from_dremio(self, driver, host, port, uid, pwd, timezone, table, objectid, sdatetime, edatetime):
  connection = pyodbc.connect(f"Driver={driver};ConnectionType=Direct;HOST={host};PORT={port};AuthenticationType=Plain;UID={uid};PWD={pwd};SSL=1;DisableCertificateVerification=1", autocommit=True)
  cursor = connection.cursor()
  query = f"SELECT DATETIME, TIMEZONE, RTLMP FROM Core.Preparation.S3.Team_Source_Yesenergy.{table}.Prices.LMP.Hourly WHERE OBJECTID = {objectid} AND DATETIME BETWEEN '{sdatetime}' AND '{edatetime}'"
  cursor.execute(query)
  records = cursor.fetchall()
  connection.close()
  columns = [column[0] for column in cursor.description]
  records = [dict(zip(columns, row)) for row in records]
  df = pd.DataFrame(records)
  for index, row in df.iterrows():
   datetime1 = row["DATETIME"].strftime("%Y-%m-%dT%H:%M:%S")+self.get_difference_to_utc(row["TIMEZONE"])
   datetime2 = pd.to_datetime(datetime1).tz_convert(pytz.timezone(timezone))
   df.loc[index, "DATETIME"] = datetime2
  df.sort_values(by=["DATETIME"], inplace=True)
  df.index = df["DATETIME"]
  df.index.name = None
  df = df.loc[:, ["RTLMP"]].copy()
  df.rename(columns={"RTLMP": "value"}, inplace=True)
  return df

 def get_actual_dalmp_from_dremio(self, driver, host, port, uid, pwd, timezone, table, objectid, sdatetime, edatetime):
  connection = pyodbc.connect(f"Driver={driver};ConnectionType=Direct;HOST={host};PORT={port};AuthenticationType=Plain;UID={uid};PWD={pwd};SSL=1;DisableCertificateVerification=1", autocommit=True)
  cursor = connection.cursor()
  query = f"SELECT DATETIME, TIMEZONE, DALMP FROM Core.Preparation.S3.Team_Source_Yesenergy.{table}.Prices.LMP.Hourly WHERE OBJECTID = {objectid} AND DATETIME BETWEEN '{sdatetime}' AND '{edatetime}'"
  cursor.execute(query)
  records = cursor.fetchall()
  connection.close()
  columns = [column[0] for column in cursor.description]
  records = [dict(zip(columns, row)) for row in records]
  df = pd.DataFrame(records)
  for index, row in df.iterrows():
   datetime1 = row["DATETIME"].strftime("%Y-%m-%dT%H:%M:%S")+self.get_difference_to_utc(row["TIMEZONE"])
   datetime2 = pd.to_datetime(datetime1).tz_convert(pytz.timezone(timezone))
   df.loc[index, "DATETIME"] = datetime2
  df.sort_values(by=["DATETIME"], inplace=True)
  df.index = df["DATETIME"]
  df.index.name = None
  df = df.loc[:, ["DALMP"]].copy()
  df.rename(columns={"DALMP": "value"}, inplace=True)
  return df