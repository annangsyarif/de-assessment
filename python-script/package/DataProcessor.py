import pandas
import logging

class DataProcessor:
    def __init__(self, csv_filepath):
        self.csv_filepath = csv_filepath
        self

    def read_csv(self):
        logging.info(f"Read data from {self.csv_filepath}")
        data = pandas.read_csv(self.csv_filepath)
        
        return data
    
    def aggregate_1(self, emp):
        data = self.read_csv()

        logging.info("Processing data ...")
        df = pandas.merge(data, emp, left_on=["employee_id"], right_on=["employe_id"], how="left")
        df["year"] = pandas.to_datetime(df["date"]).dt.year
        df["month"] = pandas.to_datetime(df["date"]).dt.month
        df["day"] = pandas.to_datetime(df["date"]).dt.day
        df["total_hour"] = (pandas.to_datetime(df["checkout"]).dt.hour - pandas.to_datetime(df["checkin"]).dt.hour).fillna(8)
        data = (df[["year", "month", "day", "branch_id", "total_hour"]].groupby(["year", "month", "day", "branch_id"]).sum()).reset_index()

        logging.info("Data successfully processed.")
        return data
