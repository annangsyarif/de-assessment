from DataProcessor import DataProcessor
from Postgres import Postgres

import unittest
import csv
import logging

class TestCSVToPostgreSQL(unittest.TestCase):
    def setUp(self):
        self.emp_data = [
            ["employe_id", "branch_id", "salary", "join_date"],
            [1,3,7500000,"2018-08-23"],
            [2,4,7500000,"2017-08-23"],
            [3,3,5000000,"2019-08-23"]
        ]

        self.ts_data = [
            ["timesheet_id", "employee_id", "date", "checkin", "checkout"],
            [23907432,1,"2019-08-21","08:00:00","18:00:00"],
            [23907433,2,"2019-08-21","08:00:00","18:00:00"],
            [23907434,3,"2019-08-21","08:00:00","18:00:00"]
        ]

        self.csv_filepath = 'emp.csv'
        self.table_name = "unit_test"

        self.csv_ts_data = "ts.csv"

        # Create a test CSV file
        with open(self.csv_filepath, 'w', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerows(self.emp_data)
        
        with open(self.csv_ts_data, 'w', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerows(self.ts_data)

    def test_1_read_csv(self):
        print("Test case : ", self._testMethodName)
        try:
            data_processor = DataProcessor(self.csv_filepath)
            data = data_processor.read_csv()

            self.assertEqual(data.values.tolist(), self.emp_data[1:])
        except Exception as e:
            logging.error("Failed to read CSV: ", e)
        finally:
            pass
    
    def test_2_aggregate_csv(self):
        print("Test case : ", self._testMethodName)
        try:
            # get employee data
            emp_processor = DataProcessor(self.csv_filepath)
            emp_data = emp_processor.read_csv()

            # get timesheet data
            ts_processor = DataProcessor(self.csv_ts_data)

            # aggregating data
            aggregated = ts_processor.aggregate_1(emp_data)

            self.assertEqual(aggregated.values.tolist(), [[2019, 8, 21, 3, 20], [2019, 8, 21, 4, 10]])
        except Exception as e:
            logging.error("Failed aggregating data: ", e)
        finally:
            pass


    def tearDown(self):
        import os
        os.remove(self.csv_filepath)
        os.remove(self.csv_ts_data)


if __name__ == "__main__":
    unittest.main()