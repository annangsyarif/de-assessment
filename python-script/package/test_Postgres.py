import unittest
import csv
import logging

from package.Postgres import Postgres
from package.DataProcessor import DataProcessor

class TestPostgresConnection(unittest.TestCase):
    def setUp(self):
        self.db_params = {
            'user': 'postgres',
            'password': 'admin',
            'host': 'localhost',
            'port': 5432,
            'database': 'postgres'
        }

        self.test_data = [
            ["id", "name"],
            [1, "A"],
            [2, "B"]
        ]
        self.db_params = {
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "admin",
            "database": "postgres"
        }

        self.csv_filepath = 'test.csv'
        self.table_name = "unit_test"

        # Create a test CSV file
        with open(self.csv_filepath, 'w', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerows(self.test_data)
    
    def test_3_fetch_data_from_postgresql(self):
        print("Test case : ", self._testMethodName)
        try:
            # create connection
            postgres = Postgres(self.db_params)
            conn = postgres.create_postgres_connection()

            data = postgres.get_data_from_postgres(
                f"select * from {self.table_name}"
            )

            self.assertEqual(data.values.tolist(), self.test_data[1:])

            conn.execute(f"drop table if exists {self.table_name}")
        except Exception as e:
            logging.exception("Failed to fecth data from PostgreSQL: ", str(e))
        finally:
            if conn:
                conn.close()
    
    def test_2_insert_to_postgresql(self):
        print("Test case : ", self._testMethodName)
        try:
            # read data
            data_processor = DataProcessor(self.csv_filepath)
            data = data_processor.read_csv()

            # create connection
            postgres = Postgres(self.db_params)
            conn = postgres.create_postgres_connection()

            postgres.insert_to_postgres(data, self.table_name, if_exists="replace")

        except Exception as e:
            logging.exception("Error while inserting data to PostgreSQL: ", str(e))
        finally:
            if conn:
                conn.close()

    def test_1_postgres_connection(self):
        print("Test case : ", self._testMethodName)
        try:
            postgres = Postgres(self.db_params)
            conn = postgres.create_postgres_connection()
            self.assertIsNotNone(conn)
        except Exception as e:
            logging.exception("Failed to connect to PostgreSQL: " + str(e))
        finally:
            if conn:
                conn.close()
    
    
    def tearDown(self):
        import os
        os.remove(self.csv_filepath)


if __name__ == '__main__':
    unittest.main()
