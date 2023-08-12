import logging
from sqlalchemy import create_engine
import pandas

class Postgres:
    def __init__(self, db_params):
        self.db_params = db_params
    
        
    def create_postgres_connection(self):
        try:
            logging.info("Creating connection to PostgreSQL ... ")
            conn_url = "postgresql://{}:{}@{}:{}/{}".format(
                self.db_params["user"],
                self.db_params["password"],
                self.db_params["host"],
                self.db_params["port"],
                self.db_params["database"]
            )

            db = create_engine(conn_url)
            conn = db.connect()
            logging.info("Connection created succesfully.")

            return conn
        except Exception as e:
            logging.error("Error while connecting to PostgreSQL:", e)
        finally:
            pass
    
    def insert_to_postgres(self, data, table_name, if_exists="append"):
        try:
            conn = self.create_postgres_connection()
            logging.info("Inserting data to PostgreSQL ...")
            data.to_sql(
                table_name,
                con = conn,
                if_exists = if_exists,
                index = False
            )
            logging.info("Data inserted successfully.")
        except Exception as e:
            print("Error while inserting data to PostgreSQL:", e)
        finally:
            if conn:
                conn.close()

    def get_data_from_postgres(self, qry):
        try:
            conn = self.create_postgres_connection()
            logging.info("Fetching data from PostgreSQL")
            df = pandas.read_sql(
                qry,
                conn
            )
            logging.info("Successfully fetched data.")

            return df 
        except Exception as e:
            logging.error("Failed to fetch data from PostgreSQL: ", e)
        finally:
            if conn:
                conn.close()