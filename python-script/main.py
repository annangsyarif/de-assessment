from package.DataProcessor import DataProcessor
from package.Postgres import Postgres

from datetime import datetime, timedelta
from pyhocon import ConfigFactory

def main():
    # parsing the config file
    conf = ConfigFactory.parse_file("config/config.conf")

    # create parameter to connect DB
    db_params = {
        "host": conf["databases.postgres.host"],
        "port": conf["databases.postgres.port"],
        "database": conf["timesheets.database"],
        "user": conf["databases.postgres.user"],
        "password": conf["databases.postgres.password"]
    }

    # define file path
    dt_now = datetime.now() - timedelta(days=1)
    csv_filepath = conf["timesheets.filepath"] % (
        dt_now.year,
        dt_now.month,
        dt_now.day
    )

    # create data processor
    data_processor = DataProcessor(csv_filepath)

    # read csv
    data = data_processor.read_csv()

    # create postgres connection
    postgres = Postgres(db_params)
    conn = postgres.create_postgres_connection()

    ####### insert timesheets to table #######
    postgres.insert_to_postgres(
        data,
        table_name = conf["timesheets.table_name"],
        if_exists = "append"
    )

    ####### transform data #######
    # get employee data
    emp_data = postgres.get_data_from_postgres(
        conf["aggregate.employee_table_qry"]
    )

    aggregate_data = data_processor.aggregate_1(
        emp_data
    )

    postgres.insert_to_postgres(
        aggregate_data,
        conf["aggregate.table_name"],
        if_exists = "append"
    )



if __name__ == "__main__":
    main()
