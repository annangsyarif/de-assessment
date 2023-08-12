# de-assessment
## Disclaimer
Some assumptions for the python task in this project:
* daily data will be placed in the following folder based on date
  ```
    python-script
    |  
    └───input-data
        |
        └───year
            |
            └───month
                |
                └───day
                      timesheets.csv
  ```
* only `timesheets` data will update daily with `append` mode
* aggregating is only done up to the `daily_total_hours_per_branch`, because the data will be strange if it is done up to the `daily_average_salary_per_branch`
* this function can be implement using `Apache Ariflow` or another Orchestrator

## Configuration
Config for this project `python-script` can be found at [here](https://github.com/annangsyarif/de-assessment/blob/main/python-script/config/config.conf).
>**_Note_** : Make sure you adjust the configuration before running
```conf
{
    databases {
        postgres {
            host = "localhost"
            port = 5432
            user = "postgres"
            password = "admin"
        }
    }

    timesheets {
        database = "postgres"
        table_name = "timesheets"
        filepath = "input-data/%d/%02d/%02d/timesheets.csv"
    }

    aggregate {
        database = "postgres"
        table_name = "daily_total_hour_per_branch"
        employee_table_qry = "select * from employees where resign_date is null"
    }
}
```
## Developing
Before running this project, make sure you have created table employees and timesheets with the following DDL and inserted the [data](https://github.com/annangsyarif/de-assessment/tree/main/data)
```SQL
CREATE TABLE employees (
	employe_id int4 NULL,
	branch_id int4 NULL,
	salary int4 NULL,
	join_date date NULL,
	resign_date date NULL
);

CREATE TABLE timesheets (
	timesheet_id int4 NOT NULL,
	employee_id int4 NULL,
	"date" date NULL,
	checkin time NULL,
	checkout time NULL
);
```
```shell
# Cloning this repository
$ git clone git@github.com:annangsyarif/de-assessment.git

# Install requirements using pip
$ pip install -r requirements.txt

# Unit test
$ python3 -W ignore -m unittest discover -s python-script/package -p "*UnitTest.py"

# Running the program
$ python3 -W ignore python-script/main.py
```
