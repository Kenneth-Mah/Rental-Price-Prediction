import pandas as pd
import requests

from airflow.decorators import dag, task
from datetime import datetime

import os, calendar, re
from datetime import datetime, timedelta

import pandas_gbq
from google.oauth2 import service_account


@dag(
    dag_id="rental_contracts_taskflow",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["project"],
)
def rental_contracts_taskflow():

    ##extract
    @task
    def get_token():
        my_key = os.getenv("URA_KEY")
        headers = {"AccessKey": my_key, "User-Agent": "Mozilla/5.0"}
        response = requests.get(
            "https://www.ura.gov.sg/uraDataService/insertNewToken.action",
            headers=headers,
            data=headers,
            json=headers,
        )

        if response.status_code == 200:
            return response.json()["Result"]
        else:
            return "Error"

    @task
    def extract_rental_contracts(my_token):
        my_key = os.getenv("URA_KEY")
        today = datetime.today().date()
        latest_q = pd.Timestamp(today).quarter - 1
        c = calendar.Calendar(firstweekday=calendar.MONDAY)
        data_refresh_month = {1: 4, 2: 7, 3: 10, 4: 1}  # key=quarter, value=month

        year = today.year
        month = data_refresh_month[latest_q]

        monthcal = c.monthdatescalendar(year, month)
        fourth_friday = [
            day
            for week in monthcal
            for day in week
            if day.weekday() == calendar.FRIDAY and day.month == month
        ][3]
        if today > fourth_friday:
            lst_periods = list(
                str(x)
                for x in pd.period_range(
                    end=today - timedelta(days=91), periods=12, freq="Q"
                )
            )
        else:
            lst_periods = list(
                str(x)
                for x in pd.period_range(
                    end=today - timedelta(days=91 * 2), periods=12, freq="Q"
                )
            )

        # extract data from API
        def get_rental_contracts_per_refPeriod(period):
            """period should be a string e.g. '2014Q1' means 2014 1st quarter"""
            refPeriod = period[
                2:
            ].lower()  # format should be like 14q1 instead of 2014Q1
            # refPeriod is mandatory parameter
            headers = {
                "AccessKey": my_key,
                "Token": my_token,
                "User-Agent": "Mozilla/5.0",
            }
            response = requests.get(
                f"https://www.ura.gov.sg/uraDataService/invokeUraDS?service=PMI_Resi_Rental&refPeriod={refPeriod}",
                headers=headers,
            )
            if response.status_code == 200:
                result = response.json()
            else:
                print("Unsuccessful")
            rental_contracts_per_refPeriod = pd.DataFrame()
            for ele in result["Result"]:
                curr_details = pd.DataFrame(
                    ele["rental"]
                )  # areaSqm	leaseDate	propertyType	district	areaSqft	noOfBedRoom	rent
                curr_details["street"] = ele["street"]

                # some dont have x and y fields. Set to 0.
                try:
                    curr_details["x"] = float(ele["x"])
                except KeyError:
                    curr_details["x"] = 0
                try:
                    curr_details["y"] = float(ele["y"])
                except KeyError:
                    curr_details["y"] = 0
                curr_details["project"] = ele["project"]
                rental_contracts_per_refPeriod = pd.concat(
                    [rental_contracts_per_refPeriod, curr_details], axis=0
                )
            rental_contracts_per_refPeriod["refPeriod"] = period
            return rental_contracts_per_refPeriod.reset_index(drop=True)

        rental_contracts = pd.DataFrame()
        for period in lst_periods:
            print(period)
            rental_contracts = pd.concat(
                [rental_contracts, get_rental_contracts_per_refPeriod(period)], axis=0
            )
        return rental_contracts

    ##transform
    @task
    def transform_rental_contracts(rental_contracts):
        # fill NA noOfBedRoom with -1
        rental_contracts["noOfBedRoom"] = rental_contracts.apply(
            lambda row: -1 if row["noOfBedRoom"] == "NA" else row["noOfBedRoom"], axis=1
        )

        # casting the correct datatypes
        for col in ["rent", "x", "y"]:
            rental_contracts[col] = rental_contracts[col].astype(float)

        for col in ["noOfBedRoom"]:
            rental_contracts[col] = rental_contracts[col].astype(int)

        rental_contracts = rental_contracts.reset_index(drop=True)

        # areaSqft are numerical intervals that are consecutive and mostly equal
        # convert these to a single number (the midpoint of the interval) and use this as a quantitative variable
        # there are some unbounded intervals e.g. <=1000, >3000, >8000 which will be replaced with the boundary itself
        def format_area(s):
            try:
                lower, upper = s.split("-")
                lower = int(lower)
                upper = int(upper)
                return (lower + upper) / 2
            except ValueError:
                pattern = r"(^<|>=|<=|>)(\d+)"
                match = re.match(pattern, s)
                return float(match.group(2))

        rental_contracts["areaSqft_formatted"] = rental_contracts["areaSqft"].apply(
            format_area
        )

        # change leaseDate to datetime and extract year, quarter, month as new features
        rental_contracts["leaseDate"] = rental_contracts["leaseDate"].astype(int)
        rental_contracts["leaseDate"] = rental_contracts.apply(
            lambda row: datetime(int(row["refPeriod"][:4]), row["leaseDate"] // 100, 1),
            axis=1,
        )
        rental_contracts["leaseYear"] = rental_contracts["leaseDate"].dt.year
        rental_contracts["leaseQuarter"] = rental_contracts["leaseDate"].dt.quarter
        rental_contracts["leaseMonth"] = rental_contracts["leaseDate"].dt.month

        # Since the IQR is in per square feet, we will not use areaSqm.
        # Original areaSqft will also not be used
        transformed_rental_contracts = rental_contracts.drop(
            columns=["areaSqft", "areaSqm"]
        )
        return transformed_rental_contracts

    ##load
    @task
    def load_rental_contracts(transformed_rental_contracts):
        service_account_key_file_name = os.getenv("SERVICE_ACCOUNT_KEY_FILE_NAME")
        airflow_project_directory = os.getenv("AIRFLOW_HOME")

        service_account_key_file_path = (
            airflow_project_directory + "/auth/" + service_account_key_file_name
        )

        credentials = service_account.Credentials.from_service_account_file(
            service_account_key_file_path
        )

        pandas_gbq.to_gbq(
            transformed_rental_contracts,
            "rental-price-prediction.ura_data.rental_contracts",
            project_id="rental-price-prediction",
            if_exists="replace",
            credentials=credentials,
        )

    ##extract tasks
    my_token = get_token()
    rental_contracts = extract_rental_contracts(my_token)

    ##transform tasks
    transformed_rental_contracts = transform_rental_contracts(rental_contracts)

    ##load tasks
    load_rental_contracts(transformed_rental_contracts)


##call dag
rental_contracts_dag = rental_contracts_taskflow()
