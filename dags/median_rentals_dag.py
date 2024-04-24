import pandas as pd
import requests

from airflow.decorators import dag, task
from datetime import datetime

import os

import pandas_gbq
from google.oauth2 import service_account


@dag(
    dag_id="median_rentals_taskflow",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["project"],
)
def median_rentals_taskflow():

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
    def extract_median_rentals(my_token):
        my_key = os.getenv("URA_KEY")
        headers = {"AccessKey": my_key, "Token": my_token, "User-Agent": "Mozilla/5.0"}
        response = requests.get(
            "https://www.ura.gov.sg/uraDataService/invokeUraDS?service=PMI_Resi_Rental_Median",
            headers=headers,
        )
        if response.status_code == 200:
            result = response.json()
        else:
            print("Unsuccessful")

        median_rentals = pd.DataFrame()
        for ele in result["Result"]:
            # print(ele.keys())
            curr_details = pd.DataFrame(
                ele["rentalMedian"]
            )  # refPeriod, psg75, median, psf25, district
            curr_details["street"] = ele["street"]
            curr_details["x"] = ele["x"]
            curr_details["y"] = ele["y"]
            curr_details["project"] = ele["project"]
            median_rentals = pd.concat([median_rentals, curr_details], axis=0)

        for col in ["psf75", "median", "psf25", "x", "y"]:
            median_rentals[col] = median_rentals[col].astype(float)

        median_rentals = median_rentals.reset_index(drop=True)
        return median_rentals

    ##transform
    @task
    def transform_median_rentals(median_rentals):
        for col in ["psf75", "median", "psf25", "x", "y"]:
            median_rentals[col] = median_rentals[col].astype(float)

        transformed_median_rentals = median_rentals.reset_index(drop=True)
        return transformed_median_rentals

    ##load
    @task
    def load_median_rentals(transformed_median_rentals):
        service_account_key_file_name = os.getenv("SERVICE_ACCOUNT_KEY_FILE_NAME")
        airflow_project_directory = os.getenv("AIRFLOW_HOME")

        service_account_key_file_path = (
            airflow_project_directory + "/auth/" + service_account_key_file_name
        )

        credentials = service_account.Credentials.from_service_account_file(
            service_account_key_file_path
        )

        pandas_gbq.to_gbq(
            transformed_median_rentals,
            "rental-price-prediction.ura_data.median_rentals",
            project_id="rental-price-prediction",
            if_exists="replace",
            credentials=credentials,
        )

    # extract tasks
    my_token = get_token()
    median_rentals = extract_median_rentals(my_token)

    ##transform tasks
    transformed_median_rentals = transform_median_rentals(median_rentals)

    ##load tasks
    load_median_rentals(transformed_median_rentals)


##call dag
median_rentals_dag = median_rentals_taskflow()
