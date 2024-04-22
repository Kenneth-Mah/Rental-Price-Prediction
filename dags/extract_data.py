import pandas as pd
import matplotlib.pyplot as plt
import requests
import json

from airflow.decorators import dag, task
from datetime import datetime

from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
import os, json

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
# from textwrap import wrap

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

@dag(
    dag_id='extract_data',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['project']
)
def project_taskflow():
    
    @task
    def get_token():
        my_key = os.getenv("URA_KEY")
        headers = {'AccessKey': my_key, 'User-Agent': 'Mozilla/5.0'}
        response = requests.get("https://www.ura.gov.sg/uraDataService/insertNewToken.action",
                             headers=headers, data=headers, json=headers)

        if response.status_code == 200:
            return response.json()["Result"]
        else:
            return "Error"


    @task
    def get_median_rental(my_token):
        my_key = os.getenv("URA_KEY")
        headers = {'AccessKey': my_key, "Token": my_token, 'User-Agent': 'Mozilla/5.0'}
        response = requests.get("https://www.ura.gov.sg/uraDataService/invokeUraDS?service=PMI_Resi_Rental_Median", headers=headers)
        if response.status_code == 200:
            result = response.json()
        else:
            print("Unsuccessful")
            
        median_rentals = pd.DataFrame()
        for ele in result["Result"]:
        # print(ele.keys())
            curr_details = pd.DataFrame(ele["rentalMedian"]) # refPeriod, psg75, median, psf25, district
            curr_details["street"] = ele["street"]
            curr_details["x"] = ele["x"]
            curr_details["y"] = ele["y"]
            curr_details["project"] = ele["project"]
            median_rentals = pd.concat([median_rentals, curr_details], axis=0)
        
        for col in ['psf75', 'median', 'psf25', 'x', 'y']:
            median_rentals[col] = median_rentals[col].astype(float)

        median_rentals = median_rentals.reset_index(drop=True)
        return median_rentals
    
    
    @task
    def write_rental_contracts(median_rentals, my_token):
        
        def get_rental_contracts_per_refPeriod(period, my_token):
            """period should be a string e.g. '2014Q1' means 2014 1st quarter"""
            refPeriod = period[2:].lower() # format should be like 14q1 instead of 2014Q1
            # refPeriod is mandatory parameter
            my_key = os.getenv("URA_KEY")
            headers = {'AccessKey': my_key, "Token": my_token, 'User-Agent': 'Mozilla/5.0'}
            response = requests.get(f"https://www.ura.gov.sg/uraDataService/invokeUraDS?service=PMI_Resi_Rental&refPeriod={refPeriod}", headers=headers)
            if response.status_code == 200:
                result = response.json()
            else:
                print("Unsuccessful")
            rental_contracts_per_refPeriod = pd.DataFrame()
            for ele in result["Result"]:
                curr_details = pd.DataFrame(ele["rental"]) # areaSqm	leaseDate	propertyType	district	areaSqft	noOfBedRoom	rent
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
                rental_contracts_per_refPeriod = pd.concat([rental_contracts_per_refPeriod, curr_details], axis=0)
            rental_contracts_per_refPeriod["refPeriod"] = period
            return rental_contracts_per_refPeriod.reset_index(drop=True)
        
        rental_contracts = pd.DataFrame()
        for period in median_rentals["refPeriod"].value_counts().index:
            print(period)
            rental_contracts = pd.concat([rental_contracts, get_rental_contracts_per_refPeriod(period, my_token)], axis=0)
        
        return rental_contracts
    
    @task
    def format_rental_contracts(rental_contracts):
        rental_contracts["noOfBedRoom"] = rental_contracts.apply(lambda row: -1 if row["noOfBedRoom"] == "NA" else row["noOfBedRoom"], axis=1)
        for col in ['rent', 'x', 'y']:
            rental_contracts[col] = rental_contracts[col].astype(float)

        for col in ['noOfBedRoom']:
            rental_contracts[col] = rental_contracts[col].astype(int)

        rental_contracts = rental_contracts.reset_index(drop=True)
        
        return rental_contracts
    
    @task
    def merge_rental_contracts_median_rentals(median_rentals, rental_contracts):
        merged_df = pd.merge(left=rental_contracts, right=median_rentals, how="left", on=["project", "street", "district", "x", "y", "refPeriod"])
        merged_df.to_csv("outputs/URA_data.csv", index=False)
        
        
    ##call tasks    
    my_token = get_token()
    median_rentals = get_median_rental(my_token)
    rental_contracts = write_rental_contracts(median_rentals, my_token)
    formatted_rental_contracts = format_rental_contracts(rental_contracts)
    merge_rental_contracts_median_rentals(median_rentals, formatted_rental_contracts)
    
     
##call dag   
project_dag = project_taskflow()