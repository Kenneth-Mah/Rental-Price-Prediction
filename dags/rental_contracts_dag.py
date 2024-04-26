import pandas as pd
import requests

from airflow.decorators import dag, task
from datetime import datetime

import os, calendar, re
from datetime import datetime, timedelta

import xml.etree.ElementTree as ET

from pyproj import Proj, transform, Geod

import pandas_gbq
from google.oauth2 import service_account


# Cron expression to trigger on the 20th of every month
# Cron breakdown:
# - Minute: 0
# - Hour: 0
# - Day of Month: 20
# - Month: * (every month)
# - Day of Week: * (any day of the week)
cron_schedule = "0 0 20 * *"


@dag(
    dag_id="rental_contracts_taskflow",
    schedule=cron_schedule,
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
    
    @task
    def extract_bus_stops():
        url = "https://www.lta.gov.sg/map/busService/bus_stops.xml"
        response = requests.get(url)
        bus_stops_file_path = "outputs/bus_stops.xml"
        with open(bus_stops_file_path, "wb") as f:
            f.write(response.content)
            print("File downloaded successfully")
        return bus_stops_file_path

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
    
    @task
    def transform_bus_stops(bus_stops_file_path):
        # Load and parse the XML file
        tree = ET.parse(bus_stops_file_path)
        root = tree.getroot()

        # Prepare a list to hold all bus stop data
        bus_stops = []

        # Define the projection for SVY21
        svy21_proj = Proj(init="epsg:3414")  # EPSG code for SVY21
        # Define the projection for WGS84
        wgs84_proj = Proj(init="epsg:4326")  # EPSG code for WGS84

        # Convert from WGS84 to SVY21
        def wgs84_to_svy21(latitude, longitude):
            easting, northing = transform(wgs84_proj, svy21_proj, longitude, latitude)
            return northing, easting

        # Convert from SVY21 to WGS84
        # def svy21_to_wgs84(northing, easting):
        #     lon, lat = transform(svy21_proj, wgs84_proj, easting, northing)
        #     return lat, lon

        # Iterate through each bus stop in the XML
        for bus_stop in root.findall(".//busstop"):
            # print(bus_stop)
            # Extract elements from each BusStop
            bus_stop_id = (
                bus_stop.find("details").text
                if bus_stop.find("details") is not None
                else None
            )
            bus_stop_cor = (
                bus_stop.find("coordinates")
                if bus_stop.find("coordinates") is not None
                else None
            )
            # print(type(bus_stop。or))
            # print(bus_stop_cor)
            latitude = (
                bus_stop_cor.find("lat").text
                if bus_stop_cor.find("lat") is not None
                else None
            )
            longitude = (
                bus_stop_cor.find("long").text
                if bus_stop_cor.find("long") is not None
                else None
            )

            x, y = wgs84_to_svy21(float(latitude), float(longitude))
            # Append to list
            bus_stops.append(
                {
                    "BusStopID": bus_stop_id,
                    # 'BusStopCode': bus_stop_code,
                    # 'Description': description,
                    "Latitude": latitude,
                    "Longitude": longitude,
                    "x": x,
                    "y": y,
                }
            )

        # Convert list to DataFrame
        df = pd.DataFrame(bus_stops)
        
        # remove same detail bus stop
        df = df.drop_duplicates(subset=['BusStopID'])
        
        # reorder index
        transformed_bus_stops  = df.reset_index(drop=True)
        return transformed_bus_stops
    
    @task
    def calculate_bus_stops(transformed_rental_contracts, transformed_bus_stops):
        # Initialize the SVY21 projection
        svy21_proj = Proj(init='EPSG:3414')  # EPSG code for SVY21

        # Create a geodesic object to calculate distance
        geod = Geod(ellps='WGS84')

        # Convert SVY21 coordinates to latitude and longitude
        def calculate_distance(x1, y1, lon2, lat2):
            lon1, lat1 = svy21_proj(x1, y1, inverse=True)

            # Calculate distance
            angle1, angle2, distance = geod.inv(lon1, lat1, lon2, lat2)

            return distance
        
        # calculate distance between bus stops and rental contracts
        distance_dict = {}
        for index, row in transformed_rental_contracts.iterrows():
            num_bus_stops = 0
            x1, y1 = row['x'], row['y']
            # check if x,y has been calculated before
            if (x1, y1) in distance_dict:
                num_bus_stops = distance_dict[(x1, y1)]
            else:
                for index2, row2 in transformed_bus_stops.iterrows():
                    lon2, lat2 = row2['Longitude'], row2['Latitude']
                    distance = calculate_distance(x1, y1, lon2, lat2)
                    if distance < 300: # if the distance is less than 300m
                        num_bus_stops += 1
                
                # save calculated result in table for faster access
                distance_dict[(x1, y1)] = num_bus_stops
            # add a new column to the transformed_rental_contracts with the distance between the bus stop and the rental contract
            transformed_rental_contracts.at[index, 'num_bus_stops'] = num_bus_stops
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
    bus_stops_file_path = extract_bus_stops()

    ##transform tasks
    transformed_rental_contracts = transform_rental_contracts(rental_contracts)
    transformed_bus_stops = transform_bus_stops(bus_stops_file_path)
    rental_contracts_with_bus_stops = calculate_bus_stops(transformed_rental_contracts, transformed_bus_stops)

    ##load tasks
    load_rental_contracts(rental_contracts_with_bus_stops)


##call dag
rental_contracts_dag = rental_contracts_taskflow()
