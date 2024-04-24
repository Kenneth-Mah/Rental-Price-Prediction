import pandas as pd
import requests

from airflow.decorators import dag, task
from datetime import datetime

import os

import xml.etree.ElementTree as ET

from pyproj import Proj, transform

import pandas_gbq
from google.oauth2 import service_account


@dag(
    dag_id="bus_stops_taskflow",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["project"],
)
def bus_stops_taskflow():

    ##extract
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
        transformed_bus_stops = pd.DataFrame(bus_stops)
        return transformed_bus_stops

    ##load
    @task
    def load_bus_stops(transformed_bus_stops):
        service_account_key_file_name = os.getenv("SERVICE_ACCOUNT_KEY_FILE_NAME")
        airflow_project_directory = os.getenv("AIRFLOW_HOME")

        service_account_key_file_path = (
            airflow_project_directory + "/auth/" + service_account_key_file_name
        )

        credentials = service_account.Credentials.from_service_account_file(
            service_account_key_file_path
        )

        pandas_gbq.to_gbq(
            transformed_bus_stops,
            "rental-price-prediction.ura_data.bus_stops",
            project_id="rental-price-prediction",
            if_exists="replace",
            credentials=credentials,
        )

    ##extract tasks
    bus_stops_file_path = extract_bus_stops()

    ##transform tasks
    transformed_bus_stops = transform_bus_stops(bus_stops_file_path)

    ##load tasks
    load_bus_stops(transformed_bus_stops)


##call dag
bus_stops_dag = bus_stops_taskflow()
