from airflow import DAG
from airflow.decorators import task

from datetime import datetime
import requests
import csv
import json


default_args = {
    'schedule_interval': "", 
    "start_date": datetime(2021, 10, 18)
}

with DAG(dag_id="Car_accidents", default_args=default_args) as dag:
    @task
    def download_data(link):
        response = requests.get("https://data.bloomington.in.gov/dataset/117733fb-31cb-480a-8b30-fbf425a690cd/resource/8673744e-53f2-42d1-9d05-4e412bd55c94/download/monroe-county-crash-data2003-to-2015.csv")
        return {"text": response.text}
        
    
    link = "https://data.bloomington.in.gov/dataset/117733fb-31cb-480a-8b30-fbf425a690cd/resource/8673744e-53f2-42d1-9d05-4e412bd55c94/download/monroe-county-crash-data2003-to-2015.csv"
    
    @task
    def count_number_of_accidents(response):
        print(response)
        reader = csv.DictReader(iter(response['text'].split('\n')))
        accidents_per_year = {}
        for row in reader:
            accidents_per_year[row['Year']] = accidents_per_year.get(row['Year'], 0) + 1
        return accidents_per_year
    
    @task
    def print_result(accidents_per_year):
        for year, number in accidents_per_year.items():
            print(year, number)
    

    response = download_data(link)
    accidents_per_year = count_number_of_accidents(response)
    print_result(accidents_per_year)
