from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import random
import zipfile
import os

import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin

def fetch_csv_links(url):
    # Fetch the webpage containing the list of CSV files
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for any HTTP error

    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all <a> tags containing CSV file links
    csv_links = soup.find_all('a', href=re.compile(r'.*\.csv$'))

    # Extract the href attribute from each <a> tag
    csv_file_links = [urljoin(url, link['href']) for link in csv_links]

    return csv_file_links

def select_files():
    # Randomly select a subset of CSV file links
    url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2020/' # We can change the year value here
    csv_file_links = fetch_csv_links(url)
    num_files_to_select = 25 							# This is also a variable
    selected_files = random.sample(csv_file_links, num_files_to_select)
    return selected_files
    
def zip_csv_files():
    # Path to the directory containing CSV files
    csv_directory = '/home/koushik/'

    # List all CSV files in the directory
    csv_files = [file for file in os.listdir(csv_directory) if file.endswith('.csv')]

    # Create a zip file and add CSV files to it
    with zipfile.ZipFile('/home/koushik/csv_archive.zip', 'w') as zipf:
        for file in csv_files:
            file_path = os.path.join(csv_directory, file)
            zipf.write(file_path, arcname=file)

#def zip_files(downloaded_files):
#    with zipfile.ZipFile('archive.zip', 'w') as zipf:
#        for file in downloaded_files:
#            zipf.write(file)
#    return 'archive.zip'

def move_archive():
    os.rename("csv_archive.zip", "/home/koushik/Documents/csv_archive.zip")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'data_fetch_pipeline',
    default_args=default_args,
    description='A DAG to fetch and process NOAA datasets',
    schedule_interval=None,
)

fetch_page = BashOperator(
    task_id='fetch_page',
    bash_command='wget -O page.html https://www.ncei.noaa.gov/data/local-climatological-data/access/2020/',
    dag=dag,
)

select_files_task = PythonOperator(
    task_id='select_files',
    python_callable=select_files,
    dag=dag,
)

fetch_files = BashOperator(
    task_id='fetch_files',
    bash_command="wget -P /home/koushik {{ ' '.join(ti.xcom_pull(task_ids='select_files')) }}",
    dag=dag,
)

zip_csv_task = PythonOperator(
    task_id='zip_csv_files_task',
    python_callable=zip_csv_files,
    dag=dag,
)

move_archive = PythonOperator(
    task_id='move_archive',
    python_callable=move_archive,
    dag=dag,
)

fetch_page >> select_files_task >> fetch_files >> zip_csv_task >> move_archive

