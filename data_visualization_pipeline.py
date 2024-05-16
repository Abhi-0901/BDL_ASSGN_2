import os
import zipfile
from datetime import datetime, timedelta
import apache_beam as beam
import pandas as pd
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.runners.interactive.interactive_beam as ib
import shutil
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'data_visualization_pipeline',
    default_args=default_args,
    description='A DAG to generate data visualization using Apache Beam',
    schedule_interval='*/1 * * * *',  # Run every 1 minute
)

# Task 1: Wait for the archive to be available
wait_for_archive = FileSensor(
    task_id='wait_for_archive',
    poke_interval=5,
    timeout=5,
    filepath='/home/koushik/Documents/csv_archive.zip',
    mode='poke',
    dag=dag,
)

# Task 2: Check if the file is a valid archive and unzip its contents into individual CSV files
check_and_unzip = BashOperator(
    task_id='check_and_unzip',
    bash_command='''
        if [ ! -d "/home/koushik/Documents/csv_directory" ]; then
            echo "Starting unzip at $(date)"
            unzip -t /home/koushik/Documents/csv_archive.zip
            echo "Test unzip completed at $(date)"
            unzip /home/koushik/Documents/csv_archive.zip -d /home/koushik/Documents/csv_directory
            echo "Unzip completed at $(date)"
        else
            echo "Files already unzipped, skipping."
        fi
    ''',
    dag=dag,
)

# Task 3: Extract the contents of the CSV into a data frame and filter the dataframe based on the required fields
def process_csv_files():
    def process_row(row):
        fields = ['HourlyWindSpeed','HourlyDryBulbTemperature']
        # Assuming the row is a dictionary-like object (e.g., pd.Series or dict)
        data = {field: row[field] for field in fields if field in row}
        lat, lon = row['LATITUDE'], row['LONGITUDE']
        return (lat, lon, data)

    class CsvToDict(beam.DoFn):
        def process(self, element):
            file_path = element
            df = pd.read_csv(file_path)
            for _, row in df.iterrows():
                yield row.to_dict()

    # Define the Beam pipeline
    p = beam.Pipeline(runner='DirectRunner')

    csv_files = [os.path.join('/home/koushik/Documents/csv_directory/', f) for f in os.listdir('/home/koushik/Documents/csv_directory/') if f.endswith('.csv')]

    # Read CSV files, process rows, and collect results
    result = (
        p
        | 'Create file paths' >> beam.Create(csv_files)
        | 'Read CSV and convert to dict' >> beam.ParDo(CsvToDict())
        | 'Process rows' >> beam.Map(process_row)
    )

    # Run the pipeline
    pipeline_result = p.run()
    pipeline_result.wait_until_finish()

process_csv_files_task = PythonOperator(
    task_id='process_csv_files',
    python_callable=process_csv_files,
    dag=dag,
)


#Task 4
def csv_to_dffilter(path_folder): # This function converts the csv file into the required tuple format
    with beam.Pipeline(runner='DirectRunner') as pipeline: #This creates the pipeline in direct runner mode in beam
        csv_files = [os.path.join('/home/koushik/Documents/csv_directory/', f) for f in os.listdir('/home/koushik/Documents/csv_directory/') if f.endswith('.csv')]
        result = (
            pipeline 
            | beam.Create(csv_files)
            | beam.Map(csv_df)
            | beam.Map(monthly_average)
            | beam.io.WriteToText('/home/koushik/Documents/tuple_data.txt')
        )
text_address='/home/koushik/Documents/tuple_data.txt-00000-of-00001' #This is the address of the text file stored by beam
#Now let us create the function that will compute the monthly averages for the same latitude and longitude 
def compute_monthly_average_task(data_list):
    data_df = pd.DataFrame(data_list, columns=['Month', 'Latitude', 'Longitude', 'Temperature', 'WindSpeed'])
    data_df.dropna(inplace=True)
    data_df['Temperature']=(data_df['Temperature']-32)*5/9
    final_result = []
    result_df = data_df.groupby(['Month', 'Latitude', 'Longitude']).agg({'Temperature': 'mean', 'WindSpeed': 'mean'}).reset_index() #Use groupby to find average temperature and wind speed for each month
    for _, row in result_df.iterrows():
        lat = row['Latitude']
        longi = row['Longitude']
        month = row['Month']
        temp = row['Temperature']
        speed = row['WindSpeed']
        final_result.append([lat, longi, month, temp, speed])
    return json.dumps(final_result)

compute_monthly_averages_task = PythonOperator(
    task_id='compute_monthly_averages',
    python_callable=csv_to_dffilter,
    op_kwargs={"path_folder":'/home/koushik/Documents/csv_directory/'},
    dag=dag,
)

delete_files = BashOperator(
    task_id='delete_files',
    bash_command='rm -rf /home/koushik/Documents/csv_directory/*',
    dag=dag,
)

# Set task dependencies
wait_for_archive >> check_and_unzip >> process_csv_files_task >> compute_monthly_averages_task >> delete_files
