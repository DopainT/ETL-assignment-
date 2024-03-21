from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner':'Dopain',
    'start_date':days_ago(0),
    'email':'learner.tzc@gmail.com',
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    }

dag = DAG(
    dag_id = 'ETL_toll_data',
    schedule_interval = timedelta(days=1),
    default_args = default_args,
    description = 'Apache Airflow Final Assignment',
)

download_data = BashOperator(
    task_id = 'download_data',
    bash_command = 'wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz',
    dag = dag,
)

unzip_data =  BashOperator(
    task_id = 'unzip',
    bash_command = 'tar -zxvf tolldata.tgz',
    dag = dag,
    )

csv_data =  BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d "," -f1-4  vehicle-data.csv >  csv_data.csv',
    dag = dag,
)


tsv_data =  BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'tr $"\t" "," <  tollplaza-data.tsv | cut -d "," -f5-7 >  tsv_data.csv',
    dag = dag,
)

fixed_width_data = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = 'tr -s " " "," <  payment-data.txt | cut -d "," -f10-11 >  fixed_width_data.csv',
    dag = dag,
)

consolidate_data = BashOperator(
    task_id = 'combine_data',
    bash_command = 'paste  csv_data.csv  tsv_data.csv  fixed_width_data.csv >  extracted_data.csv',
    dag = dag,
)

download_data >> unzip_data >> [csv_data,tsv_data,fixed_width_data] >> consolidate_data
