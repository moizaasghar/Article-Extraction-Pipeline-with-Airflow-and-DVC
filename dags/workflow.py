from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.utils.dates import days_ago
import requests
from bs4 import BeautifulSoup
import re
import json
import os

import requests
from bs4 import BeautifulSoup

def extract_data(**kwargs):
    urls = ["https://www.dawn.com", "https://www.bbc.com"]
    data = []
    for url in urls:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        articles = soup.find_all('article')
        for article in articles:
            title_tag = article.find('h2')
            if title_tag and title_tag.find('a'):  # Check if the <h2> tag contains an <a> tag
                title = title_tag.text
                link = title_tag.find('a')['href']  # Extract the href attribute
            else:
                title = 'No title'
                link = None  # Default to None if no link is found
            
            description = article.find('p').text if article.find('p') else 'No description'
            data.append({'title': title, 'description': description, 'link': link})
    kwargs['ti'].xcom_push(key='extracted_data', value=data)


def transform_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract', key='extracted_data')
    transformed_data = []
    for item in data:
        cleaned_title = clean_text(item['title'])
        cleaned_description = clean_text(item['description'])
        link = item['link']  # Access the link directly from the item
        transformed_data.append({
            'title': cleaned_title,
            'description': cleaned_description,
            'link': link  # Include the link in the transformed data
        })
    return transformed_data

def clean_text(text):
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Replace multiple spaces with a single space
    text = re.sub(r'\s+', ' ', text)
    # Convert text to lowercase to maintain consistency
    text = text.lower()
    # Optionally remove all punctuation (uncomment the next line to activate)
    # text = re.sub(r'[^\w\s]', '', text)
    return text.strip()

def store_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transform')
    if not os.path.exists('data'):
        os.makedirs('data')
    filename = 'data/processed_data.json'
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)
    
    # Check if DVC is already initialized
    if not os.path.exists('.dvc'):
        # Initialize DVC
        os.system('dvc init')

    # Google Drive folder ID
    folder_id = '1CrnwaAf6vAz4WjERX4vvedkB-3N8HOuA'

    # Add a DVC remote for Google Drive
    os.system(f'dvc remote add -d mygdrive gdrive://{folder_id}')

    # Add data to DVC and push it to the remote
    os.system('dvc add data/processed_data.json')
    os.system('dvc commit')
    os.system('dvc push')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'article_extraction',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

store = PythonOperator(
    task_id='store',
    python_callable=store_data,
    provide_context=True,
    dag=dag,
)

extract >> transform >> store