import csv
import os
import requests

def read_csv(file_path):
    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield row

def send_data_to_api(data, file_name):
    api_url = f'https://127.0.0.1:8000/{file_name}'
    
    response = requests.post(api_url, json=data, verify=False)
    
    if response.status_code == 200:
        print("Data successfully sent to the API")
    else:
        print(f"Application failure. Status code: {response.status_code}")

def main():
    folder_path = 'data/'
    files = ['hired_employees.csv', 'departments.csv', 'jobs.csv']

    data_count = 0
    batch_size = 1000
    batch_data = []

    for file in files:
        file_path = os.path.join(folder_path, file)
        file_name, _ = os.path.splitext(file)

        for row in read_csv(file_path):
            batch_data.append(row)
            data_count += 1

            if data_count == batch_size:
                send_data_to_api(batch_data, file_name)
                data_count = 0
                batch_data = []

    # Send any remaining batch that did not reach 1000 lines
    if batch_data:
        send_data_to_api(batch_data)