import csv
import os
import requests
from auth_task import get_token

def read_csv(file_path, file_name):
    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        if file_name == 'departments':
            for row in reader:
                row_data = {
                    "id_departments": int(row[0]),
                    "department": row[1]
                    } 
                yield row_data
        elif file_name == 'jobs':
            for row in reader:
                row_data = {
                    "id_jobs": int(row[0]),
                    "job": row[1]
                    } 
                yield row_data
        elif file_name == 'hired_employees':
            for row in reader:
                row_data = {
                    "id_hired_employees": int(row[0]),
                    "name": row[1],
                    "datetime": row[2],
                    "department_id": row[3],
                    "job_id": (row[4])
                    } 
                yield row_data


def send_data_to_api(data, file_name):
    api_url = f'http://127.0.0.1:8000/{file_name}'
    print(api_url)
    token = get_token()

    print(token)
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.post(api_url, json=data, headers=headers, verify=False)
    
    if response.status_code == 200 or response.status_code == 201:
        print("Data successfully sent to the API")
    else:
        print(response.text)
        print(f"Application failure. Status code: {response.status_code}")

def main():
    folder_path = 'data/'
    files = ['jobs.csv', 'departments.csv', 'hired_employees.csv']

    data_count = 0
    batch_size = 1000
    batch_data = []

    for file in files:
        file_path = os.path.join(folder_path, file)
        print(file_path)
        file_name = os.path.splitext(file)[0]
        print(file_name)

        for row in read_csv(file_path, file_name):
            batch_data.append(row)
            data_count += 1
            if data_count == batch_size:
                print("----->", data_count)
                send_data_to_api(batch_data, file_name)
                data_count = 0
                batch_data = []

        # Send any remaining batch that did not reach 1000 lines
        if batch_data:
            print("----->", data_count)
            send_data_to_api(batch_data, file_name)
            data_count = 0
            batch_data = []

main()