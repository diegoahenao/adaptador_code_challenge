import os
import requests

USERNAME_AUTH=os.environ.get("USERNAME_AUTH")
PASS_AUTH=os.environ.get("PASS_AUTH")

def get_token():
    api_url = f'http://127.0.0.1:8000/auth/token'
    body = {"username": USERNAME_AUTH, "password": PASS_AUTH}
    
    response = requests.post(api_url, data=body, verify=False)
    
    if response.status_code == 200:
        print("Authorized successfully")
    else:
        print(f"Credentials verification failure. Status code: {response.status_code}")
    
    return response.json()["token"]

get_token()