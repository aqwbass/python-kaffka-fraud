import requests
import json

if __name__ == '__main__':
    url = 'http://0.0.0.0:5000/'
    # Opening JSON file
    f = open('csvjson.json', )
    data = json.load(f)
    check = 1
    for i in data:
        x = requests.post(url, json=i)
        if check == 5 :
            break
        check += 1
