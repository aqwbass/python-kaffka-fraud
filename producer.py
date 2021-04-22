import time
import json
from kafka import KafkaProducer
import pandas as pd

class Transaction:
    def __init__(self,amt_1,atm_id,card_no,date,fr_acct,fraud,lat,lon,time,to_acct,trans):
        self.amt_1 = amt_1
        self.atm_id = atm_id
        self.card_no = card_no 
        self.date = date 
        self.fr_acct = fr_acct
        self.fraud = fraud
        self.lat = lat
        self.lon = lon
        self.time = time
        self.to_acct = to_acct
        self.trans = trans

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=json_serializer)
if __name__ == "__main__":
    while 1 == 1:
        print("start....")
        transaction_data=pd.read_csv('temp3.csv', delimiter=',')
        for index, row in transaction_data.iterrows():
            transaction = Transaction(row['amt_1'],row['atm_id'],row['card_no'],row['date'],row['fr_acct'],row['fraud'],row['lat'],row['lon'],row['time'],row['to_acct'],row['trans'])
            producer.send('transaction_input',transaction.__dict__)
            print(transaction.__dict__)
            time.sleep(3)
