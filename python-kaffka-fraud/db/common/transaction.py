import json


class Transaction:
    def __init__(self, amt_1, atm_id, card_no, date, fr_acct, fraud, lat, lon, time, to_acct, trans, resp):
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
        self.resp = resp

    @property
    def toJson(self):
        return json.dumps(
            {
                "card_no": self.card_no,
                "date": self.date,
                "time": self.time,
                "amt_1": self.amt_1,
                "trans": self.trans,
                "lat": self.lat,
                "lon": self.lon,
                "atm_id": self.atm_id,
                "fr_acct": self.fr_acct,
                "to_acct": self.to_acct,
                "resp": self.resp,
                "fraud": self.fraud
            }
        ).encode('utf-8')


if __name__ == "__main__":
    t = Transaction(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, None, 12)
    t1 = json.loads(t.toJson)
    print(t1)
