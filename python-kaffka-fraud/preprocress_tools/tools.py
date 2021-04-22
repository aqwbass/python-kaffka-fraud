import json
from math import radians, cos, sin, asin, sqrt
from datetime import datetime


# คำนวนผลต่าง
def diff_amt(amt, avg):
    diff = amt - avg
    if diff < 0:
        return 0
    if avg == 0:
        return 0
    return diff


# คำนวน ระยะห่างพิกัด
def haversine(lon1, lat1, lon2, lat2):
    if lon1 == 0:
        return 0
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r


# check ว่าเป็น เวลากลางคืน ?
def in_between(now):
    start = datetime.strptime('210000', '%H%M%S').time()
    end = datetime.strptime('045900', '%H%M%S').time()
    now = datetime.strptime(now, '%H:%M:%S').time()
    if start <= end:
        status = start <= now < end
    else:  # over midnight e.g., 23:30-04:15
        status = start <= now or now < end
    if status:
        return 1
    else:
        return 0


def preprocress_model(json_obj, previous, now):
    now_5_minit = json.loads(now['freq_sum_avg'][4])
    previous_5_minit = json.loads(previous['previous_avg'][4])
    now_1_hour = json.loads(now['freq_sum_avg'][3])
    previous_1_hour = json.loads(previous['previous_avg'][3])
    now_2_hour = json.loads(now['freq_sum_avg'][2])
    previous_2_hour = json.loads(previous['previous_avg'][2])
    now_1_day = json.loads(now['freq_sum_avg'][1])
    previous_1_day = json.loads(previous['previous_avg'][1])
    now_total = json.loads(now['freq_sum_avg'][0])
    previous_total = json.loads(previous['previous_avg'][0])
    # ระยะห่างจากที่ที่ ทำรายการครั้งล่าสุด หน่วย km
    lat_lon = json.loads(previous["lat_lon"])

    return json.dumps(
        {
            "amt_1": json_obj["amt_1"],
            "card_no": json_obj["card_no"],
            "date": json_obj["date"],
            "fraud": json_obj["fraud"],
            "time": json_obj["time"],
            "freq_5_minitues": now_5_minit["freq"],
            "sumary_5_minitues": now_5_minit["sumary"],
            "average_5_minitues": now_5_minit["average"],
            "diff_5_minitues": diff_amt(json_obj["amt_1"], previous_5_minit["previous_avg"]),
            "freq_1_hour": now_1_hour["freq"],
            "sumary_1_hour": now_1_hour["sumary"],
            "average_1_hour": now_1_hour["average"],
            "diff_1_hour": diff_amt(json_obj["amt_1"], previous_1_hour["previous_avg"]),
            "freq_2_hour": now_2_hour["freq"],
            "sumary_2_hour": now_2_hour["sumary"],
            "average_2_hour": now_2_hour["average"],
            "diff_2_hour": diff_amt(json_obj["amt_1"], previous_2_hour["previous_avg"]),
            "freq_1_day": now_1_day["freq"],
            "sumary_1_day": now_1_day["sumary"],
            "average_1_day": now_1_day["average"],
            "diff_1_day": diff_amt(json_obj["amt_1"], previous_1_day["previous_avg"]),
            "freq_total": now_total["freq"],
            "sumary_total": now_total["sumary"],
            "average_total": now_total["average"],
            "diff_total": diff_amt(json_obj["amt_1"], previous_total["previous_avg"]),
            "diff_lat_lon": haversine(json_obj["lon"], json_obj["lat"], lat_lon["lon"],
                                      lat_lon["lat"]),
            "is_night": in_between(json_obj["time"])
        }
    )
