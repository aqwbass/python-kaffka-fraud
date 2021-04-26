import json


def previous_avg_to_json(obj):
    # print(obj)
    return json.dumps(
        {
            "previous_avg": float(obj[0])
        }
    )


def lat_lon_to_json(obj):
    return json.dumps(
        {
            "lat": float(obj[0]),
            "lon": float(obj[1])
        }
    )
def invalid_pin_to_json(obj1, obj2):
    return json.dumps(
        {
            "freq_total": obj1[0],
            "freq_invalid_pin": obj2[0]
        }
    )

def to_json(obj):
    return json.dumps(
        {
            "freq": obj[0],
            "sumary": obj[1],
            "average": float(obj[2])
        }
    )
