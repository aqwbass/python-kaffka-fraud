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


def to_json(obj):
    return json.dumps(
        {
            "freq": obj[0],
            "sumary": obj[1],
            "average": float(obj[2])
        }
    )
