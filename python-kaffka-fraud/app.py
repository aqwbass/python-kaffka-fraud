import json

from db import connect, insertGenerates, queryPrevious, queryState, insertPreItems
from preprocress_tools import preprocress_model
from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route('/', methods=['POST'])
def pre_items():
    # รับ input เป็น json format
    json_object = request.json

    # query ข้อมูล ย้อนหลัง return เป็น array มีค่าข้างในเป็น json มีทั้งหมด 5 ตำแหน่ง ใน array
    # ใน json มี date , card_no , previous_avg
    previous_list = queryPrevious(json_object)
    # print(previous_list)

    # insert transaction
    insertGenerates(json_object)

    # query transaction return เป็น array มีค่าข้างในเป็น json มีทั้งหมด 5 ตำแหน่ง ใน array
    # ใน json มี freq , sumary , average
    query_list = queryState(json_object)

    pre_items = preprocress_model(json_object, previous_list, query_list)

    insertPreItems(pre_items)

    return jsonify(json.loads(pre_items))


if __name__ == '__main__':
    # read sql script
    # with app.open_resource('./db/intlms_isc_dev.sql' , mode='r') as f:
    #     DB = connect()
    #     DB.cursor().execute(f.read())
    # DB.commit()
    # DB.close()

    app.run(host='0.0.0.0', debug=True)
