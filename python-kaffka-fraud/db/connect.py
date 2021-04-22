import json

import psycopg2

from .config import config
from .common.tools import previous_avg_to_json, to_json, lat_lon_to_json


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


# input : json body
# insert transaction
def insertGenerates(json_object):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        postgres_insert_query = """INSERT INTO generates ("date", "time", card_no, lat, lon, amt_1, fr_acct, to_acct, atm_id, trans, resp, fraud) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

        record_to_insert = (
            json_object["date"], json_object["time"], str(json_object["card_no"]), json_object["lat"],
            json_object["lon"],
            json_object["amt_1"], str(json_object["fr_acct"]), str(json_object["to_acct"]), json_object["atm_id"],
            json_object["trans"], json_object["resp"], str(json_object["fraud"]))

        # print(record_to_insert)
        # execute a statement
        cur.execute(postgres_insert_query, record_to_insert)

        # commit execution
        conn.commit()

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# input : json body
# ทำการ query ดู average ย้อน หลัง 5 นาที ,1 ชั่วโมง ,2 ชั่วโมง ,1 วัน
# และ ย้อนหลังทั้งหมดของ card_no ที่รับมา ในวันนั้นๆ
def queryPrevious(json_object):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)

        # ตรงนี้ต้องไป รับค่ามา
        card_no = str(json_object["card_no"])
        date = str(json_object["date"])
        time = str(json_object["time"])

        # ----sql script-----

        # average amount ไม่แยกตู้ atm ย้อนหลัง ทั้งหมด
        sql1 = "select avg(amt_1::int) PreviousAvg from generates group by card_no , trans , date , fraud  having  card_no like %s and fraud not like'1'"

        # average amount ไม่แยกตู้ atm ย้อนหลัง 1 วัน
        sql2 = "select avg(amt_1::int) PreviousAvg from generates group by card_no , trans  , date  having date::date > %s::date - interval '1 day' and card_no like %s"

        # average amount ไม่แยกตู้ atm ย้อนหลัง 2 ชม.
        sql3 = "select avg(amt_1::int) PreviousAvg from (select date ,card_no ,atm_id ,trans,amt_1  from generates group by card_no , trans , atm_id , date , time , amt_1  having time::time > %s::time - interval '2 hour' and card_no like %s) a group by date"

        # average amount ไม่แยกตู้ atm ย้อนหลัง 1 ชม.
        sql4 = "select avg(amt_1::int) PreviousAvg from (select date ,card_no ,atm_id ,trans,amt_1  from generates group by card_no , trans , atm_id , date , time , amt_1  having time::time > %s::time - interval '1 hour' and card_no like %s) a group by date"

        # average amount ไม่แยกตู้ atm ย้อนหลัง 5 น.
        sql5 = "select avg(amt_1::int) PreviousAvg from (select date ,card_no ,atm_id ,trans,amt_1  from generates group by card_no , trans , atm_id , date , time , amt_1  having time::time > %s::time - interval '5 minutes' and card_no like %s) a group by date"

        # lat lon ของ transaction ก่อนหน้า
        sql6 = "select lat ,lon from generates WHERE card_no LIKE %s ORDER BY date, time DESC LIMIT 1"
        # execute a statement
        previous_avg = []
        with conn:
            with conn.cursor() as curs:
                curs.execute(sql1, (card_no,))
                display = curs.fetchone()
                if display is None:
                    display = [0]
                previous_avg.append(previous_avg_to_json(display))

        with conn:
            with conn.cursor() as curs:
                curs.execute(sql2, (date, card_no,))
                display = curs.fetchone()
                if display is None:
                    display = [0]
                previous_avg.append(previous_avg_to_json(display))

        with conn:
            with conn.cursor() as curs:
                curs.execute(sql3, (time, card_no,))
                display = curs.fetchone()
                if display is None:
                    display = [0]
                previous_avg.append(previous_avg_to_json(display))

        with conn:
            with conn.cursor() as curs:
                curs.execute(sql4, (time, card_no,))
                display = curs.fetchone()
                if display is None:
                    display = [0]
                previous_avg.append(previous_avg_to_json(display))

        with conn:
            with conn.cursor() as curs:
                curs.execute(sql5, (time, card_no,))
                display = curs.fetchone()
                if display is None:
                    display = [0]
                previous_avg.append(previous_avg_to_json(display))

        with conn:
            with conn.cursor() as curs:
                curs.execute(sql6, (card_no,))
                display = curs.fetchone()
                if display is None:
                    display = [0, 0]
                lat_lon = lat_lon_to_json(display)

        # display the PostgreSQL database
        return {
            "previous_avg": previous_avg,
            "lat_lon": lat_lon
        }

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def queryState(json_object):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)

        # ตรงนี้ต้องไป รับค่ามา
        card_no = str(json_object["card_no"])
        date = str(json_object["date"])

        # ----sql script-----

        # freq transaction ทั้งหมด และ sum amount ไม่แยกตู้ atm ย้อนหลัง ทั้งหมด
        sql1 = "select count(trans) freq,sum(amt_1::int) sumary,avg(amt_1::int) average from generates group by card_no , trans  , date , fraud  having  card_no like %s and fraud not like '1'"

        # freq transaction ทั้งหมด และ sum amount ไม่แยกตู้ atm ย้อนหลัง 1 วัน
        sql2 = "select count(trans) freq,sum(amt_1::int) sumary,avg(amt_1::int) average from generates group by card_no , trans  , date  having date::date > %s::date - interval '1 day' and card_no like %s"

        # freq transaction ทั้งหมด และ sum amount ไม่แยกตู้ atm ย้อนหลัง 2 ชั่วโมง
        sql3 = "select count(trans) freq,sum(amt_1::int) sumary,avg(amt_1::int) average from generates group by card_no , trans  , date  having date::date > %s::date - interval '2 hour' and card_no like %s"

        # freq transaction ทั้งหมด และ sum amount ไม่แยกตู้ atm ย้อนหลัง 1 ชั่วโมง
        sql4 = "select count(trans) freq,sum(amt_1::int) sumary,avg(amt_1::int) average from generates group by card_no , trans  , date  having date::date > %s::date - interval '1 hour' and card_no like %s"

        # freq transaction ทั้งหมด และ sum amount ไม่แยกตู้ atm ย้อนหลัง 5 นาที
        sql5 = "select count(trans) freq,sum(amt_1::int) sumary,avg(amt_1::int) average from generates group by card_no , trans  , date  having date::date > %s::date - interval '5 minutes' and card_no like %s"

        # std lat lon ใน 1 วัน
        sql6 = "select lat, lon from generates where date::date > %s::date - interval '1 day' and card_no like %s"

        # execute a statement
        freq_sum_avg = []
        with conn:
            with conn.cursor() as curs:
                curs.execute(sql1, (card_no,))
                display = curs.fetchone()
                freq_sum_avg.append(to_json(display))

        with conn:
            with conn.cursor() as curs:
                curs.execute(sql2, (date, card_no,))
                display = curs.fetchone()
                freq_sum_avg.append(to_json(display))

        with conn:
            with conn.cursor() as curs:
                curs.execute(sql3, (date, card_no,))
                display = curs.fetchone()
                freq_sum_avg.append(to_json(display))

        with conn:
            with conn.cursor() as curs:
                curs.execute(sql4, (date, card_no,))
                display = curs.fetchone()
                freq_sum_avg.append(to_json(display))

        with conn:
            with conn.cursor() as curs:
                curs.execute(sql5, (date, card_no,))
                display = curs.fetchone()
                freq_sum_avg.append(to_json(display))

        # display the PostgreSQL database
        return {
            "freq_sum_avg": freq_sum_avg
        }

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# input : json body
# insert preprocress transacion
def insertPreItems(json_object):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        postgres_insert_query = """
        INSERT INTO pre_items (amt_1, card_no, "date", fraud, "time", freq_5_minitues, sumary_5_minitues, average_5_minitues, diff_5_minitues, freq_1_hour, sumary_1_hour, average_1_hour, diff_1_hour, freq_2_hour, sumary_2_hour, average_2_hour, diff_2_hour, freq_daily, sumary_daily, average_daily, diff_daily, freq_total, sumary_total, average_total, diff_total,diff_lat_lon,is_night) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        json_object = json.loads(json_object)
        record_to_insert = (
            json_object["amt_1"], str(json_object["card_no"]), json_object["date"], str(json_object["fraud"]),
            json_object["time"],
            json_object["freq_5_minitues"], json_object["sumary_5_minitues"], json_object["average_5_minitues"],
            json_object["diff_5_minitues"],
            json_object["freq_1_hour"], json_object["sumary_1_hour"], json_object["average_1_hour"],
            json_object["diff_1_hour"],
            json_object["freq_2_hour"], json_object["sumary_2_hour"], json_object["average_2_hour"],
            json_object["diff_2_hour"],
            json_object["freq_1_day"], json_object["sumary_1_day"], json_object["average_1_day"],
            json_object["diff_1_day"],
            json_object["freq_total"], json_object["sumary_total"], json_object["average_total"],
            json_object["diff_total"],
            json_object["diff_lat_lon"], str(json_object["is_night"]),
        )
        # print(record_to_insert)
        # execute a statement
        cur.execute(postgres_insert_query, record_to_insert)

        # commit execution
        conn.commit()

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()