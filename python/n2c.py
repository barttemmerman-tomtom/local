import json
import psycopg2
import requests
import sys
import time
from configparser import ConfigParser
import timeit
from shapely.geometry import Point, LineString
from shapely import wkt

API_URL_MDS = 'http://mdbf-prod.maps-data-analytics-prod.amiefarm.com/n2c/mds'
#API_URL_MDS = 'http://localhost:8090/mds'
API_URL_MNR = 'http://mdbf-prod.maps-data-analytics-prod.amiefarm.com/n2c/mnr'

print("API_URL_MDS: {}".format(API_URL_MDS))


class CodeTimer:
    def __init__(self, name=None):
        self.name = " '"  + name + "'" if name else ''

    def __enter__(self):
        self.start = timeit.default_timer()

    def __exit__(self, exc_type, exc_value, traceback):
        self.took = (timeit.default_timer() - self.start) * 1000.0
        print('Code block' + self.name + ' took: ' + str(self.took) + ' ms')


def config(filename='n2c.config', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def connect():
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

def disconnect(connection):
    if connection is not None:
        connection.close()
        print('Database connection closed.')


def rest_call(stretch_id, stretch_wkt):

    params = {
        "id": stretch_id,
        "wkt": stretch_wkt
    }

    retry = 0
    response = None
    while True:
        try:
            response = requests.post(API_URL_MDS, headers=None, json=params)

        except requests.exceptions.RequestException as e:
            print("unable to access N2C service MDS {}".format(e))
            raise Exception("no response, check the service url={}".format(API_URL_MDS))
        
        if response.status_code > 299:
            print("response={}".format(response))

            if response.status_code < 500:
                retry += 1
                if retry <= 3:
                    waiting_time = pow(2, (retry - 1))
                    print("retrying in {}s".format(waiting_time))
                    time.sleep(waiting_time)
                    continue

        break

    if not response.ok:
        print("a problem occurred HTTP={} reason={} (id={} wkt={})".format(response.status_code, response.reason, stretch_id, stretch_wkt))
        return None

    decoder = json.JSONDecoder()
    data = json.loads(response.content.decode('utf8').replace("'", '"'))
    n2c = data["highestN2C"]
    return n2c


def rest_call_mnr(stretch_id, stretch_wkt, country):

    params = {
        "id": stretch_id,
        "wkt": stretch_wkt,
        "country": country
    }

    retry = 0
    response = None
    while True:
        try:
            response = requests.post(API_URL_MNR, headers=None, json=params)

        except requests.exceptions.RequestException as e:
            print("unable to access N2C service MNR {}".format(e))
            raise Exception("no response, check the service url={}".format(API_URL_MNR))
        
        if response.status_code > 299:

            print("response={}".format(response))

            if response.status_code < 500:
                retry += 1
                if retry <= 3:
                    waiting_time = pow(2, (retry - 1))
                    print("retrying in {}s".format(waiting_time))
                    time.sleep(waiting_time)
                    continue

        break

    if not response.ok:
        print("a problem occurred HTTP={} reason={} (id={} wkt={})".format(response.status_code, response.reason, stretch_id, stretch_wkt))
        return None

    decoder = json.JSONDecoder()
    data = json.loads(response.content.decode('utf8').replace("'", '"'))
    n2c = data["highestN2C"]

    return n2c


def update_record(connection, stretch_id, n2c):
    try:
        with connection:
            with connection.cursor() as cursor:
                print("updating n2c={} for stretchId={}".format(n2c, stretch_id))
                cursor.execute("""
                    UPDATE legacy_error_analysis.error_logs_n2c_missing
                    set net2class='{}'
                    where new_stretch_id='{}'
                    """.format(n2c, stretch_id))

    except (Exception, psycopg2.DatabaseError) as error:
        print("could not update {}".format(error))
        print(error)


def process_records(connection):
    try:
        with connection.cursor() as cursor:

            cursor.execute("""
                SELECT el.id, s.id, el.coord, net2class, s.wkt, el.iso
                from legacy_error_analysis.error_logs_n2c_missing el 
                join routes.stretch s on s.id = el.new_stretch_id 
                where (net2class = -1 or net2class is null) 
                and new_stretch_id is not null
                """)

            std_count = 0
            for row in cursor:
                    error_id = row[0]
                    stretch_id = row[1]
                    error_coord = row[2]
                    stretch_wkt = row[4]
                    country = row[5]

                    with CodeTimer('n2c MNR udpate (id={} country={} stretch_id={})'.format(error_id, country, stretch_id)):
                        # n2c = rest_call_mnr(stretch_id=stretch_id, stretch_wkt=stretch_wkt, country=country)
                        n2c = rest_call(stretch_id=stretch_id, stretch_wkt=stretch_wkt)
                        if n2c:
                            update_record(connection, stretch_id, n2c)

                    # offset_2m = .00001797

                    std_count += 1
                    if std_count > 10:
                        std_count = 1
                        sys.stdout.flush()


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def build_fake_stretch(lon, lat):
    offset__degrees_1m = .000008983
    return LineString([
        Point(lon - offset__degrees_1m, lat - offset__degrees_1m), 
        Point(lon + offset__degrees_1m, lat + offset__degrees_1m)
        ]).wkt


def process_records_mdbf1(connection):
    try:
        with connection.cursor() as cursor:

            cursor.execute("""
                SELECT el.id, el.longitude, el.latitude
                from legacy_error_analysis.error_logs_n2c_missing el 
                where new_stretch_id is null
                """)

            for row in cursor:
                    error_id = row[0]
                    error_lon = row[1]
                    error_lat = row[2]

                    stretch_wkt = build_fake_stretch(error_lon, error_lat)

                    with CodeTimer('n2c udpate (id={} stretch_id={})'.format(error_id, stretch_id)):
                        n2c = rest_call(stretch_id=stretch_id, stretch_wkt=stretch_wkt)
                        if n2c:
                            update_record(connection, stretch_id, n2c)



    except (Exception, psycopg2.DatabaseError) as error:
        print(error)



if __name__ == '__main__':
    with connect() as connection:
        process_records(connection)

    print("from the Eagles team: thank you for flying with us.")
