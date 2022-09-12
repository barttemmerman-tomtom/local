import psycopg2
import json

def point_to_wkt(point):
    return str(point["longitude"]) + " " + str(point["latitude"]) + ", "


def route_to_wkt(points):
    transformed = list(map(point_to_wkt, points))

    linestring = "LINESTRING ("
    for point in transformed:
        linestring = linestring + point
    linestring = linestring[:-2] + ")"

    return linestring


def get_where(record):
    where_condition = "points= '" + str(record[0]) + "'"
    return where_condition

def get_wkt(record):
    print("record" + str(record))
    o = json.loads(record.lstrip('points'))
    print("json" + str(json))
    print("type(json)={}".format(type(o)))
    return (route_to_wkt(o))

try:
    connection = psycopg2.connect(user = "haystack_dba",
                                  password = "haystack_dba",
                                  host = "haystack.cribqwrzghuv.eu-west-1.rds.amazonaws.com",
                                  port = "5432",
                                  database = "haystack")

    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    print ( connection.get_dsn_parameters(),"\n")

    count = 0
    cursor.execute("select points from routing_results.freq_pts r where wkt is null")
    #cursor.itersize = 10
    for record in cursor.fetchall():
        print(count)
        update_query = "update routing_results.freq_pts set wkt = '" + get_wkt(record[0]) + "' where " + get_where(record)
        cursor.execute(update_query)
        #print(update_query)

        if count % 100 == 0:
            connection.commit()
        count+=1

    connection.commit()


finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
