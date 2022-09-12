import psycopg2


def point_to_wkt(point):
    return str(point["longitude"]) + " " + str(point["latitude"]) + ", "


def route_to_wkt_from_string(route):
    parsed = json.loads(route)
    route_to_wkt(parsed)


def route_to_wkt(parsed):
    routes = parsed["routes"]
    first_route = routes[0]
    legs = first_route["legs"]
    first_leg = legs[0]
    points = first_leg["points"]

    transformed = list(map(point_to_wkt, points))

    linestring = "LINESTRING ("
    for point in transformed:
        linestring = linestring + point
    linestring = linestring[:-2] + ")"

    return linestring


def get_where(record):
    where_condition = "departure_time= '" + str(record[0]) + "' and original_lat = " + str(record[2]) + " and original_lon =" + str(record[3])
    return where_condition

def get_wkt(record):
    json = record[7]
    return (route_to_wkt(json))


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
    cursor.execute("select * from routing_results.routes r where wkt is null limit 1000")
    #cursor.itersize = 10
    for record in cursor.fetchall():
        print(count)
        update_query = "update routing_results.routes set wkt = '" + get_wkt(record) + "' where " + get_where(record)
        cursor.execute(update_query)

        if count % 250 == 0:
            connection.commit()
        count+=1

    connection.commit()


except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
