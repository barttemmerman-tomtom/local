import json


def point_to_wkt(point):
    return str(point["latitude"]) + " " + str(point["longitude"]) + ", "


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

