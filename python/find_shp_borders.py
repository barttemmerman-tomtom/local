import datetime
import logging
import sys
import argparse

from shapely.geometry import Polygon
import fiona

logging.basicConfig(stream=sys.stderr, level=logging.INFO)

parser = argparse.ArgumentParser("argument parser")
parser.add_argument('--input', help="input shp file", type=str, required=False)
parser.add_argument('--mehtod', help="input shp file", type=str, required=False)

args = parser.parse_args()
input_shp_file = args.input
coversion_method = args.method

conversions = [ "from_tomtom" : lambda c:(c[0]/100000, c[1]/100000),
                "to_tomtom" : lambda c:(c[0]*100000, c[1]*100000)]

print(args)


segment_lookup = {}

with fiona.open("c:/tmp/sofathon/areas.shp", "r") as source:

    # Copy the source schema and add two new properties.
    sink_schema = source.schema

    for feature in source:

        try:

            # If any feature's polygon is facing "down" (has rings
            # wound clockwise), its rings will be reordered to flip
            # it "up".
            geom = feature["geometry"]
            assert geom["type"] == "Polygon"

            rings = geom["coordinates"]

            for ring in rings:
                for i in range(len(ring) - 1):
                    normalized_segement = normalize_segment([ring[i], ring[i+1]])

                    if normalized_segement in segment_lookup.keys():
                        print(f"shared segment: {segment}", segment=normalized_segement)
                        segment_lookup[normalized_segement] += 1
                    else:
                        segment_lookup[normalized_segement] = 0


            sink.write(feature)

        except Exception as e:
            logging.exception("Error processing feature %s:", f["id"])

# Create a sink for processed features with the same format and
# coordinate reference system as the source.
with fiona.open(
    "c:/tmp/sofathon/areas_converted.shp",
    "w",
    crs=source.crs,
    driver=source.driver,
    schema=sink_schema,
) as sink:
    for segment in segment_lookup.keys():
        if segment_lookup[segment]




def normalize_segment(segment):
    if hash_coordinate(segment[0]) < hash_coordinate(segment[1]):
        return segment
    else:
        return [segment[1], segment[0]]

def hash_coordinate(coordinate):
    return coordinate[0] + coordinate[1]