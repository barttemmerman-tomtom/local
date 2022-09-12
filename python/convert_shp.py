import datetime
import logging
import sys
import argparse

from shapely.geometry import Polygon, Point
import fiona

logging.basicConfig(stream=sys.stderr, level=logging.INFO)

parser = argparse.ArgumentParser("argument parser")
parser.add_argument('--input', help="input shp file", type=str, required=False)
parser.add_argument('--method', help="input shp file", type=str, required=False)

args = parser.parse_args()
input_shp_file = args.input
coversion_method = args.method

conversions = { "divide" : lambda c:(c[0]/100000, c[1]/100000),
                "multiply" : lambda c:(c[0]*100000, c[1]*100000)}

print(args)

with fiona.open(input_shp_file, "r") as source:

    # Copy the source schema and add two new properties.
    sink_schema = source.schema

    # Create a sink for processed features with the same format and
    # coordinate reference system as the source.
    with fiona.open(
        f"{input_shp_file}_converted.shp",
        "w",
        crs=source.crs,
        driver=source.driver,
        schema=sink_schema,
    ) as sink:

        for f in source:

            try:

                # If any feature's polygon is facing "down" (has rings
                # wound clockwise), its rings will be reordered to flip
                # it "up".
                geom = f["geometry"]

                if geom["type"] == "Polygon":
                    rings = geom["coordinates"]

                    geom["coordinates"] = [[ (c[0]/100000, c[1]/100000) for c in ring] for ring in rings]
                    f["geometry"] = geom
                else:
                    assert geom["type"] == "Point"

                    geom[x] = geom[x]/100000, c[1]/100000) for c in ring] for ring in rings]



                sink.write(f)

            except Exception as e:
                logging.exception("Error processing feature %s:", f["id"])
