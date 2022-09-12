import polyline
import sys

decode = "gi~gbB}~_dGbFrKrBkCz@mAnDsF"

def rfix(coordinate):
    return round(coordinate, 8)

print(f"line to decode:{decode}")

l = polyline.decode(decode, precision=5)
l2 = list(map(lambda c: (c[1]/10,c[0]/10.), l))

wkt_string = "LINESTRING (" + ",".join(map(lambda t: "%s %s" % (rfix(t[0]), rfix(t[1])), l2)) + ")"

print(f"wkt:\n{wkt_string}")


