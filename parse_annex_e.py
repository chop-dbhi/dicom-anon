#!/usr/bin/env python3
import argparse
import sys
parser = argparse.ArgumentParser()
parser.add_argument("filename")
args = parser.parse_args()

try:
    input_file = open(args.filename, encoding="UTF-8")
except IOError as e:
    print("Unable to open input file", file=sys.stderr)
    sys.exit()

attribute_name = None
group = None
item = None

print("{")
for index, line in enumerate(input_file):
    if (index % 2) == 0:
        # Attribute name line
        line = line.split("\t")
        group = "0x%s" % line[1].split(",")[0].strip()[1:]
        item = "0x%s" % line[1].split(",")[1].strip()[:-1]
        attribute_name = line[0].strip()
    else:
        print("\t(%s,%s): %s, # %s" % (group, item, line.split("\t")[:-1], attribute_name))
print("}")







