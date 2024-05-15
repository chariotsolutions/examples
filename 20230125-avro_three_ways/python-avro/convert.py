#!/usr/bin/env python3

""" Converts an example ND/JSON file into an Avro file using the "avro" package.

    Invocation:

        PYTHONPATH=./lib convert.py SCHEMA_FILE JSON_FILE AVRO_RILE
    """

import avro.schema
from   avro.datafile import DataFileReader, DataFileWriter
from   avro.io import DatumReader, DatumWriter
from   datetime import datetime, timezone
import dateutil.parser
import decimal
import json
import sys


def transform_record(json_str):
    """ Transforms a single source record into a record that FastAvro
        can write. Since it writes an entire array of records at a time,
        we can call this from a list transformation.
        """
    data = json.loads(json_str, parse_float=str, parse_int=str)
    return {
        "eventType":    data['eventType'],
        "eventId":      data['eventId'],
        "timestamp":    dateutil.parser.parse(data['timestamp']).replace(tzinfo=timezone.utc),
        "userId":       data['userId'],
        "itemsInCart":  int(data['itemsInCart']),
        "totalValue":   decimal.Decimal(data['totalValue']).shift(2).to_integral_value(decimal.ROUND_HALF_EVEN)
    }


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        sys.exit(1)
    
    schema_file = sys.argv[1]
    json_file = sys.argv[2]
    avro_file = sys.argv[3]
    
    with open(schema_file) as f:
        schema = avro.schema.parse(f.read())
        
    with DataFileWriter(open(avro_file, "wb"), DatumWriter(), schema) as writer:
        with open(json_file, "r") as f:
            for line in f.readlines():
                writer.append(transform_record(line))