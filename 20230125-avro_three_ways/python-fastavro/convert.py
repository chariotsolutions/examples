#!/usr/bin/env python3

""" Converts an example ND/JSON file into an Avro file using the "avro" package.

    Invocation:

        PYTHONPATH=./lib convert.py SCHEMA_FILE JSON_FILE AVRO_RILE
    """

from   datetime import datetime, timezone
import dateutil.parser
import decimal
import fastavro
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
        "totalValue":   decimal.Decimal(data['totalValue'])
    }


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        sys.exit(1)
    
    schema_file = sys.argv[1]
    json_file = sys.argv[2]
    avro_file = sys.argv[3]
    
    with open(schema_file) as f:
        schema = fastavro.parse_schema(json.load(f))
    
    with open(json_file, "r") as f:
        transformed = [transform_record(line) for line in f.readlines()]
        
    with open(avro_file, 'wb') as f:
        fastavro.writer(f, schema, transformed)