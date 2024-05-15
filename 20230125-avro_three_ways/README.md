In this directory you'll find both Java and Python implementations of a program that converts
[JSON](https://www.json.org/json-en.html) to [Avro](https://avro.apache.org/).

My primary goal for these examples is how to use logical types, not to be an exhaustive
reference of Avro features.


## The JSON Data

The [example data file](example.json) contains multiple "checkout complete" events that might
be generated from an eCommerce website.

```
{
  "eventType": "checkoutComplete",
  "eventId": "dae0e6cc-19e7-4669-b850-9861af09a2f6",
  "timestamp": "2021-08-03 05:11:24.044",
  "userId": "bdb4fd7e-9ddb-469a-8e1e-f2c88bfbaa51",
  "itemsInCart": 1,
  "totalValue": 23.25
}
```

This event has a couple of peculiarites that make an Avro representation more useful than the
JSON version:

* The `timestamp` field is a string. It's not quite ISO-8601 format: there's a space between
  the date and time, and there's no timezone. If this were stored directly in your data lake
  each of your queries must parse it and assign the correct timezone. By writing the data in
  Avro, we can do that once.

* The `totalValue` field represents money, but as a JSON number, it will often be interpreted
  in floating point. Making things more challenging, the actual file contains rows that use
  different numbers of digits to the right of the decimal. As with the timestamp, we can parse
  it once, and store it with the proper number of decimals (using banker's rounding, if needed).

* The `eventId` and `userId` fields are UUIDs. I have chosen _not_ to store these using the
  Avro UUID logical type, but leave it as a (simple) exercise for the reader.


## The Avro Schema

The [schema file](example.avsc) defines each of the fields in the records, as a JSON file:

```
{
  "namespace": "com.chariotsolutions.example.avro",
  "type": "record",
  "name": "CheckoutComplete",
  "fields": [
    {
      "name": "eventType",
      "type": "string"
    },
    {
      "name": "eventId",
      "type": "string"
    },
    {
      "name": "timestamp",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      }
    },
    {
      "name": "userId",
      "type": "string"
    },
    {
      "name": "itemsInCart",
      "type": "int"
    },
    {
      "name": "totalValue",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 16,
        "scale": 2
      }
    }
  ]
}
```

The [Avro Schema Specification](https://avro.apache.org/docs/1.11.1/specification/) is your
guide to the meaning of this file.


## Java

The Java implementation uses Avro's `GenericRecord` and `GenericDatumWriter` to produce the
output file. There are a few things that I want to call out about this implementation:

* For a real-world use-case with fixed-format records, I would actually use the
  [Avro code generator](https://avro.apache.org/docs/1.11.1/getting-started-java/#serializing-and-deserializing-with-code-generation)
  to produce helper classes.

* I use [Jackson Databind](https://github.com/FasterXML/jackson-databind) to parse the
  source JSON. This library is commonly used for Java applications, so chances are good
  that you will already have it in your dependency tree. Its `ObjectMapper` offers
  multiple configuration options; most important for this application being to read
  decimal numbers as `BigDecimal` rather than floating-point:

  ```
  ObjectMapper mapper = new ObjectMapper();
  mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
  ```

  Beware that this does not configure the parser to read _all_ numbers as `BigDecimal`.
  If it sees an integer value, it will choose a size-appropriate integral type
  (`Integer`, `Long`, or `BigInteger`). As a result, I explicitly convert the parsed
  value to a string, then construct the `BigDecimal` from that string (and explicitly
  set the scale of the decimal value to match the schema):


  ```
  BigDecimal totalValue = new BigDecimal(data.get("totalValue").toString())
                          .setScale(2, RoundingMode.HALF_EVEN);
  ```

* Physical types can be stored directly into `GenericDatum`. Logical types must first
  be converted into the appropriate physical type, and the Avro documentation doesn't
  say how to do this! The answer has three parts (which I learned from examining a
  generated class):

  First, there are converters for each of the logical types. Here are the two that I'm
  using:

  ```
  private final static TimestampMillisConversion  TIMESTAMP_CONVERTER = new TimestampMillisConversion();
  private final static DecimalConversion          DECIMAL_CONVERTER = new DecimalConversion();
  ```

  Second, you need to retrieve the field-specific logical type definitions from the
  schema. It's a bit confusing, until you realize that a schema is a recursive structure:
  not only do you have a schema for the record as a whole, each field has its own schema.

  ```
  LogicalType timestampLT = schema.getField("timestamp").schema().getLogicalType();
  LogicalType totalValueLT = schema.getField("totalValue").schema().getLogicalType();
  ```

  Lastly, you pass that logical type definition to the converter, along with the field value:

  ```
  datum.put("timestamp",  TIMESTAMP_CONVERTER.toLong(timestamp, schema, timestampLT));
  datum.put("totalValue", DECIMAL_CONVERTER.toBytes(totalValue, schema, totalValueLT));
  ```

### Building and Running

> The commends in this section assume that you're in the `java` directory.

Build with Maven:

```
mvn clean package
```

The build artifact is an "uberJar" that contains all dependencies. It's also configured with
the `Main` class as an entry-point. It expects three parameters: the path to the Avro schema
file, the path to the source JSON file, and the path to the output file:

```
java -jar target/example-avro-*.jar ../example.avsc ../example.json ../example.avro
```


## Python

There are two variants of the Python implementation: one using the
[avro](https://avro.apache.org/docs/1.11.1/getting-started-python/) package from Apache,
and one using the [fastavro](https://fastavro.readthedocs.io/en/latest/) package. Of the
two, I consider `fastavro` to be simpler to use and with fewer quirks. 

Both examples use a `transform_record()` function to parse the source JSON and transform
it as needed by the Avro writer. 

*  Both versions use the [python-dateutil](https://pypi.org/project/python-dateutil/)
   library to parse the `timestamp` field, because it's almost-but-not-quite an ISO-8601
   timestamp. I explicitly set the timezone to UTC, since it's not included in the string
   representation. The `avro` library needs this: it cannot support a naive `datetime`.
   The `fastavro` library, by comparison, assumes UTC.

*  The `fastavro` library handles the fixed-point `Decimal` value without problem. The
   `avro` library, however, has the quirk that it applies the scale of the output field
   to the _unscaled_ value: so if you put `Decimal("123.45")` into a field defined with
   four decimal places, you'll read it back as `Decimal("1.2345")`. This appears to be
   intended or at least unconsidered behavior, and to work around it I scale the source
   value and take the integer portion of the result.


### Building and Running

Both examples are built the same way: using `pip` to install dependencies into a local
`lib` directory (run from the sub-project root directory):

```
pip3 install -t lib -r requirements.txt
```

To run, you must add that `lib` directory to Python's search path, and provide the path
to the Avro schema, the path to the source file, and the path to the output file:

```
PYTHONPATH=./lib ./convert.py ../example.avsc ../example.json ../example.avro
```
