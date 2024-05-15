package com.chariotsolutions.example.avro;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions.TimestampMillisConversion;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 *  Example of writing an Avro file using a <code>GenericDatumWriter</code>, including logical types.
 */
public class Main
{
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    // the source record doesn't use an ISO-8601 timestamp, so must define an explicit parser

    private final static DateTimeFormatter TIMESTAMP_PARSER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                                                                               .withZone(ZoneOffset.UTC);

    // Avro requires converters for logic types; you can learn this by generating an adaptor class from your schema

    private final static TimestampMillisConversion  TIMESTAMP_CONVERTER = new TimestampMillisConversion();
    private final static DecimalConversion          DECIMAL_CONVERTER = new DecimalConversion();


    public static void main(String[] argv)
    throws Exception
    {
        if (argv.length != 3)
        {
            System.err.println("invocation: Main SCHEMA_FILE JSON_FILE OUTPUT_FILE)");
            System.exit(1);
        }

        File schemaFile = new File(argv[0]);
        File inputFile = new File(argv[1]);
        File outputFile = new File(argv[2]);

        // need to configure Jackson to always use BigDecimal for floats; for integral numbers
        // it will pick an appropriate type based on the number's size
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

        Schema schema;
        try (InputStream in = new FileInputStream(schemaFile))
        {
            schema = new Schema.Parser().parse(in);
            logger.debug("schema: {}", schema);
        }

        // we need to extract the logical type definitions for these fields to perform the conversion
        LogicalType timestampLT = schema.getField("timestamp").schema().getLogicalType();
        LogicalType totalValueLT = schema.getField("totalValue").schema().getLogicalType();

        // need to delete the output file if it exists, because otherwise the Avro writer will throw
        outputFile.delete();

        logger.info("writing output to {}", outputFile);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        try (FileInputStream fis = new FileInputStream(inputFile);
             InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
             BufferedReader in = new BufferedReader(isr);
             DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(datumWriter))
        {
            writer.create(schema, outputFile);

            String line = null;
            while ((line = in.readLine()) != null)
            {
                Map<String,Object> data = mapper.readValue(line, HashMap.class);

                logger.trace("processing record: {}", data);

                // as example code, I want to make the separation between extracting the field values
                // and writing them explicit, to show the transformations that happen at each point

                // string values are simple

                String eventType = (String)data.get("eventType");
                String eventId = (String)data.get("eventId");
                String userId = (String)data.get("userId");

                // the timestamp value is stored as a string, so needs to be parsed

                Instant timestamp = Instant.from(TIMESTAMP_PARSER.parse(((String)data.get("timestamp"))));

                // the number of items in the cart is some form of Number, so we extract the integer value and auto-box

                Integer itemsInCart = ((Number)data.get("itemsInCart")).intValue();

                // Jackson may read the totalValue column as a BigDecimal or arbitrary integer type (int, long, or
                // BigInteger). Rather than try to figure out what it read, we just convert to a string and then
                // parse that string and set the desired scale. In case we find a value with more digits than we
                // can use, we'll perform banker's rounding when setting the scale.

                BigDecimal totalValue = new BigDecimal(data.get("totalValue").toString())
                                        .setScale(2, RoundingMode.HALF_EVEN);

                logger.trace("eventType = {}, eventId = {}, userId = {}, timestamp = {}, itemsInCart = {}, totalValue = {}",
                             eventType, eventId, userId, timestamp, itemsInCart, totalValue);

                // now it's straightforward to write those values to the Avro record

                GenericRecord datum = new GenericData.Record(schema);
                datum.put("eventType",  eventType);
                datum.put("eventId",    eventId);
                datum.put("userId",     userId);
                datum.put("itemsInCart", itemsInCart);
                datum.put("timestamp",  TIMESTAMP_CONVERTER.toLong(timestamp, schema, timestampLT));
                datum.put("totalValue", DECIMAL_CONVERTER.toBytes(totalValue, schema, totalValueLT));
                writer.append(datum);
            }
        }
    }
}
