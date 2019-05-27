package com.pallavtyagi;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class WriteToParquet {

    /**
     *
     {
     "eventId": "",
     "system": "",
     "dateStart": "",
     "dateEnd": "",
     "totalIds": "",
     "totalMissing":"",
     "missingEvents": [
     {
     "id":"",
     "type":"",
     "version":""
     "receivedAt":""
     },
     {
     "id":"",
     "type":"",
     "version":""
     "receivedAt":""
     }

     ]

     }
     *
     *
     *
     */


    public static void main(String[] s) {
            // First thing - parse the schema as it will be used
            Schema schema = parseSchema();
            List<GenericData.Record> recordList = getRecords(schema);
            writeToParquet(recordList, schema);
        }

        private static Schema parseSchema() {
            Schema.Parser parser = new    Schema.Parser();
            Schema schema = null;
            try {
                // pass path to schema
                File file = new File("data/EventSummary.avsc");
                InputStream is = new FileInputStream(file);
                schema = parser.parse(is);

            } catch (IOException e) {
                e.printStackTrace();
            }
            return schema;

        }

        private static List<GenericData.Record> getRecords(Schema schema){
            List<GenericData.Record> recordList = new ArrayList<GenericData.Record>();
            GenericData.Record record = new GenericData.Record(schema);
            // Adding 2 records
            record.put("eventId", "1");
            record.put("system", "system1");
            record.put("dateStart", "2019-01-01");
            record.put("dateEnd", "2019-01-02");
            record.put("totalIds", "102");
            record.put("totalMissing", "2");

            recordList.add(record);
            return recordList;
        }

        // {"id": "1", "type": "test", "version":"1", "receivedAt": "2019-01-01"}

        private static void writeToParquet(List<GenericData.Record> recordList, Schema schema) {
            // Path to Parquet file in HDFS
            Path path = new Path("/home/pallav/Documents/workspace/code/spark-s3/data/EventSummary3.parquet");
            ParquetWriter<GenericData.Record> writer = null;
            // Creating ParquetWriter using builder
            try {
                writer = AvroParquetWriter.
                        <GenericData.Record>builder(path)
                        .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                        .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                        .withSchema(schema)
                        .withConf(new Configuration())
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withValidation(false)
                        .withDictionaryEncoding(false)
                        .build();

                for (GenericData.Record record : recordList) {
                    writer.write(record);
                }

            }catch(IOException e) {
                e.printStackTrace();
            }finally {
                if(writer != null) {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

