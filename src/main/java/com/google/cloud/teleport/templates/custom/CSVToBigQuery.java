package com.google.cloud.teleport.templates.custom;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.templates.SchemaParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CSVToBigQuery {

  public interface Options extends PipelineOptions {
    @Validation.Required
    @Description("The GCS location of the text you'd like to process")
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);

    @Validation.Required
    @Description("JSON file with BigQuery Schema description")
    ValueProvider<String> getBigQuerySchemaPath();

    void setBigQuerySchemaPath(ValueProvider<String> value);

    @Validation.Required
    @Description("Output table to write to")
    ValueProvider<String> getOutputTable();

    void setOutputTable(ValueProvider<String> value);

    @Validation.Required
    @Description("Delimiter")
    ValueProvider<String> getDelimiter();

    void setDelimiter(ValueProvider<String> value);

    @Validation.Required
    @Description("Temporary directory for BigQuery loading process")
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);

    @Validation.Required
    @Description("BiqQuery partition date")
    ValueProvider<String> getPartitionDate();

    void setPartitionDate(ValueProvider<String> directory);

    @Validation.Required
    @Description("BiqQuery partition date")
    ValueProvider<String> getHeader();

    void setHeader(ValueProvider<String> directory);
  }

  private static final Logger LOG = LoggerFactory.getLogger(CSVToBigQuery.class);

  private static final String BIGQUERY_SCHEMA = "BigQuery Schema";
  private static final String NAME = "name";
  private static final String TYPE = "type";
  private static final String MODE = "mode";

  private static final String FILE_NAME = "FILE_NAME";
  private static final String PARTITION_DATE = "PARTITION_DATE";

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);
    pipeline
      .apply("Read from source", TextIO.read().from(options.getInputFilePattern()))
      .apply("Clean data", ParDo.of(new CleaningParDo()))
      .apply(
         "Insert into Bigquery",
         BigQueryIO.writeTableRows()
           .withSchema(
             ValueProvider.NestedValueProvider.of(
               options.getBigQuerySchemaPath(),
               new SerializableFunction<String, TableSchema>() {

                 @Override
                 public TableSchema apply(String jsonPath) {
                   return getTableSchema(jsonPath);
                 }
               }))
           .to(options.getOutputTable())
             .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
             .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
             .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory()));
    pipeline.run();
  }

  public static TableSchema getTableSchema(String jsonPath) {

    TableSchema tableSchema = new TableSchema();
    List<TableFieldSchema> fields = new ArrayList<>();
    SchemaParser schemaParser = new SchemaParser();
    JSONObject jsonSchema;

    try {
      jsonSchema = schemaParser.parseSchema(jsonPath);
      JSONArray bqSchemaJsonArray = jsonSchema.getJSONArray(BIGQUERY_SCHEMA);

      for (int i = 0; i < bqSchemaJsonArray.length(); i++) {
        JSONObject inputField = bqSchemaJsonArray.getJSONObject(i);
        TableFieldSchema field = new TableFieldSchema()
          .setName(inputField.getString(NAME))
          .setType(inputField.getString(TYPE));

        if (inputField.has(MODE)) {
          field.setMode(inputField.getString(MODE));
        }

        fields.add(field);
      }
      tableSchema.setFields(fields);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return tableSchema;
  }

  public static class CleaningParDo extends DoFn<String, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {

      LOG.info("processing row : "+c.element());

      Options options = c.getPipelineOptions().as(Options.class);

      String cleanElement = c.element().replaceAll("\0", "");

      String delimiter = options.getDelimiter().get();
      String header = options.getHeader().get();
      if (cleanElement.equalsIgnoreCase(header)) {
        LOG.info("Skipping header : "+header);
        return;
      }
      int numberOfHeaderColumns = header.length() - header.replace(delimiter, "").length() + 1;
      int numberOfRowColumns = cleanElement.length() - cleanElement.replace(delimiter, "").length() + 1;
      if (numberOfRowColumns != numberOfHeaderColumns) {
        LOG.info("Not enough columns : "+cleanElement+ "("+numberOfRowColumns+" instead of "+numberOfHeaderColumns+")");
        return;
      }
      String[] rows = cleanElement.split(delimiter);
      String[] headers = header.split(delimiter);
      TableRow row = new TableRow();
      for (int i = 0; i < rows.length; i++) {
        row.set(headers[i], rows[i].replaceAll("^([0-9]+),([0-9]+)$", "$1.$2"));
      }
      row.set(FILE_NAME, options.getInputFilePattern().get());
      if (options.getPartitionDate().isAccessible()) row.set(PARTITION_DATE, options.getPartitionDate().get());
      c.output(row);
    }
  }
}