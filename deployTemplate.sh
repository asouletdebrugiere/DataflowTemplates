mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.custom.CSVToBigQuery \
-Dexec.args="--runner=DataflowRunner \
--project=datapipeline-redoute \
--stagingLocation=gs://dataflow-redoute/staging \
--templateLocation=gs://dataflow-redoute/templates/CSVToBigQueryTemplate"