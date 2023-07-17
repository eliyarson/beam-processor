import json
from datetime import datetime
from decimal import Decimal
import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.internal.gcp.json_value import to_json_value
from bigquery_schema_generator.generate_schema import SchemaGenerator
from typing import Dict

class NdJsonProcessor(beam.DoFn):
    def __init__(self):
        return None

    def process(self, batch):
        items = batch.split("\n")
        for item in items:
            js = json.loads(item)
            record = js[0]
            record["dataflow_ingested_at"] = datetime.utcnow()
            yield record

class CsvProcessorFn(beam.DoFn):
    def process(self, element):
        record = element._asdict()
        record["dataflow_ingested_at"] = datetime.utcnow()
        yield record

class BigQueryWriter(beam.PTransform):
    def __init__(self, table_spec, method, partition_field=None, schema_side_input=None):
        self.table_spec = table_spec
        self.partition_field = partition_field if partition_field else "dataflow_ingested_at"
        self.schema_side_input = schema_side_input
        self.additional_bq_parameters = {}
        self.additional_bq_parameters["timePartitioning"] =  {"type": "DAY", "field": f"{self.partition_field}"}
        match method:
            case "batch_load":
                self.method = beam.io.WriteToBigQuery.Method.FILE_LOADS
            case "storage_api":
                self.method = beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API

        pass

    def expand(self, pcoll):
        return pcoll | "Write to BQ" >> beam.io.WriteToBigQuery(
            table=self.table_spec,
            schema="SCHEMA_AUTODETECT",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method=self.method,
            temp_file_format="NEWLINE_DELIMITED_JSON",
            additional_bq_parameters=self.additional_bq_parameters
        )

class AddSchemaFn(beam.DoFn):
    def __init__(self):
        pass

    def start_bundle(self):
        self.schema_generator = SchemaGenerator()
        self.table_schema = bigquery.TableSchema()

    @staticmethod
    def default_json_serializer(obj):
        if isinstance(obj, datetime):
            try:
                return_obj = obj.strftime("%Y-%m-%dT%H:%M:%SZ")
                return return_obj
            except:
                return ""
        elif isinstance(obj, Decimal):
            return float(obj)

    def process(self, element: Dict[str, str]):
        schema_map, error = self.schema_generator.deduce_schema(
            [json.dumps(element, default=self.default_json_serializer)]
        )
        schema = self.schema_generator.flatten_schema(schema_map)
        for field in schema:
            schema_field = bigquery.TableFieldSchema()
            schema_field.mode = field['mode']
            schema_field.name= field['name']
            schema_field.type = field['type']
            self.table_schema.fields.append(schema_field)
        yield self.table_schema