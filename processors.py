import json
from datetime import datetime
from decimal import Decimal
import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from bigquery_schema_generator.generate_schema import SchemaGenerator

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
        print(element)
        yield element

class ParseSchema(beam.DoFn):
    def __init__(self):
        return None

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

    def process(self, element):
        schema_generator = SchemaGenerator()
        schema_map, error = schema_generator.deduce_schema(
            [json.dumps(element, default=self.default_json_serializer)]
        )
        schema = schema_generator.flatten_schema(schema_map)
        table_schema = bigquery.TableSchema()
        for field in schema:
            schema_field = bigquery.TableFieldSchema()
            schema_field.mode = field['mode']
            schema_field.name= field['name']
            schema_field.type = field['type']
            table_schema.fields.append(schema_field)
        yield table_schema


class BigQueryWriter(beam.PTransform):
    def __init__(self, table_spec, method, partition_field, schema_side_input=None):
        self.table_spec = table_spec
        self.partition_field = partition_field
        self.schema_side_input = schema_side_input
        match method:
            case "batch_load":
                self.method = beam.io.WriteToBigQuery.Method.FILE_LOADS
            case "storage_api":
                self.method = beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API

        pass

    def schema_callable(self,destination, schema_side_input):
        print(destination)
        print(schema_side_input)
        schemas = list(schema_side_input)
        print(schemas[0])
        
        return schemas[0]

    def expand(self, pcoll):
        return pcoll | "Write to BQ" >> beam.io.WriteToBigQuery(
            table=self.table_spec,
            schema="SCHEMA_AUTODETECT",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method=self.method,
            # schema_side_inputs=(self.schema_side_input,),
            temp_file_format="NEWLINE_DELIMITED_JSON",
            # additional_bq_parameters={
            #     "timePartitioning": {"type": "DAY", "field": f"{self.partition_field}"}
            # },
        )



