import json
from datetime import datetime
from decimal import Decimal
import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.internal.gcp.json_value import to_json_value
from bigquery_schema_generator.generate_schema import SchemaGenerator
from typing import Dict
import pyarrow as pa
import pandas as pd


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

class ParquetFn(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path
    def process(self,batch):
        print(batch)
        df = pd.DataFrame(data=batch)
        print(df)
        print(self.input_path)
        file_path = f"{self.input_path.split('*.csv')[0]}{df['dataflow_ingested_at'].max()}.parquet"
        file_path = file_path.replace("csv","parquet")
        print(file_path)
        df.to_parquet(path=file_path)


class ParquetWriter(beam.PTransform):
    def __init__(self,file_path_prefix):
        self.data_type_mapping = {
            'STRING': pa.string(),
            'BYTES': pa.string(),
            'INTEGER': pa.int64(),
            'NUMERIC': pa.decimal128(18, 2),
            'FLOAT': pa.float64(),
            'BOOLEAN': pa.bool_(),
            'TIMESTAMP': pa.timestamp(unit='s'),
            'DATE': pa.date64(),
            'DATETIME': pa.timestamp(unit='s'),
            'ARRAY': pa.list_(),
            'STRUCT': pa.struct()
        }
        self.file_path_prefix = file_path_prefix
        pass
    def expand(self,pcoll):
        return pcoll| "Write Parquet" >> beam.io.WriteToParquet(file_path_prefix=self.file_path_prefix)

class BigQueryWriter(beam.PTransform):
    def __init__(self, table_spec, method, side_input=None,partition_field=None, schema_side_input=None):
        self.table_spec = table_spec
        self.partition_field = partition_field if partition_field else "dataflow_ingested_at"
        self.schema_side_input = schema_side_input
        self.side_input = side_input
        self.additional_bq_parameters = {}
        self.additional_bq_parameters["timePartitioning"] =  {"type": "DAY", "field": f"{self.partition_field}"}
        match method:
            case "batch_load":
                self.method = beam.io.WriteToBigQuery.Method.FILE_LOADS
            case "storage_api":
                self.method = beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API

        pass

    def expand(self, pcoll, side_input=None):
        side_input = self.side_input
        print(side_input)
        # return pcoll | "Write to BQ" >> beam.io.WriteToBigQuery(
        #     table=self.table_spec,
        #     schema="SCHEMA_AUTODETECT",
        #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        #     method=self.method,
        #     temp_file_format="NEWLINE_DELIMITED_JSON",
        #     additional_bq_parameters=self.additional_bq_parameters
        # )
        return pcoll

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
        # for field in schema:
        #     schema_field = bigquery.TableFieldSchema()
        #     schema_field.mode = field['mode']
        #     schema_field.name= field['name']
        #     schema_field.type = field['type']
        #     self.table_schema.fields.append(schema_field)
        schema_fields = []
        for field in schema:
            schema_field = f"{field['name']}:{field['type']}"
            schema_fields.append(schema_field)
        sep = ','
        schema_str =sep.join(schema_fields)
        yield element