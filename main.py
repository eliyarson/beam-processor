import argparse
import json
from argparse import Namespace
from datetime import datetime
from decimal import Decimal

import apache_beam as beam
from bigquery_schema_generator.generate_schema import SchemaGenerator


class Processor(beam.DoFn):
    def __init__(self):
        return None

    def process(self, batch):
        items = batch.split("\n")
        for item in items:
            js = json.loads(item)
            record = js[0]
            record["dataflow_ingested_at"] = datetime.utcnow()
            yield record


class ParseSchema(beam.DoFn):
    def __init__(self):
        return None

    def start_bundle(self):
        self.schema_generator = SchemaGenerator()

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
        schema_map, error = self.schema_generator.deduce_schema(
            [json.dumps(element, default=self.default_json_serializer)]
        )
        schema = self.schema_generator.flatten_schema(schema_map)
        yield schema


class BigQueryWriter(beam.PTransform):
    def __init__(self, table_spec, method, partition_field, schema_side_input):
        self.table_spec = table_spec
        self.partition_field = partition_field
        self.schema_side_input = schema_side_input
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
            schema_side_inputs=(self.schema_side_input,),
            temp_file_format="NEWLINE_DELIMITED_JSON",
            additional_bq_parameters={
                "timePartitioning": {"type": "DAY", "field": f"{self.partition_field}"}
            },
        )


class DataFlowSubmitter(object):
    def __init__(self, args: Namespace):
        self.project = args.project
        self.bucket = args.bucket
        self.input_path = args.input_path
        self.table_spec = args.table_spec
        self.partition_field = args.partition_field
        self.docker_image_path = args.docker_image_path

        if args.direct_runner and args.dataflow_runner:
            raise ValueError(
                "Please specify only one of the options. either direct runner or dataflow runner"
            )

        self.runner = "DirectRunner"
        if args.dataflow_runner:
            self.runner = "DataFlowRunner"

    def build_and_run(self):
        argv = [
            f"--project={self.project}",
            f'--job_name=text-parsing-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
            f"--staging_location=gs://{self.bucket}/text_parsing/staging/",
            f"--temp_location=gs://{self.bucket}/text_parsing/temp/",
            "--region=us-central1",
            "--experiments=use_runner_v2",
            f"--sdk_container_image={self.docker_image_path}",
            "--sdk_location=container",
            f"--runner={self.runner}",
        ]
        with beam.Pipeline(argv=argv) as pipeline:
            source = (
                pipeline
                | "Read text file" >> beam.io.ReadFromText(self.input_path)
                | "Process" >> beam.ParDo(Processor())
            )
            schema = source | "Parse schema" >> beam.ParDo(ParseSchema())

            source | "Write to BQ" >> BigQueryWriter(
                table_spec=self.table_spec,
                method="batch_load",
                partition_field=self.partition_field,
                schema_side_input=beam.pvalue.AsIter(schema),
            )


def main():
    parser = argparse.ArgumentParser(
        description="Running Apache Beam pipelines on Dataflow"
    )
    parser.add_argument("--project", type=str, required=True, help="Project id")
    parser.add_argument(
        "--bucket",
        type=str,
        required=True,
        help="Name of the bucket to host dataflow components",
    )
    parser.add_argument(
        "--input-path", type=str, required=True, help="path to input data"
    )
    parser.add_argument("--table-spec", type=str, required=True, help="Table path")
    parser.add_argument(
        "--partition-field", type=str, required=True, help="Table partition field"
    )
    parser.add_argument(
        "--docker-image-path", type=str, required=True, help="Docker image path"
    )
    parser.add_argument("--direct-runner", required=False, action="store_true")
    parser.add_argument("--dataflow-runner", required=False, action="store_true")
    args = parser.parse_args()

    runner = DataFlowSubmitter(args=args)
    runner.build_and_run()


if __name__ == "__main__":
    main()
