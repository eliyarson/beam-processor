from argparse import Namespace
from datetime import datetime
import json
import apache_beam as beam
from apache_beam.pvalue import _make_Row
from apache_beam.dataframe.io import read_csv
from apache_beam.dataframe.convert import to_pcollection
from processors import NdJsonProcessor, BigQueryWriter, CsvProcessorFn

class DataFlowSubmitter(object):
    def __init__(self, args: Namespace):
        self.project = args.project
        self.bucket = args.bucket
        self.input_path = args.input_path
        self.table_spec = args.table_spec
        self.partition_field = args.partition_field
        self.docker_image_path = args.docker_image_path
        self.source_format = args.source_format

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
            "--save_main_session",
            f"--runner={self.runner}",
        ]
        with beam.Pipeline(argv=argv) as pipeline:
            if self.source_format == "ndjson":
                pc = (
                    pipeline
                    | "Read text file" >> beam.io.ReadFromText(self.input_path)
                    | "Process" >> beam.ParDo(NdJsonProcessor())
                )
            elif self.source_format == "csv":
                source_df = (
                    pipeline
                    | "Read CSV" >> read_csv(self.input_path)
                )
                pc = to_pcollection(source_df)
                pc = pc | "Process" >> beam.ParDo(CsvProcessorFn())

            else:
                raise ValueError("Please Select a valid source format (ndjson, csv)")
            

            pc | "Write to BQ" >> BigQueryWriter(
                table_spec=self.table_spec,
                method="batch_load",
                partition_field=self.partition_field,
            )
