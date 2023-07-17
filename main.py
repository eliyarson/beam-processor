import argparse
from submiters import DataFlowSubmitter

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
    parser.add_argument(
        "--source-format", type=str, required=True, help="Source format"
    )
    parser.add_argument("--table-spec", type=str, required=True, help="Table path")
    parser.add_argument(
        "--partition-field", type=str, required=False, help="Table partition field"
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
