import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd

# pylint: disable=import-error
from utilis import check_files_different


class JoinAndTransform(beam.DoFn):
    def process(self, element, table2_dict):
        table1_row = pd.DataFrame(
            [element.split(",")],
            columns=[
                "invoice_id",
                "legal_entity",
                "counter_party",
                "rating",
                "status",
                "value",
            ],
        )
        counter_party = table1_row["counter_party"].iloc[0]
        tier = table2_dict[counter_party]
        max_rating = table1_row["rating"].max()
        sum_value_arap = table1_row[table1_row["status"] == "ARAP"]["value"].sum()
        sum_value_accr = table1_row[table1_row["status"] == "ACCR"]["value"].sum()
        legal_entity = table1_row["legal_entity"].iloc[0]
        return [
            (
                legal_entity,
                counter_party,
                tier,
                max_rating,
                sum_value_arap,
                sum_value_accr,
            )
        ]


def run_pipeline():
    
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="This script Process two datasets containing financial data, \n \
        and performs various operations on them to generate a new output file. \n \
        The steps involved include loading data, adding tiers to the financial data, \n \
        filtering and summing data, merging tables, and writing the output to a file."
    )
    parser.add_argument(
        "dataset1_path",
        type=str,
        help="Type path to your first dataset in csv format containing financial data.",
    )
    parser.add_argument(
        "dataset2_path",
        type=str,
        help="Type path to your second dataset in csv format containing tier data.",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        help="Type path to output file, or use default result.csv file.",
        default="result.csv",
    )
    args = parser.parse_args()
    dataset1_path = args.dataset1_path
    dataset2_path = args.dataset2_path
    output_path = args.output_path
    
    # Checks if file paths are different and supported.
    check_files_different(dataset1_path, dataset2_path, output_path)

    with beam.Pipeline(options=PipelineOptions()) as p:
        # Read in Dataset 1 as a PCollection
        table1 = p | "Read Dataset 1" >> beam.io.ReadFromText(
            dataset1_path, skip_header_lines=1
        )

        # Read in Dataset 2 as a PCollection and convert to dictionary
        table2_dict = (
            p
            | "Read Dataset 2" >> beam.io.ReadFromText(dataset2_path, skip_header_lines=1)
            | "Convert Dataset 2 to Dictionary" >> beam.Map(lambda x: x.split(","))
            | "Create Dataset 2 Dictionary" >> beam.CombinePerKey(lambda rows: rows[-1])
            | "Convert Dictionary to PColl" >> beam.Map(lambda x: x[1])
        )

        # Join Dataset 1 and Dataset 2 and perform transformations
        joined_tables = table1 | "Join Dataset 1 and Dataset 2" >> beam.ParDo(
            JoinAndTransform(), beam.pvalue.AsDict(table2_dict)
        )
        # Write output to file
        output = (
            joined_tables
            | "Format Output" >> beam.Map(lambda x: ",".join([str(elem) for elem in x]))
            | "Write Output"
            >> beam.io.WriteToText(output_path, file_name_suffix=".csv")
        )


if __name__ == "__main__":
    run_pipeline()
