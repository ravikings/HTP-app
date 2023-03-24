"""
This module contains functions for working with files.

Functions:
- check_files_different: Checks if three file paths are different.
- main: Reads the contents of a file and do some manipulations.
- write_file: Writes a string to a file.
"""
import logging
import argparse
import pandas as pd


# pylint: disable=import-error
from utilis import check_files_different


def load_data(file_path: str, dtype: dict) -> pd.DataFrame:
    """Load data from a CSV file into a pandas DataFrame.

    Args:
        file_path (str): Path to the CSV file.
        dtype (dict): Data types of the columns in the CSV file.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the CSV data.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file_context:
            return pd.read_csv(file_context, dtype=dtype)
    except FileNotFoundError:
        logging.error("%s not found.", file_path)
        return pd.DataFrame()



def write_output(output: pd.DataFrame, output_path: str, mode: str = "w") -> None:
    """
    Writes the output DataFrame to a CSV file.

    Args:
        output (pd.DataFrame): The DataFrame to write.
        output_path (str): The path of the output file.

    Raises:
        TypeError: If the output argument is not a DataFrame.
        TypeError: If the output_path argument is not a string.

    Returns:
        None.
    """
    # Check the input arguments
    if not isinstance(output, pd.DataFrame):
        raise TypeError("The output argument must be a DataFrame")
    if not isinstance(output_path, str):
        raise TypeError("The output_path argument must be a string")

    logging.info("Writing output to %s...", output_path)
    with open(output_path, mode, encoding="utf-8") as file_context:
        output.to_csv(file_context, index=False)


def main():
    """Process financial datasets.

    This function loads two datasets containing financial and tier data,
    respectively, and performs various operations on them to generate a new
    output file. The steps involved include loading data, adding tiers to the
    financial data, filtering and summing data, merging tables, and writing the
    output to a file.

    Args:
        None

    Returns:
        None
    """
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

    # Set up logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Checks if file paths are different and supported.
    check_files_different(args.dataset1_path, args.dataset2_path, args.output_path)

    # Define data types
    dataset1_dtype = {"value": float, "rating": float}
    dataset2_dtype = {"counter_party": str, "tier": str}

    # Load data into DataFrames
    logging.info("Loading financial data from %s...", args.dataset1_path)
    table1 = load_data(args.dataset1_path, dataset1_dtype)
    logging.info("Loading tier data from %s...", args.dataset2_path)
    table2 = load_data(args.dataset2_path, dataset2_dtype)

    # Add tiers to table1 using map
    logging.info("Adding tiers to financial data...")
    table1["tier"] = table1["counter_party"].map(
        table2.set_index("counter_party")["tier"]
    )

    # Filter and sum data in table1
    logging.info("Filtering and summing financial data...")
    table1_arap = table1.loc[table1["status"] == "ARAP"]
    table1_accr = table1.loc[table1["status"] == "ACCR"]
    sum_arap = table1_arap.groupby(["legal_entity", "counter_party", "tier"]).agg(
        sum_value_arap=("value", "sum")
    )
    sum_accr = table1_accr.groupby(["legal_entity", "counter_party", "tier"]).agg(
        sum_value_accr=("value", "sum")
    )

    # Merge tables and add max rating
    logging.info("Merging tables and adding max rating...")
    merged_table = pd.merge(
        sum_arap, sum_accr, on=["legal_entity", "counter_party", "tier"], how="outer"
    )
    merged_table["max_rating"] = table1.groupby(
        ["legal_entity", "counter_party", "tier"]
    ).agg(max_rating=("rating", "max"))

    # Reset index and reorder columns
    logging.info("Resetting index and reordering columns...")
    output = merged_table.reset_index()[
        [
            "legal_entity",
            "counter_party",
            "tier",
            "max_rating",
            "sum_value_arap",
            "sum_value_accr",
        ]
    ]

    # Write output to file
    write_output(output, args.output_path)

    # Group the output by legal_entity, counter_party, and tier,
    # and calculate the sum of the sum_value_arap and sum_value_accr columns
    logging.info("calculating the total some output")
    output_totals = output.groupby(["legal_entity", "counter_party", "tier"])[
        ["sum_value_arap", "sum_value_accr"]
    ].sum()

    # Add a new row with the totals for each group
    output_with_totals = pd.concat(
        [
            output,
            output_totals.rename(
                columns={"sum_value_arap": "total_arap", "sum_value_accr": "total_accr"}
            )
            .reset_index()
            .assign(max_rating="", sum_value_arap="", sum_value_accr="")
            .groupby(["legal_entity", "counter_party", "tier"])
            .tail(1),
        ],
        ignore_index=True,
    )

    # Sort the output by legal_entity, counter_party, and tier
    output_with_totals = output_with_totals.sort_values(
        ["legal_entity", "counter_party", "tier", "max_rating"],
        ascending=[True, True, True, False],
    )

    logging.info("writing the total output to file")
    write_output(output_with_totals, args.output_path, "a")


if __name__ == "__main__":
    main()


# TO run the code: python main.py dataset1.csv dataset2.csv
