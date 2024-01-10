#!/usr/bin/env python3

import pandas as pd
import fastparquet
import os
from os import path
import sys
from typing import List, Tuple

"""
Split an existing parquet file into smaller chunks based on 
a specific where clause.
"""


def get_next_argument(arguments: List[str], key) -> str:
    target_index = arguments.index(key) + 1
    return arguments[target_index]


def parse_arguments(arguments: List[str]) -> Tuple[str, str, str, int]:
    if "--source" in arguments:
        source_path = get_next_argument(arguments, "--source")
    elif "-s" in arguments:
        source_path = get_next_argument(arguments, "-s")
    else:
        raise KeyError(
            "The parameter --source (or -s) has to be provided to the script!"
        )

    if "--target" in arguments:
        target_dir = get_next_argument(arguments, "--target")
    elif "-t" in arguments:
        target_dir = get_next_argument(arguments, "-t")
    else:
        target_dir = "."

    if "--key" in arguments:
        key = get_next_argument(arguments, "--key")
    elif "-k" in arguments:
        key = get_next_argument(arguments, "-k")
    else:
        key = "hash"

    if "--digits" in arguments:
        digits = int(get_next_argument(arguments, "--digits"))
    elif "-d" in arguments:
        digits = int(get_next_argument(arguments, "-d"))
    else:
        digits = 1

    if not (path.exists(source_path) and source_path.endswith(".parquet")):
        raise ValueError(
            "The source file path has to point to an existing parquet file."
        )

    return source_path, target_dir, key, digits


def display_help():
    help_text = f"""
    Split an existing parquet file into subfiles based on a specified key and resolution.

    Usage:
        python split.py --help
        python split.py --source <file_path> --target <directory_path> --key <column_name> --digits <1>
    
    Arguments:
        --source or -s:{"":>4} The source parquet file to be split.
        --target or -t:{"":>4} If specified, the target folder path where the created files shall be stored. If not specified, "." is assumed.
        --key or -k:   {"":>4} The column name on which the partitioning shall be executed.If not specified, "hash" is used as default.
        --digits or -d:{"":>4} The desired resolution. Creates 10^digits new files. If not specified, 1 is used as default.
    """

    print(help_text)


def extract_file_name(path: str) -> str:
    return path.split("/")[-1]


def main():
    argv = sys.argv[1:]

    if len(argv) < 1:
        raise Exception("No arguments provided!")

    if argv[0] in ["--help", "-h"]:
        display_help()
        return

    source_path, target_dir, key, digits = parse_arguments(argv)

    source_file_name = extract_file_name(source_path)

    if not path.exists(target_dir):
        os.makedirs(target_dir)

    df = pd.read_parquet(source_path)

    # Need basis factor, to avoid putting numbers with
    # different length but same starting digit into the
    # same bucket.
    basis_factor = 10 ** (len(str(df[key].max())) - digits)

    for i in range(10**digits):
        if i == 0:
            value_df = df.loc[
                (df[key] >= i * basis_factor) & (df[key] <= (i + 1) * basis_factor)
            ]
        else:
            value_df = df.loc[
                (df[key] > i * basis_factor) & (df[key] <= (i + 1) * basis_factor)
            ]

        if len(value_df) == 0:
            continue

        target_path = f"{target_dir}/{source_file_name.replace('.parquet', '_' + str(i) + '.parquet')}"
        value_df.to_parquet(target_path)


if __name__ == "__main__":
    main()
