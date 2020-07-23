import os
import snapshot.analysis.analysis as analysis
import snapshot.analysis.demographics as demographics
from snapshot.config import app_config
from snapshot.cli.jac_snapshot import jac
from snapshot.cli.nonres_snapshot import nonres
from snapshot.cli.shelter_snapshot import shelter
from snapshot.cli.snap_snapshot import snap
from snapshot.cli.tlp_snapshot import tlp
import snapshot.foundation.fetch_files as ff


def run_all(start_dt, end_dt, production):
    # an issue with this implementation will be that the loads (for the same data)
    # occur more than once...
    # regardless the program will be much faster than a human analyst

    snapshots = (shelter, nonres, snap, tlp, jac)
    all_response = dict()

    for func in snapshots:
        response = func(start_dt, end_dt, production)
        all_response[func.__name__] = response

    return all_response


def all_cli():

    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser(description="Use to run analysis for all programs")

    parser.add_argument("start_date", type=lambda x: pd.Timestamp(x))
    parser.add_argument("end_date", type=lambda x: pd.Timestamp(x))
    parser.add_argument("--dev", action="store_true")

    args = parser.parse_args()
    start = args.start_date
    end = args.end_date
    production = True

    start_str = start.strftime("%m/%d/%Y")
    end_str = end.strftime("%m/%d/%Y")
    print(f"\nReport Period: {start_str} - {end_str}")

    if args.dev:
        print("---running in dev mode")
        production = False

    print()
    for prog_name, response_dict in run_all(start, end, production=production).items():
        name_len = len(prog_name)
        line = "--------------------"
        print(f"\n+{line}+")
        print(f"|{line[:4]}{prog_name.upper()}{line[4+name_len:]}|")
        print(f"+{line}+\n")
        for key, value in response_dict.items():
            print(f"{key}:\n {value}")
            print("------------")


if __name__ == "__main__":
    all_cli()
