import os
import snapshot.analysis.analysis as analysis
import snapshot.analysis.demographics as demographics
from snapshot.config import app_config
import snapshot.foundation.fetch_files as ff
import snapshot.utilities.dates as sud


def tlp(start_dt, end_dt, production):

    folder = sud.end_dt_to_folder(end_dt=end_dt)
    root = app_config.production_root if production else app_config.dev_root

    intake_col_name = app_config.tlp_intake_dt_column
    exit_col_name = app_config.tlp_exit_dt_column
    birthdt_col_name = app_config.tlp_birth_dt_column

    tlp_df = ff.ExcelRead(
        filename=app_config.tlp_filename,
        dir_=os.path.join(root, folder),
        parse_dates=app_config.tlp_parse_dates,
        skiprows=[app_config.tlp_skiprows],
    ).fetch_excel()

    ta = analysis.TLPAnalysis(
        prog_df=tlp_df,
        intake_col_name=intake_col_name,
        exit_col_name=exit_col_name,
        dob_col_name=birthdt_col_name,  # not sure if that's needed in this class
        rp_start_dt=start_dt,
        end_dt=end_dt,
    )
    ta.set_analysis_dfs()

    num_served_rp = (ta.num_served_rp,)
    num_intakes_rp = ta.num_intakes_rp
    num_exits_rp = ta.num_exits_rp
    num_month_end_all = ta.calculate_num_month_end(whole=True)
    alos_all = ta.calculate_avg_stay(
        short_stay=app_config.tlp_short_stay_delta, whole=True
    )
    all_util = ta.calculate_utilization(factor=12, whole=True)
    all_success_exits_cnt = ta.count_successful_exits("Type of exit", whole=True)

    ta.set_subset_dfs(subset_col="Class", subset="HUD")  # config it
    num_hud_served_rp = ta.num_subset_served_rp()
    num_hud_intakes_rp = ta.num_subset_intakes_rp()
    num_hud_exits_rp = ta.num_subset_exits_rp()
    alos_hud = ta.calculate_avg_stay(short_stay="7 days", whole=False)
    num_month_end_hud = ta.calculate_num_month_end(whole=False)
    hud_util = ta.calculate_utilization(factor=3, whole=False)
    hud_success_exits_cnt = ta.count_successful_exits("Type of exit", whole=False)

    ta.set_subset_dfs(subset_col="Class", subset="ACF")

    num_acf_intakes_rp = ta.num_subset_intakes_rp()
    num_acf_exits_rp = ta.num_subset_exits_rp()
    num_acf_served_rp = ta.num_subset_served_rp()
    alos_acf = ta.calculate_avg_stay("7 days", whole=False)
    num_month_end_acf = ta.calculate_num_month_end(whole=False)
    acf_util = ta.calculate_utilization(factor=9, whole=False)
    acf_success_exits_cnt = ta.count_successful_exits("Type of exit", whole=False)

    rp_intakes_df = ta.rp_intakes_df

    age_demos = demographics.AgeDemographics(
        input_df=rp_intakes_df,
        birthdt_col_name=birthdt_col_name,
        reference_col_name=intake_col_name,
        agg_on=intake_col_name,
        bins=app_config.tlp_age_bins,
    ).create_ages_groupby_df()

    gender_demos = demographics.GenderDemographics(
        input_df=rp_intakes_df,
        group_on=app_config.tlp_gender_column,
        agg_on=intake_col_name,
    ).create_genders_groupby_df()

    race_demos = demographics.RaceDemographics(
        input_df=rp_intakes_df,
        group_on=app_config.tlp_race_column,
        agg_on=intake_col_name,
    ).create_races_groupby_df(
        race_col=app_config.tlp_race_column,
        also_nan=["nan", "na", "~"],
        lowercase_first=True,
    )

    return {
        "number-served-rp": num_served_rp,
        "number-intakes-rp": num_intakes_rp,
        "number-exits-rp": num_exits_rp,
        "number-successes": all_success_exits_cnt,
        "alos": alos_all,
        "util": all_util,
        "number-month-end": num_month_end_all,
        "age-demos": age_demos,
        "gender-demos": gender_demos,
        "race-demos": race_demos,
        "number-served-hud": num_hud_served_rp,
        "number-hud-intakes_rp": num_hud_intakes_rp,
        "number-hud-exits-rp": num_hud_exits_rp,
        "number-hud-success": hud_success_exits_cnt,
        "hud-alos": alos_hud,
        "hud-util": hud_util,
        "number-hud-month-end": num_month_end_hud,
        "number-served-acf": num_acf_served_rp,
        "number-acf-intakes_rp": num_acf_intakes_rp,
        "number-acf-exits-rp": num_acf_exits_rp,
        "number-acf-success": acf_success_exits_cnt,
        "acf-alos": alos_acf,
        "acf-util": acf_util,
        "number-acf-month-end": num_month_end_acf,
    }


def tlp_cli():

    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser(
        description="Use to run analysis that appears on the TLP snapshot"
    )

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
    for key, value in tlp(start, end, production=production).items():
        print(f"{key}:\n {value}")
        print("------------")


if __name__ == "__main__":
    tlp_cli()
