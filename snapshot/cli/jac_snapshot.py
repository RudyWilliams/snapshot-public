import os
from snapshot.analysis import analysis, demographics
from snapshot.config import app_config
import snapshot.foundation.fetch_files as ff
import snapshot.utilities.dates as sud


def jac(start_dt, end_dt, production):

    folder = sud.end_dt_to_folder(end_dt=end_dt)
    root = app_config.production_root if production else app_config.dev_root

    pre_calc_age_column = app_config.jac_pre_calc_age_column

    if pre_calc_age_column is not None:
        print("***Attempting to use pre-calculated ages from data source")

    jac_df = ff.ExcelRead(
        filename=app_config.jac_filename,
        dir_=os.path.join(root, folder),
        parse_dates=app_config.jac_parse_dates,
        skiprows=None,
    ).fetch_excel()

    ja = (
        analysis.JACAnalysis(
            prog_df=jac_df,
            served_dt_col_name=app_config.jac_served_dt_column,
            rp_start_dt=start_dt,
            end_dt=end_dt,
            county_contract_start_dt=None,  # not implemented yet
        )
        .set_analysis_dfs()
        .set_subset_dfs(id_col=app_config.jac_id_column)
    )

    num_served_rp = ja.return_num_served(period="rp", subset="all")
    num_served_month = ja.return_num_served(period="month", subset="all")
    num_cc_served_rp = ja.return_num_served(period="rp", subset="cc")
    num_cc_served_month = ja.return_num_served(period="month", subset="cc")
    num_ntr_served_rp = ja.return_num_served(period="rp", subset="ntr")
    num_ntr_served_month = ja.return_num_served(period="month", subset="ntr")

    rp_all_ref_source_df = ja.groupby_referral_source(
        period="rp",
        subset="all",
        group_on=app_config.jac_referral_source_column,
        agg_on=app_config.jac_served_dt_column,
    )

    rp_ntr_ref_source_df = ja.groupby_referral_source(
        period="rp",
        subset="ntr",
        group_on=app_config.jac_referral_source_column,
        agg_on=app_config.jac_served_dt_column,
    )

    rp_cc_ref_source_df = ja.groupby_referral_source(
        period="rp",
        subset="cc",
        group_on=app_config.jac_referral_source_column,
        agg_on=app_config.jac_served_dt_column,
    )

    # there are no "intakes" and "exits" in the same sense as the other programs
    served_df = ja.rp_served_df
    served_ntr_df = ja.rp_ntr_served_df
    served_cc_df = ja.rp_cc_served_df

    age_demos = demographics.AgeDemographics(
        input_df=served_df,
        birthdt_col_name=app_config.jac_birth_dt_column,
        reference_col_name=app_config.jac_served_dt_column,
        agg_on=app_config.jac_id_column,
        bins=app_config.jac_age_bins,
    ).create_ages_groupby_df(pre_calc_age_column=pre_calc_age_column)

    gender_demos = demographics.GenderDemographics(
        input_df=served_df,
        group_on=app_config.jac_gender_column,
        agg_on=app_config.jac_id_column,
    ).create_genders_groupby_df()

    race_demos = demographics.RaceDemographics(
        input_df=served_df,
        group_on=app_config.jac_race_column,
        agg_on=app_config.jac_id_column,
    ).create_races_groupby_df(
        race_col=app_config.jac_race_column, also_nan=["~", "na", "nan"]
    )

    ntr_age_demos = demographics.AgeDemographics(
        input_df=served_ntr_df,
        birthdt_col_name=app_config.jac_birth_dt_column,
        reference_col_name=app_config.jac_served_dt_column,
        agg_on=app_config.jac_id_column,
        bins=app_config.jac_age_bins,
    ).create_ages_groupby_df(pre_calc_age_column=pre_calc_age_column)

    ntr_gender_demos = demographics.GenderDemographics(
        input_df=served_ntr_df,
        group_on=app_config.jac_gender_column,
        agg_on=app_config.jac_id_column,
    ).create_genders_groupby_df()

    ntr_race_demos = demographics.RaceDemographics(
        input_df=served_ntr_df,
        group_on=app_config.jac_race_column,
        agg_on=app_config.jac_id_column,
    ).create_races_groupby_df(
        race_col=app_config.jac_race_column, also_nan=["~", "na", "nan"]
    )

    cc_age_demos = demographics.AgeDemographics(
        input_df=served_cc_df,
        birthdt_col_name=app_config.jac_birth_dt_column,
        reference_col_name=app_config.jac_served_dt_column,
        agg_on=app_config.jac_id_column,
        bins=app_config.jac_age_bins,
    ).create_ages_groupby_df(pre_calc_age_column=pre_calc_age_column)

    cc_gender_demos = demographics.GenderDemographics(
        input_df=served_cc_df,
        group_on=app_config.jac_gender_column,
        agg_on=app_config.jac_id_column,
    ).create_genders_groupby_df()

    cc_race_demos = demographics.RaceDemographics(
        input_df=served_cc_df,
        group_on=app_config.jac_race_column,
        agg_on=app_config.jac_id_column,
    ).create_races_groupby_df(
        race_col=app_config.jac_race_column, also_nan=["~", "na", "nan"]
    )

    return {
        "served-rp": num_served_rp,
        "served-ntr-rp": num_ntr_served_rp,
        "served-cc-rp": num_cc_served_rp,
        "served-month": num_served_month,
        "served-ntr-month": num_ntr_served_month,
        "served-cc-month": num_cc_served_month,
        "referral-source": rp_all_ref_source_df,
        "referral-source-ntr": rp_ntr_ref_source_df,
        "referral-source-cc": rp_cc_ref_source_df,
        "age-demographics": age_demos,
        "gender-demographics": gender_demos,
        "race-demographics": race_demos,
        "ntr-age-demographics": ntr_age_demos,
        "ntr-gender-demographics": ntr_gender_demos,
        "ntr-race-demographics": ntr_race_demos,
        "cc-age-demographics": cc_age_demos,
        "cc-gender-demographics": cc_gender_demos,
        "cc-race-demographics": cc_race_demos,
    }


def jac_cli():

    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser(
        description="Use to run analysis that appears on the JAC snapshot"
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
    for key, value in jac(start, end, production=production).items():
        print(f"{key}:\n {value}")
        print("------------")


if __name__ == "__main__":
    jac_cli()
