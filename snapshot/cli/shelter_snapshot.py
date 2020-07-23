import os
import snapshot.analysis.analysis as analysis
import snapshot.analysis.demographics as demographics
from snapshot.config import app_config
import snapshot.foundation.fetch_files as ff
import snapshot.utilities.dates as sud


def shelter(start_dt, end_dt, production):
    month_start_dt = sud.calc_month_start_from_end_dt(end_dt=end_dt)
    folder = sud.end_dt_to_folder(end_dt=end_dt)
    root = app_config.production_root if production else app_config.dev_root

    vtr = ff.VTrimRead(
        fn_ylog=app_config.fn_ylog,
        fn_youth=app_config.fn_youth,
        fn_a_programs=app_config.fn_a_programs,
        dir_=os.path.join(root, folder),
    )
    dfs_dict = vtr.get_name_df_dictionary()

    sa = analysis.ShelterAnalysis(
        df_dict=dfs_dict,
        rp_start_dt=start_dt,
        month_start_dt=month_start_dt,
        end_dt=end_dt,
    )
    sa.merge_floored_base_dfs(
        intake_col=app_config.intake_dt_column,
        exit_col=app_config.exit_dt_column,
        ylog_youth_merge_on=app_config.youth_id_column,
        intermed_aprogs_merge_on=app_config.program_id_column,
    )
    sa.set_analysis_dfs(
        prog_name_col=app_config.program_name_column,
        intra_program_list=app_config.shelter_program_names,
    )

    num_served_month = sa.calc_num_served_month()
    num_served_rp = sa.num_served_rp
    num_intakes_rp = sa.num_intakes_rp
    num_exits_rp = sa.num_exits_rp

    caredays = sa.create_caredays_per_program_df()
    month_intakes_pp = sa.create_month_intakes_per_program_df()
    served_pp = sa.create_rp_served_per_program_df()
    avglos_pp = sa.create_avg_los_per_program_df()

    # demographics
    intake_df = sa.rp_intakes_df

    age_demos = demographics.AgeDemographics(
        input_df=intake_df,
        birthdt_col_name=app_config.birth_dt_column,
        reference_col_name=app_config.intake_dt_column,
        agg_on=app_config.intake_dt_column,
        bins=app_config.shelter_age_bins,
    ).create_ages_groupby_df()

    gender_demos = demographics.GenderDemographics(
        input_df=intake_df,
        group_on=app_config.gender_column,
        agg_on=app_config.intake_dt_column,
    ).create_genders_groupby_df()

    race_demos = demographics.RaceDemographics(
        input_df=intake_df,
        group_on="irrelevant",  # this gets overridden bc ethnic col present
        agg_on=app_config.intake_dt_column,
    ).create_races_groupby_df(
        race_col=app_config.race_column,
        ethnic_col=app_config.ethnic_column,
        mapping=None  # this is for if the human entered datasets are needing consistency within
        # the letter to race mapping is done internally (may be better to bring out)
    )

    return {
        "month-served": num_served_month,
        "served": num_served_rp,
        "intakes": num_intakes_rp,
        "exits": num_exits_rp,
        "caredays": caredays,
        "month-intakes-per-program": month_intakes_pp,
        "served-per-program": served_pp,
        "avglos-per-program": avglos_pp,
        "age-demographics": age_demos,
        "gender-demographics": gender_demos,
        "race-demographics": race_demos,
    }


def shelter_cli():

    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser(
        description="Use to run analysis that appears on the Shelter snapshot"
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
    for key, value in shelter(start, end, production=production).items():
        print(f"{key}:\n {value}")
        print("------------")


if __name__ == "__main__":
    shelter_cli()
