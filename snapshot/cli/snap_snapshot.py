import os
import snapshot.analysis.analysis as analysis
import snapshot.analysis.demographics as demographics
from snapshot.config import app_config
import snapshot.foundation.fetch_files as ff
import snapshot.utilities.dates as sud


def snap(start_dt, end_dt, production):
    folder = sud.end_dt_to_folder(end_dt=end_dt)
    root = app_config.production_root if production else app_config.dev_root

    vtr = ff.VTrimRead(
        fn_ylog=app_config.fn_ylog,
        fn_youth=app_config.fn_youth,
        fn_a_programs=app_config.fn_a_programs,
        dir_=os.path.join(root, folder),
    )
    dfs_dict = vtr.get_name_df_dictionary()

    sna = analysis.SNAPAnalysis(df_dict=dfs_dict, rp_start_dt=start_dt, end_dt=end_dt)
    sna.merge_floored_base_dfs(
        intake_col=app_config.intake_dt_column,
        exit_col=app_config.exit_dt_column,
        ylog_youth_merge_on=app_config.youth_id_column,
        intermed_aprogs_merge_on=app_config.program_id_column,
    )
    sna.set_analysis_dfs(
        prog_name_col=app_config.program_name_column,
        intra_program_list=app_config.snap_program_names,
    )

    num_served_rp = sna.num_served_rp
    num_intakes_rp = sna.num_intakes_rp
    num_exits_rp = sna.num_exits_rp

    intakes_df = sna.rp_intakes_df
    age_demos = demographics.AgeDemographics(
        input_df=intakes_df,
        birthdt_col_name=app_config.birth_dt_column,
        reference_col_name=app_config.intake_dt_column,
        agg_on=app_config.intake_dt_column,
        bins=app_config.snap_age_bins,
    ).create_ages_groupby_df()

    gender_demos = demographics.GenderDemographics(
        input_df=intakes_df,
        group_on=app_config.gender_column,
        agg_on=app_config.intake_dt_column,
    ).create_genders_groupby_df()

    race_demos = demographics.RaceDemographics(
        input_df=intakes_df, group_on=None, agg_on=app_config.intake_dt_column
    ).create_races_groupby_df(
        race_col=app_config.race_column,
        ethnic_col=app_config.ethnic_column,
        mapping=None,
    )

    return {
        "served-rp": num_served_rp,
        "intakes-rp": num_intakes_rp,
        "exits-rp": num_exits_rp,
        "age-demographics": age_demos,
        "gender-demographics": gender_demos,
        "race-demographics": race_demos,
    }


def snap_cli():
    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser(
        description="Use to run analysis that appears on the SNAP snapshot"
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
    for key, value in snap(start, end, production=production).items():
        print(f"{key}:\n {value}")
        print("------------")


if __name__ == "__main__":
    snap_cli()
