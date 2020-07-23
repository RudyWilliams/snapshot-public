import os
import snapshot.analysis.analysis as analysis
import snapshot.analysis.demographics as demographics
from snapshot.config import app_config
import snapshot.foundation.fetch_files as ff
import snapshot.utilities.dates as sud


def nonres(start_dt, end_dt, production):
    folder = sud.end_dt_to_folder(end_dt=end_dt)
    root = app_config.production_root if production else app_config.dev_root

    vtr = ff.VTrimRead(
        fn_ylog=app_config.fn_ylog,
        fn_youth=app_config.fn_youth,
        fn_a_programs=app_config.fn_a_programs,
        dir_=os.path.join(root, folder),
    )
    dfs_dict = vtr.get_name_df_dictionary()

    # calculate month start here
    month_start_dt = sud.calc_month_start_from_end_dt(end_dt=end_dt)

    na = analysis.NonResAnalysis(
        df_dict=dfs_dict,
        rp_start_dt=start_dt,
        month_start_dt=month_start_dt,
        end_dt=end_dt,
    )
    na.merge_floored_base_dfs(
        intake_col=app_config.intake_dt_column,
        exit_col=app_config.exit_dt_column,
        ylog_youth_merge_on=app_config.youth_id_column,
        intermed_aprogs_merge_on=app_config.program_id_column,
    )
    na.set_analysis_dfs(
        prog_name_col=app_config.program_name_column,
        intra_program_list=app_config.nonres_program_names,
    )

    contract_end_dt = sud.calculate_fy_end_dt(fy_start_dt=start_dt)
    # print(contract_end_dt)
    served_rp = na.num_served_rp
    intakes_rp = na.num_intakes_rp
    exits_rp = na.num_exits_rp

    avg_los_weeks = na.calc_avg_los()

    na.set_month_analysis_dfs()

    served_month = na.num_served_month
    intakes_month = na.num_intakes_month
    exits_month = na.num_exits_month
    at_month_end = na.num_at_month_end

    (
        intake_benchmark,
        months_remain,
    ) = na.calc_intakes_per_remaining_months_and_months_left(
        contract_end_dt=contract_end_dt,
        contract_intake_req=app_config.nonres_intake_contract_req,
    )

    intakes_df = na.rp_intakes_df

    age_demos = demographics.AgeDemographics(
        input_df=intakes_df,
        birthdt_col_name=app_config.birth_dt_column,
        reference_col_name=app_config.intake_dt_column,
        agg_on=app_config.intake_dt_column,
        bins=app_config.nonres_age_bins,
    ).create_ages_groupby_df()

    gender_demos = demographics.GenderDemographics(
        input_df=intakes_df,
        group_on=app_config.gender_column,
        agg_on=app_config.intake_dt_column,
    ).create_genders_groupby_df()

    race_demos = demographics.RaceDemographics(
        input_df=intakes_df,
        group_on=None,  # is handled internally bc of ethnicity column
        agg_on=app_config.intake_dt_column,
    ).create_races_groupby_df(
        race_col=app_config.race_column,
        ethnic_col=app_config.ethnic_column,
        mapping=None,
    )

    return {
        "contract-benchmark-per-remaining-month": intake_benchmark,
        "contract-remaining-months": months_remain,
        "served-rp": served_rp,
        "intakes-rp": intakes_rp,
        "exits-rp": exits_rp,
        "avg-los-weeks": avg_los_weeks,
        "served-month": served_month,
        "intakes-month": intakes_month,
        "exits-month": exits_month,
        "at-month-end": at_month_end,
        "age-demographics": age_demos,
        "gender-demographics": gender_demos,
        "race-demographics": race_demos,
    }


def nonres_cli():
    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser(
        description="Use to run analysis that appears on the NonRes snapshot"
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
    for key, value in nonres(start, end, production=production).items():
        print(f"{key}:\n {value}")
        print("------------")


if __name__ == "__main__":
    nonres_cli()

