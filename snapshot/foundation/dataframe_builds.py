import numpy as np
import pandas as pd
from snapshot.preproccess import clean


class MergeAnalysisBase:
    def __init__(self, df_dict, rp_start_dt, end_dt):
        self.df_dict = df_dict
        self.rp_start_dt = rp_start_dt
        self.end_dt = end_dt

    def merge_floored_base_dfs(
        self, intake_col, exit_col, ylog_youth_merge_on, intermed_aprogs_merge_on
    ):

        df = self.df_dict["y_log"].copy()
        floor_df = clean.floor_dt_columns(df, intake_col, exit_col)
        intake_constraint = floor_df[intake_col] <= self.end_dt
        rp_exit_constraint = floor_df[exit_col] >= self.rp_start_dt
        open_cases = floor_df[exit_col].isna()

        _ylog_rp_served_df = floor_df.loc[
            (intake_constraint) & (rp_exit_constraint | open_cases), :
        ].reset_index(drop=True)

        _youth_df = self.df_dict["youth"].copy()

        _a_programs_df = self.df_dict["a_programs"].copy()

        served_df = _ylog_rp_served_df.merge(
            _youth_df, on=[ylog_youth_merge_on], how="inner"
        )
        served_df = served_df.merge(
            _a_programs_df, on=[intermed_aprogs_merge_on], how="inner"
        )

        self.merged_served_df = served_df
        self.intake_col = intake_col
        self.exit_col = exit_col

        return self

    def set_analysis_dfs(self, prog_name_col, intra_program_list):
        """intake and exit dts are cleaned as returned"""

        merged_served_df = self.merged_served_df.copy()

        in_programs = merged_served_df[prog_name_col].isin(intra_program_list)
        merged_served_prog_df = merged_served_df.loc[in_programs, :].copy()

        self.rp_served_df = merged_served_prog_df

        intakes_in_rp = merged_served_prog_df[self.intake_col] >= self.rp_start_dt
        self.rp_intakes_df = merged_served_prog_df.loc[intakes_in_rp, :].copy()

        exits_in_rp = merged_served_prog_df[self.exit_col] <= self.end_dt
        self.rp_exits_df = merged_served_prog_df.loc[exits_in_rp, :]
        self.prog_name_col = prog_name_col

        return self

    # only available after the served, intakes, and exits have been called
    @property
    def num_served_rp(self):
        return len(self.rp_served_df)

    @property
    def num_intakes_rp(self):
        return len(self.rp_intakes_df)

    @property
    def num_exits_rp(self):
        return len(self.rp_exits_df)


class AnalysisBase:
    def __init__(
        self, prog_df, intake_col_name, exit_col_name, dob_col_name, rp_start_dt, end_dt
    ):
        self.prog_df = prog_df
        self.intake_col_name = intake_col_name
        self.exit_col_name = exit_col_name
        self.rp_start_dt = rp_start_dt
        self.end_dt = end_dt

    def set_analysis_dfs(self):
        self.rp_served_df = self._create_rp_served_df()
        self.rp_intakes_df = self._create_rp_intakes_df()
        self.rp_exits_df = self._create_rp_exits_df()
        return self

    def _create_rp_served_df(self):
        df = self.prog_df.copy()
        _intake_col = self.intake_col_name
        _exit_col = self.exit_col_name
        floor_df = clean.floor_dt_columns(df, _intake_col, _exit_col)
        intake_constraint = floor_df[_intake_col] <= self.end_dt
        rp_exit_constraint = floor_df[_exit_col] >= self.rp_start_dt
        open_cases = floor_df[_exit_col].isna()

        return_df = floor_df.loc[
            (intake_constraint) & (rp_exit_constraint | open_cases), :
        ].reset_index(drop=True)
        return return_df

    def _create_rp_intakes_df(self):
        df = self.rp_served_df.copy()
        intakes_in_rp = df[self.intake_col_name] >= self.rp_start_dt
        return df.loc[intakes_in_rp, :]

    def _create_rp_exits_df(self):
        # served already has exit_dt >= rp_start_dt
        df = self.rp_served_df.copy()
        exits_in_rp = df[self.exit_col_name] <= self.end_dt
        return df.loc[exits_in_rp, :]

    @property
    def num_served_rp(self):
        return len(self.rp_served_df)

    @property
    def num_intakes_rp(self):
        return len(self.rp_intakes_df)

    @property
    def num_exits_rp(self):
        return len(self.rp_exits_df)


def groupby_df_build(_df, group_on, agg_on, agg_func, also_nan=None):

    not_mean_aggfunc = (agg_func == "count") or (agg_func == "sum")
    mean_aggfunc = agg_func == "mean"

    if _df.empty:
        return pd.DataFrame({}, columns=[agg_func, "percent"])

    df = _df.copy()
    n_total = len(_df)

    df_trimmed = df.loc[:, [group_on, agg_on]]
    if also_nan is not None:
        df_trimmed = df_trimmed.replace(also_nan, np.nan)
    grpon_nans = df_trimmed[group_on].isna()
    n_nans = grpon_nans.sum()
    df_trimmed_cleaned = df_trimmed.loc[~grpon_nans, :]

    df_grouped = df_trimmed_cleaned.groupby(group_on).agg(agg_func)
    df_grouped.columns = [agg_func]

    if not_mean_aggfunc:
        df_grouped.loc["TOTAL", agg_func] = df_grouped[agg_func].sum()

    elif mean_aggfunc:
        df_grouped.loc["TOTAL_AVG", agg_func] = df_trimmed[agg_on].mean()
        df_grouped["mean"] = df_grouped["mean"].round(2)
    else:
        raise ValueError('Only "count", "sum", & "mean" available')

    if agg_func in ["count", "sum"]:
        df_grouped["percent"] = (
            df_grouped[agg_func] / df_grouped.loc["TOTAL", agg_func] * 100
        ).round(2)
    else:
        df_grouped["percent"] = np.nan

    df_grouped.loc["nan", "count"] = n_nans
    df_grouped.loc["nan", "percent"] = round(n_nans / n_total * 100.0, 2)

    return df_grouped


if __name__ == "__main__":

    pass
