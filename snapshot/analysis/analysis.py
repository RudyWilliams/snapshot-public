from collections import namedtuple
import pandas as pd
from snapshot.foundation.dataframe_builds import (
    MergeAnalysisBase,
    AnalysisBase,
    groupby_df_build,
)
from snapshot.preproccess import clean
import snapshot.utilities.dates as sud


class ShelterAnalysis(MergeAnalysisBase):
    def __init__(self, df_dict, rp_start_dt, month_start_dt, end_dt):
        super().__init__(df_dict, rp_start_dt, end_dt)
        self.month_start_dt = month_start_dt

    # only available after setting the analysis dfs
    # this makes exit_col, intake_col prog_name_col available as well

    def calc_num_served_month(self):
        month_exits = self.rp_served_df[self.exit_col] >= self.month_start_dt
        opens = self.rp_served_df[self.exit_col].isna()
        served_month_df = self.rp_served_df.loc[month_exits | opens, :]
        return len(served_month_df)

    def create_month_intakes_per_program_df(self):
        rp_intakes_df = self.rp_intakes_df.copy()
        mo_intakes_subset = rp_intakes_df[self.intake_col] >= self.month_start_dt
        mo_intakes_df = rp_intakes_df[mo_intakes_subset]
        count_per_prog_df = groupby_df_build(
            mo_intakes_df,
            group_on=self.prog_name_col,
            agg_on=self.intake_col,
            agg_func="count",
        )
        return count_per_prog_df

    def create_caredays_per_program_df(self):
        # create columns for calculating care days
        intake_col = self.intake_col
        exit_col = self.exit_col
        end = pd.to_datetime(self.end_dt)
        rp_start = pd.to_datetime(self.rp_start_dt)
        care_df = self.rp_served_df.copy()

        trunc_to_rp_start = care_df[intake_col] < rp_start
        trunc_to_end = care_df[exit_col] > end
        fill_as_end = care_df[exit_col].isna()

        care_df.loc[trunc_to_rp_start, intake_col] = rp_start
        care_df.loc[trunc_to_end | fill_as_end, exit_col] = end

        care_df["care_days"] = (
            care_df[exit_col] - care_df[intake_col] + pd.to_timedelta("1 day")
        )

        sum_per_prog_df = groupby_df_build(
            care_df, group_on=self.prog_name_col, agg_on="care_days", agg_func="sum"
        )
        sum_per_prog_df["sum"] = sum_per_prog_df["sum"].dt.days
        return sum_per_prog_df

    def create_rp_served_per_program_df(self):
        count_per_prog_df = groupby_df_build(
            self.rp_served_df,
            group_on=self.prog_name_col,
            agg_on=self.intake_col,
            agg_func="count",
        )
        return count_per_prog_df

    def create_avg_los_per_program_df(self):
        rp_exits_df = self.rp_exits_df.copy()
        rp_exits_df["length_of_stay"] = (
            rp_exits_df[self.exit_col] - rp_exits_df[self.intake_col]
        ).divide(pd.to_timedelta("1 day"))
        mean_per_prog_df = groupby_df_build(
            rp_exits_df,
            group_on=self.prog_name_col,
            agg_on="length_of_stay",
            agg_func="mean",
        )
        return mean_per_prog_df


class NonResAnalysis(MergeAnalysisBase):
    def __init__(self, df_dict, rp_start_dt, month_start_dt, end_dt):
        super().__init__(df_dict, rp_start_dt, end_dt)
        self.month_start_dt = month_start_dt

    # rp served, intakes, and exits done in base class

    def set_month_analysis_dfs(self):
        self.month_served_df = self._create_month_served_df()
        self.month_intakes_df = self._create_month_intakes_df()
        self.month_exits_df = self._create_month_exits_df()
        return self

    @property
    def num_served_month(self):
        return len(self.month_served_df)

    @property
    def num_intakes_month(self):
        return len(self.month_intakes_df)

    @property
    def num_exits_month(self):
        return len(self.month_exits_df)

    @property
    def num_at_month_end(self):
        return self.num_served_month - self.num_exits_month

    def calc_intakes_per_remaining_months_and_months_left(
        self, contract_end_dt, contract_intake_req
    ):
        """assumption is that the start of contract and RP will be the same"""
        contract_intakes = self.num_intakes_rp
        months_left = sud.num_months_togo(self.month_start_dt, contract_end_dt)
        intakes_left = contract_intake_req - contract_intakes
        if months_left == 0:
            return intakes_left, months_left
        ins_per_month = round(intakes_left / months_left, 1)
        return ins_per_month, months_left

    def _create_month_served_df(self):
        df = self.rp_served_df
        # intake constraint already considered in entire served
        month_exit = df[self.exit_col] >= self.month_start_dt
        month_open = df[self.exit_col].isna()
        return df.loc[(month_exit | month_open), :].copy()

    def _create_month_intakes_df(self):
        df = self.month_served_df.copy()
        intake_in_month = df[self.intake_col] >= self.month_start_dt
        return df.loc[intake_in_month, :].copy()

    def _create_month_exits_df(self):
        df = self.month_served_df.copy()
        exit_gt_mon_start = df[self.exit_col] >= self.month_start_dt
        exit_lt_end = df[self.exit_col] <= self.end_dt
        return df.loc[exit_gt_mon_start & exit_lt_end, :].copy()

    def calc_avg_los(self):
        exit_df = self.rp_exits_df.copy()
        exit_df["los_wks"] = (
            exit_df[self.exit_col] - exit_df[self.intake_col]
        ) / pd.Timedelta("7 days")
        return round(exit_df["los_wks"].mean(), 1)


class SNAPAnalysis(MergeAnalysisBase):
    pass  # SNAP just needs the number of served, intakes, and exits


class TLPAnalysis(AnalysisBase):
    """
    Downside of this implementation is that setting the type subset dfs
    gets done more than once...
    """

    def __init__(
        self, prog_df, intake_col_name, exit_col_name, dob_col_name, rp_start_dt, end_dt
    ):
        super().__init__(
            prog_df, intake_col_name, exit_col_name, dob_col_name, rp_start_dt, end_dt
        )

    def set_subset_dfs(self, subset_col, subset):
        """
        Call this method to set the subset based on the within program subset. 
        Then analysis can be run on them.
        """
        served = self.rp_served_df
        intakes = self.rp_intakes_df
        exits = self.rp_exits_df

        in_subset_served = served[subset_col] == subset
        in_subset_intake = intakes[subset_col] == subset
        in_subset_exit = exits[subset_col] == subset

        self.subset_served_df = served.loc[in_subset_served, :].copy()
        self.subset_intakes_df = intakes.loc[in_subset_intake, :].copy()
        self.subset_exits_df = exits.loc[in_subset_exit, :].copy()
        return self

    def num_subset_served_rp(self):
        try:
            return len(self.subset_served_df)
        except AttributeError:
            print("Must call .set_subset_dfs() method first")
            return

    def num_subset_intakes_rp(self):
        try:
            return len(self.subset_intakes_df)
        except AttributeError:
            # change to warn logging
            print("Must call .set_subset_dfs() method first")
            return

    def num_subset_exits_rp(self):
        try:
            return len(self.subset_exits_df)
        except AttributeError:
            # change to warn logging
            print("Must call .set_subset_dfs() method first")
            return

    def count_successful_exits(self, success_col, whole=True):
        if whole:
            df = self.rp_exits_df
        else:
            try:
                df = self.subset_exits_df
            except AttributeError:
                raise AttributeError(
                    "Attribute: .subset_exits_df not set. Call .set_subset_dfs() first."
                )
        return df[success_col].str.lower().str.startswith("s").sum()

    def calculate_avg_stay(self, short_stay, whole=True):
        short_stay_timedelta = pd.Timedelta(short_stay)
        if whole:
            df = self.rp_exits_df.copy()
        else:
            df = self.subset_exits_df.copy()
        if df.empty:
            return pd.Timedelta("0 days")

        df["_los"] = df[self.exit_col_name] - df[self.intake_col_name]
        df["_short_stay"] = df["_los"] <= short_stay_timedelta
        return (df.loc[~df["_short_stay"], "_los"]).mean()

    def calculate_utilization(self, factor, whole=True):
        exit_col = self.exit_col_name
        intake_col = self.intake_col_name
        num_days = self.end_dt - self.rp_start_dt + pd.Timedelta("1 day")
        avail = factor * num_days
        if whole:
            df = self.rp_served_df
            capped_df = clean.cap_dates(
                df=df,
                intake_col=intake_col,
                exit_col=exit_col,
                start_dt=self.rp_start_dt,
                end_dt=self.end_dt,
            )
            capped_df["util"] = (
                capped_df[exit_col] - capped_df[intake_col] + pd.Timedelta("1 day")
            )
            actual = capped_df["util"].sum()
            return round(actual / avail * 100, 2)

        try:
            df = self.subset_served_df
        except AttributeError:
            raise AttributeError(
                "Attribute: .subset_served_df not set. Call .set_subset_dfs() first."
            )
        else:
            capped_df = clean.cap_dates(
                df=df,
                intake_col=intake_col,
                exit_col=exit_col,
                start_dt=self.rp_start_dt,
                end_dt=self.end_dt,
            )
            capped_df["util"] = (
                capped_df[exit_col] - capped_df[intake_col] + pd.Timedelta("1 day")
            )
            actual = capped_df["util"].sum()
            return round(actual / avail * 100, 2)

    def calculate_num_month_end(self, whole=True):
        """
        If whole=False then the set method must be called first and the 
        method uses the latest subset.
        """
        if whole:
            return self.num_served_rp - self.num_exits_rp
        return self.num_subset_served_rp() - self.num_subset_exits_rp()


class JACAnalysis:
    def __init__(
        self,
        prog_df,
        served_dt_col_name,
        rp_start_dt,
        end_dt,
        county_contract_start_dt=None,
    ):
        self.prog_df = prog_df
        self.served_dt_col_name = served_dt_col_name
        self.rp_start_dt = rp_start_dt
        self.end_dt = end_dt
        self.county_contract_start_dt = county_contract_start_dt
        self.month_start_dt = sud.calc_month_start_from_end_dt(self.end_dt)

    def set_analysis_dfs(self):
        self.rp_served_df = self._create_served_df(_start_dt=self.rp_start_dt)
        self.month_served_df = self._create_served_df(_start_dt=self.month_start_dt)
        return self

    def set_subset_dfs(self, id_col):
        """In JAC, there exists no distinction btwn served/intakes/exits"""
        try:
            served_rp = self.rp_served_df
            served_month = self.month_served_df
        except AttributeError:
            raise AttributeError("Must call .set_analysis_dfs() method first")

        # probably safe to hardcode the cc part of it
        is_cc_rp = served_rp[id_col].str.lower().str.endswith("cc")
        is_cc_month = served_month[id_col].str.lower().str.endswith("cc")

        self.rp_cc_served_df = served_rp.loc[is_cc_rp, :].copy()
        self.rp_ntr_served_df = served_rp.loc[~is_cc_rp, :].copy()

        self.month_cc_served_df = served_month.loc[is_cc_month, :].copy()
        self.month_ntr_served_df = served_month.loc[~is_cc_month, :].copy()

        self._served_data_dict = {
            "rp": {
                "all": self.rp_served_df,
                "cc": self.rp_cc_served_df,
                "ntr": self.rp_ntr_served_df,
            },
            "month": {
                "all": self.month_served_df,
                "cc": self.month_cc_served_df,
                "ntr": self.month_ntr_served_df,
            },
        }
        return self

    def return_num_served(self, period, subset):
        """
            subset can be 'cc', 'ntr', or 'all'
            period can be 'rp' or 'month'
        """
        return len(self._served_data_dict[period][subset])

    def groupby_referral_source(self, period, subset, group_on, agg_on):
        input_df = self._served_data_dict[period][subset]
        ref_source_df = groupby_df_build(
            _df=input_df, group_on=group_on, agg_on=agg_on, agg_func="count"
        )
        return ref_source_df

    def _create_served_df(self, _start_dt):
        df = self.prog_df.copy()
        served_dt_col = self.served_dt_col_name

        floor_df = clean.floor_dt_columns(df, served_dt_col)

        intake_constraint = floor_df[served_dt_col] <= self.end_dt
        exit_constraint = floor_df[served_dt_col] >= _start_dt
        # there will be no nans to take into consideration

        return_df = floor_df.loc[
            (intake_constraint) & (exit_constraint), :
        ].reset_index(drop=True)
        return return_df


if __name__ == "__main__":

    pass
