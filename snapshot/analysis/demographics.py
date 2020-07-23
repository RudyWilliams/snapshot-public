from typing import List, Tuple, Dict
import pandas as pd
from snapshot.config import app_config
from snapshot.preproccess import clean
from snapshot.foundation.dataframe_builds import groupby_df_build


class Demographics:
    def __init__(self, input_df: pd.DataFrame, agg_on: str) -> None:
        self.input_df = input_df
        self.agg_on = agg_on


class AgeDemographics(Demographics):
    def __init__(
        self,
        input_df: pd.DataFrame,
        birthdt_col_name: str,
        reference_col_name: str,
        agg_on: str,
        bins: List[Tuple[int, int]],
    ) -> None:
        self.birthdt_col_name = birthdt_col_name
        self.reference_col_name = reference_col_name
        self.bins = bins
        super().__init__(input_df, agg_on)

    def _create_age_df(self):
        if self.input_df.empty:
            return pd.DataFrame(
                {},
                columns=[self.birthdt_col_name, self.reference_col_name, self.agg_on],
            )
        df = self.input_df.copy()
        df["age"] = (
            df[self.reference_col_name] - df[self.birthdt_col_name]
        ) / pd.to_timedelta("365 days")
        return df

    def _create_age_bin_df(self, pre_calc_age_column=None) -> pd.DataFrame:
        if pre_calc_age_column is None:
            age_df = self._create_age_df()
        else:
            age_df = self.input_df.copy()
            # the bin part expects the name to be "age"
            age_df = age_df.rename(columns={pre_calc_age_column: "age"})
        if age_df.empty:
            return pd.DataFrame({}, columns=list(age_df.columns) + ["age_category"])
        bin_intervals = pd.IntervalIndex.from_tuples(self.bins, closed="left")
        category_series = pd.cut(age_df["age"], bins=bin_intervals)
        df_copy = age_df.copy()
        df_copy["age_category"] = category_series.astype(str)
        return df_copy

    def create_ages_groupby_df(self, pre_calc_age_column=None):
        age_bin_df = self._create_age_bin_df(pre_calc_age_column=pre_calc_age_column)
        self.age_bin_df = age_bin_df
        if age_bin_df.empty:
            return pd.DataFrame({}, columns=["count", "percent"])
        return groupby_df_build(
            age_bin_df, "age_category", self.agg_on, "count", also_nan="nan"
        )


class GenderDemographics(Demographics):
    def __init__(self, input_df, group_on, agg_on):
        self.group_on = group_on
        super().__init__(input_df, agg_on)

    def _clean_gender_column(self):
        return clean.clean_gender_column(self.input_df, self.group_on)

    def create_genders_groupby_df(self):
        if self.input_df.empty:
            return pd.DataFrame({}, columns=["count", "percent"])
        cleaned_df = self._clean_gender_column()
        return groupby_df_build(
            cleaned_df, "_cleaned_gender", self.agg_on, "count", also_nan="undetermined"
        )


class RaceDemographics(Demographics):
    def __init__(self, input_df, group_on, agg_on):
        self.group_on = group_on
        super().__init__(input_df, agg_on)

    def create_races_groupby_df(
        self,
        race_col,
        ethnic_col=None,
        mapping=None,
        also_nan="nan",
        lowercase_first=False,
    ):
        if ethnic_col is None:
            if lowercase_first:  # useful for JAC and TLP (human entered entries)
                self.input_df[race_col] = self.input_df[race_col].str.lower()
            return groupby_df_build(
                self.input_df,
                self.group_on,
                self.agg_on,
                "count",
                also_nan=also_nan,  # not nec since NaN is the only NaN value
            )

        stdRC = clean.EthnicRaceColumnCombiner(
            self.input_df, race_col, ethnic_col, app_config.demo_codes
        )
        races_df = stdRC.create_snapshot_race_df()

        gby_df = groupby_df_build(
            races_df, "snapshot_race", self.agg_on, "count", also_nan=also_nan
        )
        return gby_df


if __name__ == "__main__":
    pass
