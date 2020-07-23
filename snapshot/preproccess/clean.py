import pandas as pd
from snapshot.config import app_config


def floor_dt_columns(df, *args):
    if df.empty:
        return df
    flr_df = df.copy()
    for col in args:
        flr_df.loc[:, col] = flr_df.loc[:, col].dt.floor("D")
    return flr_df


def clean_gender_column(df, gender_column):
    if df.empty:
        return df
    _df = df.copy()
    _df[gender_column] = _df[gender_column].str.lower()
    guess_male = _df[gender_column].str.startswith("m", na=False)
    guess_female = _df[gender_column].str.startswith("f", na=False)
    # neither = ~guess_female & ~guess_male
    _df["_cleaned_gender"] = "undetermined"
    _df.loc[guess_male, "_cleaned_gender"] = "male"
    _df.loc[guess_female, "_cleaned_gender"] = "female"
    return _df


def cap_dates(df, intake_col, exit_col, start_dt, end_dt):
    _df = df.copy()
    exit_gt_end = _df[exit_col] > end_dt
    exit_nan = _df[exit_col].isna()
    intake_lt_start = _df[intake_col] < start_dt
    _df.loc[exit_gt_end | exit_nan, exit_col] = end_dt
    _df.loc[intake_lt_start, intake_col] = start_dt
    return _df


def standardize_race_column(df, race_column, mapping):
    _df = df.copy()
    if _df.empty:
        return _df
    # races = mapping.keys()
    for race in mapping:
        is_race = _df[race_column].str.lower().isin(mapping[race])
        _df.loc[is_race, race_column] = race
    return _df


class EthnicRaceColumnCombiner:
    def __init__(self, df, race_column, ethnic_column, config_mapping):
        self.df = df
        self.race_column = race_column
        self.ethnic_column = ethnic_column
        self.config_mapping = config_mapping

    @property
    def hispanic_code(self):
        return self.config_mapping["ethnicity"]["Hispanic"]

    def create_snapshot_race_df(self):
        if self.df.empty:
            return pd.DataFrame({}, columns=["count", "percent"])

        race_codes = self.config_mapping["race"]
        races = race_codes["known"]
        specific_races = set(race_codes.keys()) - set(["known"])
        multi_other = [r for r in races if r not in specific_races]

        races_df = self.df.copy()
        is_hispanic = races_df[self.ethnic_column] == self.hispanic_code
        is_other = races_df[self.race_column].isin(multi_other)

        races_df.loc[is_hispanic, "snapshot_race"] = "Hispanic"
        races_df.loc[(~is_hispanic) & (is_other), "snapshot_race"] = "Multi/Other"

        for s in specific_races:
            is_race = races_df[self.race_column] == s
            races_df.loc[(~is_hispanic) & (is_race), "snapshot_race"] = race_codes[s]

        return races_df


if __name__ == "__main__":
    pass
