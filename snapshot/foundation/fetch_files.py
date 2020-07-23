from functools import lru_cache
import os
import pandas as pd


class VTrimRead:
    def __init__(self, fn_ylog, fn_youth, fn_a_programs, dir_):
        self.fn_ylog = fn_ylog
        self.fn_youth = fn_youth
        self.fn_a_programs = fn_a_programs
        self.dir = dir_

    @property
    def fp_ylog(self):
        return os.path.join(self.dir, "v", "w", self.fn_ylog)

    @property
    def fp_youth(self):
        return os.path.join(self.dir, "v", "w", self.fn_youth)

    @property
    def fp_a_programs(self):
        return os.path.join(self.dir, "v", "w", self.fn_a_programs)

    def get_name_df_dictionary(self):
        y_log = self._get_y_log()
        youth = self._get_youth()
        a_programs = self._get_a_programs()
        return {y_log.name: y_log, youth.name: youth, a_programs.name: a_programs}

    @lru_cache(maxsize=None)
    def _get_y_log(self):
        cols = ["intake_dt", "exit_dt", "prog_id", "youth_id", "discharge"]
        parse_dates = ["intake_dt", "exit_dt"]
        df = _fetch_csv(
            self.fp_ylog, cols, parse_dates, infer_dt=True, search_in=self.dir
        )
        df.name = "y_log"
        return df

    @lru_cache(maxsize=None)
    def _get_youth(self):
        cols = ["youth_id", "gender", "race", "ethnic", "birth_dt"]
        parse_dates = ["birth_dt"]
        df = _fetch_csv(
            self.fp_youth, cols, parse_dates, infer_dt=True, search_in=self.dir
        )
        df.name = "youth"
        return df

    @lru_cache(maxsize=None)
    def _get_a_programs(self):
        cols = ["prog_name", "prog_id"]
        df = _fetch_csv(
            self.fp_a_programs,
            cols,
            parse_dates=False,
            infer_dt=False,
            search_in=self.dir,
        )
        df.name = "a_programs"
        return df


class ExcelRead:
    def __init__(self, filename, dir_, skiprows, parse_dates, cols=None):
        self.filename = filename
        self.dir = dir_
        self.skiprows = skiprows
        self.parse_dates = parse_dates
        self.cols = cols

    @property
    def filepath(self):
        return os.path.join(self.dir, self.filename)

    def fetch_excel(self):
        return pd.read_excel(
            self.filepath,
            header=0,
            skiprows=self.skiprows,
            usecols=self.cols,
            parse_dates=self.parse_dates,
        )


def _fetch_csv(fpath, cols, parse_dates, infer_dt, search_in):
    in_expected_loc = os.path.exists(fpath)
    if in_expected_loc:
        df = pd.read_csv(
            fpath,
            header=0,
            usecols=cols,
            parse_dates=parse_dates,
            infer_datetime_format=infer_dt,
        )

    else:
        fname = os.path.basename(fpath)
        new_fp = _file_search(search_in, fname)
        if new_fp is None:
            raise FileNotFoundError(
                f"Could not locate file in {search_in} or subdirectories."
            )
        try:
            df = pd.read_csv(
                new_fp,
                header=0,
                usecols=cols,
                parse_dates=parse_dates,
                infer_datetime_format=infer_dt,
            )
        except Exception as e:
            raise ValueError(f"Error in building DF: {e}")

    return df


def _file_search(search_in, fname):
    for dirpath, _, filenames in os.walk(search_in, topdown=True):
        return os.path.join(dirpath, fname) if fname in filenames else None


if __name__ == "__main__":
    pass
