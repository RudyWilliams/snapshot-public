import calendar
from dateutil.relativedelta import relativedelta
import datetime
import pandas as pd
from snapshot.config import app_config


def num_months_togo(start_dt, end_dt):
    return relativedelta(end_dt, start_dt).months


def calculate_fy_end_dt(fy_start_dt):
    start_end_month = fy_start_dt + relativedelta(months=+11)
    end_yr = start_end_month.year
    end_month = start_end_month.month
    end_day = calendar.monthrange(end_yr, end_month)[-1]
    return datetime.date(end_yr, end_month, end_day)


def calc_month_start_from_end_dt(end_dt):
    return pd.Timestamp(year=end_dt.year, month=end_dt.month, day=1)


def end_dt_to_folder(end_dt):
    """if the naming convention ever changes this will need to be updated"""
    month_name = app_config.month_map[end_dt.month]["name"]
    year = end_dt.year
    return f"{month_name} {year}"


if __name__ == "__main__":
    d1 = datetime.date(2019, 8, 31)
    d2 = datetime.date(2020, 6, 30)
    print(num_months_togo(d1, d2))
    print(calculate_fy_end_dt(d2))
