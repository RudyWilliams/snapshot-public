# import pathlib
import os
import yaml


with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

production_root = os.path.normpath(config["paths"]["production"]["root"])
dev_root = os.path.normpath(config["paths"]["dev"]["root"])
demo_codes = config["network_data_presets"]["codes"]
exp_fy_start = config["expected_fy_start"]

month_map = config["month_map"]

# network presets
ntwk_presets = config["network_data_presets"]

fn_ylog = ntwk_presets["fn_ylog"]
fn_youth = ntwk_presets["fn_youth"]
fn_a_programs = ntwk_presets["fn_a_programs"]
intake_dt_column = ntwk_presets["intake_dt_column"]
exit_dt_column = ntwk_presets["exit_dt_column"]
birth_dt_column = ntwk_presets["birth_dt_column"]
youth_id_column = ntwk_presets["youth_id_column"]
gender_column = ntwk_presets["gender_column"]
race_column = ntwk_presets["race_column"]
ethnic_column = ntwk_presets["ethnic_column"]
program_name_column = ntwk_presets["program_name_column"]
program_id_column = ntwk_presets["program_id_column"]
intake_dt_column = ntwk_presets["intake_dt_column"]

# uniques to programs
shelter_program_names = ntwk_presets["shelter_program_names"]
shelter_age_bins = [tuple(b) for b in config["shelter"]["age_bins"]]

nonres_program_names = ntwk_presets["nonres_program_names"]
nonres_age_bins = [tuple(b) for b in config["nonres"]["age_bins"]]
nonres_intake_contract_req = config["nonres"]["contract_requirements"]["fy_intakes"]

snap_program_names = ntwk_presets["snap_program_names"]
snap_age_bins = [tuple(b) for b in config["snap"]["age_bins"]]

tlp_filename = config["tlp"]["filename"]
tlp_age_bins = [tuple(b) for b in config["tlp"]["age_bins"]]
tlp_parse_dates = config["tlp"]["parse_dates"]
tlp_skiprows = config["tlp"]["skiprows"]
tlp_short_stay_delta = config["tlp"]["short_stay_delta"]

tlp_presets = config["tlp"]["presets"]
tlp_intake_dt_column = tlp_presets["intake_dt_column"]
tlp_exit_dt_column = tlp_presets["exit_dt_column"]
tlp_birth_dt_column = tlp_presets["birth_dt_column"]
tlp_gender_column = tlp_presets["gender_column"]
tlp_race_column = tlp_presets["race_column"]
tlp_subset_column = tlp_presets["tlp_subset_column"]
tlp_subsets = tlp_presets["tlp_subsets"]


jac_filename = config["jac"]["filename"]
jac_parse_dates = config["jac"]["parse_dates"]

jac_presets = config["jac"]["presets"]
jac_served_dt_column = jac_presets["served_dt_column"]
jac_birth_dt_column = jac_presets["birth_dt_column"]
jac_id_column = jac_presets["id_column"]
jac_gender_column = jac_presets["gender_column"]
jac_race_column = jac_presets["race_column"]
jac_referral_source_column = jac_presets["referral_source_column"]
jac_pre_calc_age_column = jac_presets["pre_calc_age_column"]
jac_age_bins = [tuple(b) for b in config["jac"]["age_bins"]]

if __name__ == "__main__":
    print()
