import os

import pandas as pd

from vax.utils.gsheets import GSheet
from vax.process import process_location
from vax.cmd.utils import get_logger, print_eoe


logger = get_logger()


def main_process_data(paths, google_credentials: str, google_spreadsheet_vax_id: str, skip_complete: list = None,
                      skip_monotonic: dict = {}, skip_anomaly: dict = {}):
    print("-- Processing data... --")
    # Get data from sheets
    logger.info("Getting data from Google Spreadsheet...")
    gsheet = GSheet(
        google_credentials,
        google_spreadsheet_vax_id
    )
    df_manual_list = gsheet.df_list()

    # Get automated-country data
    logger.info("Getting data from output...")
    automated = gsheet.automated_countries
    filepaths_auto = [paths.tmp_vax_out(country) for country in automated]
    df_auto_list = [pd.read_csv(filepath) for filepath in filepaths_auto]

    # Concatenate
    vax = df_manual_list + df_auto_list

    # Process locations
    def _process_location(df):
        monotonic_check_skip = skip_monotonic.get(df.loc[0, "location"], [])
        anomaly_check_skip = skip_anomaly.get(df.loc[0, "location"], [])
        return process_location(df, monotonic_check_skip, anomaly_check_skip)

    logger.info("Processing and exporting data...")
    vax = [
        _process_location(df) for df in vax if df.loc[0, "location"].lower() not in skip_complete
    ]

    # Export
    for df in vax:
        country = df.loc[0, "location"]
        df.to_csv(paths.pub_vax_loc(country), index=False)
    df = pd.concat(vax).sort_values(by=["location", "date"])
    df.to_csv(paths.tmp_vax_all, index=False)
    gsheet.metadata.to_csv(paths.tmp_met_all, index=False)
    logger.info("Exported ✅")
    print_eoe()
