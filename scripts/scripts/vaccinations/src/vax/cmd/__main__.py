import argparse

from vax.cmd._config import get_config
from vax.cmd import main_get_data, main_process_data, main_generate_dataset
from vax.cmd.export import main_export
from vax.cmd.check_with_r import test_check_with_r
from vax.utils.paths import Paths


def main():
    config = get_config()
    paths = Paths(config.project_dir)
    creds = config.CredentialsConfig()

    if config.display:
        print(config)
    
    print(config.mode)

    if "get" in config.mode:
        cfg = config.GetDataConfig()
        main_get_data(
            paths,
            cfg.parallel,
            cfg.njobs,
            cfg.countries,
            creds.greece_api_token,
            cfg.skip_countries,
        )
    if "process" in config.mode:
        cfg = config.ProcessDataConfig()
        main_process_data(
            paths,
            creds.google_credentials,
            creds.google_spreadsheet_vax_id,
            cfg.skip_complete,
            cfg.skip_monotonic_check,
        )
    if "generate" in config.mode:
        if config.check_r:
            test_check_with_r(paths)
        else:
            main_generate_dataset(
                paths,
            )
    if "export" in config.mode:
        main_export(
            paths,
            creds.owid_cloud_table_post
        )


if __name__ == "__main__":
    main()
