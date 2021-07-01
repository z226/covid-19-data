import os
from datetime import datetime


def get_project_dir(err: bool = False):
    project_dir = os.environ.get("OWID_COVID_PROJECT_DIR")
    if err and project_dir is None:
        raise ValueError(
            "Please have  ${OWID_COVID_PROJECT_DIR}."
        )
    return project_dir


def export_timestamp(timestamp_filename: str):
    timestamp_filename = os.path.join(get_project_dir(), "public", "data", "internal", "timestamp", timestamp_filename)
    with open(timestamp_filename, "w") as timestamp_file:
        timestamp_file.write(datetime.utcnow().replace(microsecond=0).isoformat())
