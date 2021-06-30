import os


def get_project_dir(err: bool = False):
    project_dir = os.environ.get("OWID_COVID_PROJECT_DIR")
    if err and project_dir is None:
        raise ValueError(
            "Please have  ${OWID_COVID_PROJECT_DIR}."
        )
    return project_dir
    