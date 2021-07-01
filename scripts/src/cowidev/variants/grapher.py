from cowidev.grapher.csv.core import Grapheriser


def run_grapheriser(input_path: str, output_path: str):
    Grapheriser(
        pivot_column="variant",
        pivot_values="perc_sequences",
        fillna_0=True,
    ).run(input_path, output_path)


def run_explorerizer(input_path: str, output_path: str):
    raise NotImplementedError("Not yet implemented")


def run_db_updater(input_path: str):
    raise NotImplementedError("Not yet implemented")