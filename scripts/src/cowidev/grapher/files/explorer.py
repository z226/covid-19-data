import json
from datetime import datetime

import pandas as pd


class Exploriser:
    def __init__(self) -> None:
        pass

    def pipe_nan_to_none(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.where(
            pd.notnull(df),
            None
        )

    def pipe_to_dict(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.to_dict(orient="list")

    def pipeline(self, input_path: str) -> dict:
        df = pd.read_csv(input_path)
        df = (
            df
            .pipe(self.pipe_nan_to_none)
            .pipe(self.pipe_to_dict)
        )
        return df

    def to_json(self, obj):
        return json.dumps(
            obj,
            # Use separators without any trailing whitespace to minimize file size.
            # The defaults (", ", ": ") contain a trailing space.
            separators=(",", ":"),
            # The json library by default encodes NaNs in JSON, but this is invalid JSON.
            # By having this False, an error will be thrown if a NaN exists in the data.
            allow_nan=False
        )

    def run(self, input_path: str, output_path: str):
        data = self.pipeline(input_path)
        with open(output_path, "w") as f:
            f.write(self.to_json(data))
