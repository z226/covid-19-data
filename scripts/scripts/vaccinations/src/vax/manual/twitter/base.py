import os
import pandas as pd


COLUMN_METRICS_ALL = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]


class TwitterCollectorBase:
    
    def __init__(self, api, username: str, location: str, add_metrics_nan: bool = False, paths=None, num_tweets=100):
        self.username = username
        self.location = location
        self.tweets = api.get_tweets(self.username, num_tweets)
        self.add_metrics_nan = add_metrics_nan
        self.paths = paths
        self.tweets_relevant = []

    def _propose_df(self):
        raise NotImplementedError

    def propose_df(self):
        df = (
            self._propose_df()
            .sort_values("date")
            .reset_index(drop=True)
            .pipe(self._add_metrics)
            .pipe(self._order_columns)
        )
        return df

    def _add_metrics(self, df):
        if isinstance(self.add_metrics_nan, list):
            for col in self.add_metrics_nan:
                df = df.assign(**{col: pd.NA})
        elif self.add_metrics_nan:
            for col in COLUMN_METRICS_ALL:
                if col not in df.columns:
                    df = df.assign(**{col: pd.NA})
        return df

    def _order_columns(self, df):
        column_metrics = []
        column_optional = []
        for col in COLUMN_METRICS_ALL:
            if col in df.columns:
                column_metrics.append(col)
        if "media_url" in df:
            column_optional.append("media_url")

        df = df[["date"] + column_metrics + ["source_url"]+ column_optional + ["text"]]
        return df

    def build_post_url(self, tweet_id: str):
        return f"https://twitter.com/{self.username}/status/{tweet_id}"

    def to_csv(self, output_path=None):
        df = self.propose_df()
        if output_path is None:
            if self.paths is not None:
                output_path = self.paths.tmp_vax_out_proposal(self.location)
            else:
                raise AttributeError("Either specify attribute `paths` or method argument `output_path`")
        df.to_csv(output_path, index=False)