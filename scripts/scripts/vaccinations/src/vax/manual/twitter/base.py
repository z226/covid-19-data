import os
import pandas as pd


COLUMN_METRICS_ALL = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]


class TwitterCollectorBase:
    
    def __init__(self, api, username: str, location: str, add_metrics_nan: bool = False):
        self.username = username
        self.location = location
        self.tweets = api.get_tweets(self.username)
        self.add_metrics_nan = add_metrics_nan

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
        if self.add_metrics_nan:
            for col in COLUMN_METRICS_ALL:
                if col not in df.columns:
                    df = df.assign(**{col: pd.NA})
        return df

    def _order_columns(self, df):
        column_metrics = []
        for col in COLUMN_METRICS_ALL:
            if col in df.columns:
                column_metrics.append(col)
        
        df = df[["date"] + column_metrics + ["media_url", "source_url", "text"]]
        return df

    def build_post_url(self, tweet_id: str):
        return f"https://twitter.com/{self.username}/status/{tweet_id}"

    def to_csv(self, output_folder: str):
        df = self.propose_df()
        df.to_csv(os.path.join(output_folder, self.lcoation), index=False)