import re
from datetime import timedelta

import pandas as pd

from vax.manual.twitter.base import TwitterCollectorBase


class Panama(TwitterCollectorBase):
    def __init__(self, api, paths=None):
        super().__init__(
            api=api,
            username="MINSAPma",
            location="Panama",
            add_metrics_nan=True,
            paths=paths,
        )
    
    def _propose_df(self):
        regex = r"Comunicado N° (\d{3,4}).*"
        data = []
        for tweet in self.tweets:
            match = re.search(regex, tweet.full_text)
            if match:
                data.append({
                    "date": (tweet.created_at - timedelta(days=1)).strftime("%Y-%m-%d"),
                    "text": tweet.full_text,
                    "source_url": self.build_post_url(tweet.id),
                    "media_url": tweet.extended_entities["media"][1]["media_url_https"],
                    "num": match.group(1),
                })
        df = pd.DataFrame(data)
        df = df.drop_duplicates(subset=["num"], keep="last")
        return df


def main(api, paths):
    Panama(api, paths).to_csv()
