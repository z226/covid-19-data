import pandas as pd
import re

from vax.manual.twitter.base import TwitterCollectorBase
from vax.utils.utils import clean_count


class Uganda(TwitterCollectorBase):
    def __init__(self, api):
        super().__init__(
            username="MinofHealthUG",
            location="Uganda",
            api=api,
        )
    
    def _propose_df(self):
        regex_1 = r"Results of COVID-19 tests .*"
        regex_2 = r"against COVID-19: ([\d,]+)"
        data = []
        for tweet in self.tweets:
            if re.search(regex_1, tweet.full_text):
                if "media" in tweet.entities:
                    data.append({
                        "date": tweet.created_at.strftime("%Y-%m-%d"),
                        "text": tweet.full_text,
                        "source_url": self.build_post_url(tweet.id),
                        "media_url": tweet.entities["media"][0]["media_url_https"],
                    })
            elif re.search(regex_2, tweet.full_text):
                total_vaccinations = re.search(regex_2, tweet.full_text).group(1)
                data.append({
                    "date": tweet.created_at.strftime("%Y-%m-%d"),
                    "total_vaccinations": clean_count(total_vaccinations),
                    "text": tweet.full_text,
                    "source_url": self.build_post_url(tweet.id),
                })
        return pd.DataFrame(data)
