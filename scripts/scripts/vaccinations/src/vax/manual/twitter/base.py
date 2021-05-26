import os


class TwitterCollectorBase:
    
    def __init__(self, username: str, location: str, api):
        self.username = username
        self.location = location
        self.tweets = api.get_tweets(self.username)

    def propose_df(self):
        raise NotImplementedError

    def build_post_url(self, tweet_id: str):
        return f"https://twitter.com/{self.username}/status/{tweet_id}"

    def to_csv(self, output_folder: str):
        df = self.propose_df()
        df.to_csv(os.path.join(output_folder, self.lcoation), index=False)