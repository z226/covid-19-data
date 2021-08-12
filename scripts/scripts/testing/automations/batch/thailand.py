import pandas as pd


def main():
    URL = "https://raw.githubusercontent.com/wiki/djay/covidthailand/tests_pubpriv"

    df = pd.read_json(URL)[["Date", "Tests"]].rename(
        columns={"Tests": "Daily change in cumulative total"}
    )

    df.loc[:, "Country"] = "Thailand"
    df.loc[:, "Units"] = "tests performed"
    df.loc[:, "Source URL"] = URL
    df.loc[:, "Source label"] = "Thailand Department of Medical Sciences"
    df.loc[:, "Notes"] = pd.NA
    # df = df.assign(**{
    #     "Country": "Thailand",
    #     "Units": "tests performed",
    #     "Source label": 1,
    # })


if __name__ == "__main__":
    main()
