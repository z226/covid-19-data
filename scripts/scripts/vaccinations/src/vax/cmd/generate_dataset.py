import os

import pandas as pd


VAX_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
# Inputs
SUB_POP_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "../../input/owid/subnational_population_2020.csv"))
CONTINENTS_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "../../input/owid/continents.csv"))
EU_COUNTRIES_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "../../input/owid/eu_countries.csv"))
ISO_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "../../input/iso/iso3166_1_alpha_3_codes.csv"))
METADATA_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "./metadata.preliminary.csv"))
VAX_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "./vaccinations.preliminary.csv"))
# Outputs
AUTOMATED_STATE_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "./automation_state.csv"))
LOCATIONS_FILE = os.path.abspath(os.path.join(VAX_ROOT_DIR, "./public/data/vaccinations/locations.csv"))


# Load files
subnational_pop = pd.read_csv(SUB_POP_FILE, usecols=["location", "population"])
continents = pd.read_csv(CONTINENTS_FILE, usecols=["Entity", "Unnamed: 3"])
eu_countries = pd.read_csv(EU_COUNTRIES_FILE, usecols=["Country"], squeeze=True).tolist()


# Aggregates
AGGREGATES = {
    "World": {
        "excluded_locs": ["England", "Northern Ireland", "Scotland", "Wales"], 
        "included_locs": None
    },
    "European Union": {
        "excluded_locs": None, 
        "included_locs": eu_countries
    }
}

for continent in ["Asia", "Africa", "Europe", "North America", "Oceania", "South America"]:
    AGGREGATES[continent] = {
        "excluded_locs": None,
        "included_locs": continents.loc[continents["Unnamed: 3"] == continent, "Entity"].tolist()
    }


def generate_automation_file(df: pd.DataFrame):
    return df.sort_values(by=["automated", "location"], ascending=[False, True])[["location", "automated"]]


def generate_locations_file(df_metadata: pd.DataFrame, df_vax: pd.DataFrame, df_iso: pd.DataFrame):
    def _pretty_vaccine(vaccines):
        return ",".join(sorted(v.strip() for v in vaccines.split(',')))
    df_vax = (
        df_vax
        .sort_values(by=["location", "date"])
        .drop_duplicates(subset=["location"], keep="last")
        .assign(vaccines=df_vax.vaccine.apply(_pretty_vaccine))
        .rename(columns={
            "date": "last_observation",
            "source_url": "source_website"
        })
    )

    if len(df_metadata) != len(df_vax):
        raise ValueError("Missmatch between vaccination data and metadata!")

    return (
        df_vax
        .merge(df_metadata, on="location")
        .merge(df_iso, on="location")
    )[["location", "iso_code", "vaccines", "last_observation", "source_name", "source_website"]]


def main():
    # Load data
    metadata = pd.read_csv(METADATA_FILE)
    vax = pd.read_csv(VAX_FILE, parse_dates=["date"])
    iso = pd.read_csv(ISO_FILE, parse_dates=["date"])

    # Metadata
    automated_state = generate_automation_file(metadata)
    metadata = generate_locations_file(metadata, vax, iso)
    automated_state.to_csv(AUTOMATED_STATE_FILE, index=False)
    metadata.to_csv(LOCATIONS_FILE, index=False)

    # Select columns
    vax = vax[["date", "location", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]]
    """
    # Select columns
    vax <- vax[, c("date", "location", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated")]

    # Add regional aggregates
    for (agg_name in names(AGGREGATES)) {
        vax <- add_aggregate(
            vax,
            aggregate_name = agg_name,
            included_locs = AGGREGATES[[agg_name]][["included_locs"]],
            excluded_locs = AGGREGATES[[agg_name]][["excluded_locs"]]
        )
    }
    """
if __name__ == "__main__":
    main()
