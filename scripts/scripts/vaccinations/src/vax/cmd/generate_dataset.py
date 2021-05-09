import os
import itertools
from datetime import datetime
from collections import ChainMap
from math import isnan

import pandas as pd


class DatasetGenerator:

    def __init__(self, vaccinations_file, metadata_file, project_dir):
        # Inputs
        self.vaccinations_file = vaccinations_file
        self.metadata_file = metadata_file
        self.iso_file = os.path.join(project_dir, "scripts/input/iso/iso3166_1_alpha_3_codes.csv")
        self.population_file = os.path.join(project_dir, "scripts/input/un/iso3166_1_alpha_3_codes.csv")
        self.population_sub_file = os.path.join(project_dir, "scripts/input/owid/subnational_population_2020.csv")
        self.continents_file = os.path.join(project_dir, "scripts/input/owid/continents.csv")
        self.eu_file = os.path.join(project_dir, "scripts/input/owid/eu_countries.csv")
        # Outputs
        self.locations_file = os.path.join(project_dir, "public/data/vaccinations/locations.csv")
        self.automated_file = os.path.abspath(os.path.join(project_dir, "automation_state.csv"))
        self.vaccinations_file = os.path.abspath(os.path.join(project_dir, "public/data/vaccinations/vaccinations.csv"))
        # Others
        self.aggregates = self.build_aggregates()

    def build_aggregates(self):
        continents = pd.read_csv(self.continents_file, usecols=["Entity", "Unnamed: 3"])
        eu_countries = pd.read_csv(self.eu_file, usecols=["Country"], squeeze=True).tolist()
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
        return AGGREGATES

    def pipeline_automated(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .sort_values(by=["automated", "location"], ascending=[False, True])
            [["location", "automated"]]
        )

    def pipeline_locations(self, df_vax: pd.DataFrame, df_metadata: pd.DataFrame,
                           df_iso: pd.DataFrame) -> pd.DataFrame:
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

    def _get_aggregate(self, df, agg_name, included_locs, excluded_locs):
        # Take rows that matter
        agg = df[~df.location.isin(self.aggregates.keys())]  # remove aggregated rows
        if excluded_locs is not None:
            agg = agg[~agg.location.isin(excluded_locs)]
        elif included_locs is not None:
            agg = agg[agg.location.isin(included_locs)]
        
        # Get full location-date grid
        agg = (
            pd.DataFrame(
                itertools.product(
                    agg.location.unique(),
                    agg.date.unique()
                ),
                columns=[agg.location.name, agg.date.name]
            )
            .merge(agg, on=["date", "location"], how="outer")
            .sort_values(by=["location", "date"])
        )
        
        # NaN: Forward filling + Zero-filling if all metric is NaN
        cols = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]
        grouper = agg.groupby("location")
        for col in cols:
            agg[col] = grouper[col].apply(lambda x: x.fillna(0) if x.isnull().all() else x.fillna(method="ffill"))

        # Aggregate
        agg = (
            agg
            .groupby("date").sum()
            .reset_index()
            .assign(location=agg_name)
        )
        agg = agg[agg.date.dt.date < datetime.now().date()]
        return agg

    def pipe_aggregates(self, df: pd.DataFrame) -> pd.DataFrame:
        aggs = []
        for agg_name, value in self.aggregates.items():
            aggs.append(
                get_aggregate(
                    df=df,
                    agg_name=agg_name,
                    included_locs=self.aggregates[agg_name]["included_locs"],
                    excluded_locs=self.aggregates[agg_name]["excluded_locs"]
                )
            )
        return pd.concat([vax] + aggs, ignore_index=True)

    def pipe_daily(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(by=["location", "date"])
        df = df.assign(new_vaccinations=df.groupby("location").total_vaccinations.diff())
        df.loc[df.date.diff().dt.days > 1, "new_vaccinations"] = None
        df = df.sort_values(["location", "date"])
        return df

    def _add_smoothed(self, df: pd.DataFrame) -> pd.DataFrame:
        # Range where total_vaccinations is registered
        dt_min = df.dropna(subset=["total_vaccinations"]).date.min()
        dt_max = df.dropna(subset=["total_vaccinations"]).date.max()
        df_nan = df[(df.date < dt_min) | (df.date > dt_max)]
        # Add missing dates
        df = (
            df
            .merge(
                pd.Series(pd.date_range(dt_min, dt_max), name="date"),
                how="right",
            )
            .sort_values(by="date")
        )
        # Calculate and add smoothed vars
        new_interpolated_smoothed = (
            df
            .total_vaccinations
            .interpolate(method='linear')
            .diff()
            .rolling(7, min_periods=1)
            .mean()
            .apply(lambda x: round(x) if not isnan(x) else x)
        )
        df = df.assign(
            new_vaccinations_smoothed=new_interpolated_smoothed
        )
        # Add missing dates
        df = pd.concat([df, df_nan], ignore_index=True).sort_values("date")
        df.loc[:, "location"] = df.location.dropna().iloc[0]
        return df

    def pipe_smoothed(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.groupby("location").apply(self._add_smoothed).reset_index(drop=True)

    def get_population(self, subnational_pop: pd.DataFrame) -> pd.DataFrame:
        # Build population dataframe
        column_rename = {
            "entity": "location",
            "population": "population"
        }
        pop = pd.read_csv(self.population_file, usecols=column_rename.keys()).rename(columns=column_rename)
        pop = pd.concat([pop, self.subnational_pop], ignore_index=True)
        # Group territories
        location_rename = {
            "United States": [
                "American Samoa",
                "Micronesia (country)",
                "Guam",
                "Marshall Islands",
                "Northern Mariana Islands",
                "Puerto Rico",
                "Palau",
                "United States Virgin Islands",
            ]
        }
        location_rename = ChainMap(*[{vv: k for vv in v} for k, v in location_rename.items()])
        pop.loc[:, "location"] = pop.location.replace(location_rename)
        pop = pop.groupby("location", as_index=False).sum()
        return pop

    def pipe_capita(self, df: pd.DataFrame) -> pd.DataFrame:
        # Get data
        subnational_pop = pd.read_csv(self.population_sub_file, usecols=["location", "population"])
        pop = self.get_population(subnational_pop)
        df = df.merge(pop, on="location")
        # Get covered countries
        locations = df.location.unique()
        ncountries = subnational_pop.location.tolist() + list(self.aggregates.keys())
        countries_covered = list(filter(lambda x: x not in ncountries, locations))
        # Obtain per-capita metrics
        df = df.assign(
            total_vaccinations_per_hundred=(df.total_vaccinations * 100 / df.population).round(2),
            people_vaccinated_per_hundred=(df.people_vaccinated * 100 / df.population).round(2),
            people_fully_vaccinated_per_hundred=(df.people_fully_vaccinated * 100 / df.population).round(2),
            new_vaccinations_smoothed_per_million=(df.new_vaccinations_smoothed * 1000000 / df.population).round(),
        )
        df.loc[:, "people_fully_vaccinated"] = df.people_fully_vaccinated.replace({0: pd.NA})
        df.loc[vax.people_fully_vaccinated.isnull(), "people_fully_vaccinated_per_hundred"] = pd.NA
        return df.drop(columns=["population"])

    def pipe_vax_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        # Sanity checks
        if not (df.total_vaccinations.dropna() >= 0).all():
            raise ValueError(" Negative values found! Check values in `total_vaccinations`.")
        if not (df.new_vaccinations_smoothed.dropna() >= 0).all():
            raise ValueError(" Negative values found! Check values in `new_vaccinations_smoothed`.")
        if not (df.new_vaccinations_smoothed_per_million.dropna() <= 120000).all():
            raise ValueError(" Huge values found! Check values in `new_vaccinations_smoothed_per_million`.")
        return df

    def pipeline_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            ["date", "location", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]
            .pipe(self.pipe_aggregates)
            .pipe(self.pipe_daily)
            .pipe(self.pipe_smoothed)
            .pipe(self.pipe_capita)
            .pipe(self.pipe_vax_checks)
            .sort_values(by=["location", "date"])
        )

    def run(self):
        files = []
        df_metadata = pd.read_csv(self.metadata_file)
        df_vaccinations = pd.read_csv(self.vaccinations_file, parse_dates=["date"])
        df_iso = pd.read_csv(self.iso_file)

        # Metadata  
        automated_state = df_metadata.pipe(self.build_automation_df)  # Export to AUTOMATED_STATE_FILE
        files.append((automated_state.copy(), self.automated_file))
        metadata = df_vaccinations.pipe(self.build_locations_df, df_metadata, df_iso)  # Export to LOCATIONS_FILE
        files.append((metadata.copy(), self.metadata_file))

        # Vaccinations
        vax = df_vaccinations.pipe(self.pipeline_vaccinations)
        files.append((vax.copy(), self.vaccinations_file))

        # generate_vaccinations_file(copy(vax))
        # generate_grapher_file(copy(vax))
        # generate_html(metadata)

    def export(self, automated_file, locations_file):
        raise NotImplementedError()


def main():
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
