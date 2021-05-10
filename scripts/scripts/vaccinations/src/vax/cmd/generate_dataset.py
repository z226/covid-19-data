import os
import itertools
from datetime import datetime
from collections import ChainMap
from math import isnan
import json

import pandas as pd


class Bucket(object):
    def __init__(self, **kwargs):
        self._dict = kwargs
        self.__dict__.update(kwargs)


class DatasetGenerator:

    def __init__(self, project_dir, vaccinations_file, metadata_file):
        # Inputs
        self.inputs = Bucket(
            project_dir=project_dir,
            vaccinations=vaccinations_file,
            metadata=metadata_file,
            iso=os.path.join(project_dir, "scripts/input/iso/iso3166_1_alpha_3_codes.csv"),
            population=os.path.join(project_dir, "scripts/input/un/population_2020.csv"),
            population_sub=os.path.join(project_dir, "scripts/input/owid/subnational_population_2020.csv"),
            continent_countries=os.path.join(project_dir, "scripts/input/owid/continents.csv"),
            eu_countries=os.path.join(project_dir, "scripts/input/owid/eu_countries.csv"),
        )
        
        # Outputs
        self.outputs = Bucket(
            locations=os.path.join(project_dir, "public/data/vaccinations/locations.csv"),
            automated=os.path.abspath(os.path.join(project_dir, "automation_state.csv")),
            vaccinations=os.path.abspath(os.path.join(project_dir, "public/data/vaccinations/vaccinations.csv")),
            vaccinations_json=os.path.abspath(os.path.join(project_dir, "public/data/vaccinations/vaccinations.json")),
            grapher=os.path.abspath(os.path.join(project_dir, "scripts/grapher/COVID-19 - Vaccinations.csv")),
        )
        
        # Others
        self.aggregates = self.build_aggregates()

    def build_aggregates(self):
        continent_countries = pd.read_csv(self.inputs.continent_countries, usecols=["Entity", "Unnamed: 3"])
        eu_countries = pd.read_csv(self.inputs.eu_countries, usecols=["Country"], squeeze=True).tolist()
        aggregates = {
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
            aggregates[continent] = {
                "excluded_locs": None,
                "included_locs": (
                    continent_countries.loc[continent_countries["Unnamed: 3"] == continent, "Entity"].tolist()
                )
            }
        return aggregates

    def pipeline_automated(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate DataFrame for automated states."""
        return (
            df
            .sort_values(by=["automated", "location"], ascending=[False, True])
            [["location", "automated"]]
            .reset_index(drop=True)
        )

    def pipeline_locations(self, df_vax: pd.DataFrame, df_metadata: pd.DataFrame,
                           df_iso: pd.DataFrame) -> pd.DataFrame:
        """Generate DataFrame for locations."""
        def _pretty_vaccine(vaccines):
            return ", ".join(sorted(v.strip() for v in vaccines.split(',')))
        df_vax = (
            df_vax
            .sort_values(by=["location", "date"])
            .drop_duplicates(subset=["location"], keep="last")
            .rename(columns={
                "date": "last_observation_date",
                "source_url": "source_website"
            })
        )

        if len(df_metadata) != len(df_vax):
            raise ValueError("Missmatch between vaccination data and metadata!")

        return (
            df_vax
            .assign(vaccines=df_vax.vaccine.apply(_pretty_vaccine)) # Keep only last vaccine set
            .merge(df_metadata, on="location")
            .merge(df_iso, on="location")
        )[["location", "iso_code", "vaccines", "last_observation_date", "source_name", "source_website"]]

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
                self._get_aggregate(
                    df=df,
                    agg_name=agg_name,
                    included_locs=self.aggregates[agg_name]["included_locs"],
                    excluded_locs=self.aggregates[agg_name]["excluded_locs"]
                )
            )
        return pd.concat([df] + aggs, ignore_index=True)

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

    def get_population(self, df_subnational: pd.DataFrame) -> pd.DataFrame:
        # Build population dataframe
        column_rename = {
            "entity": "location",
            "population": "population"
        }
        pop = pd.read_csv(self.inputs.population, usecols=column_rename.keys()).rename(columns=column_rename)
        pop = pd.concat([pop, df_subnational], ignore_index=True)
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
        df_subnational = pd.read_csv(self.inputs.population_sub, usecols=["location", "population"])
        pop = self.get_population(df_subnational)
        df = df.merge(pop, on="location")
        # Get covered countries
        locations = df.location.unique()
        ncountries = df_subnational.location.tolist() + list(self.aggregates.keys())
        countries_covered = list(filter(lambda x: x not in ncountries, locations))
        # Obtain per-capita metrics
        df = df.assign(
            total_vaccinations_per_hundred=(df.total_vaccinations * 100 / df.population).round(2),
            people_vaccinated_per_hundred=(df.people_vaccinated * 100 / df.population).round(2),
            people_fully_vaccinated_per_hundred=(df.people_fully_vaccinated * 100 / df.population).round(2),
            new_vaccinations_smoothed_per_million=(df.new_vaccinations_smoothed * 1000000 / df.population).round(),
        )
        df.loc[:, "people_fully_vaccinated"] = df.people_fully_vaccinated.replace({0: pd.NA})
        df.loc[df.people_fully_vaccinated.isnull(), "people_fully_vaccinated_per_hundred"] = pd.NA
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
            [["date", "location", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]]
            .pipe(self.pipe_aggregates)
            .pipe(self.pipe_daily)
            .pipe(self.pipe_smoothed)
            .pipe(self.pipe_capita)
            .pipe(self.pipe_vax_checks)
            .sort_values(by=["location", "date"])
        )

    def pipe_vaccinations_out(self, df: pd.DataFrame, df_iso: pd.DataFrame) -> pd.DataFrame:
        df = df.merge(df_iso, on="location")
        df = df.rename(columns={
            "new_vaccinations_smoothed": "daily_vaccinations",
            "new_vaccinations_smoothed_per_million": "daily_vaccinations_per_million",
            "new_vaccinations": "daily_vaccinations_raw",
        })
        return df

    def pipe_grapher_out(self, df: pd.DataFrame):
        df = df.rename(columns={
            "date": "Year",
            "location": "Country",
        })[[
            "Country",
            "Year",
            "total_vaccinations",
            "people_vaccinated",
            "people_fully_vaccinated",
            "new_vaccinations",
            "new_vaccinations_smoothed",
            "total_vaccinations_per_hundred",
            "people_vaccinated_per_hundred",
            "people_fully_vaccinated_per_hundred",
            "new_vaccinations_smoothed_per_million",
        ]]
        df.loc[:, "location"] = (df.Year - datetime(2021, 1, 21)).dt.days
        return df

    def run(self):
        print("load")
        files = []
        df_metadata = pd.read_csv(self.inputs.metadata)
        df_vaccinations = pd.read_csv(self.inputs.vaccinations, parse_dates=["date"])
        df_iso = pd.read_csv(self.inputs.iso)

        # Metadata  
        print("automated")
        df_automated = df_metadata.pipe(self.pipeline_automated)  # Export to AUTOMATED_STATE_FILE
        files.append((df_automated.copy(), self.outputs.automated))
        print("locations")
        df_locations = df_vaccinations.pipe(self.pipeline_locations, df_metadata, df_iso)  # Export to LOCATIONS_FILE
        files.append((df_locations.copy(), self.outputs.locations))

        # Vaccinations
        print("vax")
        df_vaccinations_base = df_vaccinations.pipe(self.pipeline_vaccinations)
        #files.append((vax.copy(), self.outputs.vaccinations))
        
        # Generate final files
        df_vaccinations = df_vaccinations_base.pipe(self.pipe_vaccinations_out, df_iso)
        files.append((df_vaccinations.copy(), self.outputs.vaccinations))
        # json_vaccinations = self.jsonify(df_vaccinations)
        # files.append((json_vaccinations, self.outputs.vaccinations_json))

        df_grapher = df_vaccinations_base.pipe(self.pipe_grapher_out)
        files.append((df_grapher.copy(), self.outputs.grapher))

        # Export
        for obj, path in files:
            if path.endswith(".csv"):
                obj.to_csv(path, index=False)
            elif path.endswith(".json"):
                with open(path, 'w') as f:
                    json_string = json.dumps(obj, default=lambda o: o.__dict__, sort_keys=True, indent=2)
                    f.write(obj)
        return vax, automated_state, metadata
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
