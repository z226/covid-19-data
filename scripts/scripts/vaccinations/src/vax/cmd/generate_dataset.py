import os
import tempfile
import itertools
from datetime import datetime
from collections import ChainMap
from math import isnan
import glob
import json

import pandas as pd

from vax.cmd.utils import get_logger, print_eoe
from vax.utils.checks import VACCINES_ACCEPTED


logger = get_logger()

class Bucket(object):
    def __init__(self, **kwargs):
        self._dict = kwargs
        self.__dict__.update(kwargs)


class DatasetGenerator:

    def __init__(self, inputs, outputs):
        # Inputs
        self.inputs = inputs
        # Outputs
        self.outputs = outputs
        # Others
        self.aggregates = self.build_aggregates()
        self._countries_covered = None

    @property
    def column_names_int(self):
        return [
            'total_vaccinations',
            'people_vaccinated',
            'people_fully_vaccinated',
            'daily_vaccinations_raw',
            'daily_vaccinations',
            'daily_vaccinations_per_million',
            'new_vaccinations_smoothed',
            'new_vaccinations_smoothed_per_million',
            'new_vaccinations'
        ]

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
        logger.info("Building aggregate regions")
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
        logger.info("Adding daily metrics")
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
        logger.info("Adding smoothed variables")
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
        logger.info("Adding per-capita variables")
        # Get data
        df_subnational = pd.read_csv(self.inputs.population_sub, usecols=["location", "population"])
        pop = self.get_population(df_subnational)
        df = df.merge(pop, on="location")
        # Get covered countries
        locations = df.location.unique()
        ncountries = df_subnational.location.tolist() + list(self.aggregates.keys())
        self._countries_covered = list(filter(lambda x: x not in ncountries, locations))
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
        logger.info("Sanity checks")
        # Sanity checks
        if not (df.total_vaccinations.dropna() >= 0).all():
            raise ValueError(" Negative values found! Check values in `total_vaccinations`.")
        if not (df.new_vaccinations_smoothed.dropna() >= 0).all():
            raise ValueError(" Negative values found! Check values in `new_vaccinations_smoothed`.")
        if not (df.new_vaccinations_smoothed_per_million.dropna() <= 120000).all():
            raise ValueError(" Huge values found! Check values in `new_vaccinations_smoothed_per_million`.")
        return df

    def pipe_to_int(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Converting INT columns to int")
        # Ensure Int types
        cols = df.columns
        count_cols = [col for col in self.column_names_int if col in cols]
        df[count_cols] = df[count_cols].astype("Int64").fillna(pd.NA)
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
            .pipe(self.pipe_to_int)
            .sort_values(by=["location", "date"])
        )

    def pipe_vaccinations_csv(self, df: pd.DataFrame, df_iso: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .merge(df_iso, on="location")
            .rename(columns={
                "new_vaccinations_smoothed": "daily_vaccinations",
                "new_vaccinations_smoothed_per_million": "daily_vaccinations_per_million",
                "new_vaccinations": "daily_vaccinations_raw",
            })
            [[
                'location',
                'iso_code',
                'date',
                'total_vaccinations',
                'people_vaccinated',
                'people_fully_vaccinated',
                'daily_vaccinations_raw',
                'daily_vaccinations',
                'total_vaccinations_per_hundred',
                'people_vaccinated_per_hundred',
                'people_fully_vaccinated_per_hundred',
                'daily_vaccinations_per_million',
            ]]
        )

    def pipe_vaccinations_json(self, df: pd.DataFrame) -> list:
        location_iso_codes = df[["location", "iso_code"]].drop_duplicates().values.tolist()
        metrics = [column for column in df.columns if column not in {"location", "iso_code"}]
        df = df.assign(date=df.date.apply(lambda x: x.strftime("%Y-%m-%d")))
        return [
            {
                "country": location,
                "iso_code": iso_code,
                "data": [
                    {
                        **x[i]} 
                        for i, x in df.loc[(df.location == location) & (df.iso_code == iso_code), metrics].stack().
                        groupby(level=0)
                ]
            }
            for location, iso_code in location_iso_codes
        ]

    def pipe_manufacturer_select_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df[[
                "location",
                "date",
                "vaccine",
                "total_vaccinations",
            ]]
            .sort_values(["location", "date", "vaccine"])
        )

    def pipe_manufacturer_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        if not df.vaccine.drop_duplicates().isin(VACCINES_ACCEPTED).all():
            raise ValueError("Non valid vaccines found in manufacturer file! Check vax.utils.checks.VACCINES_ACCEPTED")
        return df

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_manufacturer_select_cols)
            .pipe(self.pipe_manufacturer_checks)
            .pipe(self.pipe_to_int)
        )

    def pipe_grapher(self, df: pd.DataFrame, date_ref: datetime = datetime(2020, 1, 21),
                     fillna: bool = False) -> pd.DataFrame:
        df = (
            df
            .rename(columns={
                "date": "Year",
                "location": "Country",
            })
            .assign(Year=(df.date - date_ref).dt.days)
        )
        columns_first = ["Country", "Year"]
        columns_rest = [col for col in df.columns if col not in columns_first]
        col_order = columns_first + columns_rest
        df = (
            df
            [col_order]
            .sort_values(col_order)
        )
        if fillna:
            df[columns_rest] = df.groupby(["Country"])[columns_rest].fillna(method="ffill").fillna(0)
        return df

    def pipe_manufacturer_pivot(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pivot(
                index=["location", "date"],
                columns="vaccine",
                values="total_vaccinations"
            )
            .reset_index()
        )

    def pipeline_manufacturer_grapher(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .pipe(self.pipe_manufacturer_pivot)
            .pipe(self.pipe_grapher, date_ref=datetime(2021, 1, 1), fillna=True)
            .pipe(self.pipe_to_int)
        )

    def pipe_locations_to_html(self, df: pd.DataFrame) -> pd.DataFrame:
        # build table
        country_faqs = {
            "Israel",
            "Palestine",
        }
        faq = ' (see <a href="https://ourworldindata.org/covid-vaccinations#frequently-asked-questions">FAQ</a>)'
        df = df.assign(
            location=(
                df.location
                .apply(lambda x: f"<td><strong>{x}</strong>{faq if x in country_faqs else ''}</td>")
            ),
            source=(
                '<td><a href="' + df.source_website + '">' + df.source_name + '</a></td>'
            ),
            last_observation_date=(
                df.last_observation_date
                .apply(lambda x: f"<td>{x.strftime('%b. %e, %Y')}</td>")
            ),
            vaccines=(
                df.vaccines
                .apply(lambda x: f"<td>{x}</td>")
            )
        )[["location", "source", "last_observation_date", "vaccines"]]
        df.columns = [col.capitalize().replace("_", " ") for col in df.columns]
        body = ("<tr>" + df.sum(axis=1) + "</tr>").sum(axis=0)
        header = "<tr>" + "".join(f"<th>{col}</th>" for col in df.columns) + "</tr>"
        html_table = f"<table><tbody>{header}{body}</tbody></table>"
        coverage_info = f"Vaccination against COVID-19 has now started in {len(self._countries_covered)} locations."
        html_table = (
            f'<div class="wp-block-full-content-width"><p><strong>{coverage_info}</strong></p>{html_table}</div>\n'
        ).replace("  ", " ")
        return html_table

    def export(self, df_automated: pd.DataFrame, df_locations: pd.DataFrame, df_vaccinations: pd.DataFrame,
               df_manufacturer: pd.DataFrame, json_vaccinations: dict, df_grapher: pd.DataFrame,
               df_manufacturer_grapher: pd.DataFrame, html_table: str):
        # Export
        files = [
            (df_automated, self.outputs.automated),
            (df_locations, self.outputs.locations),
            (df_vaccinations, self.outputs.vaccinations),
            (df_manufacturer, self.outputs.manufacturer),
            (json_vaccinations, self.outputs.vaccinations_json),
            (df_grapher, self.outputs.grapher),
            (df_manufacturer_grapher, self.outputs.grapher_manufacturer),
            (html_table, self.outputs.html_table),
        ]
        for obj, path in files:
            if path.endswith(".csv"):
                obj.to_csv(path, index=False)
            elif path.endswith(".json"):
                with open(path, 'w') as f:
                    json.dump(obj, f, indent=2)  # default=lambda o: o.__dict__, sort_keys=True
            elif path.endswith(".html"):
                with open(path, "w") as f:
                    f.write(obj)
            else:
                raise ValueError("Format not supported. Currently only csv, json and html are accepted!")

    def run(self):
        print("-- Generating dataset... --")
        logger.info("1/9 Loading input data...")
        try:
            df_metadata = pd.read_csv(self.inputs.metadata)
            df_vaccinations = pd.read_csv(self.inputs.vaccinations, parse_dates=["date"])
        except FileNotFoundError:
            raise FileNotFoundError(
                "Internal files not found! Make sure to run `proccess-data` step prior to running `generate-dataset`."
            )
        df_iso = pd.read_csv(self.inputs.iso)
        files_manufacturer = glob.glob(self.inputs.manufacturer)
        df_manufacturer = pd.concat(
            (pd.read_csv(filepath, parse_dates=["date"]) for filepath in files_manufacturer),
            ignore_index=True
        )
        
        # Metadata  
        logger.info("2/9 Generating `automated_state` table...")
        df_automated = df_metadata.pipe(self.pipeline_automated)  # Export to AUTOMATED_STATE_FILE
        logger.info("3/9 Generating `locations` table...")
        df_locations = df_vaccinations.pipe(self.pipeline_locations, df_metadata, df_iso)  # Export to LOCATIONS_FILE

        # Vaccinations
        logger.info("4/9 Generating `vaccinations` table...")
        df_vaccinations_base = df_vaccinations.pipe(self.pipeline_vaccinations)
        df_vaccinations = df_vaccinations_base.pipe(self.pipe_vaccinations_csv, df_iso)
        logger.info("5/9 Generating `vaccinations` json...")
        json_vaccinations = df_vaccinations.pipe(self.pipe_vaccinations_json)

        # Manufacturer
        logger.info("6/9 Generating `manufacturer` table...")
        df_manufacturer = df_manufacturer.pipe(self.pipeline_manufacturer)

        # Grapher
        logger.info("7/9 Generating `grapher` table...")
        df_grapher = df_vaccinations_base.pipe(self.pipe_grapher)
        df_manufacturer_grapher = df_manufacturer.pipe(self.pipeline_manufacturer_grapher)

        # HTML
        logger.info("8/9 Generating HTML...")
        html_table = df_locations.pipe(self.pipe_locations_to_html)

        # Export
        logger.info("9/9 Exporting files...")
        self.export(
            df_automated,
            df_locations,
            df_vaccinations,
            df_manufacturer,
            json_vaccinations,
            df_grapher,
            df_manufacturer_grapher,
            html_table,
        )


def main_generate_dataset(paths):
    # Select columns
    inputs = Bucket(
        project_dir=paths.project_dir,
        vaccinations=paths.tmp_vax_all,
        metadata=paths.tmp_met_all,
        iso=os.path.join(paths.project_dir, "scripts/input/iso/iso3166_1_alpha_3_codes.csv"),
        population=os.path.join(paths.project_dir, "scripts/input/un/population_2020.csv"),
        population_sub=os.path.join(paths.project_dir, "scripts/input/owid/subnational_population_2020.csv"),
        continent_countries=os.path.join(paths.project_dir, "scripts/input/owid/continents.csv"),
        eu_countries=os.path.join(paths.project_dir, "scripts/input/owid/eu_countries.csv"),
        manufacturer=os.path.join(paths.project_dir, "scripts/scripts/vaccinations/output/by_manufacturer/*.csv")
    )
    outputs = Bucket(
        locations=os.path.join(paths.project_dir, "public/data/vaccinations/locations.csv"),
        automated=os.path.abspath(os.path.join(paths.project_dir, "scripts/scripts/vaccinations/automation_state.csv")),
        vaccinations=os.path.abspath(os.path.join(paths.project_dir, "public/data/vaccinations/vaccinations.csv")),
        vaccinations_json=(
            os.path.abspath(os.path.join(paths.project_dir, "public/data/vaccinations/vaccinations.json"))
        ),
        manufacturer=(
            os.path.abspath(os.path.join(paths.project_dir,
            "public/data/vaccinations/vaccinations-by-manufacturer.csv"))
        ),
        grapher=os.path.abspath(os.path.join(paths.project_dir, "scripts/grapher/COVID-19 - Vaccinations.csv")),
        grapher_manufacturer=os.path.abspath(os.path.join(paths.project_dir, "scripts/grapher/COVID-19 - Vaccinations by manufacturer.csv")),
        html_table=os.path.abspath(os.path.join(paths.project_dir, "scripts/scripts/vaccinations/source_table.html")),
    )
    generator = DatasetGenerator(inputs, outputs)
    generator.run()
