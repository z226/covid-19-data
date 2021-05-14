# Vaccination update automation

Vaccination data is updated on a daily basis. For some countries, the update is done by means of an automated process,
while others require some manual work. To keep track of the currently automated processes, check [this
table](automation_state.csv). 

 and [`batch`](src/vax/batch) and [`incremental`](src/vax/incremental) folders for
automated scripts.

### Content
1. [Directory content](#1-directory-content)
2. [Development environment](#2-development-environment)
3. [The data pipeline](#3-the-data-pipeline)
4. [Generated files](#4-generated-files)
5. [Other functions](#5-other-functions)
6. [Contribute](#6-contribute)
7. [FAQs](#7-FAQs)

## 1. Directory content
This directory contains the following files:


| File name      | Description |
| ----------- | ----------- |
| [`output/`](output)      | Temporary automated imports are placed here.       |
| [`src/vax/`](src/vax)      | Scripts to automate country data imports.       |
| [`config.yaml`](config.yaml)      | Data pipeline configuration.       |
| [`us_states/input/`](us_states/input)      | Data for US-state vaccination data updates.       |
| [`MANIFEST.in`](MANIFEST.IN), [`setup.py`](setup.py), [`requirements.txt`](requirements.txt), [`requirements-flake.txt`](requirements-flake.txt)      |     Library development related files   |
| [`automation_state.csv`](automation_state.csv)      |     List if the data import for a location is automated (TRUE) or manual (FALSE)   |
| [`source_table.html`](source_table.html)      | Table with sources used, used in OWID website.       |
| [`vax_update.sh.template`](vax_update.sh.template)      | Template to push vaccination update changes.       |

_*Only most relevant files have been listed_ 


## 2. Development environment
<details open>
<summary>Show steps ...</summary>

Make sure you have a working environment with R and python 3 installed. We use Python >= 3.7.

You can check:

```
$ python --version
```

### Install python requirements
In your environment (shell), run:

```
$ pip install -e .
```

This will install our development library `owid-covid19-vaccination-dev`. Additionally, it will install the command
`cowid-vax`, which provides all the necessary tools to run the data pipeline.

```txt
usage: cowid-vax [-h] [-c COUNTRIES] [-p] [-j NJOBS] [-s] [--config CONFIG] [--credentials CREDENTIALS] [--checkr]
                 {get-data,process-data,generate-dataset,export,all}

Execute COVID-19 vaccination data collection pipeline.

positional arguments:
  {get-data,process-data,generate-dataset,export,all}
                        Choose a step: i) `get-data` will run automated scripts, 2) `process-data` will get csvs generated in
                        1 and collect all data from spreadsheet, 3) `generate-dataset` generate the output files, 4) `export`
                        to generate all final files, 5) `all` will run all steps sequentially.

optional arguments:
  -h, --help            show this help message and exit
  -c COUNTRIES, --countries COUNTRIES
                        Run for a specific country. For a list of countries use commas to separate them (only in mode get-
                        data)E.g.: peru, norway. Special keywords: 'all' to run all countries, 'incremental' to run
                        incrementalupdates, 'batch' to run batch updates. Defaults to all countries. (default: all)
  -p, --parallel        Execution done in parallel (only in mode get-data). (default: False)
  -j NJOBS, --njobs NJOBS
                        Number of jobs for parallel processing. Check Parallel class in joblib library for more info (only in
                        mode get-data). (default: -2)
  -s, --show-config     Display configuration parameters at the beginning of the execution. (default: False)
  --config CONFIG       Path to config file (YAML). Will look for file in path given by environment variable
                        `$OWID_COVID_VAX_CONFIG_FILE`. If not set, will default to ~/.config/cowid/config.yaml (default:
                        /Users/lucasrodes/repos/covid-19-data/scripts/scripts/vaccinations/config.yaml)
  --credentials CREDENTIALS
                        Path to credentials file (JSON). If a config file is being used, the value ther will be prioritized.
                        (default: vax_dataset_config.json)
  --checkr              Compare results from generate-dataset with results obtained with former generate_dataset.R script.It
                        requires that the R script is previously run (without removing temporary files vax & metadata)!
                        (default: False)

```

### Configuration file

To correctly run the pipeline, make sure to have a valid configuration file. We currently use
[config.yaml](config.yaml). This file contains data used throughout the different pipeline stages.

```yaml
global:
  project_dir: !ENV ${OWID_COVID_PROJECT_DIR}
  credentials: !ENV ${OWID_COVID_VAX_CREDENTIALS_FILE}
pipeline:
  get-data:
    parallel: True
    countries:
    njobs: -2
  process-data:
    skip_complete:
    skip_monotonic_check:
        - Northern Ireland
        - Malta
        - Romania
        - Sweden
  generate-dataset:
```

Our config.yaml configuration requires to previously set environment variables `${OWID_COVID_PROJECT_DIR}` and
`${OWID_COVID_VAX_CREDENTIALS_FILE}`. We recommend defining them in `~/.bashrc` or `/.bash_profile`. For instance:

```sh
export OWID_COVID_PROJECT_DIR=/Users/username/projects/covid-19-data
export OWID_COVID_VAX_CREDENTIALS_FILE=${OWID_COVID_PROJECT_DIR}/scripts/scripts/vaccinations/vax_dataset_config.json
```

### Credentials file
The environment variable `${OWID_COVID_VAX_CREDENTIALS_FILE}` corresponds to the path to the credentials file. This is internal. Google-related fields require a valid OAuth JSON credentials file (see [gsheets
  documentation](https://gsheets.readthedocs.io/en/stable/#quickstart)). The file should have the following structure:
```json
{
    "greece_api_token": "[GREECE_API_TOKEN]",
    "owid_cloud_table_post": "[OWID_CLOUD_TABLE_POST]",
    "google_credentials": "[CREDENTIALS_JSON_PATH]",
    "google_spreadsheet_vax_id": "[SHEET_ID]"
}
```

### Check the style
We use [flake8](https://flake8.pycqa.org/en/latest/) to check the style of our code. The configuration lives in file 
[tox.ini](tox.ini). To check the style, simply run

```sh
$ tox
```
**Note**: This requires tox to be installed (`$ pip install tox`)
</details>

## 3. The data pipeline
To update the data, prior to runing the code, make sure to correctly [set up the development environment](#development-environment).

### Manual data updates

Check for new updates and manually add them in the internal spreadsheet:
- See this repo's [pull requests](https://github.com/owid/covid-19-data/pulls) and [issues](https://github.com/owid/covid-19-data/issues).
- Look for new data based on previously-used source URLs.

### Automated process
Run the following script:

```
$ cowid-vax
```

Runing it like this, without specifying argument `--config`, will try to load the configuration using environment variable
`${OWID_COVID_VAX_CONFIG_FILE}`. See `cowid-vax --help` for more details.


By default this will do the following:
1. Run the scrips for [batch](src/vax/batch) and [incremental](src/vax/incremental) updates. It will then generate
  individual country files and save them in [`output`](output).
2. Collect manually updated data from the spreadsheet and data generated in (1). Process this data, and generate public country data in
  [`country_data`](../../../public/data/vaccinations/country_data/), as well as temporary files 
  `vaccinations.preliminary.csv` and `metadata.preliminary.csv`.
3. Generate pipeline output files: 
    - [`public/data/vaccinations/locations.csv`](../../../public/data/vaccinations/locations.csv)
    - [`scripts/scripts/vaccinations/automation_state.csv`](automation_state.csv)
    - [`public/data/vaccinations/vaccinations.csv`](../../../public/data/vaccinations/vaccinations.csv)
    - [`public/data/vaccinations/vaccinations.json`](../../../public/data/vaccinations/vaccinations.json)
    - [`public/data/vaccinations/vaccinations-by-manufacturer.html`](../../../public/data/vaccinations/vaccinations-by-manufacturer.csv)
    - [`scripts/grapher/COVID-19 - Vaccinations.csv`](../../../scripts/grapher/COVID-19%20-%20Vaccinations.csv)
    - [`scripts/grapher/COVID-19 - Vaccinations by manufacturer.csv`](../../../scripts/grapher/COVID-19%20-%20Vaccinations%20by%20manufacturer.csv)
    - [`scripts/scripts/vaccinations/source_table.html`](source_table.html)

**Notes**:
- This step might crash for some countries, as the automation scripts might no longer (or temporarily) work
(e.g. due to changes in the source). Try to keep the scripts up to date.
- Optionally you can use positional argument `get-data`, `process-data` and `generate-dataset` to only execute a
  particular step.

```sh
$ cowid-vax get-data
$ cowid-vax process-data
$ cowid-vax generate-dataset
```

To use [config.yaml](config.yaml), you can:
  - Set environment variable `${OWID_COVID_VAX_CONFIG_FILE}` to file's path.
  - Save config under `~/.config/cowid/config.yaml` and run.
  - Run `$ cowid-vax --config config.yaml`, explicitly specifying the path to the config file.
If above was not possible, use arguments passed via the command call, i.e. `--parallel`, `--countries`, etc.


For more details check `$ cowid-vax --help`.

### Megafile generation

This will update the complete COVID dataset, which also includes all vaccination metrics:

```
$ python ../megafile.py
```

**Note**: you can use [vax_update.sh.template](vax_update.sh.template) as an example of how to automate data updates and push them to the repo.

## 4. Generated files
Once the automation is successfully executed (see [Update the data](#update-the-data) section), the following files are updated:

| File name      | Description |
| ----------- | ----------- |
| [`vaccinations.csv`](../../../public/data/vaccinations/vaccinations.csv)      | Main output with vaccination data of all countries.       |
| [`vaccinations.json`](../../../public/data/vaccinations/vaccinations.json)   | Same as `vaccinations.csv` but in JSON format.        |
| [`country_data`](../../../public/data/vaccinations/country_data/)   | Individual country CSV files.        |
| [`locations.csv`](../../../public/data/vaccinations/locations.csv)   | Country-level metadata.        |
| [`vaccinations-by-manufacturer.csv`](../../../public/data/vaccinations/vaccinations-by-manufacturer.csv)   | Secondary output with vaccination by manufacturer for a select number of countries.        |
| [`COVID-19 - Vaccinations.csv`](../../grapher/COVID-19%20-%20Vaccinations.csv)   | Internal file for OWID grapher on vaccinations.        |
| [`COVID-19 - Vaccinations by manufacturer.csv`](../../grapher/COVID-19%20-%20Vaccinations%20by%20manufacturer.csv)   | Internal file for OWID grapher on vaccinations by manufacturer.        |

_You can find more information about these files [here](../../../public/data/vaccinations/README.md)_.

## 5. Other functions
### Tracking
It is extremely usefull to get some insights on which data are we tracking (and which are we not). This can be done with
the tool `cowid-vax-track`. Find below some use cases.

**Note**: Use uption `--to-csv` to export results as csv files (a default filename is used).

#### Which countries are missing?
<details closed>
<summary>Show</summary>
Run 

```
$ cowid-vax-track countries-missing
```
Countries are given from most to least populated.
</details>

#### Which countries have been updated unfrequently?
<details closed>
<summary>Show</summary>
Get the list of countries sorted by least frequently updated. The update frequency is defined by the ratio between the 
number of days with an update and the number of days of observation (i.e. days since first update).

```
$ cowid-vax-track countries-least-updatedfreq
```

Countries are given from least to most frequently updated.
</details>

#### Which countries haven't been updated for some time?
<details closed>
<summary>Show</summary>
Get the list of countries and their last update by running:

```
$ cowid-vax-track countries-last-updated
```

Countries are given from least to most recently updated.
</details>

#### Which countries have been updated few times?
<details closed>
<summary>Show</summary>
Get the list of countries least updated (in absolute counts):

```
$ cowid-vax-track countries-least-updated
```

Countries are given from least to most frequently updated.
</details>

#### Which vaccines are missing?
<details closed>
<summary>Show</summary>
Get the list of countries with missing vaccines:

```
$ cowid-vax-track vaccines-missing
```

Countries are given from the one with the least to the one with he most number of untracked vaccines.
</details>


## 6. Contribute
We welcome contributions to the projects! Note that due to the nature of our pipeline, **we cannot accept pull requests
for manually imported country data**. To see which countries are automated and which reaquire manual import, check
[this file](automation_state.csv).


### Report new data values
To report new values for a country/location, first check if the imports for that country/territory are automated. You
can check column `automated` in [this file](automation_state.csv).

- If the country imports are automated (`TRUE` value in file above), note that the new value might be added in next
  update. **Only report new values if the data is missing for more than 48 hours!** Report the new data as a [pull request](https://github.com/owid/covid-19-data/compare).
- If the country imports are not automated, i.e. data is manually added, (`FALSE` value in file above) you can report
  new data in any of the following ways:
  - Open a [new issue](https://github.com/owid/covid-19-data/issues/new), reporting the data and the corresponding
    source.
  - If you plan to contribute regularly to a specific country/location, consider opening a dedicated issue. This way,
    we can easily back-track the data addded for that country/location.
  - If this seems too complicated, alternatively, you may simply add a comment to thread
[#230](https://github.com/owid/covid-19-data/issues/230). 

**Note**: We only accept official sources or news correctly citing official sources.
### Add new automated data collections
The scripts that automate country imports are located in [`src/vax/batch`](src/vax/batch) or
[`src/vax/incremental`](src/vax/incremental), depending on whether they import the data in batch (i.e. all the
timeseries) or incrementally (last value).

We welcome pull requests automations and improvements on our automations. Follow the steps bellow:

1. Create a script and place it in [`src/vax/batch`](src/vax/batch) or
[`src/vax/incremental`](src/vax/incremental) depending, on whether it is an incremental or batch update (see [#250](https://github.com/owid/covid-19-data/issues/250)
for more details).
2. Test that it is working and stable.
3. Issue a pull request and wait for a review.



More details: [#230](https://github.com/owid/covid-19-data/issues/230),
[#250](https://github.com/owid/covid-19-data/issues/250)

### Accepting pull requests
Due to how our pipeline operates at the moment, pull requests are only accepted under certain conditions. These include,
but are not limited to, the following:

- Code improvements / bug fixes. As an example, you can take [#465](https://github.com/owid/covid-19-data/pull/465).
- Updates on the data for countries with automated data imports and incremental processes (this countries are found
  [here](src/vax/incremental)). For this case, you can create a PR modifying the corresponding file in [output
  folder](https://github.com/owid/covid-19-data/tree/master/scripts/scripts/vaccinations/output). Create the pull
  request only if the daily update already ran but did not update the corresponding country.

You can of course, and we appreciate it very much, create pull requests for other cases.

Note that files in [public folder](https://github.com/owid/covid-19-data/tree/master/public) are not to be manually modified.
## 7. FAQs

### Any question or suggestion?
Kindly open an [issue](https://github.com/owid/covid-19-data/issues/new). If you have a technical proposal, feel free to
open a [pull request](https://github.com/owid/covid-19-data/compare)

### An automation no longer works (internal)
If you detect that an automation is no longer working, and the process seems like it can't be fixed at the moment:
- Set its state to `automated = FALSE` in the `LOCATIONS` tab of the internal spreadsheet.
- Add a new tab in the spreadsheet to manually input the country data. Make sure to include the historical data from the [`output`](output) file.
- Delete the automation script and automated CSV output to avoid confusion.
