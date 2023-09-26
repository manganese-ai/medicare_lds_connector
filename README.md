# FFS Claims Sandbox (using DBT)

Based off the Tuva Medicare LDS Connector, see their readme in Tuva_README.md

QUESTIONS
- Tuva only seems set up to run one year at a time. There's a single `cms_hcc_payment_year` variable and the beneficiary data (I think) only requires one row per patient, so it does not accommodate changes over time.

## Set up

Install dbt 1.2+ using poetry:
- `pip install poetry`
- `poetry init`
- `poetry add dbt-snowflake`
- `poetry shell`

Run the following
- `dbt deps`
- `dbt init` and enter your manganese snowflake creds
- `dbt debug`
- `dbt --version` # should have an up-to-date dbt-core and snowflake plugin

If applicable: go to `~/.dbt/profiles.yml` and update role

## Seed
Claims data is seeded from sample LDS data available on [CMS](https://data.cms.gov/collection/synthetic-medicare-enrollment-fee-for-service-claims-and-prescription-drug-event). They generated their data using [Synthea](https://github.com/synthetichealth/synthea/wiki/Basic-Setup-and-Running).
- Place these files in the `seeds/` folder
- If the database is empty then you need to run: `dbt seed` - only need to run one time to seed the initial tables
- Note 9/26: we seeded directly to snowflake, instead of using dbt seed (see `notebooks/snowflake_conn` folder). The original data live in the `schmidt-futures` repo in the `data/synthetic_ffs/preprocessed` folder.

## Run
- `dbt build --select path:./models`
- `dbt build --select cms_hcc`
- `dbt build --select pmpm`
