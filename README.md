# New Zealand Motor Vehicle Register Explorer

A simple Streamlit app that lets you explore the data in the MVR. The data is hosted in MotherDuck. Ask Jenny for access if you need it.

- [Data source - the Motor Vehicle Registry dataset from NZTA](https://nzta.govt.nz/resources/new-zealand-motor-vehicle-register-statistics/new-zealand-vehicle-fleet-open-data-sets). 
- [Data dictionary](https://docs.google.com/spreadsheets/d/153bzOAGHSAmMhO3kRpc8Phu2sF21YPtu0c2WJ9Hl6Q0/edit?gid=315789064#gid=315789064) (what each column means)

[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://nz-mvr.streamlit.app/)

## How to run locally

Make sure you have Python installed on your computer first.

1. Install the requirements

   ```
   pip install -r requirements.txt
   ```

2. Copy the file `.streamlit/secrets.toml.template` and rename the duplicate to `.streamlit/secrets.toml`. Get the access token from the [MotherDuck](https://app.motherduck.com/) dev@rewiring.nz account (look in our shared passwords manager for credentials).  Go to Settings > Integrations > Access Tokens, click `Create token`, and add a descriptive name for who is using this to access the database, e.g. `Jenny's laptop`. `Create token` and copy the token. Paste it into `.streamlit/secrets.toml`
 
3. Run the app

   ```
   streamlit run app.py
   ```

## How to update

### Add a new table to the DB

1. Download the updated MVR CSV from [NZTA](https://nzta.govt.nz/resources/new-zealand-motor-vehicle-register-statistics/new-zealand-vehicle-fleet-open-data-sets).
1. Log into [Motherduck](https://app.motherduck.com/) with the `dev@rewiring.nz` credentials.
1. In the sidebar under `Attached databases`, find the database `mvr` and the schema `main`. Click the `...` menu next to `main` > `Create table from files`.
1. Select the downloaded MVR, set `Table name` to something like `nov2025`, database and schema should be `mvr.main`, and `Ignore errors` should be turned off so that it does stop and tell you if some rows are failing to parse.

### Add the new table options to the UI

1. Add the table name from above to the `TABLES` list in constants.py. Optionally, you can also set the `DEFAULT_TABLE` value to your newly updated one.
