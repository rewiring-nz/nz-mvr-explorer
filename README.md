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

2. Get the secret token from MotherDuck (ask Jenny for access) and add it to `.streamlit/secrets.toml`
 
3. Run the app

   ```
   streamlit run app.py
   ```