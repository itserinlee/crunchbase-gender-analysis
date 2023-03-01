# Overview

This project (a work-in-progress) is about analyzing Crunchbase data in order to learn about gender equity in start-ups and VC funding.

There is one main dataset from which all others are derived that relates start-ups, gender, and funding data: ***where 1 row = 1 company***. I only pulled those founded after 1989 to visualize a more relevant period. On any given row, there is information on the industry sector, the genders of the founders, and the funding history of any given company. Note: only U.S. companies were queried, but the founders of those orgs are not necessarily from the U.S.

The remaining datasets bin the data in the following ways: ***by investor***, ***by education level of the founder***, ***by industry*** (to find where the disparities are), ***by U.S. state***, and ***by total funding for every year*** between 1990-2021.

## Collecting Start-Up, Gender & Funding Info

<p align="center">
<img src=img/crunch_diagram.png title="Entity-Relationship Model" width="80%"/>
</p>


## Data Visualizations

***"Diverse" means founders, who were identified as female, non-binary, or unspecified.***

<br>

<p align="center">
<img src=img/funding_per_year.png title="Funding Total Over Time" width="80%"/>
</p>

Data for the above graph from `year_funded.csv` & code from `bin_year_funded.py`

<p align="center">
<img src=img/funding_per_year_by_gender.png title="Funding Over Time By Gender" width="80%"/>
</p>

Data for the above graph from `year_funded.csv` & code from `bin_year_funded.py`

<p align="center">
<img src=img/female_funding_wrt_time.png title="Female Funding History" width="80%"/>
</p>

Data for the above graph: `year_funded.csv` & code from `rates.py`

<p align="center">
<img src=img/male_funding_wrt_time.png title="Male Funding History" width="80%"/>
</p>

Data for the above graph: `year_funded.csv` & code from `rates.py`

<p align="center">
<img src=img/femaleVelocity.png title="Velocity of Female Funding" width="80%"/>
</p>

Data for the above graph: `year_funded.csv` & code from `rates.py`

<p align="center">
<img src=img/maleVelocity.png title="Velocity of Male Funding" width="80%"/>
</p>

Data for the above graph: `year_funded.csv` & code from `rates.py`

<p align="center">
<img src=img/femaleAcceleration.png title="Acceleration of Female Funding" width="80%"/>
</p>

Data for the above graph: `year_funded.csv` & code from `rates.py`

<p align="center">
<img src=img/maleAcceleration.png title="Acceleration of Male Funding" width="80%"/>
</p>

Data for the above graph: `year_funded.csv` & code from `rates.py`

<p align="center">
<img src=img/treemap_disparity.png title="Treemap of Industries With Respect to Disparity" width="80%"/>
</p>

Data for the above graph from `industries_count.csv` & code from `bin_industries.py`

<p align="center">
<img src=img/treemap_equality.png title="Treemap of Industries With Respect to Equality" width="80%"/>
</p>

Data for the above graph from `industries_count.csv` & code from `bin_industries.py`

<p align="center">
<img src=img/funding_stages_90-100_female.png title="Female Founders by Funding Stages" width="80%"/>
</p>

Data for the above graph: `org_funding.csv` & code from `post_process.py`

<p align="center">
<img src=img/industries_count.png title="Count of Founders by Industries" width="80%"/>
</p>

Data for the above graph: `industries_count.csv` & code from `bin_industries.py`

<p align="center">
<img src=img/industries_fraction.png title="Fraction of Founders by Industries" width="80%"/>
</p>

Data for the above graph: `industries_fraction.csv` & code from `bin_industries.py`

<p align="center">
<img src=img/count_founders_state.png title="Count of Founders by State" width="80%"/>
</p>

Data for the above graph: `state_count.csv` & code from `bin_states.py`

<p align="center">
<img src=img/fraction_founders_state.png title="Fraction of Founders by State" width="80%"/>
</p>

Data for the above graph: `state_fraction.csv` & code from `bin_states.py`

<p align="center">
<img src=img/fraction_by_degrees.png title="Investments by Founder Education Level" width="80%"/>
</p>

Data for the above graph: `degrees_fraction.csv` & code from `process_degrees.py`


## How Datasets Were Created

To produce the organizations.csv:
  1. crunch_library.py
  2. organizations.py
  3. people.py

`organizations.csv`: where 1 row = 1 company with gender info

To produce the org_funding.csv:
  1. crunch_library.py
  2. organizations.py
  3. people.py
  4. funding_rounds.py
  5. post_process.py

`org_funding.csv`: where 1 row = 1 company with gender info and the money raised at each funding round  (see last column)

More info: I used 3 queries to get this data from 3 relevant collections: organizations, funding rounds, and people. I pulled from the people collection for the gender of each founder and associated them back to their respective organization. I got the history of each company's funding from the funding rounds collection and added the history as a list within a column called `moneyRaised`.


To produce the binned data of total funding per each year since 1990:
  1. bin_year_funded.py 

`year_funded.csv`: binned by year starting from 1990, where 1 row = aggregate data over 1 year


To produce the binned data by industry:
  1. bin_industries.py

`industries_fraction.csv`: contains binned data for *fraction* of gender within each industry where 1 row = aggregate data for all companies for a given industry

`industries_count.csv`: contains binned data for *total count* of genders of founders within each industry where 1 row = aggregate data for all companies for a given industry

To produce the binned data by investor:
  1. bin_investors.py

`investors_fraction.csv`: contains binned data by investor where 1 row = aggregate data for all companies for a given investor

To produce the binned data by U.S. state:
  1. bin_states.py

`state_fraction.csv`: contains binned data of the *fraction* of founders by gender within each state where 1 row = aggregate data for all companies for a given state

`state_count.csv`: contains binned data of the *total* founders by gender within each state where 1 row = aggregate data for all companies for a given state

To produce the binned data for founder's education level by money invested:
  1. process_degrees.py

`degrees_fraction.csv`: contains binned data of the *fraction* of money invested towards founders by gender where 1 row = aggregate data for different education levels for a given gender

`degrees_count.csv`: contains binned data of the *total* money invested towards founders by gender where 1 row = aggregate data for different education levels for a given gender
# crunchbase-gender-analysis
