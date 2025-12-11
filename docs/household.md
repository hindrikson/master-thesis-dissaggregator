# Household Consumption

Here we describe how the consumption of private households is disaggreagted spatially and temporally.

# Spatial Disaggreagation
The spatial disaggreagation can be done by two different approaches:
- Top-Down: uses population data to distribute consumption based on the number of inhabitants in each area.
- Bottom-Up: uses the number of households to distribute consumption based on the number of households in each area.

Currently the Top-Down approach is not available. 

## Problem description
When using disagg_household_power(by="population")

The function elc_consumption_HH(year=year) is called to get the total electricity consumption of private households in Germany for a given year.
This function originally calls **table 2 "Stromverbrauch private Haushalte, 1991..2018"** in **MWh**
This table returns only one row with one value, which is the total consumption of private households in Germany for the specified year.

**Problem**: Only works until 2018.

Is there a table that returns the total electricity consumption of private households in Germany for years after 2018?

## Only Bottom-Up available
So far the only option available is using disagg_household_power(by="households"), which uses the Bottom-Up approach.

In this approach first the disaggregator fetches to total of energy consumption per household, in the whole germany.
It fetches table **table 13 "Stromverbrauch nach Haushaltsgröße"** for it. The table has 6 rows 0 (1 person), 1 (2 persons), 3 (4 persons) 4, (5 persons), 5 (6 persons).
This table, it seems, return not the total consumption for the whole germany according to the houshold type, but rather
a typical anual consumption for each type.

Then the function fetches **table 14 "Anzahl Haushalte nach Haushaltsgröße, 1990..2030"**.
This table after a bit of processing returns the number of households types per region.
Then this table is then multiplied by by the results of the table before (13). 
There is an option to scale these values by the population size.
(**Note** The method here is not exactly clear to me. 
It seems like the values returned by the table 13 is the total anual energy consumption for one only household type, like a typical energy consumption ammount for a specific household size.
This way it does make sense then to multiply this value to the number of each households types in each region to get the total energy consumption.
This way each region the energy consumption for each kind of household type.

The final dataframe looks like this:
| hh_size | 1          | 2          | ... | 5         | 6            |
|---------|------------|------------|-----|-----------|--------------|
| DE111   | 287072.910 | 277795.914 | ... | 52705.848 | 25970.568 |
| DE112   | 116986.836 | 166203.012 | ... | 40849.848 | 17220.840 |


**Problems**:
- table 13 only has data for 2015
- table 14 only has data for 2011

**Solution**:
**Table 55 "Stromverbrauch der Haushalte nach Haushaltsgroesse, 1990..2060"**. It returns a dataframe with the total consumption of every region
in **kWh/a** according to an internal_id. The internal ids are: 1, 2, 3, 4, 5 (**NOTE** The description in opendata is not completely right)
1=total, 2=1-person households, 3=2-person households, 4=3-person households, 5>=4-person or more households 
The table then looks like this:
| id_spatial | id_region_type | id_region | year | internal_id | value        |
|------------|----------------|-----------|------|-------------|--------------|
| 55         | 4              | 16055000  | 2015 | [5, 0]      | 1.010070e+07 |
| 55         | 4              | 16056000  | 2015 | [1, 0]      | 5.848985e+06 |

The intervals specify different household sizes. To get all the different households per region in one table either:
- **DO NOT** specify a internal ID.
- set internal_id=None when using database_get(internal_id=None)
- interval_id_1=1 returns the total of 2, 3, 4 and 5 summed up.

**NOTE**: until 2017 the the second internal id is always 0. From 2018 onwards the second internal is corresponds to different scnearios:
1 = moderate births and life expectancy, low migration balance (G2L2W1)
2 = moderate births, life expectancy and net migration (G2L2W2)
3 = moderate births and life expectancy, high net migration (G2L2W3)

The function **households_power_consumption** is implemented to process and filter the data for 2018 onwards.


To emulate the same behaviour as table 2, for each table fetched for a specific internal_id, one needs to sum all the values.
Then create a dataframe with 6 rows, one with the some of each internal_id. 

**Problem** is that the final values are much higher than the values returned by the table 13. It could be that the table 13 is returning the values alredy in MGh or GWh instead of KWh.

The code below return a table in the following format, which is similar to what the code does, but we here do not convert the id_region to NUTS3.

| hh_size   | 1             | 2             | 3             | 4            |
|-----------|---------------|---------------|---------------|--------------|
| id_region |               |               |               |              |
| 1001000   | 30040.658666  | 42941.384366  | 50385.640258  | 14811.731628 |
| 1002000   | 108827.216963 | 102579.088951 | 135499.867323 | 39584.394376 |
| 1003000   | 97054.032124  | 95356.901099  | 139368.337659 | 30336.402315 |
| 1004000   | 24363.297943  | 34077.322383  | 51558.870259  | 17216.851353 |
| 1051000   | 36899.554847  | 52130.750475  | 93218.887541  | 32756.462776 |


The function implemented in 

```python
df_2 = data.database_get(
    dimension="spatial",
    table_id=55,
    year=year,
    internal_id=None,
    force_update=True,
)
# internal_id values in df_2 are lists, convert them to tuples for easier handling
df_2["internal_id"] = df_2["internal_id"].apply(lambda x: tuple(x))

# exclude rows where internal_id is (1, 0)
df_2 = df_2[df_2["internal_id"] != (1, 0)]

# create a column "hh_size" in df_2 based on internal_id as follows:
# if internal_id is (2, 0), hh_size is 1
# if internal_id is (3, 0), hh_size is 2
# if internal_id is (4, 0), hh_size is 3
# if internal_id is (5, 0), hh_size is 4
df_2["hh_size"] = df_2["internal_id"].apply(
    lambda x: x[0] - 1 if x[1] == 0 and x[0] in [2, 3, 4, 5] else None
)

# rearrange the dataframe so that the columns are the hh_size, the rows are the id_region, and the values
# are the "value" column
df_2_pivot = df_2.pivot(index="id_region", columns="hh_size", values="value")
# divide all values in df_2_pivot by 1e3 to convert from KWh to MWh
df_2_pivot = df_2_pivot / 1e3
print(df_2_pivot.head())
```

# Temporal Disaggregation
For the temporal disaggregation the function *temporal.make_zve_load_profiles()* is used.
This function gets the total electricity consumption based on households types, exactly as descripted above.

I then creates percentages by dividing each entry by the sum of all the region consumption accorss households types.

The function then calls zve_percentages_applications(). It returns the local table called "percentages_applications.csv", which looks like this:
| Application |    1    |    2    |    3    |    4    |     5     |
|-------------|---------|---------|---------|---------|-----------|
| Light       | 0.101   | 0.091   | 0.088   | 0.095   | 0.104133  |
| Cooking     | 0.079   | 0.103   | 0.092   | 0.093   | 0.084681  |
| Dishwashing | 0.025   | 0.044   | 0.053   | 0.061   | 0.062319  |
| Washing     | 0.039   | 0.042   | 0.048   | 0.052   | 0.055956  |
| Tumbler     | 0.025   | 0.047   | 0.067   | 0.082   | 0.088637  |
| Hotwater    | 0.140   | 0.132   | 0.122   | 0.109   | 0.106637  |
| Office      | 0.154   | 0.129   | 0.125   | 0.122   | 0.120549  |
| TV_Audio    | 0.129   | 0.126   | 0.128   | 0.114   | 0.107593  |
| Other       | 0.072   | 0.067   | 0.069   | 0.068   | 0.077770  |
| Fridge      | 0.150   | 0.121   | 0.101   | 0.091   | 0.079363  |
| Circulation | 0.062   | 0.053   | 0.060   | 0.064   | 0.062407  |
| Freezer     | 0.024   | 0.045   | 0.047   | 0.049   | 0.049956  |

According to the DemandRegio report, this table represents the percentage of electricity consumption according to the household type.

The function zve_application_profiles() loads the local table "application_profiles.csv", which looks like this:

| HH_size | Day | Season | Application | 93       | 94       | 95       |
|---------|-----|--------|-------------|----------|----------|----------|
| 5       | 3   | 1      | ...         | 0.013386 | 0.009107 | 0.005975 |
| 5       | 3   | 1      | ...         | 0.003093 | 0.002843 | 0.004099 |
| 5       | 3   | 2      | ...         | 0.006692 | 0.004214 | 0.002661 |
| 5       | 3   | 2      | ...         | 0.000000 | 0.000000 | 0.000000 |

The column Application has application numbers from 1-9. There are 96 (0 - 95) intervals showing for a household type, how much in percent does a certain application consumes 
in a specific 15 interval of a day (of a specific season). I guess there are just 9 because fridge, circulation and freezer work all the time, so they are excluded from the calculation. 

Next the function zve_percentages_baseload() is called, which loads a local table called "percentages_baseload.csv". This table looks like this:

| Application |   1   |   2   |   3   |   4   |   5   |
|-------------|-------|-------|-------|-------|-------|
| Light       | 0.01  | 0.01  | 0.01  | 0.01  | 0.01  |
| Cooking     | 0.01  | 0.01  | 0.01  | 0.01  | 0.01  |
| Dishwashing | 0.01  | 0.01  | 0.01  | 0.01  | 0.01  |
| Washing     | 0.01  | 0.01  | 0.01  | 0.01  | 0.01  |
| Tumbler     | 0.01  | 0.01  | 0.01  | 0.01  | 0.01  |
| Hotwater    | 0.20  | 0.20  | 0.20  | 0.20  | 0.20  |
| Office      | 0.50  | 0.50  | 0.50  | 0.50  | 0.50  |
| TV_Audio    | 0.10  | 0.10  | 0.10  | 0.10  | 0.10  |
| Other       | 0.25  | 0.25  | 0.25  | 0.25  | 0.25  |
| Fridge      | 1.00  | 1.00  | 1.00  | 1.00  | 1.00  |
| Circulation | 1.00  | 1.00  | 1.00  | 1.00  | 1.00  |
| Freezer     | 1.00  | 1.00  | 1.00  | 1.00  | 1.00  |

This table indicates what fraction of each appliance's load is constant "baseload" vs. activity-dependent (e.g., fridges are 100% baseload, lighting is only 1% baseload)

Then in the function database_shapes_gisco() the coordinates for every region in germany are fetched.

**Note** information like the number of days for each season, or how many days there are for february, is based on the year 2012.
Also the case for the function probability_light_needed.

The function uses 9 time slices combining 3 day types (Weekday, Saturday, Sunday) with 3 seasons (Winter, Transition, Summer). For example, "WD_Win" means weekday in winter, which represents 103 days in 2012.

**The main loop starts for every region:** (The explanation below is taken from ClaudeAI)
For each German region, the function:

- Calculates light probability: Uses latitude/longitude to determine when artificial light is needed based on sunrise/sunset times for each season
 
- Builds activity-based profiles: For the 9 activity-dependent appliances (Light, Cooking, Dishwashing, Washing, Tumbler, Hotwater, Office, TV_Audio, Other), it:

- Takes the activity profiles from the time-use survey
Multiplies lighting by the probability that it's dark outside
Combines activity-based load with constant baseload
Weights by household size distribution in that region


- Adds baseload appliances: The last 3 appliances (Fridge, Circulation, Freezer) run constantly, so their load is distributed evenly across all time steps

- Creating Annual Time Series
The function converts the 9 representative day profiles into a full 8760-hour year by:
    - Mapping each day to its day type (weekday/Saturday/Sunday) and season based on month 
    - Looking up the corresponding profile and hour
    - Normalizing to create distribution keys that sum to 1.0

- Output
The function returns a DataFrame with hourly load profiles for the entire year for each region, saved as a CSV file. These profiles represent the typical electricity demand pattern, accounting for behavioral differences across household sizes, appliance usage patterns, and natural light availability.










