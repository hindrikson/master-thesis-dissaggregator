# Household Consumption

Here we describe how the consumption of private households is disaggreagted spatially and temporally.

## Spatial Disaggreagation
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

The original table is **table 13 "Stromverbrauch nach Haushaltsgröße"** which returns 6 rows for each household size category, for the specified year.

**Problem**: It has only data for 2015, and the format is different.

**Table 55 "Stromverbrauch der Haushalte nach Haushaltsgroesse, 1990..2060"**. It returns a dataframe with the total consumption of every region
in **kWh/a** according to an internal_id. The internal ids are: 1, 2, 3, 4, 5 (**NOTE** The description in opendata is not completely right)
1=total, 2=1-person households, 3=2-person households, 4=3-person households, 5>=4-person households 
The table then looks like this:
| id_spatial | id_region_type | id_region | year | internal_id | value        |
|------------|----------------|-----------|------|-------------|--------------|
| 55         | 4              | 16055000  | 2015 | [5, 0]      | 1.010070e+07 |
| 55         | 4              | 16056000  | 2015 | [1, 0]      | 5.848985e+06 |

The intervals specify different household sizes. To get all the different households per region in one table either:
- **DO NOT** specify a internal ID.
- set internal_id=None when using database_get(internal_id=None)
- interval_id_1=1 returns the total of 2, 3, 4 and 5 summed up.

To emulate the same behaviour as table 2, for each table fetched for a specific internal_id, one needs to sum all the values.
Then create a dataframe with 6 rows, one with the some of each internal_id. 
**Problem** is that the final values are much higher than the values returned by the table 13. It could be that the table 13 is returning the values alredy in MGh or GWh instead of KWh.

The code below return a table in the following format, which is similar to what the code does, but we here do not convert the id_region to NUTS3.

| hh_size   | 1             | 2             | 3             | 4            |
|-----------|---------------|---------------|---------------|--------------|
| id_region |               |               |               |              |
|-----------|---------------|---------------|---------------|--------------|
| 1001000   | 30040.658666  | 42941.384366  | 50385.640258  | 14811.731628 |
| 1002000   | 108827.216963 | 102579.088951 | 135499.867323 | 39584.394376 |
| 1003000   | 97054.032124  | 95356.901099  | 139368.337659 | 30336.402315 |
| 1004000   | 24363.297943  | 34077.322383  | 51558.870259  | 17216.851353 |
| 1051000   | 36899.554847  | 52130.750475  | 93218.887541  | 32756.462776 |



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



