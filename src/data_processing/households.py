from src import logger
from src.data_access.api_reader import (
    get_income_per_capita,
    get_power_consumption_by_HH_size,
)
from src.utils.utils import fix_region_id


def adjust_by_income(df, year):
    # WARNING: Income data is only available up to 2016
    if year > 2016:
        year = 2016
    income_keys = (
        get_income_per_capita(year=year) / get_income_per_capita(year=year).mean()
    )
    return df.multiply(income_keys, axis=0)


def households_power_consumption(
    year: int,
    use_cache: bool = True,
    scenario_id: int = 2,
    weight_by_income: bool = False,
) -> "pd.DataFrame":
    """
    Wrapper function to get households' power consumption by household size.

    Args:
        year: The year for which to fetch data
        use_cache: Whether to use cached responses
        scenario_id: Scenario ID to filter the data:
            1 = moderate births and life expectancy, low migration balance (G2L2W1)
            2 = moderate births, life expectancy and net migration (G2L2W2)
            3 = moderate births and life expectancy, high net migration (G2L2W3)

    Returns:
        DataFrame containing energy consumption data by household size in MWh.
    """
    if 2018 > year > 2060:
        raise ValueError("Year must be between 2018 and 2060")

    df = get_power_consumption_by_HH_size(year=year, use_cache=use_cache)

    df["id_region"] = df["id_region"].apply(fix_region_id)

    # filter df to only include rows where internal_id[1] == scenario_id
    df = df[df["internal_id[1]"] == scenario_id]

    # exclude rows where internal_id[0] is 1 (1 is the sum over all household sizes)
    df = df[df["internal_id[0]"] != 1]

    df["hh_size"] = df["internal_id[0]"].apply(
        lambda x: x - 1 if x in [2, 3, 4, 5] else None
    )

    # rearrange the dataframe so that the columns are the hh_size, the rows are the id_region, and the values
    # are the "value" column
    df = df.pivot(index="id_region", columns="hh_size", values="value")

    # divide all values in df_2_pivot by 1e3 to convert from KWh to MWh
    df = df / 1e3

    if weith_by_income:
        logger.warning(
            "Income per capita is only available up to 2016. Adjusting households' power consumption by income per capita."
        )
        df = adjust_by_income(df, year)

    return df
