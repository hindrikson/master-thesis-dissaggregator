from src.data_access.api_reader import get_power_consumption_by_HH_size


def households_power_consumption(
    year: int, use_cache: bool = True, scenario_id: int = 2
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
    df = get_power_consumption_by_HH_size(year=year, use_cache=use_cache)

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

    return df
