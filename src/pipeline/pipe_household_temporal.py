from src.data_processing.households import households_power_consumption  # noqa
from src.data_processing.households import adjust_by_income  # noqa
from src import logger


def disaggregate_households_power(
        by: str, year: int,  weight_by_income: bool =False, scale_by_pop: bool =False
):
    """
    Spatial disaggregation of elc. power in [GWh/a] by key (weighted by income)

    Parameters
    ----------
    by : str
        must be one of ['households', 'population']
    weight_by_income : bool, optional
        Flag if to weight the results by the regional income (default False)
    orignal : bool, optional
        Throughput to function households_per_size,
        A flag if the results should be left untouched and returned in
        original form for the year 2011 (True) or if they should be scaled to
        the given `year` by the population in that year (False).

    Returns
    -------
    pd.DataFrame or pd.Series
    """
    # bottom up by using the number of househods per household size
    df = households_power_consumption(year=year, weight_by_income=weight_by_income)

    return df
