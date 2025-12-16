from src.data_processing.households import households_power_consumption  # noqa
from src.data_processing.households import adjust_by_income  # noqa
from src.data_processing.temporal import get_CTS_power_slp  # noqa
from src.configs.mappings import federal_state_dict
from src import logger
import pandas as pd
import numpy as np


def temporal_disaggregation_households_slp(
    by: str, year: int, weight_by_income: bool = False, scale_by_pop: bool = False
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
    sv_yearly = households_power_consumption(
        year=year, weight_by_income=weight_by_income
    )

    sv_yearly = sv_yearly.sum(axis=1)  # sum across household sizes
    sv_yearly.name = "value"
    sv_yearly = sv_yearly.to_frame().assign(
        BL=lambda x: [
            federal_state_dict().get(int(i[:-3])) for i in x.index.astype(str)
        ]
    )

    total_sum = sv_yearly.value.sum()

    # Create empty 15min-index'ed DataFrame for target year
    idx = pd.date_range(start=str(year), end=str(year + 1), freq="15min")[:-1]
    DF = pd.DataFrame(index=idx)

    for state in federal_state_dict().values():
        logger.info("Working on state: {}.".format(state))
        sv_lk = (
            sv_yearly.loc[lambda x: x["BL"] == state]
            .drop(columns=["BL"])
            .assign(SLP=lambda x: "H0")
        )
        logger.info("... creating state-specific load-profiles")
        slp_bl = get_CTS_power_slp(state, year=year)
        # Plausibility check:
        assert slp_bl.index.equals(idx), "The time-indizes are not aligned"
        # Create 15min-index'ed DataFrames for current state
        cols = sv_lk.drop(columns=["SLP"]).columns
        sv_lk_ts = (
            pd.DataFrame(index=idx, columns=cols).fillna(0.0).infer_objects(copy=False)
        )

        logger.info("... assigning load-profiles")
        # Calculate load profile for each LK
        slp = "H0"
        lp_lk = pd.DataFrame(
            np.multiply(
                slp_bl[[slp]].values, sv_lk.drop(columns=["SLP"]).transpose().values
            ),
            index=slp_bl.index,
            columns=sv_lk.index,
        )
        # save intermediate results
        sv_lk_ts = pd.concat([sv_lk_ts, lp_lk], axis=1).drop(columns=["value"])

        # Concatenate the state-wise results
        DF = pd.concat([DF, sv_lk_ts], axis=1).dropna()

    # Plausibility check:
    msg = (
        "The sum of yearly consumptions (={:.3f}) and the sum of disaggrega"
        "ted consumptions (={:.3f}) do not match! Please check algorithm!"
    )
    disagg_sum = DF.sum().sum()
    assert np.isclose(total_sum, disagg_sum), msg.format(total_sum, disagg_sum)

    # merge Eisenach (16056) into Wartburgkreis (16063)
    if "16063" in DF.columns and "16056" in DF.columns:
        # Add Eisenach data to Wartburgkreis
        DF["16063"] = DF["16063"] + DF["16056"]
        # Remove the old Eisenach column
        DF = DF.drop(columns=["16056"])
        print("Merged Eisenach (16056) into Wartburgkreis (16063)")

    # Convert columns to integer type to match with industry and cts datasets
    DF.columns = DF.columns.astype(int)

    return DF
