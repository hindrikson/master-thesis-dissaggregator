import os
from time import time

import pandas as pd
import tomllib

pd.options.display.max_columns = 50


from src.pipeline.pipe_household_temporal import temporal_disaggregation_households_slp
from src.pipeline.pipe_temporal import disaggregate_temporal

with open("config.toml", "rb") as f:
    config = tomllib.load(f)

RESULTS_PATH = config["generate_timeseries"]["results_path"]


def save_dirs(result_path):
    # Create directories if they don't exist
    os.makedirs(os.path.join(result_path, "industry"), exist_ok=True)
    os.makedirs(os.path.join(result_path, "cts"), exist_ok=True)
    os.makedirs(os.path.join(result_path, "households"), exist_ok=True)

    ind_path = os.path.join(result_path, "industry")
    cts_path = os.path.join(result_path, "cts")
    hh_path = os.path.join(result_path, "households")
    return ind_path, cts_path, hh_path


def main(year: int, formats: list):
    start = time()
    print("\nCreating regional time series for year:", year)

    print("\nDisaggregating households...")
    df_households = temporal_disaggregation_households_slp(by="households", year=year)
    print("Disaggregation of Households completed.")

    print("\nDisaggregating industry...")
    df_industry = disaggregate_temporal(
        energy_carrier="power",
        sector="industry",
        year=year,
        force_preprocessing=True,
        float_precision=10,
        save_to_cache=False,
    )
    print("Disaggregation of Industry completed.")

    print("\nDisaggregating cts...")
    df_cts = disaggregate_temporal(
        energy_carrier="power",
        sector="cts",
        year=year,
        force_preprocessing=True,
        float_precision=10,
        save_to_cache=False,
    )
    print("Disaggregation of CTS completed.\n")

    # Extract regions from df_industry (first level of column MultiIndex)
    industry_regions = df_industry.columns.get_level_values(0).unique()
    cts_regions = df_cts.columns.get_level_values(0).unique()
    household_regions = df_households.columns

    # create sets
    industry_regions_str = set(str(r).zfill(5) for r in industry_regions)
    cts_regions_str = set(str(r).zfill(5) for r in cts_regions)
    household_regions_str = set(household_regions)

    # Find the differences
    only_in_cts = cts_regions_str - industry_regions_str
    only_in_industry = industry_regions_str - household_regions_str
    only_in_households = household_regions_str - industry_regions_str

    print("Number of regions in df_industry:", len(industry_regions_str))
    print("Number of regions in df_households:", len(household_regions_str))
    print("\nRegions match:", industry_regions_str == household_regions_str)

    if only_in_cts:
        print(f"\nRegions only in df_cts ({len(only_in_cts)}):")
        print(sorted(only_in_cts))
        raise ValueError("Regions in df_cts and df_industry do not match!")

    if only_in_industry:
        print(f"\nRegions only in df_industry ({len(only_in_industry)}):")
        print(sorted(only_in_industry))
        raise ValueError("Regions in df_industry and df_households do not match!")

    if only_in_households:
        print(f"\nRegions only in df_households ({len(only_in_households)}):")
        print(sorted(only_in_households))
        raise ValueError("Regions in df_households and df_industry do not match!")

    ind_dir, cts_dir, hh_dir = save_dirs(RESULTS_PATH)

    for format in formats:
        cts_path = cts_dir + f"/temporal_disaggregation_power_cts_{year}.{format}"
        industry_path = (
            ind_dir + f"/temporal_disaggregation_power_industry_{year}.{format}"
        )
        household_path = (
            hh_dir + f"/temporal_disaggregation_households_power_slp_{year}.{format}"
        )

        if format == "csv":
            try:
                print("Saving CSV files...")
                df_cts.to_csv(cts_path)
                print("CTS file saved successfully.")
                df_industry.to_csv(industry_path)
                print("Industry file saved successfully.")
                df_households.to_csv(household_path)
                print("Household file saved successfully.")
                print("CSV files saved successfully.\n")
            except Exception as e:
                print("Error saving files:", e)
            continue

        try:
            print("Saving pickle files...")
            df_cts.to_pickle(cts_path)
            print("CTS file saved successfully.")
            df_industry.to_pickle(industry_path)
            print("Industry file saved successfully.")
            df_households.to_pickle(household_path)
            print("Household file saved successfully.")
            print("Pickle files saved successfully.\n")
        except Exception as e:
            print("Error saving files:", e)
            raise e

    end = time()
    # print time in minutes
    print("Time taken: {:.2f} minutes".format((end - start) / 60))


if __name__ == "__main__":
    years = [2022, 2023, 2024]
    formats = ["pkl"]
    for year in years:
        main(year, formats)
        print("Year {} completed.\n".format(year))
