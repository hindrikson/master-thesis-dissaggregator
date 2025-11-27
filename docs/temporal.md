# Temporal

The function **disaggregate_temporal()** disaggregates the temporal data for a given energy carrier and sector. Its main functions are: 
- disagg_applications_efficiency_factor()
- disaggregate_temporal_industry()
- specific functions for disaggregating for CTS

Below is a detailed explanation from the top-level function down to its sub-functions. We break down the function only for the case where the energy carrier is power. Petrol and gas are not yet included here.

## Breakdown
- disaggregate_temporal()
    - disagg_applications_efficiency_factor()
        - get_consumption_data_per_indsutry_sector_energy_carrier()
            - Uses the get_consumption_data() function as explained in the [consumption](./consumption.md) documentation.
            - Filter the consumption data for the given energy carrier and sector (industry or CTS).
        - dissagregate_for_applications()
            - get_application_dissaggregation_factors()
                * returns the application dissaggregation factors for a given industry and energy carrier. 
                * For the industry sector == 'industry', and energy_carrier == 'power' the funcion calls:
                    * load_decomposition_factors_power()
                        - the power consumption in each wz is distributed according to share 
                          of certain applications within the industry (lighting, heating, IT equipment, air conditioning, etc.)
                        - this decomposition is loaded from data/raw/dimensionless/decomposition_factors.xlsx and sheet "Endenergieverbrauch Strom".
                          I do not know yet where these data come from.
                          **Sample values:**
                            | WZ | Beleuchtung | IKT      | Klimakälte | Prozesskälte | Mechanische Energie |
                            |----|-------------|----------|------------|--------------|---------------------|
                            | 1  | 0.255814    | 0.046512 | 0.093023   | 0.023256     | 0.418605            |
                    * load_decomposition_factors_temperature_industry()
                        - the power consumption of industries from 5-33 is distributed according to temperature-dependent applications (heating, cooling, etc.)
                        - this decomposition is loaded from data/raw/dimensionless/decomposition_factors.xlsx and sheet "Endenergieverbrauch Strom".
                          I do not know yet where these data come from.
                          **Sample values:**:
                        - | industry_sectors | process_heat_below_100C | process_heat_100_to_200C | process_heat_200_to_500C | process_heat_above_500C |
                          |---|----------|----------|----------|-----|
                          | 5 | 0.103753 | 0.666667 | 0.229581 | 0.0 |
                * The *get_application_dissaggregation_factors()* then returns a dataframe with both the power factors and temperature factors merged.
                    - | industry_sectors | lighting | information_communication_technology | space_cooling | process_cooling | mechanical_energy | space_heating | hot_water | process_heat_below_100C | process_heat_100_to_200C | process_heat_200_to_500C | process_heat_above_500C |
                      |---|----------|----------|----------|-----|----------|-----|-----|----------|----------|----------|-----|
                      | 5 | 0.031746 | 0.015873 | 0.015873 | 0.0 | 0.888889 | 0.0 | 0.0 | 0.004941 | 0.031746 | 0.010932 | 0.0 |
            - The *disagg_applications_default()* takes the df of disaggregation factors and the consumption data and performs the disaggregation by multiplying the consumption with the disaggregation factors.
            - Returns a dataframe with the consumption disaggregated by application. This disaggregation is primarily needed for CTS, where efficiency rates vary by application (e.g., lighting vs. heating). For industry sectors (5-33), a single efficiency rate is applied per sector to all its applications.        
        - apply_efficiency_factor()
            - Returns the consumption with efficiency factors applied.
            * load_efficiency_rate()
                - This file loads the file *data/raw/temporal/Efficiency_Enhancement_Rates_Applications.xlsx*, and returns a dataframe with the efficiency rates for a sector (wz) and energy_carrier.
                - Sample for industry:
                    - | industry_sector |   5    |   6    |   7    |   8    |   9    |   10   |   11   |   12   |
                      |-----------------|--------|--------|--------|--------|--------|--------|--------|--------|
                      | 2035            | 0.019  | 0.019  | 0.019  | 0.019  | 0.019  | 0.019  | 0.019  | 0.019  |
                      | 2045            | 0.013  | 0.013  | 0.013  | 0.013  | 0.013  | 0.013  | 0.013  | 0.013  |
                - For a given year, a compound efficiency factor is calculated by applying the Phase 1 rate (from the 2035 row) for years between 2019-2035, and then the Phase 2 rate (from the 2045 row) for any years after 2035.
            * The efficiency rates are then used to adjust the consumption data.
    ## Industry and power
    - disaggregate_temporal_industry()
        * Returns the shift load profiles for a given year. The sum of every column (state, load_profile) equals 1.
        * get_shift_load_profiles_by_year()
            * get_shift_load_profiles_by_state_and_year()
                - this function creates load shift profiles based on states and yearly holidays, weekdays, weekends days, for predefined shifts:
                - s1 (single shift) 08:00:00-16:00:00 for:
                    - S1_WT: working days only
                    - S1_WT_SA: working days + Saturdays
                    - S1_WT_SA_SO: working days + Saturdays + Sundays
                - and the same for s2 (two shifts) 06:00:00-23:00:00 and s3 24/7
                - For every 15min interval within a shift, the same proportion of load is assigned. For periods outside the shift a fixed lower proportion is assigned (as it is assumed that some load is still required).
                - Shift load profiles are very similar among states, with only small differences due to holidays.
            * E.g., for Hessen in 2020, the load shift profiles at 14:00:00 are:
                * | Timestamp           |    S1_WT | S1_WT_SA | S1_WT_SA_SO |    S2_WT | S2_WT_SA | S2_WT_SA_SO |    S3_WT | S3_WT_SA | S3_WT_SA_SO |
                  | ------------------- | -------: | -------: | ----------: | -------: | -------: | ----------: | -------: | -------: | ----------: |
                  | 2020-01-03 14:00:00 | 0.000046 | 0.000044 | 0.000042    | 0.000038 | 0.000036 | 0.000033    | 0.000034 | 0.000031 | 0.000028    |
        * shift_profile_industry()
            * This function assigns a predefined shift profile to every industry sector from 5-33. For example, industry 5 is given a S3_WT_SA profile, while industry 26 is given a S3_WT_SA profile.
        * For every region-industry combination:
            * extract the state of the region_id (first digit)
            * map the industry sector to the according shift profile of the state
            * The annual consumption is then distributed according to the shares of consumption for every industry based on its shift profiles shares (15 min).
    ## CTS and power
    - disaggregate_temporal_power_CTS()
        * federal_state_dict()
            - returns a dictionary of Bundesland abbreviations and their corresponding numerical codes.
        * load_profiles_cts_power()
            - Returns a dictionary: {1: 'L0', 2: 'L0', 3: 'G3', 35: 'G3', 36: 'G3', 37: 'G3', ... 
        * For every state the consumption dataframe is filtered according to the state number, and the function assigns a power load profile (SLP) to every CTS with the dictionary mapping from the function above.
        * The resulting dataframe:
            * | industry_sector | 1001       | 1002        | SLP |
              |----|-------------|--------------|----|
              | 1  | 249.669997  | 969.018490   | L0 |
              | 2  | 0.000000    | 89.470945    | L0 |
              | 3  | 0.000000    | 0.000000     | G3 |
              | 36 | 0.000000    | 14579.634018 | G3 |
              | 37 | 1563.897832 | 244.854069   | G3 |
        * get_CTS_power_slp()
            - load_power_load_profile()
                - This function returns the load_profile for power for a specific profile name (e.g., 'H0', 'L0', 'G3', etc.)
                - For each profile there is an excel file in 'data/raw/temporal/power_load_profiles/' where the load is distributed temporally (15 min) for a general day (not year!).
                - | Hour     | SA_WIZ | SU_WIZ | WD_WIZ | SA_SOZ | SU_SOZ | WD_SOZ | SA_UEZ | SU_UEZ | WD_UEZ |
                  |----------|------|------|------|-----|------|------|-------|------|------|
                  | 00:00:00 | 94.1 | 73.2 | 74.9 | 109 | 91.6 | 96.5 | 101.5 | 80.7 | 86.6 |
            - Returns a normalized dataframe based on the values of the static standard last profiles, with temporal load profiles for a specific state (Bundesland). These normalized values based on standard last profiles are then used to disaggregate the annual consumption for every CTS branch.
            - | Date       | Day        | Hour     | DayOfYear | WD    | SA    | SU    | WIZ   | SOZ   | UEZ   | H0        | L0        | L1        | L2        | G0        | G1        | G2        | G3        | G4        | G5        | G6        |
              |------------|------------|----------|---|-------|-------|------|------|-------|-------|----------|----------|----------|----------|----------|----------|----------|----------|----------|----------|----------|
              | 2020-01-01 | 2020-01-01 | 00:00:00 | 1 | False | False | True | True | False | False | 0.000018 | 0.000018 | 0.000017 | 0.000019 | 0.000015 | 0.000006 | 0.000017 | 0.000021 | 0.000014 | 0.000009 | 0.000017 |
        * The consumption data is then filtered for every type of load profile (SLP), whose consumption data is then multiplied by the according standard load profile of the function above. Everything is then concatenated to a final dataframe with the temporal disaggregated consumption for every CTS branch.
                 

