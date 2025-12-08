# Main functions

## Consumption
For the consumption data, the *get_consumption_data()* is used. This function has as main function the
*get_consumption_data_historical_and_future()*, which calls several sub-functions
to load and process data from various sources. We break down this function below:

- get_consumption_data_historical_and_future()
    * get_ugr_data_ranges()
        - Loads the GENESIS file (data/raw/dimensionless/ugr_2000to2020.csv)
          with the official national-level starting point for 48 sector ranges. This data is
          retrieved from "Verwendung von Energie: Deutschland, Jahre, Produktionsbereiche, Energieträger" under the link below:
          https://www-genesis.destatis.de/datenbank/online/statistic/85121/table/85121-0002
            - this function calls the *load_raw_ugr_data()* returns the file *"data/raw/dimensionless/ugr_2000to2020.csv"*. This table contains the energy consumption
              per UGR sector range (48) and year (2000-2020) for power.
            - function *load_genisis_wz_sector_mapping_file()*, loads the file *"src/configs/genisis_wz_dict.csv"*, has the mapping between UGR sector ranges (48) and WZ2008 sectors (88).
    * apply_activity_driver()
        - If the year is beyond the end year of the UGR data (2020), a projected energy demand is applied to each wz using "activity drivers", in the file 'data/raw/temporal/Activity_drivers.xlsx'. See [activity drivers](tables/activity_drivers.md) for more details.
    * get_employees_per_industry_sector_and_regional_ids()
        - get_historical_employees_by_industry_sector_and_regional_id()
            * get_historical_employees()
                - returns a dataframe from opendata.ffe API (id_spatial=18) with historical number of employees per industry sector (WZ2008) and regional id, observed on the moth 9 
                  of the given year. If the year is between 2000 and 2008, data from 2008 is used. Max year is 2018, over that, data from 2018 is used. For detailed description, see: [employee data](tables/employees.md)
            * get_future_employees()
                - returns a dataframe from opendata.ffe API (id_spatial=27) with number of employees by district and economic sector from 2012 to 2035. If
                  the year is bigger than 2035, it returns data from 2035. For detailed description, see: [employee data](tables/employees.md)
        - returns a dataframe with number of employees per industry sector (88) and regional id (400) for a given year.
    * resolve_ugr_industry_sector_ranges_by_employees()
        - Distributes the enery consumption from the 48 UGR industry sectors to the 88 WZ2008 industry sectors, 
          based on the share of employees.
            - You have the total energy between a sector range, and you distribute to the sectors inside the range based on the share of employees.
        - returns a dataframe with wz code and their consumption values for the given year.
    * load_decomposition_factors_gas() 
        - skipping...
    * get_total_gas_industry_self_consuption()
        - skipping...
    * load_decomposition_factors_power()
        - The power consumption in each wz is distributed according to share 
          of certain applications within the industry (lighting, heating, IT equipment, air conditioning, etc.)
        - This decomposition are loaded from data/raw/dimensionless/decomposition_factors.xlsx and sheet "Endenergieverbrauch Strom", and is base on literature from AGEB (Arbeitsgemeinschaft Energiebilanzen) and VDI (Verein Deutscher Ingenieure).
          **Sample values:**
            | WZ | Beleuchtung | IKT      | Klimakälte | Prozesskälte | Mechanische Energie |
            |----|-------------|----------|------------|--------------|---------------------|
            | 1  | 0.255814    | 0.046512 | 0.093023   | 0.023256     | 0.418605            |
    * calculate_self_generation()
        - returns a dataframe with an extra column for self-generation (power_incl_selfgen MWh and gas_no_selfgen MWh).
          The self generation is based on decomposition_factors dataframe from the function above,
          which includes the share of "electricity_self_generation"
    * get_regional_energy_consumption()
        - get_manufacturing_energy_consumption
            - requests the table 15 from demandregion_spatial API - Energy consumption by manufacturing, mining and quarrying industries (German Districts)
              or "Jahreserhebung über die Energieverwendung im Verarbeitenden Gewerbe sowie im Bergbau und in der Gewinnung von Steinen und Erden (Landkreise)". See [jevi.md](tables/jevi.md) for more details.
              The table includes data from 2003 up till 2017 (below 2003, uses data from 2003; above 2017, uses data from 2017)
        - returns a dataframe with regional energy consumption for gas and power per region_id
    * calculate_iteratively_industry_regional_consumption()
        - the function takes as input:
            - the consumption data with self generation per industry sector
            - the total regional energy consumption from JEVI per region_id
            - the employees per industry sector and region_id
        - Resolves the consumption per industry_sector (from UGR) to regional_ids (with the help of JEVI) in an iterative approach.
          This applies only to the industry sector with heavy energy consumption; CTS industry sector is resolved by the employees data.
        - The function distributes national industry sector consumption to 400 regions while simultaneously satisfying two constraints:
          National constraint (UGR): Total consumption per industry sector must match UGR data
          Regional constraint (JEVI): Total consumption per region must match JEVI data. It's essentially solving a bi-proportional fitting problem
        - This iterative proportional fitting method ensures consistency with two independent statistical sources:
          Top-down: National sector totals (UGR) - authoritative for industry structure.
          Bottom-up: Regional totals (JEVI) - authoritative for geographic distribution.
          By iterating between these two constraints, the algorithm finds a balanced solution 
          that respects both data sources while using employee distribution as the spatial allocation key.
          - The function first initializes a consumption matrix based on employee distribution, then iteratively scales rows and columns to match UGR and JEVI totals.

