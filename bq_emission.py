import pandas as pd
from rapidfuzz import process, fuzz
from pandas_gbq import read_gbq
import os

# Densities for common fuels (kg/liter)
DENSITY_MAP = {
    'diesel fuel': 0.832,
    'petrol': 0.74,
    'kerosene': 0.81,
    'jet fuel': 0.8,
    'heating oil': 0.85,
    'biodiesel': 0.88,
    'ethanol': 0.789,
    'aviation gasoline': 0.72,
    'marine diesel': 0.86,
    'heavy fuel oil': 0.96,
    'waste oil': 0.92,
    'shale oil': 0.85,
    'naphtha': 0.7,
    'methanol': 0.792,
    'lpg': 0.54,
    'propane': 0.493,
    'butane': 0.573,
    # Add more as needed
}

# Heating values for common fuels (kWh/kg)
HEATING_VALUE_MAP = {
    'diesel fuel': 11.8,
    'petrol': 12.0,
    'kerosene': 11.8,
    'jet fuel': 11.9,
    'heating oil': 11.8,
    'biodiesel': 9.2,
    'ethanol': 7.5,
    'aviation gasoline': 12.0,
    'marine diesel': 11.8,
    'heavy fuel oil': 11.6,
    'waste oil': 11.0,
    'shale oil': 9.5,
    'naphtha': 11.3,
    'methanol': 5.5,
    'lpg': 13.8,
    'natural gas': 13.1,
    'propane': 13.8,
    'butane': 13.7,
    'wood': 4.2,
    'charcoal': 7.5,
    'coal': 7.0,
    'anthracite': 8.0,
    'lignite': 3.5,
    'peat': 3.8,
    'biomass': 4.0,
    'municipal waste': 2.5,
    'bagasse': 2.2,
    'animal fat': 9.5,
    'tallow': 9.5,
    'refinery gas': 13.1,
    'town gas': 5.0,
    'sewage gas': 5.5,
    'landfill gas': 5.0,
    'black liquor': 2.5,
    'sludge gas': 5.0,
    'petroleum coke': 8.2,
    'sub-bituminous coal': 6.0,
    'brown coal': 3.5,
    'oil shale': 7.0,
    'syngas': 4.0
    # Add more as needed
}

def get_density(source):
    source_key = source.lower().strip()
    for fuel, density in DENSITY_MAP.items():
        if fuel in source_key:
            return density, fuel
    return None, None

def get_heating_value(source):
    source_key = source.lower().strip()
    for fuel, hv in HEATING_VALUE_MAP.items():
        if fuel in source_key:
            return hv, fuel
    return None, None

def calculate_emissions_from_bq(
    bq_table: str,
    project_id: str,
    bq_product_col: str,
    bq_emission_factor_col: str,
    bq_unit_col: str,
    input_csv: str,
    output_file: str = None,
    service_account_json: str = None,
    location: str = 'us-east1'
) -> pd.DataFrame:
    """
    Calculate CO2 emissions by fuzzy-matching input sources to a BigQuery database table.
    Args:
        bq_table: BigQuery table in 'project.dataset.table' format
        project_id: GCP project ID
        bq_product_col: Column in BQ table for product name
        bq_emission_factor_col: Column in BQ table for emission factor
        bq_unit_col: Column in BQ table for emission factor unit
        input_csv: Path to input CSV (columns: source, quantity, unit)
        output_file: (Optional) Path to save the output as Excel
        service_account_json: (Optional) Path to service account JSON for authentication
        location: (Optional) BigQuery location/region (default 'us-east1')
    Returns:
        pd.DataFrame with results
    """
    # Set up authentication if service account is provided
    if service_account_json:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_json

    # Read BigQuery table
    query = f"SELECT `{bq_product_col}` as product, `{bq_emission_factor_col}` as emission_factor, `{bq_unit_col}` as unit FROM `{bq_table}`"
    db_df = read_gbq(query, project_id=project_id, location=location)

    # Read input CSV
    input_df = pd.read_csv(input_csv)

    output_rows = []
    for idx, input_row in input_df.iterrows():
        input_source = str(input_row['source'])
        input_quantity = input_row['quantity']
        input_unit = str(input_row['unit']).lower().strip()

        # Fuzzy match source
        choices = db_df['product'].astype(str).tolist()
        match, score, match_idx = process.extractOne(
            input_source, choices, scorer=fuzz.token_sort_ratio
        )
        matched_row = db_df.iloc[match_idx]

        emission_factor = matched_row['emission_factor']
        emission_unit = str(matched_row['unit']).lower().strip()
        matched_product = matched_row['product']

        note = ''
        norm_quantity = input_quantity
        norm_unit = input_unit

        # Handle unit conversion
        if input_unit == emission_unit:
            pass  # No conversion needed
        elif input_unit == 'liter' and emission_unit == 'kg':
            density, fuel = get_density(input_source)
            if density:
                norm_quantity = float(input_quantity) * density
                norm_unit = 'kg'
                note = f"Converted {input_quantity} liter to {norm_quantity:.3f} kg using density {density} for {fuel}."
            else:
                note = f"No density found for conversion from liter to kg for '{input_source}'."
        elif input_unit == 'kwh' and emission_unit == 'kg':
            hv, fuel = get_heating_value(input_source)
            if hv:
                norm_quantity = float(input_quantity) / hv
                norm_unit = 'kg'
                note = f"Converted {input_quantity} kWh to {norm_quantity:.3f} kg using heating value {hv} kWh/kg for {fuel}."
            else:
                note = f"No heating value found for conversion from kWh to kg for '{input_source}'."
        elif input_unit == 'kg' and emission_unit == 'kwh':
            hv, fuel = get_heating_value(input_source)
            if hv:
                norm_quantity = float(input_quantity) * hv
                norm_unit = 'kwh'
                note = f"Converted {input_quantity} kg to {norm_quantity:.3f} kWh using heating value {hv} kWh/kg for {fuel}."
            else:
                note = f"No heating value found for conversion from kg to kWh for '{input_source}'."
        else:
            note = f"Unit mismatch: input '{input_unit}' vs emission factor '{emission_unit}'. No conversion rule applied."

        # Calculate emission
        try:
            total_emission = float(norm_quantity) * float(emission_factor)
        except Exception as e:
            total_emission = None
            note += f' Calculation error: {e}'

        output_rows.append({
            'source': input_source,
            'matched_product': matched_product,
            'quantity': input_quantity,
            'unit': input_unit,
            'emission_factor': emission_factor,
            'emission_factor_unit': emission_unit,
            'total_co2_emission': total_emission,
            'note': note,
            'match_score': score
        })

    output_df = pd.DataFrame(output_rows)
    if output_file:
        output_df.to_excel(output_file, index=False)
    return output_df

# Note: You need to install pandas-gbq and authenticate with Google Cloud for this to work.
# pip install pandas-gbq rapidfuzz
# gcloud auth application-default login 