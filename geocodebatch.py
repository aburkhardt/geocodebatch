import os
import sys
import time
import math
import pprint
import json

import pandas as pd
import requests
from dotenv import load_dotenv
load_dotenv()

#------------------ CONFIGURATION -------------------------------
AZURE_MAPS_KEY = os.getenv("AZURE_MAPS_KEY")
BACKOFF_TIME = 30
AZURE_BASE_URL = "https://atlas.microsoft.com/search/address/json"
AZURE_API_VERSION = "1.0"

if not AZURE_MAPS_KEY:
    print("Error: Please set AZURE_MAPS_KEY in your environment.")
    # sys.exit(1)

#------------------ FILE HANDLING -------------------------------
def get_input_filename():
    """Prompt the user for the input filename and validate its existence."""
    input_filename = input("Enter the path to the input CSV file: ").strip()
    if not os.path.isfile(input_filename):
        print(f"Error: File '{input_filename}' not found. Please check the path and try again.")
        # sys.exit(1)
    return input_filename

def get_output_filename(input_filename):
    """Generate the output filename based on the input filename."""
    input_basename = os.path.basename(input_filename)
    input_name, _ = os.path.splitext(input_basename)
    # Ensure output dir exists
    out_dir = "data"
    os.makedirs(out_dir, exist_ok=True)
    return f"{out_dir}/output-{input_name}.csv"

def load_data(input_filename, testing=False):
    """Load CSV data into a Pandas DataFrame."""
    nrows = 5 if testing else None
    data = pd.read_csv(input_filename, encoding="cp1252", low_memory=False, dtype=str, nrows=nrows)
    data.columns = [col.encode("utf-8").decode("utf-8-sig").strip() for col in data.columns]
    return data

def get_address_columns(data):
    """Display available columns and prompt user for address-related columns."""
    print("\nAvailable columns in the file:")
    for i, col in enumerate(data.columns, 1):
        print(f"{i}. {col}")

    address_columns = input("Enter the column name(s) containing the address (comma-separated if multiple): ").strip()
    if "," in address_columns:
        address_columns = [col.strip() for col in address_columns.split(",")]
    else:
        address_columns = [address_columns.strip()]

    invalid_columns = [col for col in address_columns if col not in data.columns]
    if invalid_columns:
        print(f"Error: The following column(s) do not exist in the file: {', '.join(invalid_columns)}")

    print(f"\nUsing address columns: {', '.join(address_columns)}")
    return address_columns

def combine_address_columns(data, address_columns):
    """If multiple columns are selected, combine them into a full address field."""
    if len(address_columns) > 1:
        data["Full_Address"] = data[address_columns].astype(str).agg(", ".join, axis=1)
        return "Full_Address"
    return address_columns[0]

def save_results(results, output_filename, original_data):
    """Save the geocoding results to a CSV file."""
    results_df = pd.DataFrame(results)

    merged_df = original_data.copy()
    merged_df = pd.concat([merged_df, results_df], axis=1)

    new_columns = [
        "formatted_address", "latitude", "longitude", "state", "county",
        "city", "postal_code", "country", "confidence",
        "state_senate_district", "state_house_district", "input_string"
    ]

    all_columns = list(original_data.columns) + [col for col in new_columns if col not in original_data.columns]

    merged_df = merged_df[all_columns]

    merged_df.to_csv(output_filename, encoding="utf8", index=False)
    print(f"\nResults saved to: {output_filename}")

#------------------ GEOCODING + DISTRICTS -------------------------------
def get_census_legislative_districts(lat, lng):
    """Query the U.S. Census API to get state legislative districts based on lat/lng."""
    url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
    params = {
        "x": lng,
        "y": lat,
        "benchmark": "Public_AR_Current",
        "vintage": "Current_Current",
        "format": "json",
    }

    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"[Census] Error fetching districts for ({lat}, {lng}): {e}")
        return {"state_senate_district": None, "state_house_district": None}

    state_senate = None
    state_house = None
    state_code = None

    if "result" in data and "geographies" in data["result"]:
        geographies = data["result"]["geographies"]

        if "States" in geographies:
            state_info = geographies["States"][0]
            state_code = str(state_info.get("STATE"))

        house_key = next((key for key in geographies if "State Legislative Districts - Lower" in key), None)
        senate_key = next((key for key in geographies if "State Legislative Districts - Upper" in key), None)

        if house_key and geographies.get(house_key):
            house_info = geographies[house_key][0]
            state_house = house_info.get("BASENAME")

        if senate_key and geographies.get(senate_key):
            senate_info = geographies[senate_key][0]
            state_senate = senate_info.get("BASENAME")

    if state_code != "27":
        state_senate = "Not Minnesota"

    return {
        "state_senate_district": state_senate,
        "state_house_district": state_house,
    }

def _azure_search_address(address, session=None, max_retries=5):
    """
    Call Azure Maps Search Address API with retry/backoff.
    Returns first result dict or None.
    """
    sess = session or requests.Session()
    params = {
        "api-version": AZURE_API_VERSION,
        "subscription-key": AZURE_MAPS_KEY,
        "query": address,
        "countrySet": "US",
    }

    for attempt in range(max_retries):
        try:
            resp = sess.get(AZURE_BASE_URL, params=params, timeout=20)
            status = resp.status_code

            if status == 200:
                payload = resp.json()
                results = payload.get("results") or []
                return results[0] if results else None

            if status in (429, 500, 502, 503, 504):
                sleep_for = BACKOFF_TIME * (2 ** attempt)  # exponential backoff
                print(f"[Azure] {status} throttled/server error. Retrying in {sleep_for:.1f}s...")
                time.sleep(sleep_for)
                continue

            # Other 4xx: likely bad request or quota issues; don't retry forever
            print(f"[Azure] Non-retryable HTTP {status}: {resp.text[:200]}")
            return None

        except requests.RequestException as e:
            sleep_for = BACKOFF_TIME * (2 ** attempt)
            print(f"[Azure] Request error: {e}. Retrying in {sleep_for:.1f}s...")
            time.sleep(sleep_for)

    print("[Azure] Max retries reached; skipping this address.")
    return None

def get_results(address, session=None):
    """
    Geocode an address using Azure Maps Search API and get state legislative districts.
    Returns a dict matching your original column names.
    """
    result = _azure_search_address(address, session=session)

    if result:
        try:
            pos = result.get("position") or {}
            lat = pos.get("lat")
            lng = pos.get("lon")

            district_data = get_census_legislative_districts(lat, lng) if (lat is not None and lng is not None) else {
                "state_senate_district": None, "state_house_district": None
            }

            addr = result.get("address") or {}
            return {
                "formatted_address": addr.get("freeformAddress"),
                "latitude": lat,
                "longitude": lng,
                "state": addr.get("countrySubdivision"),
                "county": addr.get("countrySecondarySubdivision"),
                "city": addr.get("municipality"),
                "postal_code": addr.get("postalCode"),
                "country": addr.get("country"),
                "confidence": result.get("score"),
                "state_senate_district": district_data["state_senate_district"],
                "state_house_district": district_data["state_house_district"],
                "input_string": address,
            }
        
        except Exception as e:
            print(f"[Azure] Parse error for '{address}': {e}")

    # Fallthrough: return empty row if no result or error
    return {
        "formatted_address": None,
        "latitude": None,
        "longitude": None,
        "state": None,
        "county": None,
        "city": None,
        "postal_code": None,
        "country": None,
        "confidence": None,
        "state_senate_district": None,
        "state_house_district": None,
        "input_string": address,
    }

# ------------------ MAIN EXECUTION -----------------------------
def main():
    input_filename = get_input_filename()
    output_filename = get_output_filename(input_filename)
    data = load_data(input_filename)

    address_columns = get_address_columns(data)
    address_column_name = combine_address_columns(data, address_columns)

    print(f"\nUsing address column(s): {address_columns}")
    print(f"\nOutput file will be saved as: {output_filename}")

    print(f"Total rows in input file: {len(data)}")
    non_empty = data[address_column_name].replace({None: pd.NA, "": pd.NA}).dropna()
    print(f"Total non-empty addresses: {len(non_empty)}")

    addresses = data[address_column_name].tolist()
    
    results = []
    session = requests.Session()
    for index, address in enumerate(addresses):
        try:
            if address is None or str(address).strip() == "" or str(address).lower() == "nan":
                results.append({
                    "formatted_address": None, "latitude": None, "longitude": None,
                    "state": None, "county": None, "city": None, "postal_code": None, "country": None,
                    "confidence": None, "state_senate_district": None, "state_house_district": None,
                    "input_string": address
                })
                continue
            result = get_results(address, session=session)
            results.append(result)

        except Exception as e:
            print(f"Error processing address '{address}': {e}")
            results.append({
                "formatted_address": None, "latitude": None, "longitude": None,
                "state": None, "county": None, "city": None, "postal_code": None, "country": None,
                "confidence": None, "state_senate_district": None, "state_house_district": None,
                "input_string": address
            })
    print(f"Saving {len(results)} results to {output_filename}")

    save_results(results, output_filename, data)

if __name__ == "__main__":
    import os.path
    main()