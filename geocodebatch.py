import os
import sys
import pprint

from geopy.geocoders import Bing
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import pandas as pd
import requests

#------------------ CONFIGURATION -------------------------------
BING_API_KEY = os.getenv('BING_API_KEY')
BACKOFF_TIME = 30

#------------------ FILE HANDLING -------------------------------
def get_input_filename():
    """Prompt the user for the input filename and validate its existence."""
    input_filename = input("Enter the path to the input CSV file: ").strip()
    if not os.path.isfile(input_filename):
        print(f"Error: File '{input_filename}' not found. Please check the path and try again.")
        sys.exit(1)
    return input_filename

def get_output_filename(input_filename):
    """Generate the output filename based on the input filename."""
    input_basename = os.path.basename(input_filename)
    input_name, _ = os.path.splitext(input_basename)
    return f"data/output-{input_name}.csv"

def load_data(input_filename, testing=False):
    """Load CSV data into a Pandas DataFrame."""
    nrows = 5 if testing else None
    data = pd.read_csv(input_filename, encoding="cp1252", low_memory=False, dtype=str, nrows=nrows)
    data.columns = [col.encode('utf-8').decode('utf-8-sig').strip() for col in data.columns]  # Normalize column names
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
        #sys.exit(1)
    
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
    print(f"\nResults saved to: {output_filename}")

    merged_df.to_csv(output_filename, encoding="utf8", index=False)

#------------------ GEOCODING FUNCTIONS -------------------------------
def get_census_legislative_districts(lat, lng):
    """Query the U.S. Census API to get state legislative districts based on lat/lng."""
    url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
    params = {
        "x": lng,
        "y": lat,
        "benchmark": "Public_AR_Census2020",
        "vintage": "Census2020_Census2020",
        "layers": "56,58",  # 54 = SD, 58 = HD
        "format": "json",
    }

    response = requests.get(url, params=params)
    data = response.json()

    state_senate = None
    state_house = None
    state_code = None

    # Check for HD - this contains both districts & state
    if "result" in data and "geographies" in data["result"]:
        if "State Legislative Districts - Lower" in data["result"]["geographies"]:
            house_info = data["result"]["geographies"]["State Legislative Districts - Lower"][0]

            state_code = house_info.get("STATE")

            state_house = house_info["BASENAME"]

            # Senate District (derived from House District by stripping last character)
            state_senate = "".join(filter(str.isdigit, house_info["BASENAME"]))

    if state_code != "27":
        state_senate = "Not Minnesota"

    return {
        "state_senate_district": state_senate,
        "state_house_district": state_house,
    }

def get_results(address, geolocator):
    """Geocode an address using Bing Maps API and get state legislative districts."""
    try:
        location = geolocator.geocode(address, exactly_one=True)

        if location:
            #pprint.pprint(location.raw)  # Debugging: Print full geocode response

            # Extract lat/lng
            lat, lng = location.latitude, location.longitude

            # Query Census API for legislative districts
            district_data = get_census_legislative_districts(lat, lng)

            return {
                "formatted_address": location.address,
                "latitude": lat,
                "longitude": lng,
                "state": location.raw.get("address", {}).get("adminDistrict"),
                "county": location.raw.get("address", {}).get("adminDistrict2"),
                "city": location.raw.get("address", {}).get("locality"),
                "postal_code": location.raw.get("address", {}).get("postalCode"),
                "country": location.raw.get("address", {}).get("countryRegion"),
                "confidence": location.raw.get("confidence"),
                "state_senate_district": district_data["state_senate_district"],
                "state_house_district": district_data["state_house_district"],
                "input_string": address,
            }

        else:
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

    except (GeocoderTimedOut, GeocoderServiceError) as e:
        print(f"Error geocoding '{address}': {e}")

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
    print(f"Total non-empty addresses: {len(data[address_column_name].dropna())}")

    addresses = data[address_column_name].tolist()
    geolocator = Bing(api_key=BING_API_KEY, timeout=10)
    results = []
    for index, address in enumerate(addresses):
        try:
            #print(f"Processing address {index+1}/{len(addresses)}: {address}") # debugging: print each address as processed
            result = get_results(address, geolocator)
            results.append(result)
        except Exception as e:
            print(f"Error processing address '{address}': {e}")
            results.append({
                "formatted_address": None, "latitude": None, "longitude": None,
                "state_senate_district": None, "state_house_district": None, "input_string": address
            })
    print(f"Saving {len(results)} results to {output_filename}")

    save_results(results, output_filename, data)

if __name__ == "__main__":
    main()