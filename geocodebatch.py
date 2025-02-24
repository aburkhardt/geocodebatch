import os
import pandas as pd
import pprint
import sys
import requests
import logging
import time

from geocoder import bing
import prettyprint

logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)
# create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

#------------------ CONFIGURATION -------------------------------

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
BING_API_KEY = os.getenv('BING_API_KEY')
# Backoff time sets how many minutes to wait between pings when your API limit is hit
BACKOFF_TIME = 30
# Set your output file name here.
output_filename = 'data/output-20250224list.csv'
# Set your input file here
input_filename = "data/20250224list.csv"
# Specify the column name in your input data that contains addresses here
address_column_name = "Address"
# Return Full Results? If True, full JSON results are included in output
RETURN_FULL_RESULTS = True

#------------------ DATA LOADING --------------------------------

# Read the data to a Pandas Dataframe
data = pd.read_csv(input_filename, encoding='cp1252')
#data = pd.read_csv(input_filename, encoding='utf8')


if address_column_name not in data.columns:
	raise ValueError("Missing Address column in input data")

# Form a list of addresses for geocoding:
# Make a big list of all of the addresses to be processed.
addresses = data[address_column_name].tolist()

#------------------	FUNCTION DEFINITIONS ------------------------

def get_results(address,api_key=BING_API_KEY):    
# Ping bing for the results
    y = bing(address)
# Results will be in JSON format - convert to dict using requests functionality    
#    results = y.json()

# if there's no results or an error, return empty results.    
#    if len(results['results']) == 0:
#        output = {
#            "formatted_address" : None,
#            "latitude": None,
#            "longitude": None,
#            "accuracy": None,
#            "google_place_id": None,
#            "type": None,
#            "postcode": None
#        }
#    else:    
#    answer = y.json['y.json'][0]
    output = {
        "formatted_address" : y.json['address'],
        "latitude": y.json['lat'],
        "longitude": y.json['lng'],
        "county": y.json['raw']['address']['adminDistrict2'],
        "accuracy": y.json['raw']['matchCodes']
    }

    # Append some other details:            
    output['input_string'] = address

    return output

#------------------ PROCESSING LOOP -----------------------------

# Ensure, before we start, that the API key is ok/valid, and internet access is ok
# test_result = bing("London, England")
# if (test_result['status'] != 'OK') or (test_result['formatted_address'] != 'London, UK'):
#    logger.warning("There was an error when testing the Geocoder.")
#    raise ConnectionError('Problem with test results from Geocode - check your API key and internet connection.')

# Create a list to hold results
results = []
# Go through each address in turn
for address in addresses:
    # While the address geocoding is not finished:
    geocoded = False
    while geocoded is not True:
        # Geocode the address
        try:
            geocode_result = get_results(address)
        except Exception as e:
            logger.exception(e)
            logger.error("Major error with {}".format(address))
            logger.error("Skipping!")
            geocoded = True
            
        # If we're over the API limit, backoff for a while and try again later.
        #if geocode_result['status'] == 'OVER_QUERY_LIMIT':
        #    logger.info("Hit Query Limit! Backing off for a bit.")
        #    time.sleep(BACKOFF_TIME * 60) # sleep for 30 minutes
        #    geocoded = False
        #else:
            # If we're ok with API use, save the results
            # Note that the results might be empty / non-ok - log this
        #    if geocode_result['status'] != 'OK':
        #        logger.warning("Error geocoding {}: {}".format(address, geocode_result['status']))
        # logger.debug("Geocoded: {}: {}".format(address, geocode_result['status']))
        results.append(geocode_result)           
        geocoded = True

    # Print status every 100 addresses
    if len(results) % 100 == 0:
    	logger.info("Completed {} of {} address".format(len(results), len(addresses)))
            
    # Every 500 addresses, save progress to file(in case of a failure so you have something!)
    if len(results) % 500 == 0:
        pd.DataFrame(results).to_csv("{}_bak".format(output_filename))

# All done
logger.info("Finished geocoding all addresses")
# Write the full results to csv using the pandas library.
pd.DataFrame(results).to_csv(output_filename, encoding='utf8')