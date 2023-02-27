import itertools
import json
import traceback
from datetime import date, datetime, timedelta
from os import makedirs
from os.path import dirname, join, isfile
from time import sleep, time

import requests
from joblib import Parallel, delayed


# United States of America airports
AIRPORTS_USA = ['ATL', 'DFW', 'DEN', 'ORD', 'LAX', 'CLT', 'MIA', 'JFK',
                'EWR', 'SFO', 'DTW', 'BOS', 'PHL', 'LGA', 'IAD', 'OAK'
               ]

AIRPORT_PAIRS = [pair for pair in itertools.product(AIRPORTS_USA, repeat = 2)
                 if pair[0] != pair[1]]

def collect_flight_data(today, hour, minute, departure_airport,
                        arrival_airport, flight_day,
                        maxExceptions=20, overwrite_data=False):
    """ Air ticket price web scraper.
    
    Collects the data and saves it in json format in the correct folder structure
    Parameters
    ----------
    today: datetime.date
        Current day, represents the day data is being collected
    hour: int
        Time the function was called
    minute: int
        Minute in which function was called
    departure_airport: str
        Three-character IATA airport code for the initial location
    arrival_airport: str
        Three-character IATA airport code for the arrival location
    flight_day: datetime.date
        Day of the flight that we will collect the data
    maxExceptions: int (default=20)
        Maximum number of attempts to collect data
    overwrite_data: bool (default=False)
        If True overwrite already computed data, if False do not overwrite
    
    Return
    ------
    success: bool
        True if the data was successfully collected, False if failure occurred
        and None if the data had already been computed
    """
    success = False
    exceptionCounter = 0
    while True:
        try:
            
            filename = join("..", "data", f"today_{today}", f"hour_{hour}_minute_{minute}",
                            f"flight_day_{flight_day}", f"{departure_airport}_to_{arrival_airport}.json")
            
            # Checks if the data has already been computed
            if isfile(filename) and not overwrite_data:
                print("Data already computed")
                success = None
                break
            
            # Read the HTML of the webpage
            URL = (f"https://www.expedia.com/api/flight/search?departureDate={flight_day}"
                   f"&departureAirport={departure_airport}&arrivalAirport={arrival_airport}")
            request_json = requests.get(URL).json()

            # Recording the search time
            request_json["search_time"] = datetime.now().isoformat()
            
            # Make directory
            makedirs(dirname(filename), exist_ok = True)

            # Saves the entire web page in json format
            with open(filename, 'w') as file:
                json.dump(request_json, file)
            
            print("SUCCESS" + "!"*20)
            success = True
            break

        except Exception:
            # Increments the exception counter
            exceptionCounter += 1

            # Displays the error and leaves the code on hold for 10 seconds
            print(f"Error detected at flight_day {flight_day} for departure_airporture"
                  f"{departure_airport} and arrival {arrival_airport}:")
            # traceback.print_exc() # Print error occurred
            sleep(10)

            # If the number of attempts has been exceeded, then go to the next run
            if exceptionCounter > maxExceptions:
                print('Skipping...')
                break
            else:
                print('Continuing...')
    return success


def runner_collect_flight_data(max_additional_day=60, maxExceptions=20,
                               n_jobs=-1, hour=None, minute=None):
    """ Runs collect_flight_data in parallel.
    Parameters
    ----------
    max_additional_day: int (default=60)
        Maximum number of attempts per flight day/departure airport/arrival airport
    maxExceptions: int (default=20)
        Maximum number of attempts to collect data
    n_jobs: int (default=-1)
        Number of machine cores that should be used for parallelism.
        By default -1 which uses all cores
    hour: int (default=None)
        Time the function was called. If the value is None, the variable
        is calculated automatically
    minute: int (default=None)
        Minute in which function was called. If the value is None,
        the variable is calculated automatically
    """
    today = date.today()
    now = datetime.now()
    flight_day_list = [today + timedelta(days = additional_day)
                       for additional_day in range(1, max_additional_day+1)]
    
    if hour is None:
        hour = now.hour
    if minute is None:
        minute = now.minute
    
    delayed_list = list()
    for flight_day in flight_day_list:
        for departure_airport, arrival_airport in AIRPORT_PAIRS:
            delayed_list.append(
                delayed(collect_flight_data)(
                    today, hour, minute, departure_airport,
                    arrival_airport, flight_day, maxExceptions
                )
            )
    Parallel(n_jobs=n_jobs , prefer="processes", verbose=1)(delayed_list)

if __name__ == "__main__":
    n_jobs=-1
    hour=None
    minute=None
    runner_collect_flight_data(n_jobs=n_jobs, hour=hour, minute=minute)
    print("Executed!\n\n")