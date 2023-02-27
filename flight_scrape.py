import itertools
import json
import traceback
from datetime import date, datetime, timedelta
from os import makedirs
from os.path import dirname, join
from time import sleep, time

import requests
from joblib import Parallel, delayed


# United States of America airports
AIRPORTS_USA = ['ATL', 'DFW', 'DEN',# 'ORD', 'LAX', 'CLT', 'MIA', 'JFK',
                # 'EWR', 'SFO', 'DTW', 'BOS', 'PHL', 'LGA', 'IAD', 'OAK'
               ]

AIRPORT_PAIRS = [pair for pair in itertools.product(AIRPORTS_USA, repeat = 2)
                 if pair[0] != pair[1]]

def collect_flight_data(today, hour, minute, departure_airport,
                        arrival_airport, flight_day, maxExceptions=20):
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

    Return
    ------
    success: bool
        True if the data was successfully collected, False otherwise
    """
    success = False
    exceptionCounter = 0
    while True:
        try:
            # Read the HTML of the webpage
            URL = (f"https://www.expedia.com/api/flight/search?departureDate={flight_day}"
                   f"&departureAirport={departure_airport}&arrivalAirport={arrival_airport}")
            request_json = requests.get(URL).json()

            # Recording the search time
            request_json["search_time"] = datetime.now().isoformat()

            # Make directory
            filename = join("..", "data", f"today_{today}", f"hour_{hour}_minute_{minute}",
                            f"flight_day_{flight_day}", f"{departure_airport}_to_{arrival_airport}.json")
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
                  "{departure_airport} and arrival {arrival_airport}:")
            # traceback.print_exc()
            sleep(10)

            # If the number of attempts has been exceeded, then go to the next run
            if exceptionCounter > maxExceptions:
                print('Skipping...')
                break
            else:
                print('Continuing...')
    return success


def runner_collect_flight_data(max_additional_day=60, maxExceptions=20, n_jobs=-1):
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
    """
    today = date.today()
    now = datetime.now()
    flight_day_list = [today + timedelta(days = additional_day)
                       for additional_day in range(1, max_additional_day+1)]
    
    delayed_list = list()
    for flight_day in flight_day_list:
        for departure_airport, arrival_airport in AIRPORT_PAIRS:
            delayed_list.append(
                delayed(collect_flight_data)(
                    today, now.hour, now.minute, departure_airport,
                    arrival_airport, flight_day, maxExceptions
                )
            )
    Parallel(n_jobs=n_jobs , prefer="processes", verbose=1)(delayed_list)

if __name__ == "__main__":
    runner_collect_flight_data()
    print("Executed!\n\n")