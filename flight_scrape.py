import itertools
import json
import traceback
from datetime import date, datetime, timedelta
from os import makedirs
from os.path import dirname, join
from time import sleep, time

import requests
from joblib import Parallel, delayed

airports = ['ATL', 'DFW', 'DEN', 'ORD', 'LAX', 'CLT', 'MIA', 'JFK',
            'EWR', 'SFO', 'DTW', 'BOS', 'PHL', 'LGA', 'IAD', 'OAK'
           ]
airport_pairs = [pair for pair in itertools.product(airports, repeat = 2)
                 if pair[0] != pair[1]]

def collect_flight_data(today, departure_airport, arrival_airport, flight_day, maxExceptions=20):
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
            filename = join("..", "data", str(today), str(flight_day),
                            f"{departure_airport}_to_{arrival_airport}.json")
            makedirs(dirname(filename), exist_ok = True)

            # Saves the entire web page in json format
            with open(filename, 'w') as file:
                json.dump(request_json, file)
            
            print("SUCESSO" + "!"*20)
            return True

        except Exception:
            # Increments the exception counter
            exceptionCounter += 1

            # Displays the error and leaves the code on hold for 10 seconds
            print(f"Error detected at flight_day {flight_day} for departure_airporture"
                  "{departure_airport} and arrival {arrival_airport}:")
            traceback.print_exc()
            sleep(10)

            # If the number of attempts has been exceeded, then go to the next run
            if exceptionCounter > maxExceptions:
                print('Skipping...')
                break
            else:
                print('Continuing...')


def runner_collect_flight_data(max_additional_day=60, maxExceptions=20, n_jobs=-1):
    today = date.today()
    flight_day_list = [today + timedelta(days = additional_day)
                       for additional_day in range(1, max_additional_day+1)]
    
    delayed_list = list()
    for flight_day in flight_day_list:
        for departure_airport, arrival_airport in airport_pairs:
            delayed_list.append(
                delayed(collect_flight_data)(
                    today, departure_airport, arrival_airport, flight_day, maxExceptions
                )
            )
    Parallel(n_jobs=n_jobs , prefer= "processes", verbose=1)(delayed_list)

if __name__ == "__main__":
    runner_collect_flight_data()