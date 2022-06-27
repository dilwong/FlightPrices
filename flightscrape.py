from datetime import date, timedelta
import itertools
import requests
import time
import os
import json
import schedule

airports = ['ATL', 'DFW', 'DEN', 'ORD', 'LAX', 'CLT', 'MIA', 'JFK', 'EWR', 'SFO', 'DTW', 'BOS', 'PHL', 'LGA', 'IAD', 'OAK']
airport_pairs = [pair for pair in itertools.product(airports, repeat = 2) if pair[0] != pair[1]]

def collect_flight_data():
    today = date.today()
    flight_day = today + timedelta(days = 1)
    end_date = today + timedelta(days = 60)
    while (today == date.today()) and (flight_day <= end_date):
        for depart, arrive in airport_pairs:
            URL = f'https://www.expedia.com/api/flight/search?departureDate={flight_day}&departureAirport={depart}&arrivalAirport={arrive}'
            filename = f'{today}/{flight_day}/{depart}_to_{arrive}.json'
            try:
                req = requests.get(URL)
            except:
                time.sleep(10)
            os.makedirs(os.path.dirname(filename), exist_ok = True)
            with open(filename, 'w') as file:
                json.dump(req.json(), file)
            if not (today == date.today()):
                break
        flight_day = flight_day + timedelta(days = 1)

schedule.every().day.at('00:02').do(collect_flight_data)

while True:
    schedule.run_pending()
    time.sleep(1)