import json
from glob import glob

import pandas as pd
from joblib import Parallel, delayed

from map_collected_data import extract_info_from_path


class FlightExtractor():
    """Structure the data collected from flight_scrape.py."""
    def __init__(self, json_paths):
        """
        Parameters
        ----------
        json_paths: list[str]
            List of json's path whose data should be structured.
        """
        self.json_paths = json_paths
        
    def structure_all_jsons(self, n_jobs=-1):
        """Structure all json's with parallel processing.

        Parameters
        ----------
        n_jobs: int (default=-1, all cores)
            Number of colors.
    
        Return
        ------
        structured_data: pd.DataFrame
            All structured json's.
        error_log_df: pd.DataFrame
            The log of problems during data structuring.
        """
        output_list = Parallel(n_jobs=n_jobs, prefer="processes", verbose=1)(
            [delayed(self._structure_json)(json_path) for json_path in self.json_paths]
        )
        
        structured_data_list = list()
        error_log_list = list()
        for structured_data, error_log_df in output_list:
            structured_data_list.append(structured_data)
            error_log_list.append(error_log_df)
        structured_data = pd.concat(structured_data_list, ignore_index=True)
        error_log_df = pd.concat(error_log_list, ignore_index=True)

        return structured_data, error_log_df

    def _structure_json(self, json_path):
        """Structure one json data.
        
        Parameters
        ----------
        json_path: str
            Json path whose data should be structured.
    
        Return
        ------
        structured_data: pd.DataFrame
            Structured json data.
        error_log_df: pd.DataFrame
            The log of problems during data structuring.
        """
        data, error_log_df = self._read_json(json_path)
        data, error_log_df = self._data_checks(data, json_path)
        
        structured_data = pd.DataFrame()
        
        if data is not None:
            try:
                structured_data_list = list()
                for flight_info, fare_info in zip(data['legs'], data['offers']):

                    is_the_same_flight = flight_info['legId'] == fare_info['legIds'][0]
                    if not is_the_same_flight:
                        continue

                    structured_flight_df = self._structure_flight_information(flight_info)
                    structured_fare_df = self._structure_fare_information(fare_info)
                    structured_df = pd.concat(
                        [structured_flight_df, structured_fare_df], axis="columns"
                    )
                    structured_data_list.append(structured_df)
                structured_data = pd.concat(structured_data_list, ignore_index=True)
                structured_collect_df = self._structure_collect_information(data, json_path)
                structured_collect_df = (
                    structured_collect_df
                    .loc[structured_collect_df.index.repeat(len(structured_data))]
                    .reset_index(drop=True)
                )
                structured_data = pd.concat([structured_collect_df, structured_data], axis="columns")
            except:
                print("json_path", json_path)
        return structured_data, error_log_df
    
    def _read_json(self, json_path):
        """Read json.

        Parameters
        ----------
        json_path: str
            Json path whose data should be structured.

        Return
        ------
        data: dict
            Json data.
        error_log_df: pd.DataFrame
            The log of problems during json reading.
        """
        try:
            with open(json_path, 'r') as file:
                data = json.load(file)
            error_log_df = pd.DataFrame(columns=["json_path", "error_message"])

        except json.JSONDecodeError:
            data = None
            error_log_df = pd.DataFrame({"json_path": [json_path],
                                         "error_message": ["Unable to read json file"]}
            )
        return data, error_log_df
    
    def _data_checks(self, data, json_path):
        """Checks whether it is possible to extract information from the data.
        
        Parameters
        ----------
        data: dict
            Json data.
        json_path: str
            Json path whose data should be structured.
        
        Return
        ------
        data: dict
            Json data.
        error_log_df: pd.DataFrame
            The log of problems during data structuring.
        """
        if data is not None:
            try:
                necessary_keys = ["legs", "offers", "search_time", "searchCities"]
                error_message = "The json does not have all the necessary keys"
                for key in necessary_keys:
                    assert key in data.keys(), error_message
                
                error_message = ("Legs and offers not same length, "
                                 f"legs = {len(data['legs'])}; offers = {len(data['offers'])}")
                assert len(data['legs']) == len(data['offers']), error_message
                
                
                error_message = ("Legs or offers with len 0. "
                                 f"legs = {len(data['legs'])}; offers = {len(data['offers'])}")
                assert len(data['legs']) > 0 and len(data['offers']) > 0, error_message
                                                
                error_log_df = pd.DataFrame(columns=["json_path", "error_message"])
            except:
                data = None
                error_log_df = pd.DataFrame({"json_path": [json_path],
                                             "error_message": [error_message]})
        return data, error_log_df
    
    def _structure_flight_information(self, flight_info):
        """Structure flight information.
        
         Parameters
        ----------
        flight_info: dict
            The information of each segment of the flight. data['legs']
        Return
        ------
        structured_data_df:pd.DataFrame
            Structured data
        """
        blocked_keys_flight = ['baggageFeesUrl'] + ['segments', 'freeCancellationBy']
        structured_data_dict = {key: [flight_info.get(key)]
                                for key in flight_info
                                if key not in blocked_keys_flight}
        structured_data_dict['freeCancellationBy'] = [
            flight_info['freeCancellationBy'].get("raw")
        ]

        blocked_keys_segments = ["departureTime", "departureTimeEpochSeconds",
                                 "arrivalTime", "arrivalTimeEpochSeconds",
                                 "arrivalAirportLocation", "arrivalAirportName",
                                 "arrivalAirportAddress", "departureAirportLocation",
                                 "departureAirportName", "departureAirportAddress",
                                 "airlineImageFileName"]
        for index, segment in enumerate(flight_info["segments"]):
            separator = "" if len(flight_info["segments"]) == 1 or index == len(flight_info["segments"]) - 1 else "||"
            for key, value in segment.items():
                if key in blocked_keys_segments:
                    continue
                value = str(value)
                if index == 0:
                    structured_data_dict[key] = [value + separator]
                else:
                    structured_data_dict[key] = [structured_data_dict.get(key, [""])[0] + value + separator]
        structured_data_df = pd.DataFrame(structured_data_dict)
        return structured_data_df
    
    def _structure_fare_information(self, fare_info):
        """Structure fare information.
        
         Parameters
        ----------
        fare_info: dict
            Information about flight fees. data['offers']
        
        Return
        ------
        structured_data_df:pd.DataFrame
            Structured data
        """
        keys_handled_separately = ["averageTotalPricePerTicket", "segmentAttributes",
                                   "loyaltyInfo", "flightFulfillmentMethod"]
        blocked_keys_fare = ["legIds", "baseFarePrice", "totalFarePrice", "totalPrice",
                             "taxesPrice", "feesPrice", "productKey", "mobileShoppingKey",
                             "baggageFeesUrl", "fareBasisCodes", "pricePerPassengerCategory"]
        blocked_keys_fare = blocked_keys_fare + keys_handled_separately

        structured_data_dict = {key: [fare_info.get(key)]
                                 for key in fare_info
                                 if key not in blocked_keys_fare}

        structured_data_dict["averageTotalPricePerTicket"] = [
            fare_info.get("averageTotalPricePerTicket", {}).get("amount")
        ]

        flightFulfillmentMethod = "||".join(fare_info.get("flightFulfillmentMethod", []))
        structured_data_dict["flightFulfillmentMethod"] = [
            None if flightFulfillmentMethod == "" else flightFulfillmentMethod
        ]

        structured_data_dict["loyaltyInfo_isBurnApplied"] = (
            fare_info.get("loyaltyInfo", {}).get("isBurnApplied")
        )

        structured_data_dict["loyaltyInfo_points_base"] = [
            fare_info.get("loyaltyInfo", {}).get("earn", {}).get('points', {}).get("base")
        ]
        structured_data_dict["loyaltyInfo_points_bonus"] = [
            fare_info.get("loyaltyInfo", {}).get("earn", {}).get('points', {}).get("bonus")
        ]
        structured_data_dict["loyaltyInfo_points_total"] = [
            fare_info.get("loyaltyInfo", {}).get("earn", {}).get('points', {}).get("total")
        ]

        for index, segment in enumerate(fare_info.get("segmentAttributes", [])):
            separator = ("" if len(fare_info["segmentAttributes"]) == 1
                            or index == len(fare_info["segmentAttributes"]) - 1
                            else "||")
            for attributes in segment:
                for key, value in attributes.items():
                    value = str(value)
                    if index == 0:
                        structured_data_dict[key] = [value + separator]
                    else:
                        structured_data_dict[key] = [
                            structured_data_dict[key][0] + value + separator
                        ]
        structured_data_df = pd.DataFrame(structured_data_dict)
        return structured_data_df
    
    def _structure_collect_information(self, data, json_path):
        """Structure information about data collection.
        
         Parameters
        ----------
        data: dict
            Json data.
        json_path: str
            Json path whose data should be structured.

        Return
        ------
        structured_data_df:pd.DataFrame
            Structured data
        """
        json_info_df = extract_info_from_path(json_path)
        operational_search_time = (json_info_df.loc[0, "data_today"] + "T"
                                   + json_info_df.loc[0, "hour"] + ":"
                                   + json_info_df.loc[0, "minute"])

        structured_data_dict = {
            "search_time": [data.get("search_time")],
            "operational_search_time":[operational_search_time],
            "flight_day": [json_info_df.loc[0, "flight_day"]],

            "origin_code": [data.get("searchCities", [{}])[0].get("code")],
            "origin_city": [data.get("searchCities", [{}])[0].get("city")],
            # "origin_country": [data.get('searchCities', [{}])[0].get("country")],

            "destination_code": [data.get("searchCities", [{}])[-1].get("code")],
            "destination_city": [data.get("searchCities", [{}])[-1].get("city")],
            # "destination_country": [data.get('searchCities', [{}])[-1].get("country")],
        }
        structured_data_df = pd.DataFrame(structured_data_dict)
        return structured_data_df
