from glob import glob
import pandas as pd
import json
from joblib import Parallel, delayed


class FlightExtractor():
    """Structure the data collected from flight_scrape.py."""
    def __init__(self, json_paths):
        """
        Parameters
        ----------
        """
        self.json_paths = json_paths
        
    def structure_all_jsons(self, n_jobs=-1):
        
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
        """Structure 1 json."""
        data, error_log_df = self._read_json(json_path)
        data, error_log_df = self._data_checks(data, json_path)
        
        structured_data = pd.DataFrame()
        
        if data is not None:
            structured_data_list = list()
            for flight_info, fare_info in zip(data['legs'], data['offers']):
                is_the_same_flight = flight_info['legId'] == fare_info['legIds'][0]
                if not is_the_same_flight:
                    continue
                structured_data_dict = self._structure_flight_information(flight_info)
                structured_data_dict.update(self._structure_fare_information(fare_info))
                structured_data_list.append(pd.DataFrame(structured_data_dict))
            structured_data = pd.concat(structured_data_list)
        return structured_data, error_log_df
    
    def _structure_flight_information(self, flight_info):
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
                    print()
                    structured_data_dict[key] = [structured_data_dict.get(key, [""])[0] + value + separator]
        return structured_data_dict
    
    def _structure_fare_information(self, fare_info):
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
        return structured_data_dict
    
    def _read_json(self, json_path):
        try:
            with open(json_path, 'r') as file:
                data = json.load(file)
            error_log_df = pd.DataFrame(columns=["json_path", "error_message"])
            
        except json.JSONDecodeError:
            data = None
            error_log_df = pd.DataFrame({"json_path": json_path,
                                         "error_message": "Unable to read json file"}
            )
        return data, error_log_df
    
    def _data_checks(self, data, json_path):
        if data is not None:
            error_log_df = pd.DataFrame(columns=["json_path", "error_message"])
            if len(data['legs']) != len(data['offers']):
                data = None
                error_log_df = pd.DataFrame({"json_path": json_path,
                                             "error_message": "Legs and offers not same length"}
                )
        return data, error_log_df
        