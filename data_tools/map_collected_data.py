import os
import re
from multiprocessing import Pool

import pandas as pd


def list_files(directory="/home/mborges/data"):
    """Iteratively lists all files and directories in a given directory.

    Parameters
    ----------
        directory: str
            The path to the directory to list.
    Returns
    -------
        files_path_list: list
            List with the full path of all files in the given directory
    """
    files_path_list = []
    stack = [directory]
    while stack:
        path = stack.pop()
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            if os.path.isfile(file_path):
                files_path_list.append(file_path)
            else:
                stack.append(file_path)
    return files_path_list

def extract_info_from_path(path):
    """
    Extracts information from a given path and returns it in a pandas dataframe.

    Parameters:
    -----------
        path: str
            The path to extract information from.

    Returns:
    --------
        pd.DataFrame:
            A dataframe containing the extracted information.
            DataFrame columns:
            - date_today : str
            - hour : int
            - minute : int
            - flight_date : str
            - origin : str
            - destination : str
    """

    # Extract the date after "today"
    data_today = re.findall(r"today_(\d{4}-\d{2}-\d{2})", path)[0]

    # Extract the hour and minute
    hour, minute = re.findall(r"hour_(\d+)_minute_(\d+)", path)[0]

    # Extract the date after "flight_day"
    flight_day = re.findall(r"flight_day_(\d{4}-\d{2}-\d{2})", path)[0]

    # Extract origin and destination from the filename
    filename = os.path.basename(path)
    origin, destination = filename.split("_to_")
    destination = destination.split(".")[0]

    # Create a dictionary with the extracted information
    info_dict = {"data_today": [data_today],
                 "hour": [hour],
                 "minute": [minute],
                 "flight_day": [flight_day],
                 "origin": [origin],
                 "destination": [destination]}

    # Return the extracted information as a dataframe
    return pd.DataFrame.from_dict(info_dict)

def extract_info_from_paths_parallel(paths):
    """
    Extracts information from a list of paths in parallel and concatenates the
    resulting dataframes.

    Parameters
    ----------
    paths : list
        List of paths to extract information from.

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing the extracted information.
    """
    with Pool(processes=os.cpu_count()) as pool:
        results = pool.map(extract_info_from_path, paths)
    df = pd.concat(results)
    return df