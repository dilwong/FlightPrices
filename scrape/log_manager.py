import os


class LogManager():
    def __init__(self, execution_date, log_path, logs_folder):
        """Initialize the class.

        Parameters
        ----------
        execution_date:  datetime.datetime
            Date of execution.
        log_path: str
            Path to the generated log file.
        logs_folder: str
            Path to the new folder the log should go in.
        """
        self.execution_date = execution_date
        self.log_path = log_path
        self.logs_folder = logs_folder
    
    def rename_and_move(self):
        """Rename and move the log file."""
        if not os.path.exists(self.logs_folder):
            os.makedirs(self.logs_folder)

        file_name = os.path.basename(self.log_path)
        path_folder = os.path.dirname(self.log_path)

        new_file_name = self.execution_date.strftime("%Y-%m-%d_%Hh_") + file_name
        new_file_path = os.path.join(path_folder, new_file_name)
        
        os.rename(self.log_path, new_file_path)
        os.replace(new_file_path, os.path.join(self.logs_folder, new_file_name))