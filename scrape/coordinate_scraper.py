from datetime import datetime


class CoordinateScraper():
    """Coordinates at what time each machine should collect data.
    
    The class assumes that data will be collected hourly.
    """
    def __init__(self, machines_number=3, machines_per_date=2, base_date=None):
        """Initialize the class.

        Parameters
        ----------
        machines_number: int (default=3)
            Total number of machines that exist to run the scraper.
        machines_per_date: int (default=2)
            Number of machines that will run the scraper per date.
        base_date: datetime.datetime (default=datetime(2000, 12, 11, 10, 27, 30))
            Base date, used as a reference to define which machine runs at what time.
        """
        assert machines_per_date <= machines_number, ("machines_per_date must be "
                                                      "less than or equal to machines_number")

        if base_date is None:
            self.base_date = datetime(2000, 12, 11, 10, 27, 30)
        else:
            assert isinstance(base_date, datetime), "base_date must be datetime object."
            self.base_date = base_date
        self.machines_number = machines_number
        self.machines_per_date = machines_per_date
        self.complete_relay_cycle = None
        self.timesheet = None
        
    def check_should_run_hour(self, date, machine_id):
        """Checks if the machine should run in a hour.
        
        Parameters
        ----------
        date: datetime.datetime
            Date to check.
        machine_id: int
            The number that identifies the machine.
        
        Return
        ------
        should_run: bool
            True if the machine should run, False otherwise.
        """
        self.__assert_machine_values(machine_id)
        self.__assert_date(date)
        should_run = self.check_should_run_date(date, machine_id)
        if should_run:
            cycle_date_index = self.get_cycle_date_index(date)
            cycle_date_index += 1
            day_hours = 24
            max_index = cycle_date_index * day_hours
            min_index = max_index - day_hours
            day_timesheet = self.get_timesheet()
            day_timesheet = day_timesheet[min_index:max_index]
            should_run = day_timesheet[date.hour] == machine_id
        return should_run
        
    
    def check_should_run_date(self, date, machine_id):
        """Checks if the machine should run in a date.
        
        Parameters
        ----------
        date: datetime.datetime
            Date to check.
        machine_id: int
            The number that identifies the machine.
        
        Return
        ------
        should_run: bool
            True if the machine should run, False otherwise.
        """
        self.__assert_machine_values(machine_id)
        self.__assert_date(date)
        
        machines_run_date = self.get_machines_run_date(date)
        should_run = machine_id in machines_run_date
        return should_run
    
    def get_cycle_date_index(self, date):
        """Get the index of the cycle that the date belongs to.
        
        Parameters
        ----------
        date: datetime.datetime
            Date to check.
        
        Return
        ------
        cycle_date_index: int
            Day of the cycle that the date belongs to.
            0 <= cycle_date_index < len(self.get_complete_relay_cycle())
        """
        complete_relay_cycle = self.get_complete_relay_cycle()
        delta_time = date - self.base_date
        cycle_date_index = delta_time.days % len(complete_relay_cycle)
        return cycle_date_index
    
    def get_timesheet(self):
        """Get the machines timesheet.
        
        Return
        ------
        timesheet: list[int]
            It is the list that keeps which machine_id must be run in each hour of a cycle.
            Example: index 0 of the list stores the machine_id that must run at hour 0 of
            the first day of the cycle; index 24 stores the machine_id that must run at
            hour 0 of the second day of the cycle.
        """
        if self.timesheet is None:
            day_hours = 24
            self.timesheet = list()
            for machines_id in self.get_complete_relay_cycle():
                repetitions = day_hours//len(machines_id) + 1
                day_timesheet = machines_id * repetitions
                if len(self.timesheet) > 0:
                    if day_timesheet[0] == self.timesheet[-1]:
                        day_timesheet.pop(0)
                self.timesheet +=  day_timesheet[0:day_hours]
        return self.timesheet
    
    def get_machines_run_date(self, date):
        """Get the machines that must run on a date.
        
        Parameters
        ----------
        date: datetime.datetime
            Date to check.
        
        Return
        ------
        machines_run_date: list
            Machine that should run.
        """
        cycle_date_index = self.get_cycle_date_index(date)
        machines_run_date = self.get_complete_relay_cycle()[cycle_date_index]
        return machines_run_date
        
    def get_complete_relay_cycle(self):
        """Get the complete cycle of the relay machine.

        Return
        ------
        complete_relay_cycle: list[list[int]]
            It is a list that keeps the lists of machines that must run on each date of a cycle.
        """
        if self.complete_relay_cycle is None:
            complete_relay_cycle = list()
            for first_machine in range(1, self.machines_number+1):
                last_machine = first_machine + self.machines_per_date -1
                last_machine = (last_machine%self.machines_number
                                if last_machine >= self.machines_number
                                else last_machine)
                if first_machine < last_machine:
                    machines_list = list(range(first_machine, last_machine+1))
                else:
                    machines_list = list(range(first_machine, self.machines_number+1)) + list(range(1, last_machine+1))
                complete_relay_cycle.append(machines_list)
                self.complete_relay_cycle = complete_relay_cycle
        return self.complete_relay_cycle    

    def __assert_machine_values(self, machine_id):
        machine_values = list(range(1, self.machines_number+1))
        assert machine_id in machine_values, (
            f"machine_id should be one of the values {machine_values},"
            f" but machine_id={machine_id}")

    def __assert_date(self, date):
        assert date > self.base_date, (f"date shold be bigger "
                                      f"than self.base_date={self.base_date}")
