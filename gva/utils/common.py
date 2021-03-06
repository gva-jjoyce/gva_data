import glob
import datetime
from typing import Optional
from .json import parse, serialize
from ..logging import get_logger


def date_range(start_date: Optional[datetime.date], end_date: Optional[datetime.date]):
    # if dates aren't provided, use today
    if not end_date:
        end_date = datetime.date.today()
    if not start_date:
        start_date = datetime.date.today()

    if end_date < start_date:
        raise ValueError("date_range: end_date must be the same or later than the start_date ")

    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + datetime.timedelta(n)


def build_context(**kwargs: dict):
    """
    Build Context takes an arbitrary dictionary and merges with a dictionary
    which reflects configuration read from a json file.

    It builds start and end dates, either from either source or defaults to
    today.
    """
    def read_config(config_file):
        # read the job configuration
        try:
            file_location = glob.glob('**/' + config_file, recursive=True).pop()
            get_logger().debug(F'Reading configuration from {file_location}.')
            with open(file_location, 'r') as f:
                config = parse(f.read())
            return config
        except:
            return {}

    # read the configuration file
    config_file = kwargs.get('config_file', 'config.json')
    config = read_config(config_file=config_file)
    if not config.get('config'):
        config['config'] = {}

    # merge the sources
    context = {**config, **kwargs}

    return context