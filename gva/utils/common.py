import glob
import orjson as json
import datetime



def date_range(start_date: datetime.date, end_date: datetime.date):
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
        file_location = glob.glob('**/' + config_file, recursive=True).pop()
        with open(file_location, 'r') as f:
            config = json.loads(f.read())
        return config

    # read the configuration file
    config_file = kwargs.get('config_file', 'config.json')
    config = read_config(config_file=config_file)
    if not config.get('config'):
        config['config'] = {}

    # merge the sources
    context = {**config, **kwargs}

    return context