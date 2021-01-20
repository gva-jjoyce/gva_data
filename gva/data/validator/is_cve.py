from .is_string import is_string

class is_cve(is_string):
    """
    Test if a variable is a valid CVE identifier
    """
    def __init__(self, **kwargs):
        # this is the regex that it appears NVD uses
        super().__init__(format='cve|CVE-[0-9]{4}-[0-9]{4,}')

    def __str__(self):
        return 'CVE'