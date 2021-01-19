from .is_string import is_string

class is_cve(is_string):

    def __init__(self, **kwargs):
        super().__init__(format=r"(?i)CVE-\d{4}-\d{4,7}")

    def __str__(self):
        return 'CVE'