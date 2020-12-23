pip install --upgrade "pytest>=6.2.1" "coverage>=5.3.1" bandit mypy

coverage run -m pytest
coverage html --include=gva*

bandit . -r -x /tests

mypy .