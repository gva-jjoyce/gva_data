pip install --upgrade "pytest>=6.2.1" "coverage>=5.3.1" "bandit>=1.7" "mypy>=0.790"

coverage run -m pytest
coverage html --include=gva*

bandit . -r -x /tests -x \tests

mypy .