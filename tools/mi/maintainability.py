"""
Maintainability Index Tester

Uses radon to calculate Maintainability Index
(see: https://radon.readthedocs.io/en/latest/intro.html)

Tools & Tests are excluded, as well as as there being an option to add a flag
to the file to exclude spefific files.

Radon itself will A grade for maintainability for scores 100 to 20, this
script sets the bar at 50.
"""
import radon.metrics
import glob
import sys

EXCLUSIONS = ['./tools/', './tests/']
LIMIT = 50

file_list = glob.iglob('./**/*.py', recursive=True)
all_okay = True

for item in file_list:
    if any([True for exclusion in EXCLUSIONS if item.startswith(exclusion)]):
        continue

    with open(item, 'r', encoding='UTF8') as code_file:
        code = code_file.read()

    maintainability_index = radon.metrics.mi_visit(code, True)

    if code.startswith('#no-maintain-checks'):
        print(F"!! #no-maintain-checks flag set in {item} - result of {maintainability_index:.2f} not enforced")
        continue

    if maintainability_index <= LIMIT:
        all_okay = False
        print(F"Maintainability Index of {item} below {LIMIT} ({maintainability_index:.2f})")

if not all_okay:
    sys.exit(1)

print('pass')
