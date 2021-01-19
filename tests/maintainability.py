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

    with open(item, 'r') as code_file:
        code = code_file.read()

    maintainability_index = radon.metrics.mi_visit(code, False)
    if maintainability_index <= LIMIT:
        all_okay = False
        print(F"Maintainability Index of {item} below {LIMIT} ({maintainability_index:.2f})")

if not all_okay:
    sys.exit(1)