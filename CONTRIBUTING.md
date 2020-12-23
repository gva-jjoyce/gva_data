# Contribution Guide


Code should look like:
- Imports on separate lines
- Four space tabs
- Variables should be in `snake_case`
- Classes should be in `PascalCase`
- Constants should be in `UPPER_CASE`
- Methods with docstrings
- PEP8 conformity preferred (with a relaxed view on line lengths)
- defs or calls with many parameters have them on different lines
- Type hints
- Self-explanatory method, class and variable names


Code should have:
- Corresponding unit/regression tests
- Attributed external sources - even if there is no explicit license requirement, The context of the source may help others reading the code later.


A note about comments:
- Computers will interpret anything, humans need help interpetting code
- Easy to read code usually runs fast enough
- Prefer readable code over verbose comments
- Comments should be more than just the code in other words


Pull requests should pass:
- bandit (secure coding practices)
- mypy (type hints)
- pytest (regression tests)
- test coverage should not be reduced (currently 65%)