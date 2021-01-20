# GVA Libraries

# What Is It?

The `gva` libraries are a set of modules to help with creating data processing systems.

# What Is In It?

[gva.flows](docs/gva.flows.md) - data pipelines   
[gva.data.readers](docs/gva.data.readers.md) - read data from various sources   
[gva.data.writers](docs/gva.data.writers.md) - write data to various locations   
[gva.data.formats](docs/gva.data.formats.md) - helper for handling data   
[gva.data.validator](docs/gva.data.validator.md) - schema conformity testing   
[gva.logging](docs/gva.logging.md) - logging routines    
[gva.maths](docs/gva.maths.md) - math routines   

More documentation is available in [the docs folder](docs/)  

# How Do I Get It?
~~~
pip install --upgrade git+https://github.com/gva-jjoyce/gva_data
~~~
or in your requirements.txt
~~~
git+https://github.com/gva-jjoyce/gva_data
~~~

# Dependencies

- [UltraJSON](https://github.com/ultrajson/ultrajson) aka `ujson` is used where `orjson` is not available - `orjson` is not available on all platforms and environments so `ujson` is a dependency to ensure a good JSON library with broad support is available

There are a number of optional dependencies which are required for specific fetures and functionality. These are listed in the [requirements-optional.txt](requirements-optional.txt) file.

# Repository Structure

### docs/
A collection of documents describing how to use various aspects of the `gva` library.

### gva/
The code for the `gva` library.

### gva/data
Data Reader and Writers, the `gva` data validation module and datatype processors.

### gva/errors
`gva` specific Error types and error handler.

### gva/flows
Modules to define and execute data pipelines.

### gva/logging
Common logging module.

### gva/maths
Maths routines for analysing data.

### gva/utils
Helper functions, either generic implementations or used across multiple other libraries.

### labs/
Jupyter Notebooks with lessons to help build familiarity with the `gva` library.

### sites/
Example Web Sites using the `gva` library to provide data access functionality. 

Note that _sites_ have different requirements and main contain code with different licencing to the `gva` library.

### tests/
Unit, Regression and Performance tests, and artefacts to support these tests.

### tools/
Stand-alone tools for use with the `gva` library but not part of the library itself.

# Credits

