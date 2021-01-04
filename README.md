# GVA Libraries

## What Is It?

A set of modules to help with creating data processing systems.

## What Is In It?

[gva.flows](docs/gva.flows.md) - data pipelines   
[gva.data.readers](docs/gva.data.readers.md) - read data from various sources   
[gva.data.writers](docs/gva.data.writers.md) - write data to various locations   
[gva.data.formats](docs/gva.data.formats.md) - helper for handling data   
[gva.data.validator](docs/gva.data.validator.md) - schema conformity testing   
[gva.logging](docs/gva.logging.md) - logging routines    

## How Do I Get It?
~~~
pip install --upgrade git+https://github.com/gva-jjoyce/gva_data
~~~
or in your requirements.txt
~~~
git+https://github.com/gva-jjoyce/gva_data
~~~

## Repository Structure

#### docs/

A collection of documents describing how to use various aspects of the `gva` library.

#### gva/

The code for the `gva` library.

#### labs/

Jupyter Notebooks with lessons to help build familiarity with the `gva` library.

#### tests/

Unit, Regression and Performance tests, and artefacts to support these tests.

#### tools/

Stand-alone tools for use with the `gva` library but not part of the library itself.
