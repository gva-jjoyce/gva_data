from setuptools import setup, find_packages  # type:ignore

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
   name='gva.data',
   version='0.1.2',
   description='GVA Data',
   long_description=long_description,
   long_description_content_type="text/markdown",
   author='390516',
   author_email='justin.joyce@lloydsbanking.com',
   packages=find_packages(),
   url="https://github.com/gva-jjoyce/gva_data",
   install_requires=[
        'google_cloud_storage',
        'xmltodict',
        'ujson',
        'networkx'
   ]
)