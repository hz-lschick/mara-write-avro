from setuptools import setup, find_packages
import re

def get_long_description():
    with open('README.md') as f:
        return re.sub('!\[(.*?)\]\(docs/(.*?)\)', r'![\1](https://github.com/hz-lschick/mara-write-avro/raw/master/docs/\2)', f.read())

setup(
    name='mara-write-avro',
    version='0.1.0',

    description='Commands to write an sql table or query into an Apache Avro file within an mara pipeline.',

    long_description=get_long_description(),
    long_description_content_type='text/markdown',

    url = 'https://github.com/hz-lschick/mara-write-avro',

    install_requires=[
        'fastavro>=0.23.4',
        'mara-db>=4.5.0',
        'mara-page>=1.5.1',
        'mara-pipelines>=3.0.0',
        'pandas>=1.0.4',
        'pandavro>=1.5.1'],

    extras_require={
        #'test': ['pytest', 'pytest_click'],
    },

    packages=find_packages(),

    author='Mara contributors',
    license='MIT',

    entry_points={},
    python_requires='>=3.6'
)
