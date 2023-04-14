from setuptools import setup, find_packages
from os import system

setup(
    name = 'datamesh_ingfw', 
    version='1.0', 
    packages=find_packages(),
)

excelToJsonConveterCmd = "pyinstaller excelToJsonConverter/excelToJsonConverter.py --specpath excelToJsonConverter --onefile --windowed"
system(excelToJsonConveterCmd)