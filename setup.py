from setuptools import setup, find_packages

setup(name='cronus',
      version='0.1.0',
      author='Ryan White',
      author_email='ryan.white4@canada.ca',
      packages=find_packages(),
      install_requires=[
            "protobuf",
            "pyarrow>=0.13",
            "simplekv>=0.12",
            "storefact",
            "dask[delayed]",
            "dask[distributed]"
            ],
      description="Metastore for Artemis")
