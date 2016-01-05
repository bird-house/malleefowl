# -*- coding: utf-8 -*-

from setuptools import find_packages
from setuptools import setup

version = '0.3.7'
description = 'Malleefowl simplifies the usage of WPS and has some supporting WPS processes.'
long_description = (
    open('README.rst').read() + '\n' +
    open('AUTHORS.rst').read() + '\n' +
    open('CHANGES.rst').read()
)

requires = [
    'PyWPS',
    'htmltmpl',
    'python-magic',
    'lxml',
    'owslib',
    'esgf-pyclient',
    'myproxyclient',
    'netCDF4',
    'pyYAML',
    'dispel4py',
    'python-swiftclient',
    'threddsclient',
    'pysolr',
    'nose',
    ]

classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: Atmospheric Science',
        ]

setup(name='malleefowl',
      version=version,
      description=description,
      long_description=long_description,
      classifiers=classifiers,
      keywords='wps pywps python malleefowl netcdf esgf',
      author='Birdhouse',
      url='https://github.com/bird-house/malleefowl',
      license = "Apache License v2.0",
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      test_suite='nose.collector',
      install_requires=requires,
      entry_points = {
          'console_scripts': [
              'wpsclient=malleefowl.wpsclient:main',
              'wpsfetch=malleefowl.fetch:main',
              ]}     
      ,
      )
