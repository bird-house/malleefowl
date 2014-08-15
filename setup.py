import os

from setuptools import find_packages
from setuptools import setup

version = '0.1.0'
description = 'PyWPS processes to support climate data processing with WPS.'
long_description = (
    open('README.rst').read() + '\n' +
    open('AUTHORS.rst').read() + '\n' +
    open('CHANGES.rst').read()
)

requires = [
    'PyWPS',
    'htmltmpl',
    'python-magic',
    'ordereddict',
    'lxml',
    'owslib',
    'pyOpenSSL',
    'netCDF4',
    'pyYAML',
    'ConcurrentLogHandler',
#    'dateutil',
    'nose',
    ]

classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: Atmospheric Science',
        ]

setup(name='Malleefowl',
      version=version,
      description=description,
      long_description=long_description,
      classifiers=classifiers,
      author='Carsten Ehbrecht',
      author_email='ehbrecht@dkrz.de',
      url='http://www.dkrz.de',
      license = "Apache License v2.0",
      keywords='pywps python malleefowl netcdf esgf',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      test_suite='nose.collector',
      install_requires=requires,
      entry_points = {
          'console_scripts': [
              'wpsclient=malleefowl.wpsclient:main',
              ]}     
      ,
      )
