# -*- coding: utf-8 -*-

from setuptools import find_packages
from setuptools import setup

version = '0.4.2'
description = 'Malleefowl simplifies the usage of WPS and has some supporting WPS processes.'
long_description = (
    open('README.rst').read() + '\n' +
    open('AUTHORS.rst').read() + '\n' +
    open('CHANGES.rst').read()
)

reqs = [line.strip() for line in open('requirements/deploy.txt')]

classifiers = [
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
      license="Apache License v2.0",
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      test_suite='malleefowl',
      install_requires=reqs,
      # entry_points = {},
      )
