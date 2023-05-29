#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [ ]

test_requirements = ['pytest>=3', ]

setup(
    author="Rick McGeer",
    author_email='rick.mcgeer@engageLively.com',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="A Python implementation of the Data Plane data structures, function, server, and client",
    install_requires=requirements,
    license="BSD license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='data_plane',
    name='data_plane',
    packages=find_packages(include=['data_plane', 'data_plane.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/rickmcgeer/data_plane',
    version='0.1.0',
    zip_safe=False,
)
