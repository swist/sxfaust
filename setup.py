#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from setuptools.command.test import test as TestCommand
import sys
import os
import re

try:  # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError:  # for pip <= 9.0.3
    from pip.req import parse_requirements

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

with open('README.md') as readme_file:
    readme = readme_file.read()

install_reqs = parse_requirements('requirements.txt', session=False)


reqs = [str(ir.req) for ir in install_reqs]


test_requirements = [
    "pytest",
    "mock"
]


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errcode = pytest.main(self.test_args)
        sys.exit(errcode)


setup(
    name='sxfaust',
    version='v1.0',
    description="Faust for SX",
    long_description=readme,
    author="Matthias Wutte",
    author_email='matthias.wutte@smaxtec.com',
    url='https://github.com/wuttem/sxfaust',
    packages=[
        'sxfaust'
    ],
    package_dir={'sxfaust': 'sxfaust'},
    include_package_data=True,
    install_requires=reqs,
    zip_safe=False,
    keywords='sxfaust',
    test_suite='tests',
    tests_require=test_requirements,
    cmdclass={'test': PyTest},
)
