# -*- coding: utf-8 -*-
#
# Copyright (C) 2019-2021 CERN.
# Copyright (C) 2019-2021 Northwestern University.
# Copyright (C)      2021 TU Wien.
#
# Invenio-RDM-Records is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Migrate DataCite metadata into InvenioRDM data model."""

import os

from setuptools import find_packages, setup


tests_require = [
    "pytest-invenio>=1.4.1,<2.0.0",
]


install_requires = [
    "datacite>=1.1.1",
    "invenio-drafts-resources>=0.11.5,<0.12.0",
    "invenio-vocabularies>=0.5.3,<0.6.0",
]

packages = find_packages()


setup(
    name="inveniordm-migrate",
    version="0.0.1",
    description=__doc__,
    keywords="invenio data model",
    license="MIT",
    author="Tom Morrell",
    author_email="tmorrell@caltech.edug",
    url="https://github.com/caltechlibrary/inveniordm-migrate",
    packages=packages,
    zip_safe=False,
    include_package_data=True,
    platforms="any",
    install_requires=install_requires,
    tests_require=tests_require,
    classifiers=[
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Development Status :: 3 - Alpha",
    ],
)
