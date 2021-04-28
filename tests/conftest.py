# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 CERN.
# Copyright (C) 2019 Northwestern University.
# Copyright (C) 2021 TU Wien.
#
# Invenio-RDM-Records is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Pytest configuration.

See https://pytest-invenio.readthedocs.io/ for documentation on which test
fixtures are available.
"""

import pytest


@pytest.fixture(scope="function")
def full_record():
    """Full record data as dict coming from the external world."""
    return {
        "pids": {
            "doi": {
                "identifier": "10.5281/inveniordm.1234",
                "provider": "datacite",
                "client": "inveniordm",
            },
        },
        "metadata": {
            "resource_type": {"type": "publication", "subtype": "publication-article"},
            "creators": [
                {
                    "person_or_org": {
                        "name": "Nielsen, Lars Holm",
                        "type": "personal",
                        "given_name": "Lars Holm",
                        "family_name": "Nielsen",
                        "identifiers": [
                            {"scheme": "orcid", "identifier": "0000-0001-8135-3489"}
                        ],
                    },
                    "affiliations": [
                        {
                            "name": "CERN",
                            "identifiers": [
                                {
                                    "scheme": "ror",
                                    "identifier": "01ggx4157",
                                },
                                {
                                    "scheme": "isni",
                                    "identifier": "000000012156142X",
                                },
                            ],
                        }
                    ],
                }
            ],
            "title": "InvenioRDM",
            "additional_titles": [
                {
                    "title": "a research data management platform",
                    "type": "subtitle",
                    "lang": "eng",
                }
            ],
            "publisher": "InvenioRDM",
            "publication_date": "2018/2020-09",
            "subjects": [{"subject": "test", "identifier": "test", "scheme": "dewey"}],
            "contributors": [
                {
                    "person_or_org": {
                        "name": "Nielsen, Lars Holm",
                        "type": "personal",
                        "given_name": "Lars Holm",
                        "family_name": "Nielsen",
                        "identifiers": [
                            {"scheme": "orcid", "identifier": "0000-0001-8135-3489"}
                        ],
                    },
                    "role": "other",
                    "affiliations": [
                        {
                            "name": "CERN",
                            "identifiers": [
                                {
                                    "scheme": "ror",
                                    "identifier": "01ggx4157",
                                },
                                {
                                    "scheme": "isni",
                                    "identifier": "000000012156142X",
                                },
                            ],
                        }
                    ],
                }
            ],
            "dates": [{"date": "1939/1945", "type": "other", "description": "A date"}],
            "languages": [{"id": "da"}, {"id": "en"}],
            "identifiers": [{"identifier": "1924MNRAS..84..308E", "scheme": "bibcode"}],
            "related_identifiers": [
                {
                    "identifier": "10.1234/foo.bar",
                    "scheme": "doi",
                    "relation_type": "cites",
                    "resource_type": {"type": "dataset"},
                }
            ],
            "sizes": ["11 pages"],
            "formats": ["application/pdf"],
            "version": "v1.0",
            "rights": [
                {
                    "title": "Creative Commons Attribution 4.0 International",
                    "scheme": "spdx",
                    "identifier": "cc-by-4.0",
                    "link": "https://creativecommons.org/licenses/by/4.0/",
                }
            ],
            "description": "Test",
            "additional_descriptions": [
                {"description": "Bla bla bla", "type": "methods", "lang": "eng"}
            ],
            "locations": [
                {
                    "geometry": {
                        "type": "Point",
                        "coordinates": [-32.94682, -60.63932],
                    },
                    "place": "test location place",
                    "description": "test location description",
                    "identifiers": [
                        {"identifier": "12345abcde", "scheme": "wikidata"},
                        {"identifier": "12345abcde", "scheme": "geonames"},
                    ],
                }
            ],
            "funding": [
                {
                    "funder": {
                        "name": "European Commission",
                        "identifier": "1234",
                        "scheme": "ror",
                    },
                    "award": {
                        "title": "OpenAIRE",
                        "number": "246686",
                        "identifier": ".../246686",
                        "scheme": "openaire",
                    },
                }
            ],
            "references": [
                {
                    "reference": "Nielsen et al,..",
                    "identifier": "0000 0001 1456 7559",
                    "scheme": "isni",
                }
            ],
        },
        "ext": {
            "dwc": {
                "collectionCode": "abc",
                "collectionCode2": 1.1,
                "collectionCode3": True,
                "test": ["abc", 1, True],
            }
        },
        "provenance": {"created_by": {"user": "1"}, "on_behalf_of": {"user": "1"}},
        "access": {
            "record": "public",
            "files": "restricted",
            "embargo": {
                "active": True,
                "until": "2131-01-01",
                "reason": "Only for medical doctors.",
            },
        },
        "files": {
            "enabled": True,
            "total_size": 1114324524355,
            "count": 1,
            "bucket": "81983514-22e5-473a-b521-24254bd5e049",
            "default_preview": "big-dataset.zip",
            "order": ["big-dataset.zip"],
            "entries": {
                "big-dataset.zip": {
                    "checksum": "md5:234245234213421342",
                    "mimetype": "application/zip",
                    "size": 1114324524355,
                    "key": "big-dataset.zip",
                    "file_id": "445aaacd-9de1-41ab-af52-25ab6cb93df7",
                }
            },
            "meta": {"big-dataset.zip": {"description": "File containing the data."}},
        },
        "notes": ["Under investigation for copyright infringement."],
    }


@pytest.fixture(scope="function")
def minimal_record():
    """Minimal record data as dict coming from the external world."""
    return {
        "pids": {},
        "access": {
            "record": "public",
            "files": "public",
        },
        "files": {
            "enabled": False,  # Most tests don't care about files
        },
        "metadata": {
            "publication_date": "2020-06-01",
            "resource_type": {"type": "image", "subtype": "image-photo"},
            "creators": [
                {
                    "person_or_org": {
                        "family_name": "Brown",
                        "given_name": "Troy",
                        "type": "personal",
                    }
                },
                {
                    "person_or_org": {
                        "name": "Troy Inc.",
                        "type": "organizational",
                    },
                },
            ],
            "title": "A Romans story",
        },
    }


@pytest.fixture()
def parent(app, db):
    """A parent record."""
    # The parent record is not automatically created when using RDMRecord.
    return RDMParent.create({})
