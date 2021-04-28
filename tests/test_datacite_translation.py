# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 CERN.
# Copyright (C) 2021 Caltech.
#
# Invenio-RDM-Records is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Reverse transfer of DataCite JSON 4.3 to InvenioRDM metadata."""

import json

from inveniordm_migrate import DataCite43Translate


def test_datacite43_translation(full_record):
    """Test serializer to DayaCide 4.3 JSON"""
    input_data = {
        "types": {"resourceTypeGeneral": "Dataset", "resourceType": "Dataset"},
        "creators": [
            {
                "name": "Nielsen, Lars Holm",
                "nameType": "Personal",
                "givenName": "Lars Holm",
                "familyName": "Nielsen",
                "nameIdentifiers": [
                    {
                        "nameIdentifier": "http://orcid.org/0000-0001-8135-3489",
                        "nameIdentifierScheme": "ORCID",
                        "schemeURI": "http://orcid.org/",
                    }
                ],
                "affiliations": [
                    {
                        "name": "CERN",
                        "affiliationIdentifier": "https://ror.org/01ggx4157",
                        "affiliationIdentifierScheme": "ROR",
                    }
                ],
            }
        ],
        "titles": [
            {"title": "InvenioRDM"},
            {
                "title": "a research data management platform",
                "titleType": "Subtitle",
                "lang": "eng",
            },
        ],
        "publisher": "InvenioRDM",
        "publicationYear": "2018",
        "subjects": [{"subject": "test", "valueURI": "test", "subjectScheme": "dewey"}],
        "contributors": [
            {
                "name": "Nielsen, Lars Holm",
                "nameType": "Personal",
                "contributorType": "Other",
                "givenName": "Lars Holm",
                "familyName": "Nielsen",
                "nameIdentifiers": [
                    {
                        "nameIdentifier": "http://orcid.org/0000-0001-8135-3489",
                        "nameIdentifierScheme": "ORCID",
                        "schemeURI": "http://orcid.org/",
                    }
                ],
                "affiliations": [
                    {
                        "name": "CERN",
                        "affiliationIdentifier": "https://ror.org/01ggx4157",
                        "affiliationIdentifierScheme": "ROR",
                    }
                ],
            }
        ],
        "dates": [
            {"date": "2018/2020-09", "dateType": "Issued"},
            {"date": "1939/1945", "dateType": "Other", "dateInformation": "A date"},
        ],
        "language": "da",
        "identifiers": [
            {"identifier": "1924MNRAS..84..308E", "identifierType": "bibcode"},
            {"identifier": "10.5281/inveniordm.1234", "identifierType": "DOI"},
        ],
        "relatedIdentifiers": [
            {
                "relatedIdentifier": "10.1234/foo.bar",
                "relatedIdentifierType": "DOI",
                "relationType": "Cites",
                "resourceTypeGeneral": "dataset",
            }
        ],
        "sizes": ["11 pages"],
        "formats": ["application/pdf"],
        "version": "v1.0",
        "rightsList": [
            {
                "rights": "Creative Commons Attribution 4.0 International",
                "rightsIdentifierScheme": "spdx",
                "rightsIdentifier": "cc-by-4.0",
                "rightsUri": "https://creativecommons.org/licenses/by/4.0/",
            }
        ],
        "descriptions": [
            {"description": "Test", "descriptionType": "Abstract"},
            {"description": "Bla bla bla", "descriptionType": "Methods", "lang": "eng"},
        ],
        "geoLocations": [
            {
                "geoLocationPoint": {
                    "pointLatitude": -32.94682,
                    "pointLongitude": -60.63932,
                },
                "geoLocationPlace": "test location place",
            }
        ],
        "fundingReferences": [
            {
                "funderName": "European Commission",
                "funderIdentifier": "1234",
                "funderIdentifierType": "ROR",
                "awardTitle": "OpenAIRE",
                "awardNumber": "246686",
                "awardURI": ".../246686",
            }
        ],
        "schemaVersion": "http://datacite.org/schema/kernel-4",
    }

    translator = DataCite43Translate()
    record = translator.dump_one(input_data)

    assert record["metadata"] == full_record
