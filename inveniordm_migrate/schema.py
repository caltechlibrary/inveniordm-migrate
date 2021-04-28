# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 CERN.
#
# Invenio-RDM-Records is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""DataCite based Schema for Invenio RDM Records."""

from marshmallow import Schema, fields, missing, post_dump
from marshmallow_utils.fields import SanitizedUnicode


class PersonOrOrgSchema43(Schema):
    """Creator/contributor common schema for v4."""

    # PIDS-FIXME: need a more escalable solution for URIs
    URIS = {
        "orcid": "http://orcid.org/",
        "gnd": "http://d-nb.info/",  # PIDS-FIXME: is this correct?
        "ror": "https://ror.org/",
        "isni": "https://isni.org",
    }

    name = fields.Str(attribute="person_or_org.name")
    nameType = fields.Str(attribute="person_or_org.type")
    givenName = fields.Str(attribute="person_or_org.given_name")
    familyName = fields.Str(attribute="person_or_org.family_name")
    nameIdentifiers = fields.Method("get_name_identifiers")
    affiliations = fields.Method("get_affiliation")

    def get_name_identifiers(self, obj):
        """Get name identifier list."""
        serialized_identifiers = []
        identifiers = obj["person_or_org"].get("identifiers", [])

        for identifier in identifiers:
            scheme = identifier["scheme"]
            value = identifier["identifier"]
            uri = self.URIS.get(scheme)

            name_id = {
                "nameIdentifier": value,
                "nameIdentifierScheme": scheme.upper(),
            }

            if uri:
                name_id["nameIdentifier"] = uri + value
                name_id["schemeURI"] = uri

            serialized_identifiers.append(name_id)

        return serialized_identifiers

    def get_affiliation(self, obj):
        """Get affiliation list."""
        serialized_affiliations = []
        affiliations = obj.get("affiliations", [])

        for affiliation in affiliations:
            name = affiliation["name"]
            aff = {
                "name": name,
            }
            identifier = affiliation.get("identifiers")
            if identifier:
                # PIDS-FIXME: DataCite accepts only one, how to decide
                identifier = identifier[0]
                scheme = identifier["scheme"]
                id_value = identifier["identifier"]
                aff["affiliationIdentifier"] = id_value
                aff["affiliationIdentifierScheme"] = scheme.upper()
                uri = self.URIS.get(scheme)
                if uri:
                    aff["affiliationIdentifier"] = uri + id_value

            serialized_affiliations.append(aff)

        return serialized_affiliations

    @post_dump(pass_many=False)
    def capitalize_name_type(self, data, **kwargs):
        """Capitalize type."""
        if data.get("nameType"):
            data["nameType"] = data["nameType"].capitalize()

        return data


class CreatorSchema43(PersonOrOrgSchema43):
    """Creator schema for v4."""


class ContributorSchema43(PersonOrOrgSchema43):
    """Contributor schema for v43."""

    contributorType = fields.Str(attribute="role")

    @post_dump(pass_many=False)
    def capitalize_contributor_type(self, data, **kwargs):
        """Capitalize type."""
        if data.get("contributorType"):
            data["contributorType"] = data["contributorType"].capitalize()

        return data


class SubjectSchema43(Schema):
    """Subjects schema for v43."""

    subject = fields.Str(attribute="subject")
    valueURI = fields.Str(attribute="identifier")
    subjectScheme = fields.Str(attribute="scheme")


class RightSchema43(Schema):
    """Rights schema for v43."""

    rights = fields.Str(attribute="title")
    rightsIdentifierScheme = fields.Str(attribute="scheme")
    rightsIdentifier = fields.Str(attribute="identifier")
    rightsUri = fields.Str(attribute="link")


class FundingSchema43(Schema):
    """Funding schema for v43."""

    funderName = fields.Str(attribute="funder.name")
    funderIdentifier = fields.Str(attribute="funder.identifier")
    funderIdentifierType = fields.Str(attribute="funder.scheme")
    awardTitle = fields.Str(attribute="award.title")
    awardNumber = fields.Str(attribute="award.number")
    # PIDS-FIXME: URI should be processed depending on the schema
    awardURI = fields.Str(attribute="award.identifier")

    @post_dump(pass_many=False)
    def uppercase(self, data, **kwargs):
        """Upper case the type."""
        if data.get("funderIdentifierType"):
            upper_type = data["funderIdentifierType"].upper()
            data["funderIdentifierType"] = upper_type

        return data


class DataCite43Schema(Schema):
    """DataCite 4.3 Marshmallow Schema."""

    # PIDS-FIXME: What about versioning links and related ids
    types = fields.Method("get_type")
    titles = fields.Method("get_titles")
    creators = fields.List(
        fields.Nested(CreatorSchema43), attribute="metadata.creators"
    )
    contributors = fields.List(
        fields.Nested(ContributorSchema43), attribute="metadata.contributors"
    )
    publisher = fields.Str(attribute="metadata.publisher")
    publicationYear = fields.Method("get_publication_year")
    subjects = fields.List(
        fields.Nested(SubjectSchema43), attribute="metadata.subjects"
    )
    dates = fields.Method("get_dates")
    language = fields.Method("get_language")
    identifiers = fields.Method("get_identifiers")
    relatedIdentifiers = fields.Method("get_related_identifiers")
    sizes = fields.List(SanitizedUnicode(), attribute="metadata.sizes")
    formats = fields.List(SanitizedUnicode(), attribute="metadata.formats")
    version = SanitizedUnicode(attribute="metadata.version")
    rightsList = fields.List(fields.Nested(RightSchema43), attribute="metadata.rights")
    descriptions = fields.Method("get_descriptions")
    geoLocations = fields.Method("get_locations")
    fundingReferences = fields.List(
        fields.Nested(FundingSchema43), attribute="metadata.funding"
    )
    schemaVersion = fields.Constant("http://datacite.org/schema/kernel-4")

    def get_type(self, obj):
        """Get resource type."""
        resource_type = obj["metadata"]["resource_type"]

        return {
            "resourceTypeGeneral": "FIXME",
            "resourceType": "FIXME",
        }

    def get_titles(self, obj):
        """Get titles list."""
        metadata = obj["metadata"]

        titles = [{"title": metadata.get("title")}]
        additional_titles = metadata.get("additional_titles", [])

        for add_title in additional_titles:
            title = {"title": add_title.get("title")}
            type_ = add_title.get("type")
            if type_:
                title["titleType"] = type_.capitalize()
            lang = add_title.get("lang")
            if lang:
                title["lang"] = lang

            titles.append(title)

        return titles

    def get_publication_year(self, obj):
        """Get publication year from edtf date."""
        # PIDS-FIXME: Make the EDTFDateString somehow access the year?
        return missing

    def get_dates(self, obj):
        """Get dates."""
        dates = [{"date": obj["metadata"]["publication_date"], "dateType": "Issued"}]

        for date in obj["metadata"].get("dates", []):
            to_append = {"date": date["date"], "dateType": date["type"].capitalize()}
            desc = date.get("description")
            if desc:
                to_append["dateInformation"] = desc

            dates.append(to_append)

        return dates or missing

    def get_language(self, obj):
        """Get language."""
        metadata = obj["metadata"]
        languages = metadata.get("languages")

        if languages:
            # PIDS-FIXME: How to choose? the first?
            return languages[0]["id"]

        return missing

    def get_identifiers(self, obj):
        """Get identifiers list."""
        serialized_identifiers = []

        # Identifiers field
        metadata = obj["metadata"]
        identifiers = metadata.get("identifiers", [])
        for id_ in identifiers:
            serialized_identifiers.append(
                {"identifier": id_["identifier"], "identifierType": id_["scheme"]}
            )

        # PIDs field
        pids = obj["pids"]
        for scheme, id_ in pids.items():
            serialized_identifiers.append(
                {"identifier": id_["identifier"], "identifierType": scheme.upper()}
            )

        return serialized_identifiers or missing

    def get_related_identifiers(self, obj):
        """Get related identifiers."""
        # PIDS-FIXME: This might get much more complex depending on the id
        serialized_identifiers = []
        metadata = obj["metadata"]
        identifiers = metadata.get("related_identifiers", [])
        for rel_id in identifiers:
            serialized_identifiers.append(
                {
                    "relatedIdentifier": rel_id["identifier"],
                    "relatedIdentifierType": rel_id["scheme"].upper(),
                    "relationType": rel_id["relation_type"].capitalize(),
                    "resourceTypeGeneral": rel_id["resource_type"]["type"],
                }
            )

        return serialized_identifiers or missing

    def get_descriptions(self, obj):
        """Get titles list."""
        metadata = obj["metadata"]
        descriptions = []

        description = metadata.get("description")
        if description:
            descriptions.append(
                {"description": description, "descriptionType": "Abstract"}
            )

        additional_descriptions = metadata.get("additional_descriptions", [])
        for add_desc in additional_descriptions:
            description = {
                "description": add_desc["description"],
                "descriptionType": add_desc["type"].capitalize(),
            }

            lang = add_desc.get("lang")
            if lang:
                description["lang"] = lang

            descriptions.append(description)

        return descriptions or missing

    def get_locations(self, obj):
        """Get locations."""
        locations = []

        for location in obj["metadata"].get("locations", []):
            place = location.get("place")
            serialized_location = {}
            if place:
                serialized_location["geoLocationPlace"] = place
            geometry = location.get("geometry")
            if geometry:
                geo_type = geometry["type"]
                # PIDS-FIXME: Scalable enough?
                # PIDS-FIXME: Implement Box and Polygon serialization
                if geo_type == "Point":
                    serialized_location["geoLocationPoint"] = {
                        "pointLatitude": geometry["coordinates"][0],
                        "pointLongitude": geometry["coordinates"][1],
                    }

            locations.append(serialized_location)
        return locations or missing
