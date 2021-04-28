# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 CERN.
#
# Invenio-RDM-Records is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""DataCite Serializers for Invenio RDM Records."""

from flask_resources.serializers import MarshmallowJSONSerializer
from marshmallow import ValidationError
from marshmallow import Schema, INCLUDE

from .schema import DataCite43Schema


class DataCite43Translate(MarshmallowJSONSerializer):
    """Marshmallow based DataCite serializer for records."""

    def __init__(self, **options):
        """Constructor."""
        super().__init__(schema_cls=DataCite43Schema, **options)

    def dump_one(self, obj):
        """Dump the object with extra information."""
        obj["metadata"] = self._schema_cls().dump(obj)
        return obj

    def dump(self, obj):
        """Dump the object with extra information."""
        return self._schema_cls().dump(obj)

    def load(self, obj):
        """Load the object."""
        try:
            return self._schema_cls().load(obj, unknown=INCLUDE)
        except ValidationError as err:
            print("ERRORRRORRORRORRR")
            print(err.messages)
            print("Valid dataVVVVV")
            print(err.valid_data)
