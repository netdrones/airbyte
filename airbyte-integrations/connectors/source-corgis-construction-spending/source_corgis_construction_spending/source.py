#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import json
from datetime import datetime
from typing import Dict, Generator

import pandas as pd
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
    SyncMode,
)
from airbyte_cdk.sources import Source


class SourceCorgisConstructionSpending(Source):
    def check(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the integration
            e.g: if a provided Stripe API token can be used to connect to the Stripe API.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.yaml file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            start_date = config.get("start_date")
            if start_date:
                datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")

            end_date = config.get("end_date")
            if end_date:
                datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")

            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except ValueError as e:
            logger.error(f"datetime parse error: {str(e)}")
            return AirbyteConnectionStatus(status=Status.FAILED, message=str(e))
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {str(e)}")

    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        """
        Returns an AirbyteCatalog representing the available streams and fields in this integration.
        For example, given valid credentials to a Postgres database,
        returns an Airbyte catalog where each postgres table is a stream, and each table column is a field.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.yaml file

        :return: AirbyteCatalog is an object describing a list of all available streams in this source.
            A stream is an AirbyteStream object that includes:
            - its stream name (or table name in the case of Postgres)
            - json_schema providing the specifications of expected schema for this stream (a list of columns described
            by their names and types)
        """
        streams = []
        stream_name = "CORGIS Construction Spending"
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "time.month": {
                    "type": "integer"
                },
                "time.month name": {
                    "type": "string"
                },
                "time.period": {
                    "type": "string"
                },
                "time.year": {
                    "type": "integer"
                },
                "annual.combined.amusement and recreation": {
                    "type": "integer"
                },
                "annual.combined.commercial": {
                    "type": "integer"
                },
                "annual.combined.communication": {
                    "type": "integer"
                },
                "annual.combined.conservation and development": {
                    "type": "integer"
                },
                "annual.combined.educational": {
                    "type": "integer"
                },
                "annual.combined.health care": {
                    "type": "integer"
                },
                "annual.combined.highway and street": {
                    "type": "integer"
                },
                "annual.combined.lodging": {
                    "type": "integer"
                },
                "annual.combined.manufacturing": {
                    "type": "integer"
                },
                "annual.combined.nonresidential": {
                    "type": "integer"
                },
                "annual.combined.office": {
                    "type": "integer"
                },
                "annual.combined.power": {
                    "type": "integer"
                },
                "annual.combined.public safety": {
                    "type": "integer"
                },
                "annual.combined.religious": {
                    "type": "integer"
                },
                "annual.combined.residential": {
                    "type": "integer"
                },
                "annual.combined.sewage and waste disposal": {
                    "type": "integer"
                },
                "annual.combined.total construction": {
                    "type": "integer"
                },
                "annual.combined.transportation": {
                    "type": "integer"
                },
                "annual.combined.water supply": {
                    "type": "integer"
                },
                "annual.private.amusement and recreation": {
                    "type": "integer"
                },
                "annual.private.commercial": {
                    "type": "integer"
                },
                "annual.private.communication": {
                    "type": "integer"
                },
                "annual.private.conservation and development": {
                    "type": "integer"
                },
                "annual.private.educational": {
                    "type": "integer"
                },
                "annual.private.health care": {
                    "type": "integer"
                },
                "annual.private.highway and street": {
                    "type": "integer"
                },
                "annual.private.lodging": {
                    "type": "integer"
                },
                "annual.private.manufacturing": {
                    "type": "integer"
                },
                "annual.private.nonresidential": {
                    "type": "integer"
                },
                "annual.private.office": {
                    "type": "integer"
                },
                "annual.private.power": {
                    "type": "integer"
                },
                "annual.private.public safety": {
                    "type": "integer"
                },
                "annual.private.religious": {
                    "type": "integer"
                },
                "annual.private.residential": {
                    "type": "integer"
                },
                "annual.private.sewage and waste disposal": {
                    "type": "integer"
                },
                "annual.private.total construction": {
                    "type": "integer"
                },
                "annual.private.transportation": {
                    "type": "integer"
                },
                "annual.private.water supply": {
                    "type": "integer"
                },
                "annual.public.amusement and recreation": {
                    "type": "integer"
                },
                "annual.public.commercial": {
                    "type": "integer"
                },
                "annual.public.communication": {
                    "type": "integer"
                },
                "annual.public.conservation and development": {
                    "type": "integer"
                },
                "annual.public.educational": {
                    "type": "integer"
                },
                "annual.public.health care": {
                    "type": "integer"
                },
                "annual.public.highway and street": {
                    "type": "integer"
                },
                "annual.public.lodging": {
                    "type": "integer"
                },
                "annual.public.manufacturing": {
                    "type": "integer"
                },
                "annual.public.nonresidential": {
                    "type": "integer"
                },
                "annual.public.office": {
                    "type": "integer"
                },
                "annual.public.power": {
                    "type": "integer"
                },
                "annual.public.public safety": {
                    "type": "integer"
                },
                "annual.public.religious": {
                    "type": "integer"
                },
                "annual.public.residential": {
                    "type": "integer"
                },
                "annual.public.sewage and waste disposal": {
                    "type": "integer"
                },
                "annual.public.total construction": {
                    "type": "integer"
                },
                "annual.public.transportation": {
                    "type": "integer"
                },
                "annual.public.water supply": {
                    "type": "integer"
                },
                "current.combined.amusement and recreation": {
                    "type": "integer"
                },
                "current.combined.commercial": {
                    "type": "integer"
                },
                "current.combined.communication": {
                    "type": "integer"
                },
                "current.combined.conservation and development": {
                    "type": "integer"
                },
                "current.combined.educational": {
                    "type": "integer"
                },
                "current.combined.health care": {
                    "type": "integer"
                },
                "current.combined.highway and street": {
                    "type": "integer"
                },
                "current.combined.lodging": {
                    "type": "integer"
                },
                "current.combined.manufacturing": {
                    "type": "integer"
                },
                "current.combined.nonresidential": {
                    "type": "integer"
                },
                "current.combined.office": {
                    "type": "integer"
                },
                "current.combined.power": {
                    "type": "integer"
                },
                "current.combined.public safety": {
                    "type": "integer"
                },
                "current.combined.religious": {
                    "type": "integer"
                },
                "current.combined.residential": {
                    "type": "integer"
                },
                "current.combined.sewage and waste disposal": {
                    "type": "integer"
                },
                "current.combined.total construction": {
                    "type": "integer"
                },
                "current.combined.transportation": {
                    "type": "integer"
                },
                "current.combined.water supply": {
                    "type": "integer"
                },
                "current.private.amusement and recreation": {
                    "type": "integer"
                },
                "current.private.commercial": {
                    "type": "integer"
                },
                "current.private.communication": {
                    "type": "integer"
                },
                "current.private.conservation and development": {
                    "type": "integer"
                },
                "current.private.educational": {
                    "type": "integer"
                },
                "current.private.health care": {
                    "type": "integer"
                },
                "current.private.highway and street": {
                    "type": "integer"
                },
                "current.private.lodging": {
                    "type": "integer"
                },
                "current.private.manufacturing": {
                    "type": "integer"
                },
                "current.private.nonresidential": {
                    "type": "integer"
                },
                "current.private.office": {
                    "type": "integer"
                },
                "current.private.power": {
                    "type": "integer"
                },
                "current.private.public safety": {
                    "type": "integer"
                },
                "current.private.religious": {
                    "type": "integer"
                },
                "current.private.residential": {
                    "type": "integer"
                },
                "current.private.sewage and waste disposal": {
                    "type": "integer"
                },
                "current.private.total construction": {
                    "type": "integer"
                },
                "current.private.transportation": {
                    "type": "integer"
                },
                "current.private.water supply": {
                    "type": "integer"
                },
                "current.public.amusement and recreation": {
                    "type": "integer"
                },
                "current.public.commercial": {
                    "type": "integer"
                },
                "current.public.communication": {
                    "type": "integer"
                },
                "current.public.conservation and development": {
                    "type": "integer"
                },
                "current.public.educational": {
                    "type": "integer"
                },
                "current.public.health care": {
                    "type": "integer"
                },
                "current.public.highway and street": {
                    "type": "integer"
                },
                "current.public.lodging": {
                    "type": "integer"
                },
                "current.public.manufacturing": {
                    "type": "integer"
                },
                "current.public.nonresidential": {
                    "type": "integer"
                },
                "current.public.office": {
                    "type": "integer"
                },
                "current.public.power": {
                    "type": "integer"
                },
                "current.public.public safety": {
                    "type": "integer"
                },
                "current.public.religious": {
                    "type": "integer"
                },
                "current.public.residential": {
                    "type": "integer"
                },
                "current.public.sewage and waste disposal": {
                    "type": "integer"
                },
                "current.public.total construction": {
                    "type": "integer"
                },
                "current.public.transportation": {
                    "type": "integer"
                },
                "current.public.water supply": {
                    "type": "integer"
                },
            },
        }

        streams.append(AirbyteStream(name=stream_name, json_schema=json_schema, supported_sync_modes=[SyncMode.full_refresh]))
        return AirbyteCatalog(streams=streams)

    def read(
            self, logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        """
        Returns a generator of the AirbyteMessages generated by reading the source with the given configuration,
        catalog, and state.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
            the properties of the spec.yaml file
        :param catalog: The input catalog is a ConfiguredAirbyteCatalog which is almost the same as AirbyteCatalog
            returned by discover(), but
        in addition, it's been configured in the UI! For each particular stream and field, there may have been provided
        with extra modifications such as: filtering streams and/or columns out, renaming some entities, etc
        :param state: When a Airbyte reads data from a source, it might need to keep a checkpoint cursor to resume
            replication in the future from that saved checkpoint.
            This is the object that is provided with state from previous runs and avoid replicating the entire set of
            data everytime.

        :return: A generator that produces a stream of AirbyteRecordMessage contained in AirbyteMessage object.
        """

        df = pd.read_csv('./construction_spending.csv')

        stream_name = "CORGIS Construction Spending"
        data = {}
        for index, row in df.iterrows():
            for col, val in zip(row.index, row.values):
                data[col] = val

        yield AirbyteMessage(
            type=Type.RECORD,
            record=AirbyteRecordMessage(stream=stream_name, data=data, emitted_at=int(datetime.now().timestamp()) * 1000),
        )
