"""csv combing plugin"""

import re
from collections.abc import Sequence
from csv import reader
from io import StringIO

from cmem_client.client import Client
from cmem_client.models.resource import Resource
from cmem_plugin_base.dataintegration.client import get_client
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.description import Icon, Plugin, PluginParameter
from cmem_plugin_base.dataintegration.entity import (
    Entities,
    Entity,
    EntityPath,
    EntitySchema,
)
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.ports import FixedNumberOfInputs, UnknownSchemaPort
from cmem_plugin_base.dataintegration.types import (
    BoolParameterType,
    IntParameterType,
    StringParameterType,
)


@Plugin(
    label="Combine CSV files",
    icon=Icon(file_name="lsicon--file-csv-outline.svg", package=__package__),
    plugin_id="combine-csv",
    description="Combine CSV files with the same structure to one dataset.",
    documentation="""Combines CSV files with the same structure to one dataset.
                     Files are identified by specifying a regex filter.""",
    parameters=[
        PluginParameter(
            param_type=StringParameterType(),
            name="delimiter",
            label="Delimiter",
            description="Delimiter in the input CSV files.",
            default_value=",",
        ),
        PluginParameter(
            param_type=StringParameterType(),
            name="quotechar",
            label="Quotechar",
            description="Quotechar in the input CSV files.",
            default_value='"',
        ),
        PluginParameter(
            param_type=StringParameterType(),
            name="regex",
            label="File name regex filter",
            description="Regular expression for filtering resources of the project.",
        ),
        PluginParameter(
            param_type=IntParameterType(),
            name="skip_lines",
            label="Skip rows",
            description="The number of rows to skip before the header row.",
            default_value=0,
        ),
        PluginParameter(
            param_type=BoolParameterType(),
            name="stop",
            label="Stop workflow if result is empty",
            description="Stop the workflow if no input files are found or all input files are "
            "empty.",
            default_value=True,
        ),
    ],
)
class CsvCombine(WorkflowPlugin):
    """Plugin to combine multiple csv files with same header."""

    def __init__(
        self,
        regex: str,
        delimiter: str = ",",
        quotechar: str = '"',
        skip_lines: int = 0,
        stop: bool = True,
    ) -> None:
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.regex = regex
        self.skip_lines = skip_lines
        self.stop = stop

        self.input_ports = FixedNumberOfInputs([])
        self.output_port = UnknownSchemaPort()

    def get_entities(self, client: Client, resources: list[Resource]) -> Entities:
        """Create and return Entities."""
        value_list = []
        entities = []
        header: list[str] = []
        processed = 0
        for resource in resources:
            self.log.info(f"adding file {resource.name}")
            csv_string = client.files.read(resource.get_id()).decode("utf-8")
            csv_list = list(
                reader(StringIO(csv_string), delimiter=self.delimiter, quotechar=self.quotechar)
            )
            if len(csv_list) < self.skip_lines + 1:
                self.log.warning(f"Header not found in file {resource.name}, skipping file.")
                continue
            file_header = [c.strip() for c in csv_list[self.skip_lines]]
            if not processed:
                header = file_header
            elif file_header != header:
                raise ValueError(f"Inconsistent headers (file {resource.name}).")
            processed += 1
            for row in csv_list[1 + self.skip_lines :]:
                strip = [c.strip() for c in row]
                value_list.append(strip)
            self.context.report.update(
                ExecutionReport(
                    entity_count=processed,
                    operation_desc="file processed" if processed == 1 else "files processed",
                )
            )
        value_list = [list(item) for item in {tuple(row) for row in value_list}]
        if not value_list:
            if self.stop:
                raise ValueError("No rows found in input files.")
            self.log.warning("No rows found in input files.")
            return Entities(entities=[], schema=EntitySchema(type_uri="", paths=[]))
        schema = EntitySchema(type_uri="urn:row", paths=[EntityPath(path=n) for n in header])
        for i, row in enumerate(value_list):
            entities.append(Entity(uri=f"urn:{i + 1}", values=[[v] for v in row]))
        return Entities(entities=entities, schema=schema)

    def execute(self, inputs: Sequence[Entities], context: ExecutionContext) -> Entities:  # noqa: ARG002
        """Execute plugin"""
        client = get_client(context)
        context.report.update(ExecutionReport(entity_count=0, operation_desc="files processed"))
        self.context = context
        resources = [
            resource
            for resource in client.files.values()
            if resource.name and re.match(rf"{self.regex}", resource.name)
        ]
        if not resources:
            if self.stop:
                raise ValueError("No input files found.")
            self.log.warning("No input files found.")
            return Entities(entities=[], schema=EntitySchema(type_uri="", paths=[]))
        return self.get_entities(client, resources)
