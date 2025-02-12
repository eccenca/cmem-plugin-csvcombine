"""csv combing plugin"""

import re
from collections.abc import Sequence
from csv import reader
from io import StringIO

from cmem.cmempy.workspace.projects.resources import get_all_resources
from cmem.cmempy.workspace.projects.resources.resource import get_resource
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
from cmem_plugin_base.dataintegration.types import IntParameterType, StringParameterType
from cmem_plugin_base.dataintegration.utils import setup_cmempy_user_access


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
            advanced=True,
        ),
    ],
)
class CsvCombine(WorkflowPlugin):
    """Plugin to combine multiple csv files with same header."""

    def __init__(self, delimiter: str, quotechar: str, regex: str, skip_lines: int) -> None:
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.regex = regex
        self.skip_lines = skip_lines

        self.input_ports = FixedNumberOfInputs([])
        self.output_port = UnknownSchemaPort()

    def get_entities(self, resources: list) -> Entities:
        """Create and return Entities."""
        value_list = []
        entities = []
        header = []
        for i, resource in enumerate(resources):
            self.log.info(f"adding file {resource['name']}")
            csv_string = get_resource(resource["project"], resource["name"]).decode("utf-8")
            csv_list = list(
                reader(StringIO(csv_string), delimiter=self.delimiter, quotechar=self.quotechar)
            )
            header = [c.strip() for c in csv_list[self.skip_lines]]
            if i == 0:
                header_ = header
                operation_desc = "file processed"
            elif header != header_:
                raise ValueError(f"inconsistent headers (file {resource['name']})")
            else:
                operation_desc = "files processed"
            for rows in csv_list[1 + self.skip_lines :]:
                strip = [c.strip() for c in rows]
                value_list.append(strip)
            self.context.report.update(
                ExecutionReport(entity_count=i + 1, operation_desc=operation_desc)
            )
        value_list = [list(item) for item in {tuple(rows) for rows in value_list}]
        schema = EntitySchema(type_uri="urn:row", paths=[EntityPath(path=n) for n in header])
        for i, rows in enumerate(value_list):
            entities.append(Entity(uri=f"urn:{i + 1}", values=[[v] for v in rows]))
        return Entities(entities=entities, schema=schema)

    def execute(self, inputs: Sequence[Entities], context: ExecutionContext) -> Entities:  # noqa: ARG002
        """Execute plugin"""
        context.report.update(ExecutionReport(entity_count=0, operation_desc="files processed"))
        self.context = context
        setup_cmempy_user_access(context.user)
        resources = [r for r in get_all_resources() if re.match(rf"{self.regex}", r["name"])]
        return self.get_entities(resources)
