"""csv combing plugin"""

import re
from collections.abc import Sequence
from csv import reader
from io import StringIO

from cmem.cmempy.workspace.projects.resources import get_all_resources
from cmem.cmempy.workspace.projects.resources.resource import get_resource
from cmem_plugin_base.dataintegration.context import ExecutionContext
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.entity import (
    Entities,
    Entity,
    EntityPath,
    EntitySchema,
)
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.types import IntParameterType, StringParameterType
from cmem_plugin_base.dataintegration.utils import setup_cmempy_user_access


@Plugin(
    label="Combine CSV files",
    plugin_id="combine-csv",
    description="Combine CSV files with the same structure to one dataset.",
    documentation="""Combines CSV files with the same structure to one dataset.
                     Files are identified by specifying a regex filter.""",
    parameters=[
        PluginParameter(
            param_type=StringParameterType(),
            name="delimiter",
            label="Delimiter",
            description="Delimiter.",
            default_value=",",
        ),
        PluginParameter(
            param_type=StringParameterType(),
            name="quotechar",
            label="Quotechar",
            description="Quotechar.",
            default_value='"',
        ),
        PluginParameter(
            param_type=StringParameterType(),
            name="regex",
            label="File name regex filter",
            description="File name regex filter.",
        ),
        PluginParameter(
            param_type=IntParameterType(),
            name="skip_lines",
            label="Skip lines",
            description="The number of lines to skip in the beginning.",
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
        self.string_parameters = ["delimiter", "quotechar", "regex"]
        self.int_parameters = ["skip_lines"]

    def get_resources_list(self) -> list:
        """Return a list with the resources"""
        return [r for r in get_all_resources() if re.match(rf"{self.regex}", r["name"])]

    def get_entities(self, data: list) -> Entities:
        """Create and return Entities."""
        value_list = []
        entities = []
        header = []
        for i, row in enumerate(data):
            self.log.info(f"adding file {row['name']}")
            csv_string = get_resource(row["project"], row["name"]).decode("utf-8")
            csv_list = list(
                reader(
                    StringIO(csv_string),
                    delimiter=self.delimiter,
                    quotechar=self.quotechar,
                )
            )
            if i == 0:
                header = [c.strip() for c in csv_list[int(self.skip_lines)]]
                hheader = header
            elif header != hheader:
                raise ValueError(f"inconsistent headers (file {row['name']})")
            for rows in csv_list[1 + int(self.skip_lines) :]:
                strip = [c.strip() for c in rows]
                value_list.append(strip)
        value_list = [list(item) for item in set(tuple(rows) for rows in value_list)]  # noqa: C401
        schema = EntitySchema(type_uri="urn:row", paths=[EntityPath(path=n) for n in header])
        for i, rows in enumerate(value_list):
            entities.append(Entity(uri=f"urn:{i + 1}", values=[[v] for v in rows]))
        return Entities(entities=entities, schema=schema)

    def process_inputs(self, inputs: Sequence[Entities]) -> None:
        """Process the inputs"""
        # accepts only one set of parametes
        paths = [e.path for e in inputs[0].schema.paths]
        values = [e[0] for e in list(inputs[0].entities)[0].values]  # noqa: RUF015
        self.log.info("Processing input parameters...")
        for path, value in zip(paths, values, strict=False):
            self.log.info(f"Input parameter {path}: {value}")
            if path not in self.string_parameters + self.int_parameters:
                raise ValueError(f"Invalid parameter: {path}")
            if path in self.int_parameters:
                try:
                    self.__dict__[path] = int(value)
                except TypeError as exc:
                    raise ValueError(f"Invalid integer value for parameter {path}") from exc
        self.log.info("Parameters OK:")
        for path in self.string_parameters + self.int_parameters:
            self.log.info(f"{path}: {self.__dict__[path]}")

    def execute(self, inputs: Sequence[Entities], context: ExecutionContext) -> Entities:
        """Execute plugin"""
        setup_cmempy_user_access(context.user)
        if inputs:
            self.process_inputs(inputs)
        list_resources = self.get_resources_list()
        return self.get_entities(list_resources)
