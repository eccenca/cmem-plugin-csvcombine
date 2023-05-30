"""csv combing plugin"""
import re
from io import StringIO
from csv import reader
from cmem.cmempy.workspace.projects.resources import get_all_resources
from cmem.cmempy.workspace.projects.resources.resource import get_resource
from cmem_plugin_base.dataintegration.utils import setup_cmempy_user_access
from cmem_plugin_base.dataintegration.context import ExecutionContext
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.types import StringParameterType, IntParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.entity import (
    Entities,
    Entity,
    EntitySchema,
    EntityPath,
)


@Plugin(
    label="Combine CSV files",
    plugin_id="combine-csv",
    description="Combines CSV files with the same structure to one dataset..",
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

    def __init__(self, delimiter, quotechar, regex, skip_lines) -> None:
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.regex = regex
        self.skip_lines = skip_lines
        self.string_parameters = ["delimiter", "quotechar", "regex"]
        self.int_parameters = ["skip_lines"]

    def get_resources_list(self):
        """Returns a list with the resources"""
        # return [r for r in get_all_resources()
        # if re.match(r"{}".format(self.regex), r["name"])]
        return [r for r in get_all_resources() if re.match(rf"{self.regex}", r["name"])]

    def get_entities(self, data):
        """Creating and returns Entities."""
        value_list = []
        entities = []
        for i, resource in enumerate(data):
            self.log.info(f"adding file {resource['name']}")
            csv_string = get_resource(resource["project"],
                                      resource["name"]).decode("utf-8")
            csv_list = list(
                reader(
                    StringIO(csv_string),
                    delimiter=self.delimiter,
                    quotechar=self.quotechar,
                )
            )
            header = [c.strip() for c in csv_list[self.skip_lines]]
            if i == 0:
                hheader = header
            else:
                if header != hheader:
                    raise ValueError(f"inconsistent headers (file {resource['name']})")
            for resources in csv_list[1 + self.skip_lines:]:
                strip = [c.strip() for c in resources]
                # if s not in value_list: value_list.append(s)
                value_list.append(strip)
        value_list = \
            [list(item) for item in set(tuple(resource) for resource in value_list)]
        schema = EntitySchema(type_uri="urn:r",
                              paths=[EntityPath(path=n) for n in header])
        for i, resources in enumerate(value_list):
            entities.append(Entity(uri=f"urn:{i+1}", values=[[v] for v in resources]))
        return Entities(entities=entities, schema=schema)

    def process_inputs(self, inputs):
        """processes the inputs"""
        # accepts only one set of parametes
        paths = [e.path for e in inputs[0].schema.paths]
        values = [e[0] for e in list(inputs[0].entities)[0].values]
        self.log.info("Processing input parameters...")
        for path, value in zip(paths, values):
            self.log.info(f"Input parameter {path}: {value}")
            if path not in self.string_parameters + self.int_parameters:
                raise ValueError(f"Invalid parameter: {path}")
            if path in self.int_parameters:
                try:
                    self.__dict__[path] = int(value)
                except Exception as exc:
                    raise ValueError(f"Invalid integer value for parameter {path}")\
                        from exc
        self.log.info("Parameters OK:")
        for path in self.string_parameters + self.int_parameters:
            self.log.info(f"{path}: {self.__dict__[path]}")

    def execute(self, inputs=(), context: ExecutionContext = ExecutionContext()):
        setup_cmempy_user_access(context.user)
        if inputs:
            self.process_inputs(inputs)
        list_resources = self.get_resources_list()
        entities = self.get_entities(list_resources)
        return entities
