import re
from cmem.cmempy.workspace.projects.resources import get_all_resources
from cmem.cmempy.workspace.projects.resources.resource import get_resource
from cmem_plugin_base.dataintegration.utils import setup_cmempy_super_user_access
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.types import StringParameterType, IntParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.entity import Entities, Entity, EntitySchema, EntityPath


@Plugin(
    label="Combine CSV files",
    plugin_id="combine-csv",
    description="Combines CSV files of the same type",
    documentation="""TBD""",
    parameters=[
        PluginParameter(
            param_type = StringParameterType(),
            name="delimiter",
            label="Delimiter",
            description="Delimiter.",
            default_value=","
        ),
        PluginParameter(
            param_type = StringParameterType(),
            name="quotechar",
            label="Quotechar",
            description="Quotechar.",
            default_value='"'
        ),
        PluginParameter(
            param_type = StringParameterType(),
            name="regex",
            label="File name regex filter",
            description="File name regex filter."
        ),
        PluginParameter(
            param_type = IntParameterType(),
            name="skip_lines",
            label="Skip lines",
            description="The number of lines to skip in the beginning.",
            default_value=0,
            advanced=True
        )
    ]
)


class CsvCombine(WorkflowPlugin):

    def __init__(
        self,
        delimiter,
        quotechar,
        regex,
        skip_lines
    ) -> None:
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.regex = regex
        self.skip_lines = skip_lines
        setup_cmempy_super_user_access()

    def get_resources_list(self):
        return [r for r in get_all_resources() if re.match(r"{}".format(self.regex), r["name"])]

    def get_entities(self, d):
        value_list = []
        entities = []
        for i, r in enumerate(d):
            self.log.info(f"adding file {r['name']}")
            b = get_resource(r["project"], r["name"]).decode("utf-8")
            h = [c.strip() for c in b.split("\n")[self.skip_lines].split(self.delimiter)]
            if i == 0:
                hh = h
            else:
                if h != hh:
                    raise ValueError(f"inconsistent headers (file {r['name']})")
            for row in b.split("\n")[self.skip_lines+1:-1]:
                s = [c.strip(self.quotechar) for c in row.split(self.delimiter)]
                #if s not in value_list: value_list.append(s)
                value_list.append(s)
        value_list = [list(item) for item in set(tuple(row) for row in value_list)]
        schema = EntitySchema(
                type_uri="urn:row",
                paths=[EntityPath(path=n) for n in h]
            )
        for i, row in enumerate(value_list):
            entities.append(
                    Entity(
                        uri=f"urn:{i+1}",
                        values=[[v] for v in row]
                    )
                )
        return Entities(entities=entities, schema=schema)


    def execute(self, inputs=()):
        r = self.get_resources_list()
        entities = self.get_entities(r)
        return entities
