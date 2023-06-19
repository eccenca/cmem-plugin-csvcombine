"""Plugin tests."""
from .utils import (
    TestExecutionContext,
)
from cmem.cmempy.workspace.projects.project import make_new_project, delete_project
from cmem.cmempy.workspace.projects.resources.resource import create_resource
from cmem_plugin_csvcombine.plugin_csvcombine import CsvCombine
from dataclasses import dataclass
import pytest


@pytest.fixture(name="project")
def _project():
    """fixture for project setup."""
    @dataclass
    class FixtureData:
        """Fixture Data for Tests"""

        project_name = "csv-combine-test-project"
        resource_one = "test-csv-one.csv"
        resource_two = "test-csv-two.csv"

    fixture = FixtureData()

    make_new_project(fixture.project_name)

    # Create the first dataset and upload the file as a resource
    with open(f'tests/fixture_dir/{fixture.resource_one}', "rb") as response_file:
        create_resource(
            project_name=fixture.project_name,
            resource_name=fixture.resource_one,
            file_resource=response_file,
            replace=True,
        )

    with open(f'tests/fixture_dir/{fixture.resource_two}', "rb") as response_file:
        create_resource(
            project_name=fixture.project_name,
            resource_name=fixture.resource_two,
            file_resource=response_file,
            replace=True,
        )

    yield fixture
    delete_project(fixture.project_name)


def test_execution(project):
    """Test plugin execution for Plain Kafka"""
    _ = project

    plugin = CsvCombine(
        delimiter=",",
        quotechar="\"",
        regex="test*.csv",
        skip_lines="0",
    )
    result = plugin.execute(inputs=[], context=TestExecutionContext())
    count = 0
    for item in result.entities:
        count += 1
        assert len(item.values) == len(result.schema.paths)
    # assert 25 items (with two duplicates) after combining two CSV files
    assert count == 25
