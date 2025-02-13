"""Plugin tests."""

from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from cmem.cmempy.workspace.projects.project import delete_project, make_new_project
from cmem.cmempy.workspace.projects.resources.resource import create_resource

from cmem_plugin_csvcombine.plugin_csvcombine import CsvCombine

from .utils import (
    TestExecutionContext,
)


@dataclass
class FixtureData:
    """Fixture Data for Tests"""

    project_name = "csv-combine-test-project"
    resource_one = "test-csv-one.csv"
    resource_two = "test-csv-two.csv"


@pytest.fixture(name="project")
def _project() -> Generator[FixtureData, Any, None]:
    """Fixture for project setup."""
    fixture = FixtureData()
    make_new_project(fixture.project_name)

    # Create the first dataset and upload the file as a resource
    with Path(f"tests/fixture_dir/{fixture.resource_one}").open("rb") as response_file:
        create_resource(
            project_name=fixture.project_name,
            resource_name=fixture.resource_one,
            file_resource=response_file,
            replace=True,
        )

    with Path(f"tests/fixture_dir/{fixture.resource_two}").open("rb") as response_file:
        create_resource(
            project_name=fixture.project_name,
            resource_name=fixture.resource_two,
            file_resource=response_file,
            replace=True,
        )

    yield fixture
    delete_project(fixture.project_name)


def test_execution(project: pytest.FixtureRequest) -> None:
    """Test plugin execution"""
    _ = project

    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="test*.csv",
        skip_lines=0,
    )
    result = plugin.execute(inputs=(), context=TestExecutionContext())
    count = 0
    for item in result.entities:
        count += 1
        assert len(item.values) == len(result.schema.paths)
    # assert 25 items (with two duplicates) after combining two CSV files
    assert count == 25  # noqa: PLR2004
