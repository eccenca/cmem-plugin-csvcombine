"""Plugin tests."""

from collections.abc import Generator
from dataclasses import dataclass
from io import BytesIO
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
    resource_three = "test-csv-three-wrong-header.csv"
    resource_empty_one = "test-empty-csv-one.csv"
    resource_empty_two = "test-empty-csv-two.csv"
    resource_header_one = "test-header-csv-one.csv"
    resource_header_two = "test-header-csv-two.csv"


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

    with Path(f"tests/fixture_dir/{fixture.resource_three}").open("rb") as response_file:
        create_resource(
            project_name=fixture.project_name,
            resource_name=fixture.resource_three,
            file_resource=response_file,
            replace=True,
        )

    create_resource(
        project_name=fixture.project_name,
        resource_name=fixture.resource_empty_one,
        file_resource=BytesIO(b""),
        replace=True,
    )

    create_resource(
        project_name=fixture.project_name,
        resource_name=fixture.resource_empty_one,
        file_resource=BytesIO(b""),
        replace=True,
    )

    create_resource(
        project_name=fixture.project_name,
        resource_name=fixture.resource_empty_two,
        file_resource=BytesIO(b""),
        replace=True,
    )

    create_resource(
        project_name=fixture.project_name,
        resource_name=fixture.resource_header_one,
        file_resource=BytesIO(b"first_name,last_name\n"),
        replace=True,
    )

    yield fixture
    delete_project(fixture.project_name)


def test_execution(project: pytest.FixtureRequest) -> None:  # noqa: ARG001
    """Test plugin execution"""
    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="^test-csv.{4}\\.csv$",
        skip_lines=0,
    )
    result = plugin.execute(inputs=(), context=TestExecutionContext())
    count = 0
    for item in result.entities:
        count += 1
        assert len(item.values) == len(result.schema.paths)
    # assert 25 items (with two duplicates) after combining two CSV files
    assert count == 25  # noqa: PLR2004


def test_execution_wrong_header(project: pytest.FixtureRequest) -> None:  # noqa: ARG001
    """Test plugin execution"""
    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="^test-csv.+\\.csv$",
        skip_lines=0,
    )
    with pytest.raises(ValueError, match="Inconsistent headers"):
        plugin.execute(inputs=(), context=TestExecutionContext())


def test_execution_no_files() -> None:
    """Test plugin execution"""
    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="none",
        skip_lines=0,
        stop=False,
    )
    result = plugin.execute(inputs=(), context=TestExecutionContext())
    assert len(list(result.entities)) == 0


def test_execution_no_files_stop() -> None:
    """Test plugin execution"""
    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="none",
        skip_lines=0,
    )
    with pytest.raises(ValueError, match="No input files found."):
        plugin.execute(inputs=(), context=TestExecutionContext())


def test_execution_empty_files_no_header(project: pytest.FixtureRequest) -> None:  # noqa: ARG001
    """Test plugin execution"""
    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="^test-empty-csv.{4}\\.csv$",
        skip_lines=0,
        stop=False,
    )
    result = plugin.execute(inputs=(), context=TestExecutionContext())
    assert len(list(result.entities)) == 0


def test_execution_empty_files_no_header_stop(project: pytest.FixtureRequest) -> None:  # noqa: ARG001
    """Test plugin execution"""
    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="^test-empty-csv.{4}\\.csv$",
        skip_lines=0,
    )
    with pytest.raises(ValueError, match="No rows found in input files."):
        plugin.execute(inputs=(), context=TestExecutionContext())


def test_execution_empty_files_header(project: pytest.FixtureRequest) -> None:  # noqa: ARG001
    """Test plugin execution"""
    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="^test-header-csv.{4}\\.csv$",
        skip_lines=0,
        stop=False,
    )
    result = plugin.execute(inputs=(), context=TestExecutionContext())
    assert len(list(result.entities)) == 0


def test_execution_empty_files_header_stop(project: pytest.FixtureRequest) -> None:  # noqa: ARG001
    """Test plugin execution"""
    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="^test-header-csv.{4}\\.csv$",
        skip_lines=0,
    )
    with pytest.raises(ValueError, match="No rows found in input files."):
        plugin.execute(inputs=(), context=TestExecutionContext())
