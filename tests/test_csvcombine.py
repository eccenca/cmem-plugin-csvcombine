"""Plugin tests."""

from collections.abc import Generator
from contextlib import suppress
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

    project_name = "csv-combine-test-cb47a2a5a5d34fcdaef634f6f2a0ea39"
    resource_one = "test-csv-one.csv"
    resource_two = "test-csv-two.csv"
    resource_three = "test-csv-three-wrong-header.csv"
    resource_empty_one = "test-empty-csv-one.csv"
    resource_empty_two = "test-empty-csv-two.csv"
    resource_header_one = "test-header-csv-one.csv"
    resource_header_two = "test-header-csv-two.csv"


@pytest.fixture
def setup1() -> Generator[FixtureData, Any, None]:
    """Fixture for project test_execution"""
    fixture = FixtureData()
    with suppress(Exception):
        delete_project(fixture.project_name)
    make_new_project(fixture.project_name)
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


@pytest.fixture
def setup2() -> Generator[FixtureData, Any, None]:
    """Fixture for test_execution_wrong_header"""
    fixture = FixtureData()
    with suppress(Exception):
        delete_project(fixture.project_name)
    make_new_project(fixture.project_name)
    with Path(f"tests/fixture_dir/{fixture.resource_one}").open("rb") as response_file:
        create_resource(
            project_name=fixture.project_name,
            resource_name=fixture.resource_one,
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
    yield fixture
    delete_project(fixture.project_name)


@pytest.fixture
def setup3() -> Generator[FixtureData, Any, None]:
    """Fixture for test_execution_no_files, test_execution_no_files_stop"""
    fixture = FixtureData()
    with suppress(Exception):
        delete_project(fixture.project_name)
    make_new_project(fixture.project_name)
    yield fixture
    delete_project(fixture.project_name)


@pytest.fixture
def setup4() -> Generator[FixtureData, Any, None]:
    """Fixture for test_execution_empty_files_no_header,
    test_execution_empty_files_no_header_stop
    """
    fixture = FixtureData()
    with suppress(Exception):
        delete_project(fixture.project_name)
    make_new_project(fixture.project_name)
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
    yield fixture
    delete_project(fixture.project_name)


@pytest.fixture
def setup5() -> Generator[FixtureData, Any, None]:
    """Fixture for test_execution_empty_files_header, test_execution_empty_files_header_stop"""
    fixture = FixtureData()
    with suppress(Exception):
        delete_project(fixture.project_name)
    make_new_project(fixture.project_name)
    create_resource(
        project_name=fixture.project_name,
        resource_name=fixture.resource_header_one,
        file_resource=BytesIO(b"first_name,last_name\n"),
        replace=True,
    )
    create_resource(
        project_name=fixture.project_name,
        resource_name=fixture.resource_header_two,
        file_resource=BytesIO(b"first_name,last_name\n"),
        replace=True,
    )
    yield fixture
    delete_project(fixture.project_name)


def test_execution(setup1: FixtureData) -> None:
    """Test plugin execution"""
    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="^test-csv.{4}\\.csv$",
        skip_lines=0,
    )
    result = plugin.execute(inputs=(), context=TestExecutionContext(setup1.project_name))
    count = 0
    for item in result.entities:
        count += 1
        assert len(item.values) == len(result.schema.paths)
    # assert 25 items (with two duplicates) after combining two CSV files
    assert count == 25  # noqa: PLR2004


def test_execution_wrong_header(setup2: FixtureData) -> None:
    """Test plugin execution"""
    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="^test-csv.+\\.csv$",
        skip_lines=0,
    )
    with pytest.raises(ValueError, match="Inconsistent headers"):
        plugin.execute(inputs=(), context=TestExecutionContext(setup2.project_name))


def test_execution_no_files(setup3: FixtureData) -> None:
    """Test plugin execution"""
    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="none",
        skip_lines=0,
        stop=False,
    )
    result = plugin.execute(inputs=(), context=TestExecutionContext(setup3.project_name))
    assert len(list(result.entities)) == 0


def test_execution_no_files_stop(setup3: FixtureData) -> None:
    """Test plugin execution"""
    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="none",
        skip_lines=0,
    )
    with pytest.raises(ValueError, match="No input files found."):
        plugin.execute(inputs=(), context=TestExecutionContext(setup3.project_name))


def test_execution_empty_files_no_header(setup4: FixtureData) -> None:
    """Test plugin execution"""
    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="^test-empty-csv.{4}\\.csv$",
        skip_lines=0,
        stop=False,
    )
    result = plugin.execute(inputs=(), context=TestExecutionContext(setup4.project_name))
    assert len(list(result.entities)) == 0


def test_execution_empty_files_no_header_stop(setup4: FixtureData) -> None:
    """Test plugin execution"""
    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="^test-empty-csv.{4}\\.csv$",
        skip_lines=0,
    )
    with pytest.raises(ValueError, match="No rows found in input files."):
        plugin.execute(inputs=(), context=TestExecutionContext(setup4.project_name))


def test_execution_empty_files_header(setup5: FixtureData) -> None:
    """Test plugin execution"""
    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="^test-header-csv.{4}\\.csv$",
        skip_lines=0,
        stop=False,
    )
    result = plugin.execute(inputs=(), context=TestExecutionContext(setup5.project_name))
    assert len(list(result.entities)) == 0


def test_execution_empty_files_header_stop(setup5: FixtureData) -> None:
    """Test plugin execution"""
    plugin = CsvCombine(
        delimiter=",",
        quotechar='"',
        regex="^test-header-csv.{4}\\.csv$",
        skip_lines=0,
    )
    with pytest.raises(ValueError, match="No rows found in input files."):
        plugin.execute(inputs=(), context=TestExecutionContext(setup5.project_name))
