"""Plugin tests."""

import unittest
from unittest.mock import Mock
from io import StringIO
from csv import writer

from cmem_plugin_csvcombine.plugin_csvcombine import CsvCombine


class TestCsvCombine(unittest.TestCase):
    def setUp(self):
        self.plugin = CsvCombine(delimiter=",", quotechar='"', regex=r".*\.csv", skip_lines=1)
        self.plugin.log = Mock()

    # def test_get_resources_list(self):
    #    self.plugin.get_all_resources = Mock(return_value=[
    #        {"project": "cmem-project", "name": "data1.csv"},
    #        {"project": "cmem-project", "name": "data2.csv"},
    #        {"project": "cmem-project", "name": "other.txt"},
    #    ])
    #    expected = [
    #        {"project": "cmem-project", "name": "data1.csv"},
    #        {"project": "cmem-project", "name": "data2.csv"},
    #    ]
    #    result = self.plugin.get_resources_list()
    #    self.assertEqual(result, expected)

    def test_get_entities(self):
        data = [
            {"project": "cmem-project", "name": "data1.csv"},
            {"project": "cmem-project", "name": "data2.csv"},
        ]
        # Create test data
        csv1 = StringIO()
        writer(csv1, delimiter=",", quotechar='"').writerows([
            ["name", "age", "gender"],
            ["Alice", "25", "F"],
            ["Bob", "35", "M"],
        ])
        csv2 = StringIO()
        writer(csv2, delimiter=",", quotechar='"').writerows([
            ["name", "age", "gender"],
            ["Charlie", "40", "M"],
            ["Dave", "30", "M"],
        ])
        self.plugin.get_resource = Mock(side_effect=[
            csv1.getvalue(),
            csv2.getvalue(),
        ])
        expected_entities = [
            {
                "uri": "urn:1",
                "values": [
                    ["A"],
                    ["25"],
                    ["F"],
                ],
            },
            {
                "uri": "urn:2",
                "values": [
                    ["B"],
                    ["35"],
                    ["M"],
                ],
            },
            {
                "uri": "urn:3",
                "values": [
                    ["C"],
                    ["40"],
                    ["M"],
                ],
            },
            {
                "uri": "urn:4",
                "values": [
                    ["D"],
                    ["30"],
                    ["M"],
                ],
            },
        ]
        expected_schema = {
            "type_uri": "urn:r",
            "paths": [
                {"path": "name"},
                {"path": "age"},
                {"path": "gender"},
            ],
        }
        result = self.plugin.get_entities(data)
        self.assertEqual(result.entities, expected_entities)
        self.assertEqual(result.schema.to_dict(), expected_schema)

    # def test_execute(self):
    #    self.plugin.get_resources_list = MagicMock(return_value=[
    #        {"project": "cmem-project", "name": "data1.csv"},
    #        {"project": "cmem-project", "name": "data2.csv"},
    #    ])
    #    self.plugin.get_entities = Mock(return_value="test_entities")
    #    result = self.plugin.execute(inputs=[],
    #                                 context={"OAUTH_GRANT_TYPE": "prefetched_token",
    #                                          "OAUTH_ACCESS_TOKEN": "my_token"})
    #    self.assertEqual(result, "test_entities")
    #    self.plugin.get_resources_list.assert_called_once()
    #    self.plugin.get_entities.assert_called_once_with([
    #        {"project": "cmem-project", "name": "data1.csv"},
    #        {"project": "cmem-project", "name": "data2.csv"},
    #    ])
