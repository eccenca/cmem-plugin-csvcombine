"""Plugin tests."""
from cmem_plugin_csvcombine.plugin_csvcombine import CsvCombine


def test_execution():
    """Test plugin execution"""
    #entities = 100
    #values = 10

    plugin = CsvCombine(delimiter="'", quotechar='"', regex="", skip_lines=0)
    #result = plugin.execute()
    #for item in result.entities:
    #    assert len(item.values) == len(result.schema.paths)

