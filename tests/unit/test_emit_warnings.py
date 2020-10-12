"""Test emit_warnings"""
from therapy.query import emit_warnings


def test_emit_warnings():
    """Test that emit_warnings works correctly."""
    expected_warnings = {
        'nbsp': 'Query contains non breaking space characters.'
    }

    actual_warnings = emit_warnings('CISPLATIN')
    assert actual_warnings is None

    actual_warnings = emit_warnings('CIS\u00A0platin')
    assert expected_warnings == actual_warnings

    actual_warnings = emit_warnings('CIS&nbsp;platin')
    assert expected_warnings == actual_warnings

    actual_warnings = emit_warnings('CIS\xa0platin')
    assert expected_warnings == actual_warnings
