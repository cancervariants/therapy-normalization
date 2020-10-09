"""Test emit_warnings"""
from therapy.query import emit_warnings


def test_emit_warnings():
    """Test that emit_warnings works correctly."""
    expected_warnings = {
        'nbsp': 'Query contains non breaking space characters.'
    }

    actual_warnings = emit_warnings('    \u0020\xa0CISPLATIN    \xa0')
    assert expected_warnings == actual_warnings

    actual_warnings = emit_warnings('\u0020\xa0\u00A0CISplatin\xa0 \t')
    assert expected_warnings == actual_warnings

    actual_warnings = emit_warnings('\u0020CIS\u00A0platin\xa0')
    assert expected_warnings == actual_warnings

    actual_warnings = emit_warnings('\u0020CIS&nbsp;platin\xa0')
    assert expected_warnings == actual_warnings

    actual_warnings = emit_warnings('\u0020CIS\xa0platin\xa0')
    assert expected_warnings == actual_warnings
