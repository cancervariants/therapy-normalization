"""Perform basic schema-level tests."""

from therapy.schemas import NAMESPACE_TO_SYSTEM_URI, NamespacePrefix


def test_namespace_to_system_uri():
    """Check that all namespace prefixes have been mapped to system URIs."""
    for namespace_prefix in NamespacePrefix:
        assert namespace_prefix in NAMESPACE_TO_SYSTEM_URI
