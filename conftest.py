"""Pytest entry-point for the Image Task Runner.

A root-level conftest.py is enough to make pytest put the repository root
on ``sys.path`` (with the default ``prepend`` import mode). That is what
allows the test suite — which does ``import app.main`` directly — to run
with a plain ``pytest -q`` invocation from the repository root, as
documented in README.md.
"""
