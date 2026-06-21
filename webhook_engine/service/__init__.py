"""Standalone HTTP microservice layer — optional, behind the ``service`` extra.

Stream services POST events to ``/v1/emit``; the engine fans them out
as signed webhooks to registered subscriptions.

Entry point: ``webhook-engine`` CLI (defined in pyproject.toml scripts).
"""
