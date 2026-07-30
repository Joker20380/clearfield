"""Isolated settings for the automated test suite.

Production uses MySQL, whose restricted hosting account cannot create a
``test_*`` database. Tests use an in-memory SQLite database instead and never
touch production data.
"""

from .settings import *  # noqa: F403


DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    },
}

PASSWORD_HASHERS = [
    "django.contrib.auth.hashers.MD5PasswordHasher",
]
