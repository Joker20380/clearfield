"""
Django settings for clearfield project.
Production-ready baseline for CLEARFIELD medical news pipeline.
"""

from pathlib import Path

from decouple import Csv, config
from dotenv import load_dotenv


# =============================================================================
# BASE
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent.parent

# .env рядом с manage.py
dotenv_path = BASE_DIR / ".env"
load_dotenv(dotenv_path, override=False)


# =============================================================================
# SECURITY
# =============================================================================

SECRET_KEY = config("SECRET_KEY")

DEBUG = config("DEBUG", default=False, cast=bool)

ALLOWED_HOSTS = config(
    "ALLOWED_HOSTS",
    default="127.0.0.1,localhost",
    cast=Csv(),
)

CSRF_TRUSTED_ORIGINS = config(
    "CSRF_TRUSTED_ORIGINS",
    default="",
    cast=Csv(),
)

# Если Django стоит за Nginx/Gunicorn и HTTPS терминируется на Nginx
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

# Включай эти параметры в .env только на рабочем HTTPS-домене
SECURE_SSL_REDIRECT = config("SECURE_SSL_REDIRECT", default=False, cast=bool)
SESSION_COOKIE_SECURE = config("SESSION_COOKIE_SECURE", default=False, cast=bool)
CSRF_COOKIE_SECURE = config("CSRF_COOKIE_SECURE", default=False, cast=bool)
SECURE_HSTS_SECONDS = config("SECURE_HSTS_SECONDS", default=0, cast=int)
SECURE_HSTS_INCLUDE_SUBDOMAINS = config(
    "SECURE_HSTS_INCLUDE_SUBDOMAINS",
    default=False,
    cast=bool,
)
SECURE_HSTS_PRELOAD = config(
    "SECURE_HSTS_PRELOAD",
    default=False,
    cast=bool,
)

X_FRAME_OPTIONS = "DENY"


# =============================================================================
# APPLICATIONS
# =============================================================================

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",

    # Local apps
    "intel.apps.IntelConfig",
]


# =============================================================================
# MIDDLEWARE
# =============================================================================

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",

    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",

    "django.middleware.csrf.CsrfViewMiddleware",

    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",

    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]


# =============================================================================
# URLS / WSGI
# =============================================================================

ROOT_URLCONF = "clearfield.urls"

WSGI_APPLICATION = "clearfield.wsgi.application"


# =============================================================================
# TEMPLATES
# =============================================================================

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [
            BASE_DIR / "templates",
        ],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",

                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]


# =============================================================================
# DATABASE
# =============================================================================

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": config("DATABASE_NAME"),
        "USER": config("DATABASE_USER"),
        "PASSWORD": config("DATABASE_PASSWORD"),
        "HOST": config("DATABASE_HOST", default="127.0.0.1"),
        "PORT": config("DATABASE_PORT", default="3306"),
        "CONN_MAX_AGE": config("DATABASE_CONN_MAX_AGE", default=0, cast=int),
        "OPTIONS": {
            "init_command": "SET sql_mode='STRICT_TRANS_TABLES'",
            "charset": "utf8mb4",
        },
    }
}


# =============================================================================
# PASSWORD VALIDATION
# =============================================================================

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]


# =============================================================================
# INTERNATIONALIZATION
# =============================================================================

LANGUAGE_CODE = config("LANGUAGE_CODE", default="ru-ru")

# Для сайта лаборатории в РФ логичнее Europe/Moscow.
# При необходимости меняется через .env.
TIME_ZONE = config("TIME_ZONE", default="Europe/Moscow")

USE_I18N = True
USE_TZ = True


# =============================================================================
# STATIC / MEDIA
# =============================================================================

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"

MEDIA_URL = "/media/"
MEDIA_ROOT = BASE_DIR / "media"


# =============================================================================
# DEFAULTS
# =============================================================================

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"


# =============================================================================
# CLEARFIELD PIPELINE SETTINGS
# =============================================================================

CLEARFIELD_DEFAULT_REGION = config("CLEARFIELD_DEFAULT_REGION", default="RU")
CLEARFIELD_DEFAULT_TOPIC = config("CLEARFIELD_DEFAULT_TOPIC", default="medicine")

CLEARFIELD_BRIEF_HOURS = config("CLEARFIELD_BRIEF_HOURS", default=72, cast=int)
CLEARFIELD_MIN_EVIDENCE = config("CLEARFIELD_MIN_EVIDENCE", default=2, cast=int)

CLEARFIELD_EXPORT_DIR = config(
    "CLEARFIELD_EXPORT_DIR",
    default=str(BASE_DIR / "exports"),
)


# =============================================================================
# OLLAMA / LLM
# =============================================================================

LLM_ENABLED = config("LLM_ENABLED", default=False, cast=bool)

OLLAMA_BASE_URL = config(
    "OLLAMA_BASE_URL",
    default="http://127.0.0.1:11434",
)

OLLAMA_MODEL = config(
    "OLLAMA_MODEL",
    default="qwen2.5:7b",
)

OLLAMA_TIMEOUT = config(
    "OLLAMA_TIMEOUT",
    default=240,
    cast=int,
)

OLLAMA_TEMPERATURE = config(
    "OLLAMA_TEMPERATURE",
    default=0.3,
    cast=float,
)

OLLAMA_TOP_P = config(
    "OLLAMA_TOP_P",
    default=0.9,
    cast=float,
)

OLLAMA_NUM_PREDICT = config(
    "OLLAMA_NUM_PREDICT",
    default=2500,
    cast=int,
)


# =============================================================================
# MEDICAL CONTENT SAFETY
# =============================================================================

MEDICAL_NEWS_DISCLAIMER = config(
    "MEDICAL_NEWS_DISCLAIMER",
    default=(
        "Материал носит информационный характер и не заменяет консультацию врача. "
        "Интерпретацию результатов анализов должен проводить специалист с учётом "
        "жалоб, анамнеза и других данных пациента."
    ),
)

MEDICAL_NEWS_DEFAULT_AUDIENCE = config(
    "MEDICAL_NEWS_DEFAULT_AUDIENCE",
    default="пациенты медицинской лаборатории",
)

MEDICAL_NEWS_DEFAULT_REGION_TEXT = config(
    "MEDICAL_NEWS_DEFAULT_REGION_TEXT",
    default="Владикавказ и Северная Осетия",
)


# =============================================================================
# LOGGING
# =============================================================================

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,

    "formatters": {
        "verbose": {
            "format": "[{asctime}] {levelname} {name}: {message}",
            "style": "{",
        },
        "simple": {
            "format": "{levelname}: {message}",
            "style": "{",
        },
    },

    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "simple",
        },
        "clearfield_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": LOG_DIR / "clearfield.log",
            "maxBytes": 5 * 1024 * 1024,
            "backupCount": 5,
            "formatter": "verbose",
            "encoding": "utf-8",
        },
        "llm_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": LOG_DIR / "llm.log",
            "maxBytes": 5 * 1024 * 1024,
            "backupCount": 5,
            "formatter": "verbose",
            "encoding": "utf-8",
        },
    },

    "loggers": {
        "django": {
            "handlers": ["console", "clearfield_file"],
            "level": config("DJANGO_LOG_LEVEL", default="INFO"),
            "propagate": True,
        },
        "intel": {
            "handlers": ["console", "clearfield_file"],
            "level": config("INTEL_LOG_LEVEL", default="INFO"),
            "propagate": False,
        },
        "intel.llm": {
            "handlers": ["console", "llm_file"],
            "level": config("LLM_LOG_LEVEL", default="INFO"),
            "propagate": False,
        },
    },
}
