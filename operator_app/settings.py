# # testing new changes from abhsihek

"""
Django settings for operator_app project.
"""

from pathlib import Path
import os
from datetime import timedelta  # 🔥 NAYA CODE: Token expiry time set karne ke liye
from dotenv import load_dotenv  # 🔥 NAYA CODE: .env file load karne ke liye

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# 🔥 Load environment variables from .env file
load_dotenv(os.path.join(BASE_DIR, ".env"))

# SECURITY WARNING: keep the secret key used in production secret!
# Agar .env mein SECRET_KEY nahi hogi, toh ye dummy fallback use karega (Local devs ke liye)
SECRET_KEY = os.getenv("SECRET_KEY", "django-insecure-local-dummy-key-for-devs-only")

# SECURITY WARNING: don't run with debug turned on in production!
# Agar .env mein DEBUG=True hoga tabhi True hoga, warna default False rahega
DEBUG = os.getenv("DEBUG", "False") == "True"

# Hosts via env (local + cloud) - .env se aayega, warna default localhost rahega
ALLOWED_HOSTS = os.getenv("ALLOWED_HOSTS", "localhost,127.0.0.1").split(",")

# Application definition
INSTALLED_APPS = [
    "daphne",  # 🔥 WEBSOCKETS KE LIYE NAYA CODE: Isko hamesha sabse upar rakhna hai
    "channels",
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "corsheaders",
    "operator_app.apps.OperatorAppConfig",
    "rest_framework",
    "rest_framework_simplejwt",  # 🔥 NAYA CODE: JWT App add kiya hai
    "api",
    "django_extensions",
]

MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

# CORS
CORS_ALLOW_ALL_ORIGINS = True
CORS_ALLOW_HEADERS = [
    "accept",
    "accept-encoding",
    "authorization",
    "content-type",
    "dnt",
    "origin",
    "user-agent",
    "x-csrftoken",
    "x-requested-with",
    "cache-control",
    "pragma",
    "if-none-match",
    "if-modified-since",
    "ngrok-skip-browser-warning",
]
CORS_ALLOW_CREDENTIALS = True

# CSRF Trusted - 🔥 ADDED BOTH .34 AND .35 IPs TO PREVENT BLOCKS
# (Inko hardcode chhod sakte hain kyunki frontend URLs mostly standard hote hain)
CSRF_TRUSTED_ORIGINS = [
    "http://localhost",  # 🔥 NAYA ADD KIYA: Android Capacitor default origin
    "http://localhost",  # 🔥 NAYA ADD KIYA: Android Capacitor secure origin
    "http://localhost:3000",
    "http://localhost:3001",
    "http://192.168.0.34:3000",
    "http://192.168.0.34:3001",
    "http://192.168.0.34:8000",
    "capacitor://localhost",
    "https://unsickerly-unbeclouded-cherish.ngrok-free.dev",
    "https://atom-dashboard-ui.vercel.app",
    "https://atom-dashboard-99yh8cbss-atomones-projects.vercel.app",
    "https://atom-dashboard-be.onrender.com",
]

# REST Framework
REST_FRAMEWORK = {
    "DEFAULT_RENDERER_CLASSES": ("rest_framework.renderers.JSONRenderer",),
    "DEFAULT_PARSER_CLASSES": ("rest_framework.parsers.JSONParser",),
    "DEFAULT_AUTHENTICATION_CLASSES": (
        "rest_framework_simplejwt.authentication.JWTAuthentication",
    ),
    # 👇 Ye har API se +05:30 aur T hata dega!
    "DATETIME_FORMAT": "%Y-%m-%d %H:%M:%S",
}

# ==============================================================================
# JWT TOKEN SETTINGS
# ==============================================================================

# SIMPLE_JWT = {
#     "ACCESS_TOKEN_LIFETIME": timedelta(hours=12),
#     "REFRESH_TOKEN_LIFETIME": timedelta(days=7),
# }

ROOT_URLCONF = "operator_app.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
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

WSGI_APPLICATION = "operator_app.wsgi.application"

# ==============================================================================
# 🔥 WEBSOCKETS KE LIYE NAYA CODE: ASGI aur Redis setup
# ==============================================================================
ASGI_APPLICATION = "operator_app.asgi.application"

CHANNEL_LAYERS = {"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}}

# Database (external Postgres)
# 🔥 Yahan se .35 IP fallback hata diya hai! Ab naye developer ko localhost milega.
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.getenv("DB_NAME", "Atomone_local"),
        "USER": os.getenv("DB_USER", "postgres"),
        "PASSWORD": os.getenv("DB_PASSWORD", "postgres"),
        "HOST": os.getenv("DB_HOST", "localhost"),
        "PORT": os.getenv("DB_PORT", "5432"),
        "OPTIONS": {
            "options": "-c search_path=production,quality,machine_maintenance,tool_maintenance,live_data,master_data,public -c timezone=Asia/Kolkata"
        },
    },
    # 'sqlserver_db': {
    #     'ENGINE': 'mssql',
    #     'NAME': os.getenv('SQL_DB_NAME'),
    #     'USER': os.getenv('SQL_DB_USER'),
    #     'PASSWORD': os.getenv('SQL_DB_PASSWORD'),
    #     'HOST': os.getenv('SQL_DB_HOST'),
    #     'PORT': '',
    #     'OPTIONS': {
    #         'driver': 'ODBC Driver 17 for SQL Server',
    #         'extra_params': 'TrustServerCertificate=yes;',  # Kyunki SSMS me 'Trust Server Certificate' checked hai
    #     },
    # }
}

# Email
EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"
EMAIL_HOST = "smtp.gmail.com"
EMAIL_PORT = 587
EMAIL_USE_TLS = True

EMAIL_HOST_USER = os.getenv("EMAIL_HOST_USER", "alertsatomone@gmail.com")
EMAIL_HOST_PASSWORD = os.getenv("EMAIL_HOST_PASSWORD", "dqchgtaihqpiparn")
DEFAULT_FROM_EMAIL = EMAIL_HOST_USER

ALERT_EMAIL_RECIPIENTS = []

# Internationalization
LANGUAGE_CODE = "en-us"
TIME_ZONE = "Asia/Kolkata"
USE_I18N = True
USE_TZ = True

# Static files
STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
MEDIA_URL = "/media/"
MEDIA_ROOT = BASE_DIR / "media"

# Default primary key field type
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"


# HTTPS par chalane ke liye terminal command:
# py manage.py runsslserver 192.168.0.34:8000

# testing new changes from abhsihek
