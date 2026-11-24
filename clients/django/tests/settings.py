"""Django settings for the tests. Each test sets TASKS to a backend on a fake server
(tests/conftest.py); this one only lets the task modules import."""

SECRET_KEY = "tests"
USE_TZ = True
INSTALLED_APPS = ["lessormq_django", "tests"]
DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}}
TASKS = {
    "default": {
        "BACKEND": "lessormq_django.LessorMQBackend",
        "QUEUES": ["default", "mail"],
        "OPTIONS": {"URL": "https://mq.test", "API_KEY": "k"},
    }
}
