import os


class BaseConfig:
    SECRET_KEY = os.getenv("SECRET_KEY", "change-me-in-production")
    JSON_SORT_KEYS = False


class DevelopmentConfig(BaseConfig):
    DEBUG = True


class TestingConfig(BaseConfig):
    TESTING = True
    DEBUG = True


class ProductionConfig(BaseConfig):
    DEBUG = False


config_map = {
    "development": DevelopmentConfig,
    "test": TestingConfig,
    "production": ProductionConfig,
}
