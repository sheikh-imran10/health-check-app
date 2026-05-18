from flask import Flask
from .config import config_map


def create_app(config_name: str = "development") -> Flask:
    app = Flask(__name__)
    app.config.from_object(config_map[config_name])

    from .routes import register_blueprints
    register_blueprints(app)

    return app
