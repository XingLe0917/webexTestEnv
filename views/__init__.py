from flask import Blueprint
from .metadata import route as metadata_route
from .view import route as view_route

main = Blueprint("main", __name__)

main.register_blueprint(metadata_route)
main.register_blueprint(view_route)


__all__ = ["main"]