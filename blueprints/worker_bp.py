

from flask import Blueprint, jsonify, request

from services.normal_service import normalize_db

worker_bp = Blueprint("worker", __name__)

@worker_bp.route("/normal", methods=["POST"])
def normal_worker():
    a =normalize_db()
    return jsonify(a)

