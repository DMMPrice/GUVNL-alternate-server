from flask import Flask, jsonify, request, g
from flask_cors import CORS
from flasgger import Swagger
from datetime import datetime
import json

from utils.transaction_logger import log_transaction

from Routes.ProcurementOutputRoutes import mongoDemandOutput_bp
from Routes.DemandDataAdditionRoutes import demandAPI
from Routes.IEXDataAdditionRoutes import iexAPI
from Routes.PlantDataAddition import plantAPI
from Routes.transaction_api import transactionAPI

app = Flask(__name__)

# ---------- Swagger Config ----------
swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": 'apispec_1',
            "route": '/apispec_1.json',
            "rule_filter": lambda rule: True,  # include all endpoints
            "model_filter": lambda tag: True,  # include all models
        }
    ],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/docs/"  # Swagger UI at http://localhost:4000/docs/
}

swagger = Swagger(app, config=swagger_config)

# ---------- CORS ----------
CORS(app, resources={r"/*": {"origins": "*"}})

# ---------- Blueprints ----------
app.register_blueprint(mongoDemandOutput_bp, url_prefix="/procurement-output")
app.register_blueprint(demandAPI, url_prefix="/demand")
app.register_blueprint(iexAPI, url_prefix="/iex")
app.register_blueprint(plantAPI, url_prefix="/plant-consumption")
app.register_blueprint(transactionAPI, url_prefix="/transaction")


# ---------- Middleware Hooks ----------
@app.before_request
def before_request_logging():
    g.request_body = None
    try:
        if request.is_json:
            g.request_body = request.get_json(silent=True, force=True)
        elif request.form:
            g.request_body = request.form.to_dict()
    except Exception:
        g.request_body = None
    g.start_time = datetime.utcnow()


@app.after_request
def after_request_logging(response):
    try:
        endpoint = request.path
        method = request.method
        request_body = g.get("request_body")
        response_status = response.status_code

        try:
            response_body = json.loads(response.get_data(as_text=True))
        except Exception:
            response_body = {"raw": response.get_data(as_text=True)}

        log_transaction(
            endpoint=endpoint,
            method=method,
            request_body=request_body,
            request_headers=request.headers,
            response_status=response_status,
            response_body=response_body
        )
    except Exception as e:
        print(f"[Middleware Logger Error] {e}")
    return response


@app.route('/')
def hello_world():
    """Root endpoint
    ---
    tags:
      - General
    responses:
      200:
        description: Returns a welcome message
    """
    return jsonify({"message": "GUVNL Alternative Server is running!"})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000, threaded=True, debug=True)
