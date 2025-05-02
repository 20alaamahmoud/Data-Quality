from flask import Flask, request, jsonify
import pandas as pd
import re
from datetime import datetime
from starter_DQ import assess_data_quality, get_quality_label, get_suggested_action


app = Flask(__name__)


@app.route("/assess", methods=["POST"])
def assess():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    filename = file.filename

    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(file)
        elif filename.endswith((".xls", ".xlsx")):
            df = pd.read_excel(file)
        else:
            return jsonify({"error": "Unsupported file type"}), 400
    except Exception as e:
        return jsonify({"error": f"Failed to read file: {str(e)}"}), 500

    report = assess_data_quality(df)
    report_json = report.to_dict(orient="records")

    # Calculate overall DQS
    valid_dqs = report[report["DQS"] != "N/A"]["DQS"]
    if not valid_dqs.empty:
        overall_dqs = round(valid_dqs.mean(), 2)
        overall_label = get_quality_label(overall_dqs)
        overall_action = get_suggested_action(overall_dqs)
    else:
        overall_dqs = "N/A"
        overall_label = "N/A"
        overall_action = "N/A"

    return jsonify({
        "report": report_json,
        "overall_dqs": overall_dqs,
        "overall_label": overall_label,
        "overall_action": overall_action
    })


if __name__ == "__main__":
    app.run(debug=True)
