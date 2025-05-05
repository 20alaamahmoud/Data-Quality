from flask import Flask, request, jsonify
import pandas as pd
from cdq import assess_carbon_data_quality, get_quality_label, get_suggested_action

app = Flask(__name__)

@app.route('/assess', methods=['POST'])
def assess():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    try:
        df = pd.read_csv(file) if file.filename.endswith('.csv') else pd.read_excel(file)
    except Exception as e:
        return jsonify({'error': f'Error reading file: {str(e)}'}), 400

    metadata_flags = {col: True for col in df.columns}
    report = assess_carbon_data_quality(df, metadata_flags)

    avg_dqs = report["DQS"].mean()
    overall_label = get_quality_label(avg_dqs)
    verifiability_score = report["Verifiability"].mean()
    pcaf_score = 1 if verifiability_score >= 0.95 else 2
    uncertainty = round((1 - verifiability_score) * 20, 2)
    estimated_emissions = df.select_dtypes(include='number').mean().mean()
    lower_bound = round(estimated_emissions * (1 - uncertainty / 100), 2)
    upper_bound = round(estimated_emissions * (1 + uncertainty / 100), 2)
    decision_recommendation = (
        "Use confidently for decision-making." if avg_dqs >= 0.8 else
        "Suitable for most decision-making with uncertainty noted." if avg_dqs >= 0.6 else
        "Use with caution; verify critical fields." if avg_dqs >= 0.4 else
        "Improve data before use."
    )
    weak_areas = report.loc[report["Verifiability"] < 0.95, "Column"].tolist()

    return jsonify({
        "report": report.to_dict(orient='records'),
        "summary": {
            "Final Data Quality": overall_label,
            "DQS Score": round(avg_dqs, 2),
            "PCAF Score": pcaf_score,
            "Uncertainty": f"±{uncertainty}%",
            "Emissions Reporting": {
                "Estimate": round(estimated_emissions, 2),
                "Range": f"{lower_bound} - {upper_bound} tCO₂e"
            },
            "Decision Use": decision_recommendation,
            "Improvement Areas": weak_areas or "None"
        }
    })

if __name__ == '__main__':
    app.run(debug=True)
