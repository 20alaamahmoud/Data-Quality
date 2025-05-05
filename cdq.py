import pandas as pd
import re
from datetime import datetime


def calculate_completeness(column):
    total = len(column)
    non_null = column.notnull().sum()
    ratio = non_null / total if total else 0
    return ratio

def calculate_accuracy(column):
    if pd.api.types.is_numeric_dtype(column):
        errors = column.apply(lambda x: x < 0 if pd.notnull(x) else False).sum()
        return 1 - (errors / len(column)) if len(column) else 0
    return 1.0  # 

def calculate_format_validity(column, pattern=r'.+'):
    valid = column.dropna().apply(lambda x: bool(re.fullmatch(pattern, str(x)))).sum()
    return valid / len(column) if len(column) else 0

def calculate_timeliness(column):
    if pd.api.types.is_datetime64_any_dtype(column):
        now = pd.Timestamp.now()
        within_one_year = column.dropna().apply(lambda x: (now - x).days <= 365).sum()
        return within_one_year / len(column) if len(column) else 0
    return 1.0

def calculate_relevance(column):
    unique = column.dropna().nunique()
    total = column.dropna().count()
    if total == 0:
        return 0
    ratio = unique / total
    return 1.0 if 0.1 < ratio < 0.9 else 0.5 

def calculate_verifiability(column):
    return 1.0 if column.notnull().all() else 0.75 

def calculate_transparency(metadata_available):
    return 1.0 if metadata_available else 0.5

def calculate_comparability(column):
    return 1.0 if column.dropna().dtype in [int, float, 'datetime64[ns]'] else 0.7

# DQS Aggregation and Assessment

def calculate_dqs(scores, weights=None):
    if not weights:
        weights = {dim: 1 for dim in scores}
    weighted_sum = sum(scores[dim] * weights.get(dim, 1) for dim in scores)
    total_weight = sum(weights.values())
    return round(weighted_sum / total_weight, 2)

def get_quality_label(score):
    if score >= 0.8:
        return "High"
    elif score >= 0.6:
        return "Good"
    elif score >= 0.4:
        return "Acceptable"
    elif score >= 0.2:
        return "Poor"
    else:
        return "Very Poor"

def get_suggested_action(score):
    if score >= 0.8:
        return "Use confidently for decision-making."
    elif score >= 0.6:
        return "Use with minor caution."
    elif score >= 0.4:
        return "Review and improve where possible."
    elif score >= 0.2:
        return "Flag for remediation."
    else:
        return "Do not use."

# Assessment function

def assess_carbon_data_quality(df, metadata_flags=None):
    results = []
    for col in df.columns:
        col_data = df[col]
        metadata_flag = metadata_flags.get(col, False) if metadata_flags else False

        scores = {
            "Completeness": calculate_completeness(col_data),
            "Accuracy": calculate_accuracy(col_data),
            "Format Validity": calculate_format_validity(col_data),
            "Timeliness": calculate_timeliness(col_data),
            "Relevance": calculate_relevance(col_data),
            "Verifiability": calculate_verifiability(col_data),
            "Transparency": calculate_transparency(metadata_flag),
            "Comparability": calculate_comparability(col_data)
        }

        dqs = calculate_dqs(scores)
        results.append({
            "Column": col,
            **scores,
            "DQS": dqs,
            "Label": get_quality_label(dqs),
            "Suggested Action": get_suggested_action(dqs)
        })

    return pd.DataFrame(results)


if __name__ == "__main__":
    path = input("Enter file path: ")
    try:
        df = pd.read_csv(path) if path.endswith(".csv") else pd.read_excel(path)
    except Exception as e:
        print(f"Error reading file: {e}")
        exit()


    
    metadata_flags = {col: True for col in df.columns}

    report = assess_carbon_data_quality(df, metadata_flags)
    print(report.to_string(index=False))
    report.to_csv("carbon_dqa_report.csv", index=False)


    # Generate Output Summary
    avg_dqs = report["DQS"].mean()
    overall_label = get_quality_label(avg_dqs)
    verifiability_score = report["Verifiability"].mean()

    # PCAF Score estimation (simplified): 1 is verified, 2 is unverified
    pcaf_score = 1 if verifiability_score >= 0.95 else 2

    # Uncertainty estimation (example logic)
    uncertainty = round((1 - verifiability_score) * 20, 2)  # up to ±20%
    estimated_emissions = df.select_dtypes(include='number').mean().mean()  # Placeholder
    lower_bound = round(estimated_emissions * (1 - uncertainty / 100), 2)
    upper_bound = round(estimated_emissions * (1 + uncertainty / 100), 2)

    
    decision_recommendation = (
        "Use confidently for decision-making." if avg_dqs >= 0.8 else
        "Suitable for most decision-making with uncertainty noted." if avg_dqs >= 0.6 else
        "Use with caution; verify critical fields." if avg_dqs >= 0.4 else
        "Improve data before use."
    )

    # Identify weak points
    weak_areas = report.loc[report["Verifiability"] < 0.95, "Column"].tolist()
    improvement_areas = ", ".join(weak_areas) if weak_areas else "None"

    print("\nData Quality Assessment Summary")
    print(f"• Final Data Quality: {overall_label} ({round(avg_dqs, 2)})")
    print(f"• PCAF Score: {pcaf_score} ({'Verified' if pcaf_score == 1 else 'Unverified'} GHG emissions data)")
    print(f"• Uncertainty: ±{uncertainty}%")
    print(f"• Emissions Reporting: {round(estimated_emissions, 2)} tCO₂e ±{uncertainty}% ({lower_bound}-{upper_bound} tCO₂e)")
    print(f"• Decision Use: {decision_recommendation}")
    print(f"• Improvement Areas: {improvement_areas}")

