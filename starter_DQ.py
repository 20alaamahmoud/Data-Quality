import pandas as pd
import re
from datetime import datetime


def calculate_completeness(column):
    total = len(column)
    non_null = column.notnull().sum()
    ratio = non_null / total
    if ratio > 0.95:
        return 1.0
    elif ratio > 0.85:
        return 0.75
    elif ratio > 0.75:
        return 0.5
    elif ratio > 0.5:
        return 0.4
    else:
        return 0.2

def calculate_format_validity(column, pattern=r'.+'):
    total = len(column)
    valid = column.dropna().apply(lambda x: bool(re.match(pattern, str(x)))).sum()
    ratio = valid / total if total > 0 else 0
    if ratio > 0.95:
        return 1.0
    elif ratio > 0.85:
        return 0.75
    elif ratio > 0.75:
        return 0.5
    elif ratio > 0.5:
        return 0.4
    else:
        return 0.2

def calculate_cross_system_consistency(column):
    unique_values = column.dropna().unique()
    unique_lower = set(map(lambda x: str(x).lower(), unique_values))
    ratio = len(unique_lower) / len(unique_values) if len(unique_values) > 0 else 1.0
    if ratio == 1.0:
        return 1.0
    elif ratio > 0.85:
        return 0.75
    elif ratio > 0.75:
        return 0.5
    elif ratio > 0.5:
        return 0.4
    else:
        return 0.2

def calculate_business_rule_compliance(column, min_val=None, max_val=None):
    if pd.api.types.is_numeric_dtype(column):
        valid = column.dropna().apply(lambda x: min_val <= x <= max_val if min_val is not None and max_val is not None else True).sum()
        ratio = valid / len(column) if len(column) > 0 else 0
        if ratio > 0.95:
            return 1.0
        elif ratio > 0.85:
            return 0.75
        elif ratio > 0.75:
            return 0.5
        elif ratio > 0.5:
            return 0.4
        else:
            return 0.2
    return "N/A"

def detect_duplicates(column):
    total = len(column)
    unique = column.nunique(dropna=False)
    duplicate_ratio = (total - unique) / total
    if duplicate_ratio < 0.01:
        return 1.0
    elif duplicate_ratio < 0.05:
        return 0.75
    elif duplicate_ratio < 0.10:
        return 0.5
    elif duplicate_ratio < 0.20:
        return 0.4
    else:
        return 0.2


# DQS Calculation

def calculate_dqs(scores):
    weights = {
        "Completeness": 2.0,
        "Format Validity": 1.5,
        "Cross-System Consistency": 1.5,
        "Business Rule Compliance": 2.0
    }
    total_weight = sum(weights.values())
    weighted_sum = 0
    for dim, score in scores.items():
        weight = weights.get(dim, 0)
        if score != "N/A":
            weighted_sum += score * weight
        else:
            total_weight -= weight

    if total_weight == 0:
        return "N/A"
    return round(weighted_sum / total_weight, 2)

def get_quality_label(dqs):
    if dqs == "N/A":
        return "N/A"
    if dqs >= 0.8:
        return "High Quality"
    elif dqs >= 0.6:
        return "Good Quality"
    elif dqs >= 0.4:
        return "Acceptable"
    elif dqs >= 0.2:
        return "Poor Quality"
    else:
        return "Very Poor"

def get_suggested_action(dqs):
    if dqs == "N/A":
        return "N/A"
    if dqs >= 0.8:
        return "Suitable for critical decision-making"
    elif dqs >= 0.6:
        return "Reliable for most business purposes"
    elif dqs >= 0.4:
        return "Usable with caution, consider improvements"
    elif dqs >= 0.2:
        return "Use only if necessary, prioritize improvement"
    else:
        return "Not recommended for use, requires remediation"

# Assessment Function

def assess_data_quality(df):
    results = []
    for col in df.columns:
        column = df[col]
        is_numeric = pd.api.types.is_numeric_dtype(column)

        completeness = calculate_completeness(column)
        format_validity = calculate_format_validity(column)
        consistency = calculate_cross_system_consistency(column)
        business_rule = calculate_business_rule_compliance(column, min_val=0, max_val=999999) if is_numeric else "N/A"

        scores = {
            "Completeness": completeness,
            "Format Validity": format_validity,
            "Cross-System Consistency": consistency,
            "Business Rule Compliance": business_rule
        }

        dqs = calculate_dqs(scores)
        label = get_quality_label(dqs)
        action = get_suggested_action(dqs)

        results.append({
            'Column': col,
            'Completeness': completeness,
            'Format Validity': format_validity,
            'Cross-System Consistency': consistency,
            'Business Rule Compliance': business_rule,
            'DQS': dqs,
            'Quality Label': label,
            'Suggested Action': action
        })
    return pd.DataFrame(results)



if __name__ == "__main__":
    file_path = input("Enter your data file: ")
    try:
        if file_path.lower().endswith(".csv"):
            df = pd.read_csv(file_path)
        elif file_path.lower().endswith((".xls", ".xlsx")):
            df = pd.read_excel(file_path)
        else:
            raise ValueError("Unsupported file type. Please upload a CSV or Excel file.")

    except Exception as e:
        print(f"Error reading file: {e}")
        exit(1)


    report = assess_data_quality(df)
    print("\nData Quality Assessment Report:\n")
    print(report.to_string(index=False))

    report.to_csv("data_quality_report.csv", index=False)
    #report.to_excel("data_quality_report.xlsx", index=False)

    valid_dqs = report[report["DQS"] != "N/A"]["DQS"]
    if not valid_dqs.empty:
        overall_dqs = round(valid_dqs.mean(), 2)
        overall_label = get_quality_label(overall_dqs)
        overall_action = get_suggested_action(overall_dqs)

        print("\nOverall Dataset Quality:")
        print(f"DQS: {overall_dqs} | Label: {overall_label} | Suggested Action: {overall_action}")
    else:
        print("\nCould not calculate overall DQS (no valid scores).")
