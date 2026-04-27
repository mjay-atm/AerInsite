import argparse
import json
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="精簡 A0010 一週農業氣象資料：只保留 weatherProfile")
    parser.add_argument("--input", required=True, help="A0010 原始 JSON 檔案路徑")
    parser.add_argument("--output", default=None, help="輸出精簡 JSON 路徑")
    return parser.parse_args()


def get_nested(data, keys, default=None):
    cur = data
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
        if cur is None:
            return default
    return cur


def build_reduced_payload(source_payload):
    root = source_payload.get("cwaopendata", {})

    resource = get_nested(root, ["resources", "resource"], default={})
    metadata = resource.get("metadata", {})
    temporal = metadata.get("temporal", {})
    valid_time = temporal.get("validTime", {})
    data_obj = resource.get("data", {})
    agr_weather = data_obj.get("agrWeatherForecasts", {})

    return {
        "source": {
            "dataid": root.get("dataid"),
            "dataset_name": root.get("datasetName"),
            "resource_name": metadata.get("resourceName"),
            "resource_description": metadata.get("resourceDescription"),
            "issue_time": temporal.get("issueTime"),
            "valid_time": {
                "start_time": valid_time.get("startTime"),
                "end_time": valid_time.get("endTime"),
            },
            "update_time": root.get("sent"),
        },
        "focus": ["weatherProfile"],
        "weather_profile_weekly": agr_weather.get("weatherProfile"),
    }


def main():
    args = parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"找不到輸入檔案: {input_path}")

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.with_name(f"{input_path.stem}.week_reduced.json")

    source_payload = json.loads(input_path.read_text(encoding="utf-8"))
    reduced = build_reduced_payload(source_payload)

    output_path.write_text(json.dumps(reduced, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"完成：已輸出精簡檔案 -> {output_path}")


if __name__ == "__main__":
    main()