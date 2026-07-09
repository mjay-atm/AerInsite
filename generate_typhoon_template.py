import argparse, json, sys
from datetime import datetime

CHINESE_LANG_CODES = {"zh-hant", "zh-tw", "zh-hans", "zh-cn"}
QUADRANT_ORDER = [("NW", "西北側"), ("NE", "東北側"), ("SW", "西南側"), ("SE", "東南側")]
CARDINAL_DIRECTION_MAP = {"N": "北", "E": "東", "S": "南", "W": "西"}

def clean_text(value): return str(value).strip()
def normalize_number(value): return str(int(value)) if value.isdigit() else value
def print_error_and_exit(message): print(message); sys.exit(1)

def direction_to_zh(direction):
    direction = clean_text(direction).upper()
    if not direction: return ""
    if any(ch not in CARDINAL_DIRECTION_MAP for ch in direction): return direction
    mapped = [CARDINAL_DIRECTION_MAP[ch] for ch in direction]
    if len(direction) == 2: mapped = mapped[::-1]
    return "".join(mapped)

def load_json(input_path):
    try:
        with open(input_path, "r", encoding="utf-8") as f: return json.load(f)
    except FileNotFoundError: print_error_and_exit(f"Input file not found {input_path}")
    except json.JSONDecodeError: print_error_and_exit(f"JSON parse failed {input_path}")

def get_latest_fix(raw_data, cyclone_index=0):
    cyclones = raw_data.get("records", {}).get("TropicalCyclones", {}).get("TropicalCyclone", [])
    if not cyclones: raise ValueError("No active tropical cyclone in current data")
    if cyclone_index < 0 or cyclone_index >= len(cyclones): raise IndexError(f"cyclone index out of range valid range is 0 to {len(cyclones) - 1}")
    cyclone = cyclones[cyclone_index]
    fixes = cyclone.get("AnalysisData", {}).get("Fix", [])
    if not fixes: raise ValueError("Selected tropical cyclone has no AnalysisData Fix data")
    return cyclone, fixes[-1]

def normalize_circle(circle):
    if not circle: return "", {}
    radius_by_dir = {}
    for item in circle.get("QuadrantRadii", {}).get("Radius", []):
        direction, value = clean_text(item.get("dir", "")).upper(), clean_text(item.get("value", ""))
        if direction and value: radius_by_dir[direction] = value
    return clean_text(circle.get("Radius", "")), radius_by_dir

def get_moving_prediction_text(fix):
    predictions = [(clean_text(p.get("lang", "")).lower(), clean_text(p.get("value", ""))) for p in fix.get("MovingPrediction", [])]
    for lang, value in predictions:
        if value and lang in CHINESE_LANG_CODES: return value.rstrip("。")
    if predictions and predictions[0][1]: return predictions[0][1].rstrip("。")
    speed = clean_text(fix.get("MovingSpeed", ""))
    direction = clean_text(fix.get("MovingDirection", "")).upper()
    direction_zh = direction_to_zh(direction)
    if speed and direction_zh: return f"以每小時{speed}公里速度，向{direction_zh}進行"
    if speed: return f"以每小時{speed}公里速度移動"
    if direction_zh: return f"向{direction_zh}移動"
    return "尚無移動資訊"

def format_circle_text(circle, level_name):
    average_radius, radius_by_dir = normalize_circle(circle)
    parts = [f"{label} {radius_by_dir[d]} 公里" for d, label in QUADRANT_ORDER if d in radius_by_dir]
    if average_radius and parts: return f"{level_name}平均暴風半徑 {average_radius} 公里({'、'.join(parts)})"
    if average_radius: return f"{level_name}平均暴風半徑 {average_radius} 公里"
    return f"尚無{level_name}平均暴風半徑資料"

def format_time_label(datetime_str):
    try:
        dt = datetime.fromisoformat(datetime_str)
        return f"{dt.day}日{dt.hour}時"
    except (TypeError, ValueError): return datetime_str or "時間未知"

def format_typhoon_identity(cyclone):
    zh, en = clean_text(cyclone.get("CwaTyphoonName", "")), clean_text(cyclone.get("TyphoonName", ""))
    cwa_ty_no, cwa_td_no = clean_text(cyclone.get("CwaTyNo", "")), clean_text(cyclone.get("CwaTdNo", ""))
    name = zh or en
    identity = f"第{normalize_number(cwa_ty_no)}號颱風" if cwa_ty_no else "颱風"
    if name: identity += name
    extra = [en] if en and en != name else []
    if cwa_td_no and not cwa_ty_no: extra.append(f"熱帶性低氣壓編號 {normalize_number(cwa_td_no)}")
    return f"{identity}（{'，'.join(extra)}）" if extra else identity

def build_template_text(cyclone, fix):
    position = f"{format_typhoon_identity(cyclone)}，{format_time_label(fix.get('DateTime', ''))}的中心位置在北緯 {clean_text(fix.get('CoordinateLatitude', ''))} 度，東經 {clean_text(fix.get('CoordinateLongitude', ''))} 度，{get_moving_prediction_text(fix)}。"
    intensity = f"中心氣壓{clean_text(fix.get('Pressure', ''))}百帕，近中心最大風速每秒{clean_text(fix.get('MaxWindSpeed', ''))}公尺，瞬間最大陣風每秒 {clean_text(fix.get('MaxGustSpeed', ''))} 公尺，{format_circle_text(fix.get('Circle15ms', {}), '七級風')}，{format_circle_text(fix.get('Circle25ms', {}), '十級風')}。"
    return f"{position}{intensity}"

def main():
    parser = argparse.ArgumentParser(description="Generate typhoon template text from CWA W C0034 005 JSON")
    parser.add_argument("--input", required=True, help="Input JSON file path")
    parser.add_argument("--output", help="Output text file path optional")
    parser.add_argument("--cyclone-index", type=int, default=0, help="Index of TropicalCyclone default 0")
    args = parser.parse_args()
    template_text = ""
    try:
        cyclone, latest_fix = get_latest_fix(load_json(args.input), args.cyclone_index)
        template_text = build_template_text(cyclone, latest_fix)
    except (ValueError, IndexError) as e: print_error_and_exit(f"Error {e}")
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f: f.write(template_text)
        print(f"Template text written to {args.output}")
    print(template_text)

if __name__ == "__main__": main()