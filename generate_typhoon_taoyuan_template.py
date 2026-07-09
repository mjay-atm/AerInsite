import argparse, json, math, sys
from datetime import datetime

CHINESE_LANG_CODES = {"zh-hant", "zh-tw", "zh-hans", "zh-cn"}
TAOYUAN_CITY_HALL_LAT, TAOYUAN_CITY_HALL_LON = 24.992956008206395, 121.30106138931436
REFERENCE_LOCATION_NAME = "桃園市政府"
COMPASS_16_ZH = ["北", "北北東", "東北", "東北東", "東", "東南東", "東南", "南南東", "南", "南南西", "西南", "西南西", "西", "西北西", "西北", "北北西"]
CARDINAL_DIRECTION_MAP = {"N": "北", "E": "東", "S": "南", "W": "西"}

def clean_text(value): return "" if value is None else str(value).strip()
def normalize_number(value):
    value = clean_text(value)
    if not value: return ""
    try:
        number = float(value)
        return str(int(number)) if number.is_integer() else value
    except ValueError:
        return value

def parse_float(value):
    value = clean_text(value)
    if not value: return None
    try: return float(value)
    except ValueError: return None

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

def parse_coordinate(value, field_name):
    raw = clean_text(value)
    if not raw: raise ValueError(f"Missing coordinate field {field_name}")
    try: return float(raw)
    except ValueError as e: raise ValueError(f"Invalid coordinate in {field_name} {raw}") from e

def haversine_distance_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def initial_bearing_degrees(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    dlon = lon2 - lon1
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360

def bearing_to_zh_direction(bearing_deg): return COMPASS_16_ZH[int((bearing_deg + 11.25) // 22.5) % 16]

def format_distance_km(distance_km):
    rounded = int(round(distance_km / 10.0) * 10)
    return str(1 if rounded == 0 and distance_km > 0 else rounded)

def normalize_speed_phrase(text): 
    text = clean_text(text).replace("以每小時", "以時速")
    text = clean_text(text).replace("速度", "")
    return text

def get_circle_radius_text(fix, circle_key, level_label):
    circle = fix.get(circle_key, {})
    if not isinstance(circle, dict): return ""
    avg_radius = normalize_number(circle.get("Radius", ""))
    max_radius_value = None
    quadrant_radii = circle.get("QuadrantRadii", {}).get("Radius", [])
    if isinstance(quadrant_radii, list):
        for radius_item in quadrant_radii:
            value = radius_item.get("value", "") if isinstance(radius_item, dict) else radius_item
            radius_value = parse_float(value)
            if radius_value is None: continue
            max_radius_value = radius_value if max_radius_value is None else max(max_radius_value, radius_value)
    max_radius = normalize_number(max_radius_value)
    if avg_radius and max_radius: return f"{level_label}平均暴風半徑{avg_radius}公里(最大為{max_radius}公里)"
    if avg_radius: return f"{level_label}平均暴風半徑{avg_radius}公里"
    if max_radius: return f"{level_label}最大暴風半徑{max_radius}公里"
    return ""

def get_wind_radius_summary_text(fix):
    radius_parts = [
        get_circle_radius_text(fix, "Circle15ms", "七級"),
        get_circle_radius_text(fix, "Circle25ms", "十級"),
    ]
    radius_parts = [part for part in radius_parts if part]
    return f"其{'；'.join(radius_parts)}" if radius_parts else ""

def get_moving_prediction_text(fix):
    predictions = [(clean_text(p.get("lang", "")).lower(), clean_text(p.get("value", ""))) for p in fix.get("MovingPrediction", [])]
    for lang, value in predictions:
        if value and lang in CHINESE_LANG_CODES: return normalize_speed_phrase(value.rstrip("。"))
    if predictions and predictions[0][1]: return normalize_speed_phrase(predictions[0][1].rstrip("。"))
    speed, direction_zh = normalize_number(fix.get("MovingSpeed", "")), direction_to_zh(fix.get("MovingDirection", ""))
    if speed and direction_zh: return f"以時速{speed}公里，向{direction_zh}進行"
    if speed: return f"以時速{speed}公里移動"
    if direction_zh: return f"向{direction_zh}移動"
    return "尚無移動資訊"

def format_time_label(datetime_str):
    try:
        dt = datetime.fromisoformat(datetime_str); return f"{dt.day}日{dt.hour}時"
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

def format_relative_location_text(fix):
    lat = parse_coordinate(fix.get("CoordinateLatitude", ""), "CoordinateLatitude")
    lon = parse_coordinate(fix.get("CoordinateLongitude", ""), "CoordinateLongitude")
    distance = haversine_distance_km(TAOYUAN_CITY_HALL_LAT, TAOYUAN_CITY_HALL_LON, lat, lon)
    bearing = initial_bearing_degrees(TAOYUAN_CITY_HALL_LAT, TAOYUAN_CITY_HALL_LON, lat, lon)
    return f"中心距離{REFERENCE_LOCATION_NAME}{bearing_to_zh_direction(bearing)}方約{format_distance_km(distance)}公里"

def build_template_text(cyclone, fix):
    base_text = f"{format_typhoon_identity(cyclone)}，{format_time_label(fix.get('DateTime', ''))}{format_relative_location_text(fix)}，{get_moving_prediction_text(fix)}。"
    wind_radius_text = get_wind_radius_summary_text(fix)
    return f"{base_text}{wind_radius_text}" if wind_radius_text else base_text

def main():
    parser = argparse.ArgumentParser(description="Generate Taoyuan City Hall customized typhoon text from CWA W C0034 005 JSON")
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