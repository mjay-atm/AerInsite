import json
import argparse
import sys

def get_typhoon_scale(max_wind_speed):
    """
    根據台灣中央氣象署標準，由近中心最大風速 (m/s) 判定颱風強度等級
    """
    try:
        speed = float(max_wind_speed)
        if speed >= 51.0:
            return "強烈颱風"
        elif speed >= 32.7:
            return "中度颱風"
        elif speed >= 17.2:
            return "輕度颱風"
        else:
            return "熱帶性低氣壓或一般氣旋"
    except (ValueError, TypeError):
        return "未知強度"

def simplify_typhoon_005(input_path, output_path):
    """
    讀取 CWA W-C0034-005 JSON，萃取颱風名稱、最新位置與強度，以及 6 小時預報資料。
    """
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
    except FileNotFoundError:
        print(f"❌ 找不到輸入檔案: {input_path}")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"❌ JSON 格式解析失敗: {input_path}")
        sys.exit(1)

    simplified_data = {
        "status": "NO_TYPHOON",
        "message": "目前無活動中之颱風資料。",
        "typhoon_details": {}
    }

    try:
        records = raw_data.get("records", {})
        tropical_cyclones = records.get("TropicalCyclones", {})
        cyclone_list = tropical_cyclones.get("TropicalCyclone", [])

        if cyclone_list and len(cyclone_list) > 0:
            cyclone = cyclone_list[0]
            simplified_data["status"] = "TYPHOON_ACTIVE"
            simplified_data["message"] = "發現活動中颱風資料"

            typhoon_name_en = cyclone.get("TyphoonName", "")
            typhoon_name_zh = cyclone.get("CwaTyphoonName", "")
            
            analysis_data = cyclone.get("AnalysisData", {})
            fix_list = analysis_data.get("Fix", [])
            latest_fix = fix_list[-1] if fix_list else {}

            forecast_data = cyclone.get("ForecastData", {})
            forecast_list = forecast_data.get("Fix", [])
            six_hour_forecast = {}
            for fc in forecast_list:
                if str(fc.get("ForecastHour")) == "6":
                    six_hour_forecast = fc
                    break

            # 取得風速並換算強度等級
            current_wind_speed = latest_fix.get('MaxWindSpeed', '')
            current_scale = get_typhoon_scale(current_wind_speed)

            forecast_wind_speed = six_hour_forecast.get('MaxWindSpeed', '') if six_hour_forecast else ''
            forecast_scale = get_typhoon_scale(forecast_wind_speed) if forecast_wind_speed else "未知強度"

            simplified_data["typhoon_details"] = {
                "颱風名稱(中文)": typhoon_name_zh,
                "颱風名稱(英文)": typhoon_name_en,
                "目前強度等級": current_scale,
                "目前觀測時間": latest_fix.get("DateTime", ""),
                "目前中心位置": f"北緯 {latest_fix.get('CoordinateLatitude', '')} 度，東經 {latest_fix.get('CoordinateLongitude', '')} 度",
                "目前中心氣壓": f"{latest_fix.get('Pressure', '')} 百帕",
                "目前近中心最大風速": f"{current_wind_speed} 公尺/秒",
                "目前瞬間最大陣風": f"{latest_fix.get('MaxGustSpeed', '')} 公尺/秒",
                "目前移動速度與方向": latest_fix.get("MovingPrediction", [{}])[0].get("value", f"每小時 {latest_fix.get('MovingSpeed', '')} 公里，向 {latest_fix.get('MovingDirection', '')} 行進"),
                
                "未來6小時預報": {
                    "預報時間": six_hour_forecast.get("InitialTime", ""),
                    "預報強度等級": forecast_scale,
                    "預報中心位置": f"北緯 {six_hour_forecast.get('CoordinateLatitude', '')} 度，東經 {six_hour_forecast.get('CoordinateLongitude', '')} 度",
                    "預報中心氣壓": f"{six_hour_forecast.get('Pressure', '')} 百帕",
                    "預報近中心最大風速": f"{forecast_wind_speed} 公尺/秒",
                    "預報移動速度與方向": f"每小時 {six_hour_forecast.get('MovingSpeed', '')} 公里，向 {six_hour_forecast.get('MovingDirection', '')} 行進"
                } if six_hour_forecast else "無 6 小時預報資料"
            }

    except Exception as e:
        print(f"⚠️ 解析 005 欄位時發生未預期的狀況: {e}")

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(simplified_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 005 颱風資料精簡與強度換算完成！")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simplify CWA W-C0034-005 JSON data.")
    parser.add_argument("--input", required=True, help="Path to raw W-C0034-005 JSON file")
    parser.add_argument("--output", required=True, help="Path to save the simplified JSON file")
    
    args = parser.parse_args()
    simplify_typhoon_005(args.input, args.output)