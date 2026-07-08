import json
import argparse
import sys

def simplify_typhoon_warning(input_path, output_path):
    """
    讀取 CWA W-C0034-001 颱風警報 JSON。
    將 description section 內的資料轉換為 Key-Value 字典，完美對應氣象署文字報格式。
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

    # 預設輸出結構
    simplified_data = {
        "status": "NO_WARNING",
        "message": "目前中央氣象署並無發布任何颱風警報。",
        "typhoon_info": {}
    }

    try:
        if "records" in raw_data and "info" in raw_data["records"]:
            info_list = raw_data["records"]["info"]
            if info_list and len(info_list) > 0:
                info = info_list[0]
                
                # 確認事件類別為颱風
                if info.get("event") == "颱風":
                    description_sections = info.get("description", {}).get("section", [])
                    
                    if description_sections:
                        simplified_data["status"] = "WARNING_ACTIVE"
                        simplified_data["message"] = "颱風警報發布中"
                        
                        # 將圖表中的欄位轉化為乾淨的 Dictionary
                        info_dict = {}
                        for sec in description_sections:
                            title = sec.get("title", "").strip()
                            value = sec.get("value", "").strip()
                            if title:
                                info_dict[title] = value
                                
                        simplified_data["typhoon_info"] = info_dict

    except Exception as e:
        print(f"⚠️ 解析欄位時發生未預期的狀況: {e}")

    # 輸出成乾淨的 JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(simplified_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 颱風資料清洗完成！(已保留各特報與動態欄位)")
    print(f"📁 已輸出至: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simplify CWA Typhoon JSON data.")
    parser.add_argument("--input", required=True, help="Path to raw W-C0034-001 JSON file")
    parser.add_argument("--output", required=True, help="Path to save the simplified JSON file")
    
    args = parser.parse_args()
    simplify_typhoon_warning(args.input, args.output)