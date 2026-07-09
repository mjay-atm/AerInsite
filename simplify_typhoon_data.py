import json
import argparse
import sys

def simplify_typhoon_warning(input_path, output_path):
    """
    讀取 CWA W-C0034-001 颱風警報 JSON。
    精簡資料，僅保留對應氣象署「文字報」格式的核心欄位 (Key-Value)。
    """
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
    except FileNotFoundError:
        print(f"找不到輸入檔案: {input_path}")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"JSON 格式解析失敗: {input_path}")
        sys.exit(1)

    # 用最乾淨的字典存放文字報資訊
    simplified_data = {}

    try:
        info_list = raw_data.get("records", {}).get("info", [])
        if info_list and len(info_list) > 0:
            info = info_list[0]
            
            # 確認事件類別為颱風
            if info.get("event") == "颱風":
                sections = info.get("description", {}).get("section", [])
                
                # 遍歷所有段落，提取如「命名與位置」、「強風特報」等欄位
                for sec in sections:
                    title = sec.get("title", "").strip()
                    value = sec.get("value", "").strip()
                    if title and value:
                        simplified_data[title] = value

    except Exception as e:
        print(f"解析欄位時發生未預期的狀況: {e}")

    # 若無颱風警報資料，給予預設提示
    if not simplified_data:
        simplified_data["狀態"] = "目前中央氣象署並無發布任何颱風警報。"

    # 輸出成乾淨的 JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(simplified_data, f, ensure_ascii=False, indent=2)
    
    print(f"颱風資料清洗完成！(已保留核心文字報欄位)")
    print(f"已輸出至: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simplify CWA Typhoon JSON data.")
    parser.add_argument("--input", required=True, help="Path to raw W-C0034-001 JSON file")
    parser.add_argument("--output", required=True, help="Path to save the simplified JSON file")
    
    args = parser.parse_args()
    simplify_typhoon_warning(args.input, args.output)