import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from google import genai


def parse_args():
    parser = argparse.ArgumentParser(description="使用 Google Gemini 依颱風警報資料產生文字描述")
    parser.add_argument("--typhoon-text", default=None, help="直接提供颱風資料文本")
    parser.add_argument("--typhoon-file", nargs="+", default=None, help="颱風資料檔案路徑 (支援 W-C0034-001 JSON)")
    parser.add_argument("--typhoon-dir", default=None, help="颱風資料資料夾路徑，會自動讀取檔案")
    parser.add_argument("--model", default="gemini-3.1-flash-lite", help="模型名稱")
    parser.add_argument("--api-key", default=None, help="Gemini API Key；未提供時改讀環境變數 GEMINI_API_KEY")
    parser.add_argument("--location", default="桃園市", help="地區名稱")
    parser.add_argument("--language", default="繁體中文", help="輸出語言")
    parser.add_argument("--style", default="簡潔、專業、易懂", help="文字風格")
    parser.add_argument("--max-tokens", type=int, default=20000, help="最大輸出 token")
    parser.add_argument("--temperature", type=float, default=0.0, help="生成溫度 (0.0有助於降低幻覺)")
    parser.add_argument("--output", default="typhoon_warning_report.txt", help="輸出檔案")
    return parser.parse_args()


def read_typhoon_input(typhoon_text, typhoon_files, typhoon_dir):
    if typhoon_text:
        return typhoon_text.strip()

    files = []
    if typhoon_files:
        files.extend(Path(path) for path in typhoon_files)

    if typhoon_dir:
        folder = Path(typhoon_dir)
        if not folder.exists() or not folder.is_dir():
            raise NotADirectoryError(f"資料夾不存在或不是資料夾: {folder}")

        allowed_suffixes = {".txt", ".json", ".csv"}
        folder_files = sorted(
            path for path in folder.iterdir()
            if path.is_file() and (path.suffix.lower() in allowed_suffixes or not path.suffix)
        )
        files.extend(folder_files)

    if not files:
        raise ValueError("請提供 --typhoon-text、--typhoon-file 或 --typhoon-dir。")

    unique_files = []
    seen = set()
    for file_path in files:
        resolved = str(file_path.resolve())
        if resolved in seen:
            continue
        seen.add(resolved)
        unique_files.append(file_path)

    blocks = []
    for file_path in unique_files:
        if not file_path.exists() or not file_path.is_file():
            raise FileNotFoundError(f"找不到颱風資料檔案: {file_path}")
        content = read_single_typhoon_file(file_path)
        blocks.append(f"### 來源檔案: {file_path.name}\n{content}")

    return "\n\n".join(blocks).strip()


def read_single_typhoon_file(file_path):
    file_path = Path(file_path)
    suffix = file_path.suffix.lower()
    raw = read_text_with_fallback(file_path)

    if suffix == ".json":
        try:
            payload = json.loads(raw)
            # 針對 CWA W-C0034-001-SP 結構進行精準萃取
            if "records" in payload and "info" in payload["records"]:
                info = payload["records"]["info"][0]
                description_sections = info.get("description", {}).get("section", [])
                
                extracted_text = "【颱風警報核心內容】\n"
                for sec in description_sections:
                    title = sec.get("title", "")
                    value = sec.get("value", "")
                    extracted_text += f"[{title}]: {value}\n"
                
                return extracted_text.strip()
            # 若非預期結構，則原樣轉回縮排 JSON
            return json.dumps(payload, ensure_ascii=False, indent=2)
        except json.JSONDecodeError as exc:
            raise ValueError(f"JSON 檔案格式錯誤: {file_path}") from exc

    return raw.strip()


def read_text_with_fallback(file_path):
    encodings = ["utf-8-sig", "utf-8", "cp950", "big5", "latin-1"]
    decode_errors = []

    for encoding in encodings:
        try:
            return file_path.read_text(encoding=encoding)
        except UnicodeDecodeError as exc:
            decode_errors.append(f"{encoding}: {exc}")

    raise UnicodeDecodeError(
        "unknown",
        b"",
        0,
        1,
        "無法解析檔案編碼，已嘗試 utf-8-sig/utf-8/cp950/big5/latin-1。"
        f"檔案: {file_path}；錯誤摘要: {' | '.join(decode_errors[:3])}",
    )


def build_prompt(typhoon_data_text, location, language, style):
    instructions = (
        "你是專業氣象預報員，為桃園市政府水務局發報。\n"
        f"請基於事實，用最客觀、簡單的方式整理【{location}】的風雨現況與推估。\n"
        "【嚴格禁令】：嚴禁延伸任何防汛處置建議、否定排除字眼或個人推論。\n"
        "【最高強制規則】：輸出的前兩行警報文字中，絕對不可出現「桃園市」三個字（冗言），請直接陳述事實或使用具體行政區（如：復興區、新屋區）。\n\n"
    )
    task = (
        f"請以{language}撰寫颱風威脅描述，風格：{style}。\n"
        "請直接輸出以下「兩段」內容（必須完全按照下方標籤名稱與格式要求，不可多出或少掉段落）：\n\n"
        "颱風概況：[請參考此句型填空：颱風強度颱風中文名字，X日X時中心距離桃園市政府的(方位)約(XXX)公里，以時速(XX)轉(XX)公里，向(XX)轉(XX)進行。其七級平均暴風半徑(XXX)公里；十級平均暴風半徑(XXX)公里。] (注意：請優先使用資料中提供距桃園市政府的數據；若資料中有提及『最大為XXX公里』再加括號補充，無則省略)\n"
        "風雨概況：[請參考此句型填空：受颱風外圍環流影響，(填寫資料提及的時間段)本地區有平均風(X)級以上或陣風(X)級以上的發生機率，請注意。] (注意：若資料另有降雨特報，請於句尾客觀補充降雨警報等級；若無明確風雨預報數據，請依事實簡述)\n\n"
        "【格式要求】：上述兩段需先行輸出，前後不得有任何額外文字、標題、Markdown 符號（如 ** 粗體）或空行。\n"
        "兩段完成後，請先打印「----分隔線----」，接著撰寫你如何從資料中客觀對應到上述行政區與風雨概況的判斷邏輯。\n\n"
        f"【中央氣象署颱風警報資料來源】:\n{typhoon_data_text}\n"
    )
    return instructions + task


def call_gemini(api_key, model, prompt, max_tokens, temperature):
    try:
        # 使用新版 google.genai SDK
        client = genai.Client(api_key=api_key) if api_key else genai.Client()
        response = client.models.generate_content(
            model=model,
            contents=prompt,
            config={
                "temperature": temperature,
                "max_output_tokens": max_tokens,
            },
        )
    except Exception as exc:
        raise RuntimeError(f"Gemini API 呼叫失敗: {exc}") from exc

    text = getattr(response, "text", None)
    if not text:
        raise RuntimeError("Gemini 回傳內容為空，請檢查輸入資料或模型設定。")
    return text.strip()


def main():
    load_dotenv()
    args = parse_args()

    api_key = args.api_key or os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("找不到 API Key，請使用 --api-key 或設定環境變數 GEMINI_API_KEY")

    typhoon_data_text = read_typhoon_input(args.typhoon_text, args.typhoon_file, args.typhoon_dir)
    prompt = build_prompt(typhoon_data_text, args.location, args.language, args.style)

    generated = call_gemini(
        api_key=api_key,
        model=args.model,
        prompt=prompt,
        max_tokens=args.max_tokens,
        temperature=args.temperature,
    )

    # 將生成的文字依照換行符號切分
    # 前 5 行為警報文案，之後的為邏輯說明
    lines = [line for line in generated.splitlines() if line.strip() != ""]
    
    # 預防 AI 沒照格式輸出，設定安全邊界
    split_index = 5
    for i, line in enumerate(lines):
        if "----分隔線----" in line:
            split_index = i
            break

    output_path = Path(args.output)
    output_path.write_text("\n".join(lines[:split_index]), encoding="utf-8")

    logic_output_path = output_path.with_name(output_path.stem + "_logic" + output_path.suffix)
    logic_explanation = "\n".join(lines[split_index:])
    logic_output_path.write_text(logic_explanation, encoding="utf-8")

    print("=== 颱風警報描述產生完成 ===")
    print("\n".join(lines[:split_index]))
    print(f"\n報告已輸出至: {output_path.resolve()}")
    print(f"邏輯已輸出至: {logic_output_path.resolve()}")


if __name__ == "__main__":
    main()
