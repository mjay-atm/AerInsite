import argparse
import json
import os
from pathlib import Path
from dotenv import load_dotenv
from google import genai

def parse_args():
    parser = argparse.ArgumentParser(description="使用 Google Gemini 依 005 精簡資料產生描述報告")
    parser.add_argument("--typhoon-file", required=True, help="精簡後的 005 JSON 檔案路徑")
    parser.add_argument("--model", default="gemini-2.5-flash", help="模型名稱") # 👈 已修復為新版預設模型
    parser.add_argument("--api-key", default=None, help="Gemini API Key")
    parser.add_argument("--output", default="typhoon_warning_report_t.txt", help="輸出檔案路徑")
    return parser.parse_args()

def build_prompt_t(typhoon_data_text):
    instructions = (
        "你是一位中央氣象署的專業氣象預報員。\n"
        "請根據提供的結構化颱風數據，嚴謹、客觀地撰寫一段簡短的颱風動態報告。\n"
        "必須完全基於資料事實，不可加入任何個人推論、建議或誇張形容詞。\n"
        "結尾加上句點。\n\n"
    )
    task = (
        "請嚴格按照以下兩行格式輸出，【每點各自限制在 30 字以內】，前後不要有任何額外的引言、標題、Markdown 符號或空行：\n"
        "颱風概況：[強度等級颱風中文名、目前的中心位置表現。嚴格限制 30 字內]\n"
        "颱風移動趨勢：[說明未來 6 小時內移動方向、速度與強度趨勢。嚴格限制 30 字內]\n\n"
        "【中央氣象署颱風結構化數據如下】:\n"
        f"{typhoon_data_text}"
    )
    return instructions + task

def main():
    load_dotenv()
    args = parse_args()

    api_key = args.api_key or os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("找不到 API Key，請設定環境變數 GEMINI_API_KEY")

    file_path = Path(args.typhoon_file)
    if not file_path.exists():
        raise FileNotFoundError(f"找不到檔案: {file_path}")
    
    with open(file_path, "r", encoding="utf-8") as f:
        data_json = json.load(f)
    
    typhoon_data_text = json.dumps(data_json.get("typhoon_details", {}), ensure_ascii=False, indent=2)

    prompt = build_prompt_t(typhoon_data_text)

    # 呼叫 Gemini (完美相容 google-genai 最新版 SDK 寫法)
    try:
        model_name = args.model.strip()
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=model_name,
            contents=prompt,
            config={
                "temperature": 0.0, # 設為 0.0 確保數據完全準確
            }
        )
    except Exception as exc:
        raise RuntimeError(f"Gemini API 呼叫失敗: {exc}") from exc

    generated_text = getattr(response, "text", "").strip()
    if not generated_text:
        raise RuntimeError("Gemini 回傳內容為空。")

    output_path = Path(args.output)
    output_path.write_text(generated_text, encoding="utf-8")

    print("=== 005 新版颱風簡報產生完成 ===")
    print(generated_text)
    print(f"\n📁 報告已成功輸出至: {output_path.resolve()}")

if __name__ == "__main__":
    main()