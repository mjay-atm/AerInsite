import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from google import genai


def parse_args():
    parser = argparse.ArgumentParser(description="使用 Google Gemini 依天氣資料文本產生天氣描述")
    parser.add_argument("--weather-text", default=None, help="直接提供天氣文本")
    parser.add_argument("--weather-file", nargs="+", default=None, help="天氣資料檔案路徑，可一次指定多個")
    parser.add_argument("--weather-dir", default=None, help="天氣資料資料夾路徑，會自動讀取檔案")
    parser.add_argument("--model", default="gemini-3-flash-preview", help="模型名稱")
    parser.add_argument("--api-key", default=None, help="Gemini API Key；未提供時改讀環境變數 GEMINI_API_KEY")
    parser.add_argument("--location", default="桃園市", help="地區名稱")
    parser.add_argument("--language", default="繁體中文", help="輸出語言")
    parser.add_argument("--style", default="簡潔、專業、易懂", help="文字風格")
    parser.add_argument("--max-tokens", type=int, default=20000, help="最大輸出 token")
    parser.add_argument("--temperature", type=float, default=0.0, help="生成溫度")
    parser.add_argument("--output", default="weather_description.txt", help="輸出檔案")
    return parser.parse_args()


def read_weather_input(weather_text, weather_files, weather_dir):
    if weather_text:
        return weather_text.strip()

    files = []
    if weather_files:
        files.extend(Path(path) for path in weather_files)

    if weather_dir:
        folder = Path(weather_dir)
        if not folder.exists() or not folder.is_dir():
            raise NotADirectoryError(f"資料夾不存在或不是資料夾: {folder}")

        allowed_suffixes = {".txt", ".json", ".csv", ".fw50"}
        folder_files = sorted(
            path for path in folder.iterdir()
            if path.is_file() and (path.suffix.lower() in allowed_suffixes or not path.suffix)
        )
        files.extend(folder_files)

    if not files:
        raise ValueError("請提供 --weather-text、--weather-file 或 --weather-dir。")

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
            raise FileNotFoundError(f"找不到天氣資料檔案: {file_path}")
        content = read_single_weather_file(file_path)
        blocks.append(f"### 來源檔案: {file_path.name}\n{content}")

    return "\n\n".join(blocks).strip()


def read_single_weather_file(file_path):
    file_path = Path(file_path)

    suffix = file_path.suffix.lower()
    raw = read_text_with_fallback(file_path)

    if suffix == ".json":
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"JSON 檔案格式錯誤: {file_path}") from exc
        return json.dumps(payload, ensure_ascii=False, indent=2)

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


def build_prompt(weather_data_text, location, language, style):
    instructions = (
        "你是氣象文字編輯助手。"
        "請根據提供的天氣資料，撰寫清楚且可對外發布的天氣描述。"
        "若資料不足，請明確指出不足之處，不要捏造。\n\n"
    )
    task = (
        f"請以{language}撰寫 {location} 的天氣描述，風格為：{style}。\n"
        "請輸出以下段落：[括號內為該段落之輸出要求]\n"
        "天氣型態：[三日內依序的天氣型態(ex:晴朗、多雲到晴、陰雨等)，以及天氣系統(ex:鋒面、梅雨、西南氣流、颱風、東北風、東北季風、冷氣團、強烈大陸冷氣團、寒流等)，不要出現天氣數據，不要有因應作為，降雨預報不提，風場變化不提；若「穩定」與「不穩定」同時出現，則優先保留「不穩定」描述，可拿掉穩定字眼。限30字以內]\n"
        "降雨預報：[三日內降雨預報，不要提及機率高低，在天氣概況文本中擷取有關降雨的描述，例如零星短暫陣雨、局部短暫陣雨、午後局部短暫陣雨或雷雨等。限30字以內]\n"
        "降雨關注區域：[桃園市境內有顯著可能性降雨的行政區，每個行政區用「、」隔開，其餘字眼不必出現。若所有行政區皆達90%以上降雨機率，則標註「桃園市全區」。]\n"
        "風勢概況：[三日內風勢變化，含風向與風力，可用蒲福風級數據輔助描述，限30字以內]\n"
        "風勢關注區域：[桃園市境內風勢較強烈的行政區，每個行政區用「、」隔開，其餘字眼不必出現]\n"
        "<PS> 以上段落撰寫的統一注意事項：用「今日、明日或N日」來描述三天內的哪一天，N為去除月份的日期，若遇到「前兩日」的情況可以用「今明」代替\n"
        f"{weather_data_text}"
        "除了上述段落之外，接著撰寫你如何判斷降雨與風勢關注區域的邏輯，並說明你是如何從天氣資料中擷取出相關資訊的。"
    )
    return instructions + task


def call_gemini(api_key, model, prompt, max_tokens, temperature):
    try:
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
        raise ValueError("找不到 API Key，請使用 --api-key 或設定環境變數 GEMINI_API_KEY。")

    weather_data_text = read_weather_input(args.weather_text, args.weather_file, args.weather_dir)
    prompt = build_prompt(weather_data_text, args.location, args.language, args.style)

    generated = call_gemini(
        api_key=api_key,
        model=args.model,
        prompt=prompt,
        max_tokens=args.max_tokens,
        temperature=args.temperature,
    )

    output_path = Path(args.output)
    output_path.write_text(generated + "\n", encoding="utf-8")

    print("=== 天氣描述產生完成 ===")
    print(generated)
    print(f"\n已輸出至: {output_path.resolve()}")


if __name__ == "__main__":
    main()