import re
import emoji
import pandas as pd
import fasttext

slang_dict_1 = pd.read_csv("preprocessing_data/slang.csv").set_index('slang')['formal'].to_dict()
slang_dict_2 = pd.read_csv("preprocessing_data/slang_indo.csv").set_index('slang')['formal'].to_dict()
slang_dict = slang_dict_1 | slang_dict_2

model = fasttext.load_model("lid.176.bin")
NOT_ID = "[NOT INDONESIAN]"
TOO_SHORT = "[TOO_SHORT]"
UNCERTAIN = "[UNCERTAIN]"
MIN_CHARS = 3

def lowering(text: str) -> str:
    text = text.lower()
    return text

def remove_html_tags(text: str) -> str:
    text = re.sub(r'<[^>]+>', '', text)
    return text

def convert_slangs(text: str) -> str:
    words = text.split()
    return " ".join(slang_dict.get(word, word) for word in words)

def remove_emojis(text: str) -> str:
    text = emoji.replace_emoji(text, replace="")
    return text

def detect_indonesian(text: str) -> str:
    try:
        text = str(text).strip()
        clean = re.sub(r"[^a-zA-Z\s]", "", text)

        if len(clean) < MIN_CHARS:
            return TOO_SHORT

        label, prob = model.predict(text.replace("\n", " "), k=1)
        lang = label[0].replace("__label__", "")

        return text if lang == "id" else NOT_ID

    except Exception:
        return UNCERTAIN

def filter_indonesian_comments(
    input_path: str,
    output_path: str,
    text_column: str = "preprocessed",
):
    df = pd.read_csv(input_path)
    print("Initial text count", len(df))

    df["preprocessed"] = (
        df[text_column]
        .astype(str)
        .apply(detect_indonesian)
    )

    df = df[df["preprocessed"] != NOT_ID]
    df = df[df["preprocessed"] != TOO_SHORT]
    df = df[df["preprocessed"] != UNCERTAIN]
    print("Final text count", len(df))
    
    df.to_csv(output_path, index=False)
    
def preprocessing(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = remove_html_tags(text)
    text = lowering(text)
    text = remove_emojis(text)
    text = convert_slangs(text)
    text = detect_indonesian(text)
    return text

df = pd.read_csv("cache/yt_comments_non_shorts_banjir_sumatera.csv", index_col=0)
print("Initial text count", len(df))
texts = df['content']
preprocessed_texts = texts.map(lambda x: preprocessing(x))
df['preprocessed'] = preprocessed_texts
df = df.dropna()
df = df[df['preprocessed'] != ""]
df = df[df["preprocessed"] != NOT_ID]
df = df[df["preprocessed"] != TOO_SHORT]
df = df[df["preprocessed"] != UNCERTAIN]
print("Final text count", len(df))
df.to_csv("preprocessed/comments_preprocessed_1.csv", index=False)