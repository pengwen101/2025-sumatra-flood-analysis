import pandas as pd
from Sastrawi.StopWordRemover.StopWordRemoverFactory import StopWordRemoverFactory
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
import swifter
from collections import Counter
from itertools import chain

factory = StopWordRemoverFactory()
stop_words = factory.get_stop_words()
factory = StemmerFactory()
stemmer = factory.create_stemmer()

stop_words_set = set(stop_words) 
comments_df = pd.read_csv("preprocessed/comments_preprocessed_1.csv")

print("Cleaning text...")
comments_df['preprocessed_full'] = (
    comments_df['preprocessed']
    .str.replace(r'[^a-zA-Z0-9\s]', ' ', regex=True)
    .str.replace(r'\s+', ' ', regex=True)
    .str.strip()
)

print("Extracting unique words...")
split_words = comments_df['preprocessed_full'].str.split()

unique_words = set(
    word for word in chain.from_iterable(split_words) 
    if word not in stop_words_set
)

print(f"Stemming {len(unique_words)} unique words...")
unique_series = pd.Series(list(unique_words))
stemmed_series = unique_series.swifter.apply(stemmer.stem)

word_to_stem_map = dict(zip(unique_series, stemmed_series))

print("Applying mapping to dataset...")

def fast_process(text, mapper):
    return " ".join([mapper[word] for word in text.split() if word in mapper])

comments_df['preprocessed_full'] = comments_df['preprocessed_full'].apply(
    lambda x: fast_process(x, word_to_stem_map)
)

comments_df.to_csv("preprocessed/comments_preprocessed_2.csv")
print("Done!")