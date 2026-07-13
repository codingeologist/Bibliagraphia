import re
import json
import nltk
import string
import pandas as pd
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from collections import Counter


nltk.download("punkt_tab")
nltk.download("stopwords")
nltk.download("wordnet")
lemmatizer = WordNetLemmatizer()


def get_book_data(version: str, book=None) -> str:
	"""get all chapter data as string"""

	df = pd.read_csv("json/verses.csv")
	df = df.loc[df["version_code"] == version]
	if book:
		df = df.loc[df["book_code"] == book]

	text = df["text"].tolist()
	text_data = ", ".join(text)
	
	return text_data


# Load Data
data = get_book_data(version="KJV")
data = re.sub(r"[^\w\s]", "", data).lower()

# Define Stopwords
kjv_stopwords = {
	"thee",
	"thou",
	"ye",
	"thy",
	"thine",
	"hath",
	"hast",
	"doth",
	"saith",
	"said",
	"say",
	"saying",
	"upon",
	"unto",
	"verily",
	"behold",
	"was",
	"shall",
	"shalt",
	"came",
	"also",
	"let",
	"went",
	"even",
	"every",
	"therefore",
	"make",
	"may",
	"away",
	"put",
	"thereof",
	"whereof",
	"forth",
	"neither",
	"brought",
	"two",
	"according",
	"took",
	"thus",
	"bring",
	"yet",
	"spake",
	"yea",
	"howbeit",
	"does",
	"doeth",
	"thither",
	"wherewith"
}

# past tense kjv -eth words where the source word ends in e
kjv_past_tense_1 = [
	"giveth",	
	"endureth"
]

# past tense kjv -eth words where the source word does not end in e
kjv_past_tense_2 = [
	"worketh",
	"liveth",
	"overcometh",
	"belongeth"
]

# past tense -ed words
# need to map these words out manually as some words do have a double meaning
# when with or without the -ed suffix such as;
# 	rear/rearer, saddle/saddled found/founded close/closed etc... (keep these)
# removing the -ed keeps the source word
kjv_past_tense_3 = [
	"pitched",
	"fulfilled",
	"wandered",
	"sowed",
	"attained",
	"nourished",
	"refreshed",
	"warned",
	"gained",
	"manifested",
	"watered",
	"distressed",
	"interpreted",
	"arrayed",
	"lacked",
	"esteemed",
	"lamented",
	"recovered",
	"published",
	"unpunished",
	"sojourned",
	"weaned",
	"rolled",
	"failed",
	"fainted",
	"erred",
	"accounted",
	"belonged",
	"redeemed",
	"appeared",
	"scattered",
	"established"
]

# -ed past tense source words ending in e
kjv_past_tense_4 = [
	"caused",
	"reproved",
	"recompensed",
	"dispersed",
	"speckled",
	"observed",
	"enlarged",
	"hoped",
	"coupled",
	"despised",
	"baptized",
	"judged",
	"prepared",
	"believed"
]

# -ed past tense source words that should not end in a double letter e.g. pp/tt
kjv_past_tense_5 = [
	"committed",
	"dipped",
	"stripped",
	"worshipped"
]

# plural -s words

# -ied words

# kjv -est words
kjv_est_words = [
	"knowest"
]

stop_words = set(stopwords.words("english")).union(kjv_stopwords)

# Tokenise, lemmatise and filter
tokens = word_tokenize(data)
filtered_words = [word for word in tokens if word not in stop_words and word.isalpha()]
filtered_words = [word if word not in kjv_past_tense_1 else word[:-2] for word in filtered_words]
filtered_words = [word if word not in kjv_past_tense_2 else word[:-3] for word in filtered_words]
filtered_words = [word if word not in kjv_past_tense_3 else word[:-2] for word in filtered_words]
filtered_words = [word if word not in kjv_past_tense_4 else word[:-1] for word in filtered_words]
lemmatized_words = [lemmatizer.lemmatize(word) for word in filtered_words]

print(f"tokens: {len(tokens)}")
print(f"filtered: {len(filtered_words)}")
print(f"lemmatised: {len(lemmatized_words)}")

# Get frequent words
counts = Counter(filtered_words)
counts = {key:val for key, val in counts.items() if val >= 10}
counts = {key:val for key, val in sorted(counts.items(), key=lambda item: item[1])}

print(f"topics: {len(counts)}")

with open("topics.json", "w", encoding="utf-8") as file:
	json.dump(counts, file, ensure_ascii=False, indent=2)

with open("tokens.json", "w", encoding="utf-8") as file:
	json.dump(tokens, file, ensure_ascii=False, indent=2)

with open("lemmatized.json", "w", encoding="utf-8") as file:
	json.dump(lemmatized_words, file, ensure_ascii=False, indent=2)

with open("filtered_words.json", "w", encoding="utf-8") as file:
	json.dump(filtered_words, file, ensure_ascii=False, indent=2)
