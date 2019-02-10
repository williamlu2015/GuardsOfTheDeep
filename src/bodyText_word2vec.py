import json
import logging
import multiprocessing
import os

import pandas as pd

from gensim.models import Word2Vec
from gensim.utils import simple_preprocess


def main():
    body_texts = _load_body_texts()
    corpus = _preprocess_body_texts(body_texts)

    _save_corpus(corpus)
    _train_model(corpus)


def _load_body_texts():
    print("Started loading bodyTexts")

    df = pd.read_csv("../dataset/dataset.csv", sep=",", usecols=[
        "bodyText"
    ], dtype={
        "bodyText": str
    })

    result = df["bodyText"].tolist()

    print("Finished loading bodyTexts")
    return result


def _preprocess_body_texts(body_texts, progress=10000):
    print("Started preprocessing bodyTexts")

    result = []
    for index, body_text in enumerate(body_texts):
        if pd.notnull(body_text):
            result.append(simple_preprocess(body_text))

        if index % progress == 0:
            print(f"Finished processing {index}th bodyText")

    print("Finished preprocessing bodyTexts")
    return result


def _save_corpus(corpus):
    print("Started saving corpus")

    os.makedirs("../corpus", exist_ok=True)
    with open("../corpus/bodyText.json", "w") as file:
        json.dump(corpus, file)

    print("Finished saving corpus")


def _train_model(corpus):
    logging.basicConfig(
        format="%(levelname)s - %(asctime)s: %(message)s", datefmt="%H:%M:%S",
        level=logging.INFO
    )

    num_cores = multiprocessing.cpu_count()
    model = Word2Vec(size=100, window=5, min_count=5, workers=num_cores)

    model.build_vocab(corpus, progress_per=10000)
    model.train(corpus, total_examples=len(corpus), epochs=10)

    os.makedirs("../models", exist_ok=True)
    model.save("../models/bodyText_word2vec.model")


if __name__ == "__main__":
    main()
