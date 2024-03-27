import argparse
import logging
import pickle
from collections import defaultdict

import gensim
from gensim.models import Word2Vec
from nltk.corpus import stopwords

from qexp import DataCache
from qexp.extractors import WikipediaRevisionExtractor, WikitextExtractor
from qexp.Pipeline import Pipeline


def main(args: argparse.Namespace):
    model_path: str = args.model_path
    profile_dump: str = args.profile_dump
    tfidf_thresh: float = args.tfidf

    logger = logging.getLogger("main")

    logger.info("Building embeddings...")

    config = args.config

    with open(profile_dump, "rb") as f:
        profiles = pickle.load(f)

    texts = defaultdict(list)
    for start, vs in profiles.items():
        text_pipeline = Pipeline.make_pipeline(
            WikipediaRevisionExtractor(
                url=config.get("wikipedia", "endpoint"),
                cache=DataCache(config.get("data", "extracts_path")),
            ),
            WikitextExtractor(
                cache=DataCache(config.get("data", "clean_path")),
            ),
        )

        texts[start].extend(text_pipeline.run(vs))

    import nltk
    nltk.download('stopwords')

    stop = list(stopwords.words("english"))

    wv_options = {
        "vector_size": 300,
        "workers": 4,
        "window": 3,
        "shrink_windows": True,
        "epochs": 7,
        "min_count": 1,
        "negative": 5,
    }

    for key, docs in texts.items():
        processed_documents = [gensim.utils.simple_preprocess(doc) for _, doc in docs]
        processed_documents = [
            [word for word in doc if word not in stop] for doc in processed_documents
        ]
        dictionary = gensim.corpora.Dictionary(processed_documents)
        corpus = [dictionary.doc2bow(doc) for doc in processed_documents]
        tfidf = gensim.models.TfidfModel(corpus)
        corpus_tfidf = tfidf[corpus]

        filtered_words = []
        for doc in corpus_tfidf:
            for word_id, score in doc:
                if score > tfidf_thresh:
                    filtered_words.append(dictionary[word_id])

        filtered_sentences = [
            [word for word in sentence if word in filtered_words]
            for sentence in processed_documents
        ]

        model = Word2Vec(filtered_sentences, **wv_options)
        model.save(f"{model_path}{key}.model")
