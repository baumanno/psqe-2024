import csv
import logging
import os
import pickle
import re
from collections import defaultdict
from pathlib import Path

from qexp import Config


def main(args):
    logger = logging.getLogger("main")

    logger.info("Building categories...")

    config = args.config

    extracts_path = Path(config.get("data", "extracts_path"))

    files = extracts_path.glob("*")
    article_categories = defaultdict(set)
    for file in files:
        article = file.stem

        with open(file, "r") as f:
            contents = f.read()

        categories = re.findall(r"\[\[(Category:.*?)[\]|\|]", contents)
        categories = [category.replace("Category:", "") for category in categories]

        for cat in categories:
            article_categories[article].add(cat)

    with open(config.get("results", "article_categories"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["article_id", "category"])
        for aid, cats in article_categories.items():
            for cat in cats:
                w.writerow([aid, cat])

    with open(config.get("data", "profiles_dump"), "rb") as f:
        profiles = pickle.load(f)

    with open(config.get("results", "user_categories"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["article_id", "profile_article"])
        for aid, profile in profiles.items():
            for article in profile:
                w.writerow([aid, article])

    return os.EX_OK
