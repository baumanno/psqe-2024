import logging
import os

import pandas as pd


def main(args):
    config = args.config

    logger = logging.getLogger("main")
    logger.info("Building search config...")

    (
        pd.read_csv(config.get("data", "article_path"))[
            ["article_id", "article_title", "parent_id", "parent_title"]
        ]
        .set_index("article_id")
        .assign(augment=lambda x: [[True, False] for _ in range(len(x))])
        .explode("augment")
        .to_csv(config.get("data", "search_config"))
    )

    return os.EX_OK
