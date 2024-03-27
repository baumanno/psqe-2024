import argparse
import logging
import sys
from pathlib import Path

from qexp import Config, experiments, steps

if __name__ == "__main__":
    levels = (
        logging.INFO,
        logging.DEBUG,
    )
    logger = logging.getLogger("main")

    parser = argparse.ArgumentParser(
        prog="main.py",
        description="Dispatches sub-commands for the required steps in the pipeline",
    )
    parser.add_argument("--verbose", "-v", action="count", default=0)
    parser.add_argument("--config", default="config.toml")
    subparsers = parser.add_subparsers(required=True, help="sub-command help")

    parser_categories = subparsers.add_parser(
        "categories", help="Collect category labels from the cached articles"
    )
    parser_categories.set_defaults(func=steps.build_categories)

    parser_embeddings = subparsers.add_parser(
        "embeddings", help="Generate embeddings for user profiles"
    )
    parser_embeddings.set_defaults(func=steps.build_embeddings)
    parser_embeddings.add_argument(
        "--tfidf",
        "-t",
        type=float,
        default=0.0,
        help="Terms with TF-IDF less than this value will not be included in training",
    )
    parser_embeddings.add_argument("model_path", help="Directory to save models")
    parser_embeddings.add_argument(
        "profile_dump", help="pickle storing the profile data"
    )

    parser_kg = subparsers.add_parser("kg", help="Build the KG of Wikipedia articles")
    parser_kg.set_defaults(func=steps.build_graph)

    parser_priming = subparsers.add_parser("priming", help="???")
    parser_priming.set_defaults(func=steps.build_priming)

    parser_profiles = subparsers.add_parser(
        "profiles", help="Build the profiles of Wikipedia articles"
    )
    parser_profiles.set_defaults(func=steps.build_profiles)

    parser_search_config = subparsers.add_parser(
        "searchconfig", help="Build the configuration for the experiment setup"
    )
    parser_search_config.set_defaults(func=steps.build_search_config)

    parser_exp_1 = subparsers.add_parser(
        "experiment1", help="Run experiment on small corpus"
    )
    parser_exp_1.add_argument("--model-path", type=str)
    parser_exp_1.add_argument("--results-path", type=str)
    parser_exp_1.set_defaults(func=experiments.experiment1)

    parser_exp_2 = subparsers.add_parser(
        "experiment2", help="Run experiment on large corpus"
    )
    parser_exp_2.add_argument("--model-path", type=str)
    parser_exp_2.add_argument("--results-path", type=str)
    parser_exp_2.set_defaults(func=experiments.experiment2)

    # Parse args for the first time to get verbosity and config
    args = parser.parse_args(sys.argv[1:])

    logging.basicConfig(
        level=levels[min(args.verbose, len(levels) - 1)],
        format="%(asctime)-15s %(name)-5s %(levelname)-8s %(message)s",
    )

    config = Config(Path(args.config))
    logging.info("Read config from {}".format(args.config))

    parser_categories.set_defaults(config=config)
    parser_embeddings.set_defaults(config=config)
    parser_kg.set_defaults(config=config)
    parser_priming.set_defaults(config=config)
    parser_profiles.set_defaults(config=config)
    parser_search_config.set_defaults(config=config)
    parser_exp_1.set_defaults(config=config)
    parser_exp_2.set_defaults(config=config)

    # Parse args again to configure dispatch
    args = parser.parse_args(sys.argv[1:])

    args.func(args)
