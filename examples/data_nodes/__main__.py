import argparse

from examples.data_nodes.simple_data_nodes import build_test_time_series
from examples.data_nodes.simple_simulated_prices import (
    test_features_from_prices_local_storage,
    test_simulated_prices,
)
from examples.data_nodes.simple_tables import build_test_simple_tables


def main():
    parser = argparse.ArgumentParser(
        description="Run SDK examples for data nodes and simple-table updaters."
    )
    parser.add_argument(
        "command",
        choices=["simulated_prices", "random_data_nodes", "duck_features", "simple_tables"],
        help="Example to run.",
    )
    args = parser.parse_args()

    if args.command == "simulated_prices":
        test_simulated_prices()
    elif args.command == "random_data_nodes":
        build_test_time_series()
    elif args.command == "duck_features":
        test_features_from_prices_local_storage()
    elif args.command == "simple_tables":
        build_test_simple_tables()


if __name__ == "__main__":
    main()
