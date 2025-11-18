import argparse

from examples.data_nodes.simple_data_nodes import build_test_time_series
from examples.data_nodes.simple_simulated_prices import (
    test_features_from_prices_local_storage,
    test_simulated_prices,
)
import mainsequence.client as msc

def main():
    curves=msc.DataNodeStorage.get_data_between_dates_from_node_identifier(node_identifier="discount_curves")

    parser = argparse.ArgumentParser(
        description="Run data node functions: simulated prices or test time series."
    )
    parser.add_argument(
        "command",
        choices=["simulated_prices", "random_data_nodes", "duck_features"],
        help="Function to run: choose 'simulated_prices' or 'random_data_nodes'",
    )
    args = parser.parse_args()

    if args.command == "simulated_prices":
        test_simulated_prices()
    elif args.command == "random_data_nodes":
        build_test_time_series()
    elif args.command == "duck_features":
        test_features_from_prices_local_storage()


if __name__ == "__main__":
    main()
