import argparse

from examples.data_nodes.simple_data_nodes import build_test_time_series


def main():
    parser = argparse.ArgumentParser(description="Run SDK examples for data nodes.")
    parser.add_argument(
        "command",
        choices=[
            "random_data_nodes",
        ],
        help="Example to run.",
    )
    args = parser.parse_args()

    if args.command == "random_data_nodes":
        build_test_time_series()


if __name__ == "__main__":
    main()
