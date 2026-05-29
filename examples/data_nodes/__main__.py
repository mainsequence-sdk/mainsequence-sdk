import argparse

from examples.data_nodes.simple_data_nodes import run_data_node_examples


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
        run_data_node_examples()


if __name__ == "__main__":
    main()
