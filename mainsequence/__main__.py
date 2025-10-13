# mainsequence/__main__.py
from .cli import app


def main():
    # Typer app is callable
    app(prog_name="mainsequence")


if __name__ == "__main__":
    main()
