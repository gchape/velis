from velis import app, banner
from velis.config import Config


def main() -> None:
    """Load configuration, set up logging and start the server."""
    config = Config.from_env()
    config.configure_logging()
    banner.show()
    app.run(config)


if __name__ == "__main__":
    main()
