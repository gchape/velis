from velis import banner
from velis.config import Config


def main() -> None:
    config = Config.from_env()
    config.configure_logging()
    banner.show()


if __name__ == "__main__":
    main()
