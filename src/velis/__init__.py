from velis.log import configure
from velis.settings import Settings


def main() -> None:
    settings = Settings.from_env()
    configure(
        level=settings.log_level,
        production=settings.production,
        log_file=settings.log_file,
    )
