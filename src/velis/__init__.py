import os


def main() -> None:
    from velis.log import configure

    configure(
        os.environ.get("VELIS_LOG_LEVEL", "INFO"),
        production=os.environ.get("VELIS_ENV") == "production",
        log_file=os.environ.get("VELIS_LOG_FILE", "/var/log/velis/velis.log"),
    )
