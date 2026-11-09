"""Fixed wire limits, measured in encoded bytes except for text character caps."""

MAX_FIELD_BYTES = 1 << 20
MAX_FRAME_BODY_BYTES = 2 * MAX_FIELD_BYTES
MAX_TAG_BYTES = 32
MAX_KEY_CHARS = 512
MAX_CHANNEL_CHARS = 256
