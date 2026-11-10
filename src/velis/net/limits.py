"""Fixed wire limits, measured in encoded bytes except for text character caps.

Frame limits are not declared here: each codec derives its largest possible
body from its own field table (see ``Codec.max_body``), so they cannot drift.
"""

MAX_FIELD_BYTES = 1 << 20
MAX_TAG_BYTES = 32
MAX_KEY_CHARS = 512
MAX_CHANNEL_CHARS = 256
MAX_UTF8_BYTES_PER_CHAR = 4
MAX_CHANNEL_BYTES = MAX_CHANNEL_CHARS * MAX_UTF8_BYTES_PER_CHAR
