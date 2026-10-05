"""All test discovery paths deny Internet connections before loading fixtures."""
from tests.network_guard import install
install()
