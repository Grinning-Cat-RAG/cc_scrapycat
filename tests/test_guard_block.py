"""A message blocked by guard-plugin is not worked on: its reply is the block."""
import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

PLUGIN_DIR = Path(__file__).resolve().parents[1]
FAST_REPLY = None


def _load():
    global FAST_REPLY
    import importlib.util
    import sys

    from cat.looking_glass.mad_hatter.plugin import Plugin

    # crawl4ai (a heavy dependency, with a browser) is not needed to check the fast reply: stand-ins if missing
    if importlib.util.find_spec("crawl4ai") is None:
        for name in ("crawl4ai", "crawl4ai.markdown_generation_strategy", "crawl4ai.processors",
                     "crawl4ai.processors.pdf"):
            sys.modules.setdefault(name, mock.MagicMock())

    plugin = Plugin(str(PLUGIN_DIR))
    plugin._load_decorated_functions()
    FAST_REPLY, = [h.function for h in plugin.hooks if h.name == "agent_fast_reply"]


class GuardBlockTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _load()

    def test_a_blocked_message_is_not_worked_on(self):
        # regression: the plugin did its work (commands, settings, network) on a message that guard-plugin blocked
        cat = mock.MagicMock()
        cat.working_memory = SimpleNamespace(user_message=SimpleNamespace(text="@scrapycat https://example.com"), context_memories=[])
        cat.working_memory.guard_blocked = id(cat.working_memory.user_message)
        self.assertIsNone(asyncio.run(FAST_REPLY(cat)))
        self.assertEqual(cat.mock_calls, [], "nothing of the Cat is used")

    def test_every_file_passes_the_security_scan(self):
        from cat.looking_glass.mad_hatter.plugin_extractor import PluginExtractor

        self.assertTrue(PluginExtractor._is_safe_plugin(str(PLUGIN_DIR)))


if __name__ == "__main__":
    unittest.main()
