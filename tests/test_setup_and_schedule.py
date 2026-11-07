"""The setup of Crawl4AI and the replacement of the scheduled job never block the event loop of the instance (every
agent and chat it serves): the setup commands run as asynchronous subprocesses, the replacement waits for a running job
asynchronously. The setup installs the browser on the machine of the instance, once per process: nothing is written in
the working directory.

Run from the core root: ``python -m pytest cat/plugins/cc_scrapycat/tests``.

The Cat and the plugin are loaded in ``setUpClass``, through the plugin loader of the Cat (which skips the tests of
the plugin): at import time this module needs only the stdlib.
"""
import asyncio
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

PLUGIN_DIR = Path(__file__).resolve().parents[1]
LOADED = set()

CRAWLER = SCHEDULER = None
HOOKS = {}
StrayCat = None


def _load():
    """Loads the plugin once, from the test classes, through the plugin loader of the Cat: the modules of the plugin
    are then taken from ``sys.modules``, under the package name the loader gives them."""
    global CRAWLER, SCHEDULER, StrayCat
    if str(PLUGIN_DIR) in LOADED:
        return
    import importlib.util

    from cat.looking_glass.mad_hatter.plugin import Plugin
    from cat import StrayCat as _StrayCat

    # crawl4ai (a heavy dependency, with a browser) is not needed here: stand-ins if missing
    if "crawl4ai" not in sys.modules and importlib.util.find_spec("crawl4ai") is None:
        for name in ("crawl4ai", "crawl4ai.markdown_generation_strategy", "crawl4ai.processors",
                     "crawl4ai.processors.pdf"):
            sys.modules.setdefault(name, mock.MagicMock())

    plugin = Plugin(str(PLUGIN_DIR))
    plugin._load_decorated_functions()
    for h in plugin.hooks:
        HOOKS.setdefault(h.name, []).append(h.function)
    fast_reply, = HOOKS["agent_fast_reply"]
    package = fast_reply.__module__.rsplit(".", 1)[0]
    CRAWLER = sys.modules[f"{package}.core.crawler"]
    SCHEDULER = sys.modules[f"{package}.scheduler"]
    StrayCat = _StrayCat
    LOADED.add(str(PLUGIN_DIR))


def _hook(name, module):
    return next(f for f in HOOKS[name] if f.__module__ == module.__name__)


def _command(seconds, code=0):
    """A setup command that takes ``seconds`` and exits with ``code``."""
    return [sys.executable, "-c", f"import time, sys; time.sleep({seconds}); sys.exit({code})"]


def _blocking_run(*args, **kwargs):
    """``subprocess.run`` of a previous version: a blocking command (never a real installation in the tests)."""
    time.sleep(0.1)
    return MagicMock(returncode=0)


def _stray_cat():
    cat = MagicMock(spec=StrayCat)
    cat.notifier = MagicMock(send_notification=AsyncMock(), send_error=AsyncMock())
    return cat


async def _with_heartbeat(coro):
    """Run ``coro`` and count how many times another coroutine ran meanwhile (every 10 ms)."""
    ticks = 0

    async def heartbeat():
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    beat = asyncio.create_task(heartbeat())
    try:
        result = await coro
    finally:
        beat.cancel()
    return result, ticks


class InTemporaryDirectory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _load()

    def setUp(self):
        self.cwd = os.getcwd()
        self.tmp = tempfile.TemporaryDirectory()
        os.chdir(self.tmp.name)
        # a fresh process: the setup was never done
        setup = getattr(CRAWLER, "_setup", None)
        if isinstance(setup, dict):
            patch.dict(setup, {"done": False}).start()
        self.addCleanup(patch.stopall)
        patch.object(CRAWLER.subprocess, "run", _blocking_run).start()

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def setup(self, *commands, settings=None, cat=None):
        patch.object(CRAWLER, "SETUP_COMMANDS", tuple(commands), create=True).start()
        return CRAWLER.crawl4ai_setup_command({"use_crawl4ai": True} if settings is None else settings, cat=cat)


class TestCrawl4AISetup(InTemporaryDirectory):
    def test_the_setup_never_blocks_the_event_loop(self):
        """Regression: the setup ran ``subprocess.run`` (installing playwright, minutes) inside the asynchronous hooks:
        the event loop, and so every request served by the instance, was blocked until it ended."""
        cat = _stray_cat()

        async def main():
            return await _with_heartbeat(self.setup(_command(0.2), _command(0.1), cat=cat))

        _, ticks = asyncio.run(main())
        self.assertGreater(ticks, 10, "other coroutines run while the setup is in progress")
        # the notifications are sent (awaited), not only created
        self.assertEqual(
            [c.args[0] for c in cat.notifier.send_notification.await_args_list],
            ["Running crawl4ai setup...", "Crawl4AI setup completed successfully."],
        )
        cat.notifier.send_error.assert_not_awaited()

    def test_the_setup_writes_nothing_in_the_working_directory(self):
        """Regression: the setup was marked done by a ``.crawl4ai`` file in the working directory."""
        asyncio.run(self.setup(_command(0)))
        self.assertEqual(os.listdir("."), [])

    def test_the_setup_runs_once_per_process(self):
        runs = []

        async def main():
            real = asyncio.create_subprocess_exec

            async def counting(*args, **kwargs):
                runs.append(args)
                return await real(*args, **kwargs)

            with patch.object(CRAWLER.asyncio, "create_subprocess_exec", counting):
                # concurrent requests (agents, chats) while the setup is in progress: it runs once
                await asyncio.gather(*(self.setup(_command(0.05)) for _ in range(3)))
                await self.setup(_command(0.05))

        asyncio.run(main())
        self.assertEqual(len(runs), 1)

    def test_a_failed_setup_is_told_and_tried_again(self):
        cat = _stray_cat()
        asyncio.run(self.setup(_command(0), _command(0, code=3), _command(0), cat=cat))
        (error,) = [c.args[0] for c in cat.notifier.send_error.await_args_list]
        self.assertIn("Error during Crawl4AI setup", error)
        self.assertEqual(
            [c.args[0] for c in cat.notifier.send_notification.await_args_list], ["Running crawl4ai setup..."],
        )
        # not done: the next request tries again
        cat = _stray_cat()
        asyncio.run(self.setup(_command(0), cat=cat))
        self.assertEqual(cat.notifier.send_notification.await_count, 2)

    def test_a_missing_command_is_told(self):
        cat = _stray_cat()
        asyncio.run(self.setup(["a-command-that-does-not-exist-anywhere"], cat=cat))
        (error,) = [c.args[0] for c in cat.notifier.send_error.await_args_list]
        self.assertIn("not found", error)

    def test_errors_without_a_chat_are_only_logged(self):
        for command in (_command(0, code=1), ["a-command-that-does-not-exist-anywhere"]):
            with self.subTest(command=command):
                agent = MagicMock()  # a CheshireCat, or the lizard: nobody to notify
                asyncio.run(self.setup(command, cat=agent))
                self.assertEqual(agent.mock_calls, [])

    def test_disabled_crawl4ai_is_not_set_up(self):
        runs = []
        with patch.object(CRAWLER.asyncio, "create_subprocess_exec", AsyncMock(side_effect=runs.append)):
            asyncio.run(self.setup(_command(0), settings={}))
        self.assertEqual(runs, [])

    def test_the_default_commands(self):
        self.assertEqual(CRAWLER.SETUP_COMMANDS, (
            ["uv", "run", "crawl4ai-setup"],
            ["uv", "run", "playwright", "install"],
            ["uv", "run", "playwright", "install-deps"],
        ))

    def test_the_lock_of_the_setup_follows_the_event_loop(self):
        """The Cat may run hooks on more than one event loop (e.g. the tests): the lock is of the running loop."""
        async def main():
            await asyncio.gather(*(self.setup(_command(0.01)) for _ in range(2)))

        for _ in range(2):
            asyncio.run(main())
            CRAWLER._setup["done"] = False


class TestHooksAwaitTheSetup(InTemporaryDirectory):
    def test_every_hook_awaits_the_setup(self):
        calls = []

        async def setup(settings, cat=None):
            await asyncio.sleep(0)
            calls.append((settings, cat))

        settings = {"use_crawl4ai": True, "scheduled_command": ""}
        lizard = MagicMock()
        lizard.mad_hatter.get_plugin.return_value.load_settings = AsyncMock(return_value=settings)

        cat = _stray_cat()
        cat.working_memory = MagicMock(user_message=MagicMock(text="@scrapycat https://example.com"))
        cat.mad_hatter.get_plugin.return_value.load_settings = AsyncMock(return_value=settings)

        package = SCHEDULER.__name__.rsplit(".", 1)[0]
        scrapycat = sys.modules[f"{package}.scrapycat"]
        with patch.object(scrapycat, "crawl4ai_setup_command", setup), \
                patch.object(scrapycat, "process_scrapycat_command", AsyncMock(return_value="done")):
            asyncio.run(_hook("lizard_notify_plugin_installation", scrapycat)("cc_scrapycat", "path", lizard))
            asyncio.run(_hook("after_lizard_bootstrap", scrapycat)(lizard))
            reply = asyncio.run(_hook("agent_fast_reply", scrapycat)(cat))
        self.assertEqual(reply.output, "done")
        self.assertEqual(calls, [(settings, None), (settings, None), (settings, cat)])


class WhiteRabbit:
    """The scheduler of the core: the job of the agent runs for ``running`` more checks, then it is scheduled."""

    def __init__(self, running=0, exists=True):
        from cat.core_plugins.white_rabbit.white_rabbit import JobStatus

        self.status = JobStatus
        self.running = running
        self.exists = exists
        self.removed = []
        self.checks = 0

    def get_job(self, job_id):
        self.checks += 1
        if not self.exists:
            return None
        if self.running > 0:
            self.running -= 1
            return MagicMock(status=self.status.RUNNING)
        return MagicMock(status=self.status.SCHEDULED)

    def remove_job(self, job_id):
        self.removed.append(job_id)
        self.exists = False


def _agent(active):
    cat = MagicMock()
    cat.agent_key = "agent"
    plugin = cat.mad_hatter.get_plugin.return_value
    plugin.id = "cc_scrapycat"
    plugin.load_settings = AsyncMock(return_value={"use_crawl4ai": False, "scheduled_command": ""})
    cat.mad_hatter.active_plugins = ["cc_scrapycat"] if active else []
    return cat


class TestScheduleReplacement(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _load()

    def run_hook(self, name, *args, white_rabbit):
        lizard = MagicMock(white_rabbit=white_rabbit)
        with patch.object(SCHEDULER, "BillTheLizard", return_value=lizard), \
                patch.object(SCHEDULER, "JOB_POLL_SECONDS", 0.05, create=True):
            return asyncio.run(_with_heartbeat(_hook(name, SCHEDULER)(*args)))

    def test_waiting_for_a_running_job_never_blocks_the_event_loop(self):
        """Regression: the replacement waited for a running job with ``time.sleep(5)`` inside the asynchronous hooks:
        the event loop, and so every request served by the instance, was blocked while the job ran."""
        white_rabbit = WhiteRabbit(running=3)
        begin = time.monotonic()
        _, ticks = self.run_hook("after_plugin_toggling_on_agent", "cc_scrapycat", _agent(False),
                                 white_rabbit=white_rabbit)
        self.assertLess(time.monotonic() - begin, 1.0)
        self.assertGreater(ticks, 8, "other coroutines run while the job is running")
        self.assertEqual(white_rabbit.removed, ["scrapycat_scheduled_scraping:agent"])
        self.assertEqual(white_rabbit.checks, 4)

    def test_the_settings_update_replaces_the_job(self):
        white_rabbit = WhiteRabbit(running=2)
        cat = _agent(True)
        with patch.object(SCHEDULER, "_setup_scrapycat_schedule", AsyncMock()) as setup:
            _, ticks = self.run_hook("after_plugin_settings_update", "cc_scrapycat", {}, cat, white_rabbit=white_rabbit)
        self.assertGreater(ticks, 5)
        self.assertEqual(white_rabbit.removed, ["scrapycat_scheduled_scraping:agent"])
        setup.assert_awaited_once_with(
            "scrapycat_scheduled_scraping:agent", {"use_crawl4ai": False, "scheduled_command": ""}, "agent",
        )

    def test_no_job_nothing_to_remove(self):
        white_rabbit = WhiteRabbit(exists=False)
        self.run_hook("after_plugin_toggling_on_agent", "cc_scrapycat", _agent(False), white_rabbit=white_rabbit)
        self.assertEqual(white_rabbit.removed, [])

    def test_the_activation_sets_up_crawl4ai_and_the_job(self):
        cat = _agent(True)
        with patch.object(SCHEDULER, "crawl4ai_setup_command", AsyncMock()) as crawl4ai, \
                patch.object(SCHEDULER, "_setup_scrapycat_schedule", AsyncMock()) as setup:
            self.run_hook("after_plugin_toggling_on_agent", "cc_scrapycat", cat, white_rabbit=WhiteRabbit())
        crawl4ai.assert_awaited_once_with({"use_crawl4ai": False, "scheduled_command": ""})
        setup.assert_awaited_once()

    def test_the_hooks_of_another_plugin_are_ignored(self):
        white_rabbit = WhiteRabbit()
        for name, args in (("after_plugin_toggling_on_agent", ("another", _agent(False))),
                           ("after_plugin_settings_update", ("another", {}, _agent(True)))):
            with self.subTest(hook=name):
                self.run_hook(name, *args, white_rabbit=white_rabbit)
        self.assertEqual((white_rabbit.checks, white_rabbit.removed), (0, []))


if __name__ == "__main__":
    unittest.main()
