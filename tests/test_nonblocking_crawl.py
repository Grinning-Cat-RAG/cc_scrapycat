"""The crawl downloads the pages and the robots.txt files without blocking the event loop of the instance (every agent
and chat it serves), with the user agent and the timeout of the agent, and without sharing connections or cookies with
the crawls of the other agents.

Run from the core root: ``python -m pytest cat/plugins/cc_scrapycat/tests``.

The Cat imports every ``.py`` file of the plugin, tests included: at import time this module needs only the stdlib,
the Cat, httpx and the plugin are loaded in ``setUpClass`` (and in the fakes, when they run).
"""
import asyncio
import sys
import time
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

PLUGIN_DIR = Path(__file__).resolve().parents[1]
LOADED = set()

CRAWLER = ROBOTS = PROCESSOR = CONTEXT = None


def _load():
    """Loads the plugin once, from the test classes: the Cat imports the modules of the plugin (tests included) again
    when it loads it, so ``sys.modules`` (where unittest looks for ``setUpModule``) may hold another copy of this module:
    the globals are set here, in the module of the test classes."""
    global CRAWLER, ROBOTS, PROCESSOR, CONTEXT
    if str(PLUGIN_DIR) in LOADED:
        return
    import importlib.util

    from cat.looking_glass.mad_hatter.plugin import Plugin

    # crawl4ai (a heavy dependency, with a browser) is not needed to crawl with the default method: stand-ins if missing
    if "crawl4ai" not in sys.modules and importlib.util.find_spec("crawl4ai") is None:
        for name in ("crawl4ai", "crawl4ai.markdown_generation_strategy", "crawl4ai.processors",
                     "crawl4ai.processors.pdf"):
            sys.modules.setdefault(name, mock.MagicMock())

    plugin = Plugin(str(PLUGIN_DIR))
    plugin._load_decorated_functions()
    fast_reply, = [h.function for h in plugin.hooks if h.name == "agent_fast_reply"]
    package = fast_reply.__module__.rsplit(".", 1)[0]
    CRAWLER = sys.modules[f"{package}.core.crawler"]
    ROBOTS = sys.modules[f"{package}.utils.robots"]
    PROCESSOR = sys.modules[f"{package}.core.processor"]
    CONTEXT = sys.modules[f"{package}.core.context"]
    LOADED.add(str(PLUGIN_DIR))


class Site:
    """Some websites, behind the real HTTP client of the plugin (``httpx.MockTransport``): ``/`` links ``/a`` and
    ``/b``, ``/a`` links ``/b``; ``robots`` is the robots.txt of every site (None: HTTP 404), ``unreachable`` the URLs
    that do not answer, ``delay`` the time every answer takes. Every page sets a cookie with the user agent that asked
    it."""

    PAGES = {
        "/": '<a href="/a">a</a> <a href="/b">b</a>',
        "/a": '<a href="/b">b</a>',
        "/b": "no links",
    }

    def __init__(self, delay=0.0, robots=None):
        self.delay = delay
        self.robots = robots
        self.unreachable = set()
        self.requests = []

    async def handle(self, request):
        import httpx

        self.requests.append(request)
        await asyncio.sleep(self.delay)
        url = str(request.url)
        if url in self.unreachable or f"{request.url.scheme}://{request.url.host}" in self.unreachable:
            raise httpx.ConnectError("injected: connection refused", request=request)
        if request.url.path == "/robots.txt":
            if self.robots is None:
                return httpx.Response(404)
            return httpx.Response(200, text=self.robots)
        if request.url.path == "/old":
            return httpx.Response(301, headers={"Location": "/b"})
        page = self.PAGES.get(request.url.path)
        if page is None:
            return httpx.Response(404, text="not found")
        return httpx.Response(
            200, text=f"<html><body>{page}</body></html>", headers={
                "Content-Type": "text/html", "Set-Cookie": f"visitor={request.headers['User-Agent']}; Path=/",
            },
        )

    def client_factory(self):
        """The client of the plugin, with its configuration, answered by the site."""
        import httpx

        def factory(ctx):
            client = ROBOTS.crawl_client(ctx)
            client._mounts = {}  # no proxy from the environment
            client._transport = httpx.MockTransport(self.handle)
            return client

        return factory

    def paths(self, host=None):
        return [r.url.path for r in self.requests if host is None or r.url.host == host]


def _ctx(user_agent="agent UA", follow_robots_txt=True, page_timeout=30, start="https://example.com/"):
    ctx = CONTEXT.ScrapyCatContext()
    ctx.user_agent = user_agent
    ctx.follow_robots_txt = follow_robots_txt
    ctx.page_timeout = page_timeout
    ctx.root_domains = {"example.com"}
    ctx.allowed_paths = {"/"}
    ctx.scheduled = True
    return ctx


def _crawl(site, *crawls):
    """Runs the crawls (ctx, start URLs) together, and returns the time they took."""

    async def main():
        with patch.object(CRAWLER, "crawl_client", site.client_factory()):
            begin = time.monotonic()
            await asyncio.gather(*(CRAWLER.crawler(ctx, MagicMock(), urls) for ctx, urls in crawls))
            return time.monotonic() - begin

    return asyncio.run(main())


class TestNonBlocking(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _load()

    def test_a_slow_site_never_blocks_the_other_requests(self):
        """Regression: the pages and the robots.txt files were downloaded with synchronous requests (``requests`` and
        ``RobotFileParser.read``) inside the asynchronous crawl: while a site answered, the event loop was blocked, and
        so every other request served by the instance."""
        site = Site(delay=0.1, robots="User-agent: *\nAllow: /\n")
        elapsed = _crawl(site, *((_ctx(f"agent {i}"), ["https://example.com/"]) for i in range(3)))
        # every crawl makes four requests of 0.1 seconds (robots.txt and three pages): three crawls in a row take 1.2
        self.assertLess(elapsed, 0.8, "the three crawls run together")
        self.assertEqual(len(site.requests), 12)

    def test_the_event_loop_keeps_running_while_a_page_downloads(self):
        site = Site(delay=0.3)

        async def main():
            ticks = 0

            async def heartbeat():
                nonlocal ticks
                while True:
                    await asyncio.sleep(0.01)
                    ticks += 1

            beat = asyncio.create_task(heartbeat())
            with patch.object(CRAWLER, "crawl_client", site.client_factory()):
                await CRAWLER.crawler(_ctx(follow_robots_txt=False), MagicMock(), ["https://example.com/b"])
            beat.cancel()
            return ticks

        self.assertGreater(asyncio.run(main()), 10)

    def test_the_client_of_a_crawl(self):
        ctx = _ctx("my agent UA", page_timeout=7)
        client = ROBOTS.crawl_client(ctx)
        self.assertEqual(client.headers["User-Agent"], "my agent UA")
        self.assertEqual((client.timeout.connect, client.timeout.read), (7, 7))
        self.assertTrue(client.follow_redirects)
        asyncio.run(client.aclose())

    def test_the_crawls_of_the_agents_share_nothing(self):
        """Regression: one ``requests.Session`` per thread, and so one for the whole event loop, served the crawls of
        every agent of the instance: the user agent of the first crawl and the cookies of every crawl went to all."""
        site = Site()
        _crawl(site, (_ctx("agent one"), ["https://example.com/"]))
        _crawl(site, (_ctx("agent two"), ["https://example.com/"]))
        # robots.txt over https and http (not found), and three pages, for every crawl
        first, second = site.requests[:5], site.requests[5:]
        self.assertEqual(len(second), 5)
        self.assertEqual({r.headers["User-Agent"] for r in first}, {"agent one"})
        self.assertEqual({r.headers["User-Agent"] for r in second}, {"agent two"})
        self.assertFalse([r for r in second if "agent one" in r.headers.get("Cookie", "")])

    def test_a_redirect_is_followed(self):
        site = Site()
        ctx = _ctx(follow_robots_txt=False)
        _crawl(site, (ctx, ["https://example.com/old"]))
        self.assertEqual(ctx.scraped_pages, ["https://example.com/old"])
        self.assertEqual(site.paths(), ["/old", "/b"])

    def test_a_page_that_does_not_answer_is_not_scraped(self):
        site = Site()
        site.unreachable.add("https://example.com/a")
        ctx = _ctx(follow_robots_txt=False)
        _crawl(site, (ctx, ["https://example.com/"]))
        self.assertEqual(sorted(ctx.scraped_pages), ["https://example.com/", "https://example.com/b"])


class TestRobots(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _load()

    def test_a_disallowed_page_is_not_downloaded(self):
        site = Site(robots="User-agent: *\nDisallow: /a\n")
        ctx = _ctx()
        _crawl(site, (ctx, ["https://example.com/"]))
        self.assertEqual(sorted(ctx.scraped_pages), ["https://example.com/", "https://example.com/b"])
        # robots.txt is read once for the domain
        self.assertEqual(site.paths(), ["/robots.txt", "/", "/b"])
        self.assertEqual(site.requests[0].headers["User-Agent"], "agent UA")

    def test_the_rules_of_the_user_agent(self):
        site = Site(robots="User-agent: agent UA\nDisallow: /\n\nUser-agent: *\nAllow: /\n")
        blocked, allowed = _ctx("agent UA"), _ctx("another UA")
        _crawl(site, (blocked, ["https://example.com/"]), (allowed, ["https://example.com/"]))
        self.assertEqual(blocked.scraped_pages, [])
        self.assertEqual(len(allowed.scraped_pages), 3)

    def test_robots_over_http_when_https_does_not_answer(self):
        site = Site(robots="User-agent: *\nDisallow: /b\n")
        site.unreachable.add("https://example.com/robots.txt")

        async def main():
            async with site.client_factory()(_ctx()) as client:
                ctx = _ctx()
                allowed = await ROBOTS.is_url_allowed_by_robots(ctx, "https://example.com/b", client)
                return allowed, ctx.robots_cache

        allowed, cache = asyncio.run(main())
        self.assertFalse(allowed)
        self.assertEqual(cache["example.com"].url, "http://example.com/robots.txt")

    def test_without_robots_every_page_is_allowed(self):
        for unreachable in (set(), {"https://example.com", "http://example.com"}):
            with self.subTest(unreachable=unreachable):
                site = Site(robots=None)
                site.unreachable = unreachable
                ctx = _ctx()
                _crawl(site, (ctx, ["https://example.com/"]))
                self.assertEqual(len(ctx.scraped_pages), 3 if not unreachable else 0)
                self.assertIsNone(ctx.robots_cache["example.com"])
                self.assertEqual(site.paths().count("/robots.txt"), 2)

    def test_robots_is_not_read_when_it_is_not_followed(self):
        site = Site(robots="User-agent: *\nDisallow: /\n")
        ctx = _ctx(follow_robots_txt=False)
        _crawl(site, (ctx, ["https://example.com/"]))
        self.assertEqual(len(ctx.scraped_pages), 3)
        self.assertNotIn("/robots.txt", site.paths())


class TestCommand(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _load()

    @staticmethod
    def _command(site, follow_robots_txt):
        cat = MagicMock()
        cat.mad_hatter.get_plugin.return_value.load_settings = AsyncMock(return_value={
            "follow_robots_txt": follow_robots_txt, "user_agent": "settings UA", "page_timeout": 5,
        })
        cat.mad_hatter.execute_hook = AsyncMock(side_effect=lambda name, data, caller: data)
        cat.rabbit_hole.ingest_file = AsyncMock()
        cat.vector_memory_handler.delete_tenant_points = AsyncMock()

        async def main():
            factory = site.client_factory()
            with patch.object(CRAWLER, "crawl_client", factory), patch.object(PROCESSOR, "crawl_client", factory):
                return await PROCESSOR.process_scrapycat_command("@scrapycat https://example.com --allow other.org", cat)

        return asyncio.run(main())

    def test_robots_is_not_preloaded_when_it_is_not_followed(self):
        site = Site(robots="User-agent: *\nDisallow: /a\n")
        result = self._command(site, follow_robots_txt=False)
        self.assertTrue(result.startswith("3 URLs successfully imported"), result)
        self.assertNotIn("/robots.txt", site.paths())

    def test_robots_is_preloaded_with_the_settings_of_the_agent(self):
        site = Site(robots="User-agent: *\nDisallow: /a\n")
        result = self._command(site, follow_robots_txt=True)
        self.assertTrue(result.startswith("2 URLs successfully imported"), result)
        # preloaded for both domains, then read from the cache
        self.assertEqual(sorted(site.paths()), ["/", "/b", "/robots.txt", "/robots.txt"])
        self.assertEqual({r.headers["User-Agent"] for r in site.requests}, {"settings UA"})


class TestPluginSafety(unittest.TestCase):
    def test_every_file_passes_the_scanner_of_the_core(self):
        from cat.looking_glass.mad_hatter.plugin_extractor import PluginExtractor

        self.assertTrue(PluginExtractor._is_safe_plugin(str(PLUGIN_DIR)))

    def test_tests_import_only_the_stdlib_at_import_time(self):
        import ast

        for test_file in sorted(Path(__file__).parent.glob("*.py")):
            tree = ast.parse(test_file.read_text(encoding="utf-8"))
            for node in tree.body:
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    names = [a.name for a in node.names] if isinstance(node, ast.Import) else [node.module or ""]
                    for name in names:
                        self.assertIn(name.split(".")[0], sys.stdlib_module_names, (test_file.name, name))


if __name__ == "__main__":
    unittest.main()
