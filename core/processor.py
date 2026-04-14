import tempfile
from cat import StrayCat
from cat.log import log
from cat.mixins import BotMixin
from cat.services.memory.models import VectorMemoryType
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.processors.pdf import PDFCrawlerStrategy, PDFContentScrapingStrategy
from typing import Dict, List, Any
import os
import urllib.parse
import time

from .context import ScrapyCatContext
from .crawler import crawler
from ..utils.url_utils import clean_url, normalize_url_with_protocol, normalize_domain, validate_url
from ..utils.robots import load_robots_txt


async def _crawl4i(url: str) -> str:
    """Use crawl4ai to extract content from a URL"""
    if not url.endswith(".pdf"):
        async with AsyncWebCrawler() as mdcrawler:
            config = CrawlerRunConfig(
                excluded_tags=["form", "header", "footer", "nav"],
                exclude_social_media_links=True,
                exclude_external_images=True,
                remove_overlay_elements=True,
            )
            md_generator = DefaultMarkdownGenerator(
                options={"ignore_links": True, "ignore_images": True}
            )
            config.markdown_generator = md_generator
            result = await mdcrawler.arun(url, config=config)
            return result.markdown

    pdf_crawler_strategy = PDFCrawlerStrategy()
    async with AsyncWebCrawler(crawler_strategy=pdf_crawler_strategy) as pdfcrawler:
        pdf_scraping_strategy = PDFContentScrapingStrategy()
        run_config = CrawlerRunConfig(scraping_strategy=pdf_scraping_strategy)
        result = await pdfcrawler.arun(url=url, config=run_config)
        if result.markdown and hasattr(result.markdown, "raw_markdown"):
            return result.markdown.raw_markdown
        return ""


async def _remove_old_vectors(metadata: Dict[str, Any], scraped_url: str, cat: BotMixin):
    metadata_to_find = {k: v for k, v in metadata.items() if v == scraped_url}
    collection_name = str(VectorMemoryType.DECLARATIVE)
    if isinstance(cat, StrayCat):
        metadata_to_find["chat_id"] = cat.id
        collection_name = str(VectorMemoryType.EPISODIC)

    await cat.vector_memory_handler.delete_tenant_points(collection_name, metadata=metadata_to_find)


async def process_scrapycat_command(user_message: str, cat: BotMixin, scheduled: bool = False) -> str:
    """Process a scrapycat command and return the result message"""
    settings: Dict[str, Any] = await cat.mad_hatter.get_plugin().load_settings()

    # Parse command arguments
    parts: List[str] = user_message.split()
    if len(parts) < 2:
        return "Usage: @scrapycat <url1> [url2 ...] [--allow <allowed_url1> [allowed_url2 ...]]"

    # Find --allow flag position
    allow_index: int = -1
    for i, part in enumerate(parts):
        if part == "--allow":
            allow_index = i
            break

    # Extract starting URLs and allowed URLs
    if allow_index == -1:
        # No --allow flag, all URLs after @scrapycat are starting URLs
        starting_urls: List[str] = [
            normalize_url_with_protocol(clean_url(url))
            for url in parts[1:]
            if validate_url(url)
        ]
        command_allowed_urls: List[str] = []
    else:
        # Split at --allow flag
        starting_urls = [
            normalize_url_with_protocol(clean_url(url))
            for url in parts[1:allow_index]
            if validate_url(url)
        ]
        # Allow more flexible validation for allowed URLs (domains without protocols are OK)
        command_allowed_urls = []
        for url in parts[allow_index + 1:]:
            cleaned_url: str = clean_url(url)
            if validate_url(cleaned_url):
                command_allowed_urls.append(cleaned_url)
            else:
                # Log validation issues for debugging
                log.warning(f"Invalid allowed URL ignored: {cleaned_url}")

    if not starting_urls:
        log.error("No valid starting URLs provided")
        return "Error: No valid starting URLs provided"

    # Initialize context for this run
    ctx: ScrapyCatContext = ScrapyCatContext()
    ctx.command = user_message  # Store the command that triggered this session
    ctx.scheduled = scheduled  # Mark if this is a scheduled run
    ctx.ingest_pdf = settings.get("ingest_pdf", False)
    ctx.skip_get_params = settings.get("skip_get_params", False)
    ctx.max_depth = settings.get("max_depth", -1)
    ctx.use_crawl4ai = settings.get("use_crawl4ai", False)
    ctx.follow_robots_txt = settings.get("follow_robots_txt", False)

    # Build allowed domains set (for single-page scraping only, no recursion)
    # 1. Add domains from settings (normalize them for consistency)
    settings_allowed_urls: List[str] = [
        normalize_url_with_protocol(url.strip()) for url in settings.get("allowed_extra_roots", "").split(",")
        if url.strip() and validate_url(url.strip())
    ]
    for url in settings_allowed_urls:
        ctx.allowed_domains.add(normalize_domain(url))

    # 2. Add domains from command --allow argument
    for url in command_allowed_urls:
        normalized_url = normalize_url_with_protocol(url)
        ctx.allowed_domains.add(normalize_domain(normalized_url))

    ctx.max_pages = settings.get("max_pages", -1)
    ctx.max_workers = settings.get("max_workers", 1)  # Default to 1 if not set
    ctx.chunk_size = settings.get("chunk_size", 512)  # Default to 512 if not set
    ctx.chunk_overlap = settings.get("chunk_overlap", 128)  # Default to 128 if not set
    ctx.page_timeout = settings.get("page_timeout", 30)  # Default to 30 seconds if not set
    ctx.user_agent = settings.get(
        "user_agent",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:55.0) Gecko/20100101 Firefox/55.0"
    )  # User agent string

    # Parse skip extensions from settings and normalize them (ensure they all start with a dot)
    skip_extensions_str = settings.get(
        "skip_extensions",
        ".jpg,.jpeg,.png,.gif,.bmp,.svg,.webp,.ico,.zip,.ods,.odt,.xls,.p7m,.rar,.mp3,.xml,.7z,.exe,.doc"
    )
    ctx.skip_extensions = [
        ext.strip() if ext.strip().startswith(".") else f".{ext.strip()}"
        for ext in skip_extensions_str.split(",") if ext.strip()
    ]

    # Extract root domains and paths from starting URLs
    ctx.root_domains = set()
    ctx.allowed_paths = set()
    for url in starting_urls:
        parsed_url: urllib.parse.ParseResult = urllib.parse.urlparse(url)
        ctx.root_domains.add(normalize_domain(parsed_url.netloc))
        # Add the path (or "/" if empty) to allowed paths
        path: str = parsed_url.path or "/"
        ctx.allowed_paths.add(path)

    # Preload robots.txt for all starting domains if robots.txt following is enabled
    if ctx.follow_robots_txt:
        all_domains = ctx.root_domains.union(ctx.allowed_domains)
        for domain in all_domains:
            load_robots_txt(ctx, domain)
        log.info(f"Robots.txt preloaded for {len(all_domains)} domains")

    log.info(
        f"ScrapyCat started: {len(starting_urls)} URLs, max_pages={ctx.max_pages}, max_depth={ctx.max_depth}, workers={ctx.max_workers}, robots.txt={ctx.follow_robots_txt}"
    )
    if ctx.allowed_domains:
        log.info(f"Single-page domains configured: {len(ctx.allowed_domains)} domains")
    if ctx.root_domains:
        log.info(f"Recursive domains configured: {len(ctx.root_domains)} domains")

    # Fire before_scraping hook with serializable context data
    try:
        context_data = ctx.to_hook_context()
        context_data = await cat.mad_hatter.execute_hook("scrapycat_before_scraping", context_data, caller=cat)
        ctx.update_from_hook_context(context_data)
    except Exception as hook_error:
        log.warning(f"Error executing before_scraping hook: {hook_error}")

    # Start crawling from all starting URLs
    response = ""
    try:
        # Record start time for the whole crawling+ingestion operation
        start_time = time.time()
        await crawler(ctx, cat, starting_urls)

        log.info(
            f"Crawling completed: {len(ctx.scraped_pages)} pages scraped, {len(ctx.failed_pages)} failed/timed out"
        )

        # Fire after_scraping hook with serializable context data
        try:
            context_data = ctx.to_hook_context()
            log.debug(
                f"Firing after_scraping hook with context data: session_id={context_data['session_id']}, command={context_data['command']}"
            )
            context_data = await cat.mad_hatter.execute_hook("scrapycat_after_scraping", context_data, caller=cat)
            ctx.update_from_hook_context(context_data)
        except Exception as hook_error:
            log.warning(f"Error executing after_scraping hook: {hook_error}")

        # Sequential ingestion after parallel scraping is complete
        if not ctx.scraped_pages:
            # Compute elapsed time even if no pages scraped
            elapsed_seconds = time.time() - start_time
            minutes = round(elapsed_seconds / 60.0, 2)

            if ctx.failed_pages:
                return f"No pages were successfully scraped. {len(ctx.failed_pages)} pages failed or timed out in {minutes} minutes."
            return f"No pages were successfully scraped in {minutes} minutes."

        ingested_count: int = 0
        for i, scraped_url in enumerate(ctx.scraped_pages):
            metadata: Dict[str, str] = {
                "url": scraped_url,
                "source": scraped_url,
                "session_id": ctx.session_id,
                "command": ctx.command
            }
            await _remove_old_vectors(metadata, scraped_url, cat)
            try:
                if ctx.use_crawl4ai:
                    # Use crawl4ai for content extraction
                    try:
                        markdown_content: str = await _crawl4i(scraped_url)
                        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".md", delete=False) as f:
                            f.write(markdown_content)
                            output_file = f.name

                        await cat.rabbit_hole.ingest_file(cat, file=output_file, metadata=metadata)
                        os.remove(output_file)
                    except Exception as crawl4ai_error:
                        log.warning(
                            f"crawl4ai failed for {scraped_url}, falling back to default method: {str(crawl4ai_error)}")
                        # Fallback to the default method
                        await cat.rabbit_hole.ingest_file(cat, file=scraped_url, metadata=metadata)
                else:
                    # Use default ingestion method
                    await cat.rabbit_hole.ingest_file(cat, file=scraped_url, metadata=metadata)

                ingested_count += 1

                # Send progress update
                if not ctx.scheduled and isinstance(cat, StrayCat):
                    await cat.notifier.send_notification(
                        f"Ingested {ingested_count}/{len(ctx.scraped_pages)} pages - Currently processing: {scraped_url}"
                    )
            except Exception as e:
                ctx.failed_pages.append(scraped_url)  # Track failed pages in context
                log.error(f"Page ingestion failed: {scraped_url} - {str(e)}")
                # Continue with next page even if one fails

        log.info(f"Ingestion completed: {ingested_count} successful, {len(ctx.failed_pages)} failed")
        # Compute elapsed time in minutes (rounded to 2 decimal places)
        elapsed_seconds = time.time() - start_time
        minutes = round(elapsed_seconds / 60.0, 2)

        # Build response message
        response: str = f"{ingested_count} URLs successfully imported in {minutes} minutes"
        if ctx.failed_pages:
            response: str = f"{ingested_count} URLs successfully imported, {len(ctx.failed_pages)} failed or timed out in {minutes} minutes"
    except Exception as e:
        error_msg = str(e)
        log.error(f"ScrapyCat operation failed: {error_msg}")
        response = f"ScrapyCat failed: {error_msg}"
    finally:
        # Fire after_ingestion hook with serializable context data
        try:
            context_data = ctx.to_hook_context()
            log.debug(
                f"Firing after_ingestion hook with context data: session_id={context_data['session_id']}, command={context_data['command']}")
            await cat.mad_hatter.execute_hook("scrapycat_after_ingestion", context_data, caller=cat)
            ctx.update_from_hook_context(context_data)
        except Exception as hook_error:
            log.warning(f"Error executing after_ingestion hook: {hook_error}")

    return response
