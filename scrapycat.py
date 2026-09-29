from cat import hook, StrayCat, AgenticWorkflowOutput, BillTheLizard
from typing import Dict, Any

from .core.crawler import crawl4ai_setup_command
from .core.processor import process_scrapycat_command


@hook(priority=9)
async def lizard_notify_plugin_installation(plugin_id: str, plugin_path: str, lizard: BillTheLizard):
    settings = await lizard.mad_hatter.get_plugin().load_settings()
    await crawl4ai_setup_command(settings)


@hook(priority=9)
async def after_lizard_bootstrap(lizard: BillTheLizard):
    settings = await lizard.mad_hatter.get_plugin().load_settings()
    await crawl4ai_setup_command(settings)


@hook(priority=9)
async def agent_fast_reply(cat: StrayCat) -> AgenticWorkflowOutput | None:
    # a message blocked by guard-plugin is not worked on: its reply is the block
    if getattr(cat.working_memory, "guard_blocked", None) == id(cat.working_memory.user_message):
        return None

    user_message: str = cat.working_memory.user_message.text

    if not user_message.startswith("@scrapycat"):
        return None

    # Check if only scheduled scraping is enabled
    settings: Dict[str, Any] =await cat.mad_hatter.get_plugin().load_settings()
    if settings.get("only_scheduled", False):
        # Skip processing and let the chatbot handle it normally
        return None

    # Process the scrapycat command using the extracted function
    await crawl4ai_setup_command(settings, cat=cat)
    result = await process_scrapycat_command(user_message=user_message, cat=cat)
    return AgenticWorkflowOutput(output=result)


# Empty hook placeholders to skip the warning about missing hooks
@hook()
def scrapycat_before_scraping(context: Dict[str, Any], cat: StrayCat):
    return context

@hook()
def scrapycat_after_scraping(context: Dict[str, Any], cat: StrayCat):
    return context

@hook()
def scrapycat_after_ingestion(context: Dict[str, Any], cat: StrayCat):
    return context