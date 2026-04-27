import asyncio
import json
import logging
import os
from contextlib import AsyncExitStack

from agents.mcp import MCPServerSse
from agents import AsyncOpenAI, Agent, Runner, RunConfig, ModelSettings, FileSearchTool, handoff, SQLiteSession
from utils.config import Settings
from utils.model_provider import CustomModelProvider
from agents.extensions import handoff_filters
from agents.extensions.handoff_prompt import RECOMMENDED_PROMPT_PREFIX

AGENT_MD_PATH = "AGENT.md"
_log = logging.getLogger("db_assistant")


def _null_string(val) -> str:
    """Unwrap Go sql.NullString JSON: {"String": "...", "Valid": true}."""
    if isinstance(val, dict):
        return val.get("String", "") if val.get("Valid") else ""
    return str(val) if val else ""


async def initialize_schema(settings) -> None:
    """Connect to MCP, fetch table list + per-table metadata, write AGENT.md."""
    schema = settings.yandex.METADATA_SCHEMA
    async with MCPServerSse(
        name="SchemaInit",
        params={"url": settings.yandex.GET_INFO_MCP_URL, "timeout": 60},
        cache_tools_list=False,
        client_session_timeout_seconds=30,
    ) as mcp:
        list_result = await mcp.call_tool(
            "get_table_list", {"schemaName": schema, "tableName": "%"}
        )
        list_data = json.loads(list_result.content[0].text)
        table_names = [t["name"] for t in list_data.get("tables", [])]

        lines = [
            f"# Database schema: {schema}",
            "",
            "Use this schema reference when writing SQL for the user.",
            "",
        ]
        for table_name in table_names:
            meta_result = await mcp.call_tool(
                "get_metadata", {"schemaName": schema, "tableName": table_name}
            )
            meta_data = json.loads(meta_result.content[0].text)
            tables = meta_data.get("tables", [])
            if not tables:
                continue
            table = tables[0]
            table_desc = _null_string(table.get("description"))
            desc_suffix = f" — {table_desc}" if table_desc else ""
            lines.append(f"## {table_name}{desc_suffix}")
            lines.append("")
            lines.append("| Column | Type | Description |")
            lines.append("|--------|------|-------------|")
            for col in table.get("columns") or []:
                col_desc = _null_string(col.get("description"))
                lines.append(f"| {col['name']} | {col['type']} | {col_desc} |")
            lines.append("")

        content = "\n".join(lines)
        with open(AGENT_MD_PATH, "w", encoding="utf-8") as f:
            f.write(content)
        _log.info("AGENT.md written: %d tables in schema '%s'", len(table_names), schema)


def _load_schema_context() -> str:
    try:
        with open(AGENT_MD_PATH, encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return ""


class YandexAssistant:
    def __init__(self, settings, sid, hooks=None):
        self.settings = settings
        self._client = AsyncOpenAI(
            base_url=self.settings.yandex.URL,
            api_key=self.settings.yandex.AUTH,
            project=self.settings.yandex.FOLDER_ID,
        )
        self._rc = RunConfig(
            model_provider=CustomModelProvider(self.settings.yandex.MODEL, self._client),
        )
        self._exit_stack = AsyncExitStack()
        self._hooks = hooks
        self._session = SQLiteSession(session_id=sid, db_path="chat.db")
        self._getMetadata = None
        self._assistant = None
        self._metaAssistant = None
        self._maskingAssistant = None

    async def __aenter__(self):
        self._getMetadata = await self._exit_stack.enter_async_context(
            MCPServerSse(
                name="GetMetadata",
                params={
                    "url": self.settings.yandex.GET_INFO_MCP_URL,
                    "timeout": 60,
                },
                cache_tools_list=True,
                client_session_timeout_seconds=30,
            )
        )
        self._metaAssistant = Agent(
            name="MetadataAgent",
            instructions=self.settings.yandex.METADATA_INSTRUCTION,
            mcp_servers=[self._getMetadata],
        )
        self._maskingAssistant = Agent(
            name="DataMaskingAgent",
            instructions=self.settings.yandex.MASKING_INSTRUCTION,
            tools=[
                FileSearchTool(
                    max_num_results=5,
                    vector_store_ids=[self.settings.yandex.MASKING_INDEX_ID],
                )
            ],
        )
        schema_context = _load_schema_context()
        self._assistant = Agent(
            name="AssistantAgent",
            instructions=(
                f"{RECOMMENDED_PROMPT_PREFIX}\n{self.settings.yandex.ASSISTANT_INSTRUCTION}"
                + (f"\n\n{schema_context}" if schema_context else "")
            ),
            handoffs=[
                handoff(
                    agent=self._maskingAssistant,
                    input_filter=handoff_filters.remove_all_tools,
                ),
                handoff(
                    agent=self._metaAssistant,
                    input_filter=handoff_filters.remove_all_tools,
                ),
            ],
            model_settings=ModelSettings(tool_choice="auto", reasoning={"effort": "high"}),
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.aclose()

    async def one_shot(self, message: str) -> str:
        try:
            response = await Runner.run(
                self._assistant,
                message,
                run_config=self._rc,
                hooks=self._hooks,
                session=self._session,
            )
            return response.final_output or "No response from assistant"
        except Exception as e:
            return f"Error: {e}"
