import json
import logging
import time
from contextlib import AsyncExitStack

from agents.mcp import MCPServerSse
from agents import AsyncOpenAI, Agent, Runner, RunConfig, ModelSettings, handoff, SQLiteSession
from utils.config import Settings
from utils.model_provider import CustomModelProvider
from agents.extensions import handoff_filters
from agents.extensions.handoff_prompt import RECOMMENDED_PROMPT_PREFIX

AGENT_MD_PATH = "AGENT.md"
_log = logging.getLogger("db_assistant")

_METADATA_TOOLS: frozenset[str] = frozenset({
    "get_metadata", "get_table_list", "get_statistics",
    "get_indexes", "get_db_parameters", "get_relationships",
})
_DATA_TOOLS: frozenset[str] = frozenset({"run_wide_sql"})


class _FilteredMCP(MCPServerSse):
    """MCPServerSse that exposes only the tools in *allowed*.
    When sio/sid are provided, wide_sql calls are mirrored to the browser."""

    def __init__(self, *args, allowed: frozenset[str], sio=None, sid=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._allowed = allowed
        self._sio = sio
        self._sid = sid

    async def list_tools(self, run_context=None, agent=None):
        all_tools = await super().list_tools(run_context, agent)
        return [t for t in all_tools if t.name in self._allowed]

    async def call_tool(self, tool_name: str, arguments: dict | None):
        if tool_name == "run_wide_sql" and self._sio and self._sid:
            sql = (arguments or {}).get("sql", "")
            await self._sio.emit("agent_sql_start", {"sql": sql}, to=self._sid)
            t0 = time.monotonic()
            result = await super().call_tool(tool_name, arguments)
            elapsed = round(time.monotonic() - t0, 2)
            try:
                rows = json.loads(result.content[0].text)
                if isinstance(rows, list):
                    await self._sio.emit(
                        "agent_sql_end",
                        {"rows": rows, "count": len(rows), "elapsed": elapsed},
                        to=self._sid,
                    )
                else:
                    raise ValueError(result.content[0].text)
            except Exception as exc:
                text = str(exc) if not isinstance(exc, ValueError) else exc.args[0]
                await self._sio.emit(
                    "agent_sql_end", {"error": text, "elapsed": elapsed}, to=self._sid
                )
            return result
        return await super().call_tool(tool_name, arguments)


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
            "# Database schema",
            "",
            f"**Default schema: `{schema}`**",
            "",
            f"Always use `{schema}` as the schema qualifier in all SQL queries "
            f"(e.g. `{schema}.table_name`, `FROM {schema}.orders`). "
            "Never ask the user to specify the schema — it is always `{schema}`.".replace("{schema}", schema),
            "",
            "Use the table reference below when writing SQL for the user.",
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

            # Indexes
            try:
                idx_result = await mcp.call_tool(
                    "get_indexes", {"schemaName": schema, "tableName": table_name}
                )
                indexes = json.loads(idx_result.content[0].text).get("indexes") or []
                if indexes:
                    lines.append("**Indexes:**")
                    lines.append("")
                    lines.append("| Index | Columns | Unique | PK |")
                    lines.append("|-------|---------|--------|----|")
                    for idx in indexes:
                        cols = ", ".join(c["name"] for c in (idx.get("columns") or []))
                        lines.append(
                            f"| {idx['name']} | {cols} "
                            f"| {'✓' if idx.get('unique') else ''} "
                            f"| {'✓' if idx.get('is_pk') else ''} |"
                        )
                    lines.append("")
            except Exception as e:
                _log.warning("get_indexes failed for %s: %s", table_name, e)

            # FK relationships (depth=1)
            try:
                rel_result = await mcp.call_tool(
                    "get_relationships",
                    {"schemaName": schema, "tableName": table_name, "depth": 1},
                )
                relations = json.loads(rel_result.content[0].text).get("relations") or []
                outgoing = [r for r in relations if r.get("direction") == "outgoing"]
                incoming = [r for r in relations if r.get("direction") == "incoming"]
                if outgoing:
                    lines.append("**Foreign keys:**")
                    lines.append("")
                    lines.append("| Column | → Table | → Column |")
                    lines.append("|--------|---------|----------|")
                    for rel in outgoing:
                        nd = rel.get("node", {})
                        ref = f"{nd.get('schema', schema)}.{nd.get('table', '?')}"
                        lines.append(f"| {rel.get('from_column', '?')} | {ref} | {rel.get('to_column', '?')} |")
                    lines.append("")
                if incoming:
                    lines.append("**Referenced by:**")
                    lines.append("")
                    lines.append("| Table | Column | → Column |")
                    lines.append("|-------|--------|----------|")
                    for rel in incoming:
                        nd = rel.get("node", {})
                        ref = f"{nd.get('schema', schema)}.{nd.get('table', '?')}"
                        lines.append(f"| {ref} | {rel.get('from_column', '?')} | {rel.get('to_column', '?')} |")
                    lines.append("")
            except Exception as e:
                _log.warning("get_relationships failed for %s: %s", table_name, e)

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
    def __init__(self, settings, sid, hooks=None, sio=None):
        self.settings = settings
        self._sio = sio
        self._sid = sid
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
        self._metaMCP = None
        self._dataMCP = None
        self._assistant = None
        self._metaAssistant = None
        self._dataAssistant = None

    async def __aenter__(self):
        mcp_params = {"url": self.settings.yandex.GET_INFO_MCP_URL, "timeout": 60}

        self._metaMCP = await self._exit_stack.enter_async_context(
            _FilteredMCP(
                name="MetadataMCP",
                params=mcp_params,
                allowed=_METADATA_TOOLS,
                cache_tools_list=True,
                client_session_timeout_seconds=30,
            )
        )
        self._dataMCP = await self._exit_stack.enter_async_context(
            _FilteredMCP(
                name="DataMCP",
                params=mcp_params,
                allowed=_DATA_TOOLS,
                sio=self._sio,
                sid=self._sid,
                cache_tools_list=True,
                client_session_timeout_seconds=30,
            )
        )

        schema_context = _load_schema_context()
        instruction_suffix = f"\n\n{schema_context}" if schema_context else ""

        self._metaAssistant = Agent(
            name="MetadataAgent",
            instructions=self.settings.yandex.METADATA_INSTRUCTION + instruction_suffix,
            mcp_servers=[self._metaMCP],
        )
        self._dataAssistant = Agent(
            name="DataAgent",
            instructions=self.settings.yandex.DATA_INSTRUCTION + instruction_suffix,
            mcp_servers=[self._dataMCP],
        )
        self._assistant = Agent(
            name="AssistantAgent",
            instructions=(
                f"{RECOMMENDED_PROMPT_PREFIX}\n{self.settings.yandex.ASSISTANT_INSTRUCTION}"
            ),
            handoffs=[
                handoff(agent=self._dataAssistant, input_filter=handoff_filters.remove_all_tools),
                handoff(agent=self._metaAssistant, input_filter=handoff_filters.remove_all_tools),
            ],
            model_settings=ModelSettings(tool_choice="auto", reasoning={"effort": "low"}),
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
                max_turns=self.settings.yandex.MAX_TURNS,
            )
            return response.final_output or "No response from assistant"
        except Exception as e:
            return f"Error: {e}"
