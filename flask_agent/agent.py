import asyncio

from contextlib import AsyncExitStack

from agents.mcp import MCPServerSse
from agents import AsyncOpenAI, Agent, Runner, RunConfig, ModelSettings, FileSearchTool, set_tracing_disabled, handoff, set_default_openai_client, SQLiteSession
from utils.config import Settings
from utils.logger import yLogger
from utils.hooks import hooks
from utils.model_provider import CustomModelProvider
from agents.extensions import handoff_filters
from agents.extensions.handoff_prompt import RECOMMENDED_PROMPT_PREFIX

class YandexAssistant:
    def __init__(self, settings, sid):
        self.settings = settings
	# create openAI client
        self._client = AsyncOpenAI(
            base_url=self.settings.yandex.URL,
            api_key=self.settings.yandex.AUTH,
            project=self.settings.yandex.FOLDER_ID,
        )
        self._rc = RunConfig(
            model_provider=CustomModelProvider(self.settings.yandex.MODEL, self._client),
        )
        self._exit_stack = AsyncExitStack()

        self._getMetadata = None
        self._instructions = None
        self._assistant = None
        self._metaAssistant = None
        self._maskingAssistant = None
        self._session = SQLiteSession(session_id=sid, db_path="chat.db")
    async def __aenter__(self):
        # register MCP server
        self._getMetadata = await self._exit_stack.enter_async_context(
            MCPServerSse(
                name="GetMetadata",
                params={
                   "url": self.settings.yandex.GET_INFO_MCP_URL,
                   "timeout": 60,
                },
                cache_tools_list=True,
                client_session_timeout_seconds=30
            )
        )
        # first child agent
        self._metaAssistant = Agent(
            name="MetadataAgent",
            instructions=self.settings.yandex.METADATA_INSTRUCTION,
            model="MCP",
            mcp_servers=[self._getMetadata],
        )

        # second child agent
        self._maskingAssistant = Agent(
            name="DataMaskingAgent",
            instructions=self.settings.yandex.MASKING_INSTRUCTION,
            model="RAG",
            tools=[
                FileSearchTool(
                    max_num_results=5,
                    vector_store_ids=[self.settings.yandex.MASKING_INDEX_ID],
                )
            ],
        )
	# parent agent
        self._assistant = Agent(
            name="AssistantAgent",
            model=self.settings.yandex.MODEL,
            instructions=f"{RECOMMENDED_PROMPT_PREFIX}\n{self.settings.yandex.ASSISTANT_INSTRUCTION}",
            handoffs=[
                handoff(
                    agent=self._maskingAssistant,
                    input_filter=handoff_filters.remove_all_tools,
                ),
                handoff(
                    agent=self._metaAssistant ,
                    input_filter=handoff_filters.remove_all_tools,
                )],
            model_settings=ModelSettings(tool_choice="auto", reasoning={"effort": "high"})
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.aclose()

    async def one_shot(self, message: str) -> str:
        try:
            response = await Runner.run(
                self._assistant,
                message,
                run_config = self._rc,
                session = self._session
            )
            output = response.final_output or "No response from assistent"
#            print(f"output AGENT: {output}")
            return output
        except Exception as e:
            #print(f"Assistent got error: {e}")
            return f"Error: {e}"

