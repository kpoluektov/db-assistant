from agents import (
    Model,
    ModelProvider,
    OpenAIResponsesModel,
)

class CustomModelProvider(ModelProvider):
    def __init__(self, model,client):
        self._Model = model
        self.client = client

    def get_model(self, model_name: str | None) -> Model:
        #print(model_name)
        return OpenAIResponsesModel(model=self._Model, openai_client=self.client)
