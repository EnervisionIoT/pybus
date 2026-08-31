from typing import override

from google import genai
from google.genai.types import EmbedContentConfig

from pybus.application.interfaces import Embedder


class GenAI(Embedder):
    def __init__(self, api_key: str):
        self.client: genai.Client = genai.Client(api_key=api_key)

    @override
    async def embed(self, content: str, output_dimensionality: int = 768) -> list[float]:
        response = await self.client.aio.models.embed_content(  # type: ignore
            model="gemini-embedding-2-preview",
            contents=content,
            config=EmbedContentConfig(output_dimensionality=output_dimensionality),
        )
        assert response.embeddings is not None and response.embeddings[0].values is not None
        return response.embeddings[0].values
