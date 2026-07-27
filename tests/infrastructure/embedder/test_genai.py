from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pybus.infrastructure.embedder.genai import GenAI


@pytest.fixture
def mock_client():
    with patch("pybus.infrastructure.embedder.genai.genai.Client") as mock_cls:
        client = MagicMock()
        client.aio.models.embed_content = AsyncMock()
        mock_cls.return_value = client
        yield client


@pytest.fixture
def embedder(mock_client) -> GenAI:
    return GenAI(api_key="test-key")


async def test_embed_calls_embed_content_with_expected_arguments(
    embedder: GenAI, mock_client: MagicMock
):
    embedding_values = [0.1, 0.2, 0.3]
    response = MagicMock()
    response.embeddings = [MagicMock(values=embedding_values)]
    mock_client.aio.models.embed_content.return_value = response

    result = await embedder.embed("some text", output_dimensionality=256)

    assert result == embedding_values
    _, kwargs = mock_client.aio.models.embed_content.call_args
    assert kwargs["model"] == "gemini-embedding-2-preview"
    assert kwargs["contents"] == "some text"
    assert kwargs["config"].output_dimensionality == 256


async def test_embed_raises_when_response_has_no_embeddings(
    embedder: GenAI, mock_client: MagicMock
):
    response = MagicMock()
    response.embeddings = None
    mock_client.aio.models.embed_content.return_value = response

    with pytest.raises(AssertionError):
        await embedder.embed("some text", output_dimensionality=256)
