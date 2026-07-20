from abc import ABC, abstractmethod


class Embedder(ABC):
    @abstractmethod
    def embed(self, content: str, output_dimensionality: int) -> list[float]:
        """
        Generate an embedding for the given content.

        Args:
            content: The content to embed.
            output_dimensionality: The desired dimensionality of the embedding.

        Returns:
            A list of floats representing the embedding.
        """
        raise NotImplementedError()
