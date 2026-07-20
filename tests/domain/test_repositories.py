import pytest

from pybus.domain.repositories import GenericRepository


def test_generic_repository_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        GenericRepository()  # type: ignore[abstract]
