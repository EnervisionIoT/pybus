import io

import pytest
from pydantic import ValidationError

from pybus.domain.value_objects import FileObject, ValueObject


class Money(ValueObject):
    amount: int
    currency: str


class Discount(Money):
    pass


def test_value_type_computed_field_is_class_name():
    assert Money(amount=1, currency="USD").value_type == "Money"
    assert Discount(amount=1, currency="USD").value_type == "Discount"


def test_registry_contains_all_subclasses():
    assert "Money" in ValueObject._registry
    assert "Discount" in ValueObject._registry


def test_deserialize_dispatches_to_registered_subclass():
    data = {"value_type": "Discount", "amount": 10, "currency": "USD"}
    instance = ValueObject.deserialize(data)
    assert isinstance(instance, Discount)
    assert instance.amount == 10


def test_value_object_is_frozen():
    money = Money(amount=1, currency="USD")
    with pytest.raises(ValidationError):
        money.amount = 2


def test_file_object_computes_size_from_stream():
    content = b"hello world"
    file_obj = FileObject(
        filename="hello.txt",
        content_type="text/plain",
        size=0,
        stream=io.BytesIO(content),
    )

    assert file_obj.size == len(content)


def test_file_object_accepts_a_stream_over_2mb():
    """The cap was a policy living in a value object. utility stores bill
    photos up to 15MB, and a phone photo of a paper bill is routinely 4-8MB;
    a framework-wide 2MB refusal made `Storage` unusable for the first
    service that wanted it. The size is still measured, never trusted --
    callers decide what is too large."""
    content = b"x" * (3 * 1024 * 1024)
    file = FileObject(
        filename="big.bin",
        content_type="application/octet-stream",
        size=0,
        stream=io.BytesIO(content),
    )
    assert file.size == len(content)


def test_file_object_to_bytes_reads_full_content_regardless_of_cursor_position():
    content = b"hello world"
    file_obj = FileObject(
        filename="hello.txt",
        content_type="text/plain",
        size=0,
        stream=io.BytesIO(content),
    )
    file_obj.stream.seek(4)  # move cursor away from start before reading

    result = file_obj.to_bytes()

    assert result == content
    # Calling it again proves to_bytes() always seeks to 0 before reading.
    assert file_obj.to_bytes() == content
