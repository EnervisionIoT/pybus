from pybus.application.commands import Command
from pybus.domain.mixins import TypeRegistryMixin


class CreateWidget(Command[str]):
    name: str


def test_command_generates_unique_id_by_default():
    a = CreateWidget(name="foo")
    b = CreateWidget(name="foo")
    assert a.id != b.id


def test_command_is_a_type_registry_mixin():
    assert issubclass(Command, TypeRegistryMixin)
    assert CreateWidget(name="foo").message_type == "CreateWidget"


def test_command_deserialize_dispatches_to_registered_subclass():
    data = {"message_type": "CreateWidget", "name": "bar", "id": CreateWidget(name="x").id}
    instance = Command.deserialize(data)
    assert isinstance(instance, CreateWidget)
    assert instance.name == "bar"
