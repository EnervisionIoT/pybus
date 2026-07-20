from pydantic import BaseModel

from pybus.application.queries import Query
from pybus.domain.mixins import TypeRegistryMixin


class GetWidget(Query[str]):
    widget_id: str


def test_query_generates_unique_id_by_default():
    a = GetWidget(widget_id="1")
    b = GetWidget(widget_id="1")
    assert a.id != b.id


def test_query_is_a_plain_base_model_not_type_registry_mixin():
    assert issubclass(Query, BaseModel)
    assert not issubclass(Query, TypeRegistryMixin)
    assert not hasattr(GetWidget(widget_id="1"), "message_type")
