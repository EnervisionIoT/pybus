from pybus.application.common.pagination import PaginationQuery


def test_pagination_query_defaults():
    query = PaginationQuery()
    assert query.page == 1
    assert query.size == 10


def test_pagination_query_overrides():
    query = PaginationQuery(page=3, size=25)
    assert query.page == 3
    assert query.size == 25
