from collections.abc import AsyncGenerator, Generator

import pytest

from dishka import (
    Provider,
    Scope,
    make_async_container,
    make_container,
    provide,
)
from dishka.exceptions import ExitError


class ConnA:
    pass


class ConnB:
    pass


class ConnC:
    pass


class AppResource:
    pass


class SessionResource:
    pass


def test_sync_exit_order():
    events = []

    class MyProvider(Provider):
        @provide(scope=Scope.APP)
        def a(self) -> Generator[ConnA, None, None]:
            events.append("create-a")
            yield ConnA()
            events.append("close-a")

        @provide(scope=Scope.APP)
        def b(self) -> Generator[ConnB, None, None]:
            events.append("create-b")
            yield ConnB()
            events.append("close-b")

        @provide(scope=Scope.APP)
        def c(self) -> Generator[ConnC, None, None]:
            events.append("create-c")
            yield ConnC()
            events.append("close-c")

    container = make_container(MyProvider())
    container.get(ConnA)
    container.get(ConnB)
    container.get(ConnC)
    container.close()
    assert events == [
        "create-a", "create-b", "create-c",
        "close-c", "close-b", "close-a",
    ]


@pytest.mark.asyncio
async def test_async_exit_order():
    events = []

    class MyProvider(Provider):
        @provide(scope=Scope.APP)
        async def a(self) -> AsyncGenerator[ConnA, None]:
            events.append("create-a")
            yield ConnA()
            events.append("close-a")

        @provide(scope=Scope.APP)
        async def b(self) -> AsyncGenerator[ConnB, None]:
            events.append("create-b")
            yield ConnB()
            events.append("close-b")

        @provide(scope=Scope.APP)
        async def c(self) -> AsyncGenerator[ConnC, None]:
            events.append("create-c")
            yield ConnC()
            events.append("close-c")

    container = make_async_container(MyProvider())
    await container.get(ConnA)
    await container.get(ConnB)
    await container.get(ConnC)
    await container.close()
    assert events == [
        "create-a", "create-b", "create-c",
        "close-c", "close-b", "close-a",
    ]


def test_sync_exit_failure_does_not_stop_others():
    events = []

    class MyProvider(Provider):
        @provide(scope=Scope.APP)
        def a(self) -> Generator[ConnA, None, None]:
            events.append("create-a")
            yield ConnA()
            events.append("close-a")

        @provide(scope=Scope.APP)
        def b(self) -> Generator[ConnB, None, None]:
            events.append("create-b")
            yield ConnB()
            events.append("close-b")
            raise ValueError("b failed")

        @provide(scope=Scope.APP)
        def c(self) -> Generator[ConnC, None, None]:
            events.append("create-c")
            yield ConnC()
            events.append("close-c")
            raise RuntimeError("c failed")

    container = make_container(MyProvider())
    container.get(ConnA)
    container.get(ConnB)
    container.get(ConnC)
    with pytest.raises(ExitError) as exc_info:
        container.close()
    assert events == [
        "create-a", "create-b", "create-c",
        "close-c", "close-b", "close-a",
    ]
    errors = exc_info.value.exceptions
    assert len(errors) == 2
    assert isinstance(errors[0], RuntimeError)
    assert isinstance(errors[1], ValueError)


@pytest.mark.asyncio
async def test_async_exit_failure_does_not_stop_others():
    events = []

    class MyProvider(Provider):
        @provide(scope=Scope.APP)
        async def a(self) -> AsyncGenerator[ConnA, None]:
            events.append("create-a")
            yield ConnA()
            events.append("close-a")

        @provide(scope=Scope.APP)
        async def b(self) -> AsyncGenerator[ConnB, None]:
            events.append("create-b")
            yield ConnB()
            events.append("close-b")
            raise ValueError("b failed")

        @provide(scope=Scope.APP)
        async def c(self) -> AsyncGenerator[ConnC, None]:
            events.append("create-c")
            yield ConnC()
            events.append("close-c")
            raise RuntimeError("c failed")

    container = make_async_container(MyProvider())
    await container.get(ConnA)
    await container.get(ConnB)
    await container.get(ConnC)
    with pytest.raises(ExitError) as exc_info:
        await container.close()
    assert events == [
        "create-a", "create-b", "create-c",
        "close-c", "close-b", "close-a",
    ]
    errors = exc_info.value.exceptions
    assert len(errors) == 2
    assert isinstance(errors[0], RuntimeError)
    assert isinstance(errors[1], ValueError)


def test_sync_child_scope_exits_before_parent():
    events = []

    class MyProvider(Provider):
        @provide(scope=Scope.APP)
        def app_resource(self) -> Generator[AppResource, None, None]:
            events.append("create-app")
            yield AppResource()
            events.append("close-app")

        @provide(scope=Scope.REQUEST)
        def session_resource(self) -> Generator[SessionResource, None, None]:
            events.append("create-request")
            yield SessionResource()
            events.append("close-request")

    container = make_container(MyProvider())
    container.get(AppResource)
    with container(scope=Scope.REQUEST) as request_container:
        request_container.get(SessionResource)
    assert events == ["create-app", "create-request", "close-request"]
    container.close()
    assert events == [
        "create-app", "create-request", "close-request", "close-app",
    ]


@pytest.mark.asyncio
async def test_async_child_scope_exits_before_parent():
    events = []

    class MyProvider(Provider):
        @provide(scope=Scope.APP)
        async def app_resource(self) -> AsyncGenerator[AppResource, None]:
            events.append("create-app")
            yield AppResource()
            events.append("close-app")

        @provide(scope=Scope.REQUEST)
        async def session_resource(
                self,
        ) -> AsyncGenerator[SessionResource, None]:
            events.append("create-request")
            yield SessionResource()
            events.append("close-request")

    container = make_async_container(MyProvider())
    await container.get(AppResource)
    async with container(scope=Scope.REQUEST) as request_container:
        await request_container.get(SessionResource)
    assert events == ["create-app", "create-request", "close-request"]
    await container.close()
    assert events == [
        "create-app", "create-request", "close-request", "close-app",
    ]


@pytest.mark.asyncio
async def test_async_start_scope_chain_exits_innermost_first():
    events = []

    class MyProvider(Provider):
        @provide(scope=Scope.APP)
        async def app_resource(self) -> AsyncGenerator[AppResource, None]:
            events.append("create-app")
            yield AppResource()
            events.append("close-app")

        @provide(scope=Scope.REQUEST)
        async def request_resource(
                self,
                app_resource: AppResource,
        ) -> AsyncGenerator[SessionResource, None]:
            events.append("create-request")
            yield SessionResource()
            events.append("close-request")

    container = make_async_container(MyProvider(), start_scope=Scope.REQUEST)
    await container.get(SessionResource)
    await container.close()
    assert events == [
        "create-app", "create-request", "close-request", "close-app",
    ]
