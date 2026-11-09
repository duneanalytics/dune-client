import asyncio
from contextlib import suppress

import pytest
from aiohttp import ClientPayloadError, web

from dune_client.client_async import AsyncDuneClient


@pytest.mark.parametrize("next_page", [False, True])
@pytest.mark.parametrize("failure", ["cancel", "truncated"])
def test_interrupted_csv_body_frees_connection(next_page, failure):
    asyncio.run(_interrupt_csv_body(next_page, failure))


async def _interrupt_csv_body(next_page, failure):
    release_body = asyncio.Event()
    truncate_body = asyncio.Event()
    requests = []

    async def serve(request):
        requests.append(request.path)
        if next_page and len(requests) == 1:
            return web.Response(
                body=b"id\n1\n",
                headers={"x-dune-next-uri": f"{base_url}/page2", "x-dune-next-offset": "1"},
            )
        if len(requests) == (2 if next_page else 1):
            response = web.StreamResponse(headers={"Content-Length": "10000"})
            await response.prepare(request)
            await response.write(b"id\n")
            if failure == "truncated":
                await truncate_body.wait()
                request.transport.close()
            else:
                await release_body.wait()
            return response
        return web.Response(body=b"id\n2\n")

    app = web.Application()
    app.router.add_get("/{path:.*}", serve)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base_url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
    client = AsyncDuneClient(
        api_key="test", base_url=base_url, connection_limit=1, request_timeout=10
    )
    download = None
    try:
        async with client:
            connector = client._session.connector
            download = asyncio.create_task(client.get_execution_results_csv("cancel"))
            async with asyncio.timeout(3):
                while len(requests) < (2 if next_page else 1) or not any(
                    protocol._payload is not None and protocol._payload._waiter is not None
                    for protocol in connector._acquired
                ):
                    await asyncio.sleep(0.001)
            if failure == "cancel":
                download.cancel()
                error = asyncio.CancelledError
            else:
                truncate_body.set()
                error = ClientPayloadError
            with pytest.raises(error):
                await asyncio.wait_for(download, 3)

            result = await asyncio.wait_for(client.get_execution_results_csv("second"), 3)
            assert result.data.getvalue() == b"id\n2\n"
            assert not release_body.is_set()
            assert len(requests) == (3 if next_page else 2)
    finally:
        release_body.set()
        truncate_body.set()
        if download is not None and not download.done():
            download.cancel()
            with suppress(asyncio.CancelledError):
                await download
        await client.disconnect()
        await runner.cleanup()
