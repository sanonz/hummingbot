import os
import aiohttp
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, RESTResponse


class RESTConnection:
    def __init__(self, aiohttp_client_session: aiohttp.ClientSession):
        self._client_session = aiohttp_client_session

    @staticmethod
    def _get_proxy_url():
        """Get proxy URL from environment variables."""
        proxy_vars = ['ALL_PROXY', 'HTTP_PROXY', 'HTTPS_PROXY', 'all_proxy', 'http_proxy', 'https_proxy']
        for var in proxy_vars:
            proxy_url = os.getenv(var)
            if proxy_url:
                return proxy_url
        return None

    async def call(self, request: RESTRequest) -> RESTResponse:
        # Get proxy URL from environment variables
        proxy_url = self._get_proxy_url()

        aiohttp_resp = await self._client_session.request(
            method=request.method.value,
            url=request.url,
            params=request.params,
            data=request.data,
            headers=request.headers,
            proxy=proxy_url,  # Pass proxy to individual request
        )

        resp = await self._build_resp(aiohttp_resp)
        return resp

    @staticmethod
    async def _build_resp(aiohttp_resp: aiohttp.ClientResponse) -> RESTResponse:
        resp = RESTResponse(aiohttp_resp)
        return resp
