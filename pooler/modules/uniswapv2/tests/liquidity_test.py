import asyncio
import json

import httpx
from redis import asyncio as aioredis

from ..utils.core import get_pair_reserves
from pooler.utils.redis.rate_limiter import load_rate_limiter_scripts
from pooler.utils.redis.redis_conn import provide_async_redis_conn_insta


@provide_async_redis_conn_insta
async def fetch_liquidityUSD_rpc(
    pair_address,
    block_num,
    redis_conn: aioredis.Redis = None,
):
    """
    This function is used to fetch the liquidity in USD for a given pair address and block number. It requires an async Redis connection instance to be provided. The function first loads rate limiting Lua scripts using the provided Redis connection. Then, it calls the `get_pair_reserves` function to retrieve the pair reserves for the given pair address and block number. The result is stored in the `data` variable. The function then extracts the total reserves in USD from the `block_pair_total_reserves` dictionary and returns the sum of the `token0USD` and `token1USD` values.

    Parameters:
    - `pair_address` (str): The address of the pair.
    - `block_num` (int): The block number.
    - `redis_conn` (aioredis.Redis, optional): The async Redis connection instance. Defaults to None.

    Returns:
    - `float`: The liquidity in USD for the given pair address and block number.
    """
    rate_limiting_lua_scripts = await load_rate_limiter_scripts(redis_conn)
    data = await get_pair_reserves(
        loop,
        rate_limiting_lua_scripts,
        pair_address,
        block_num,
        block_num,
        redis_conn=redis_conn,
    )
    block_pair_total_reserves = data.get(block_num)
    return (
        block_pair_total_reserves['token0USD'] +
        block_pair_total_reserves['token1USD']
    )


def fetch_liquidityUSD_graph(pair_address, block_num):
    """

    Fetches the liquidity in USD for a given pair address and block number from the Uniswap V2 API.

    Args:
        pair_address (str): The address of the pair.
        block_num (int): The block number to fetch the liquidity from.

    Returns:
        float: The liquidity in USD for the given pair address and block number.

    Raises:
        None

    Example:
        liquidity = fetch_liquidityUSD_graph('0x1234567890abcdef', 1000000)

    """
    uniswap_url = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2'
    uniswap_payload = (
        '{"query":"{\\n pair(id: \\"' +
        str(pair_address) +
        '\\",block:{number:' +
        str(
            block_num,
        ) +
        '}) {\\n reserveUSD \\n token0 { \\n     symbol \\n } \\n token1 {'
        ' \\n      symbol \\n    }\\n  } \\n }" }'
    )
    print(uniswap_payload)
    headers = {'Content-Type': 'application/plain'}
    response = httpx.post(
        url=uniswap_url,
        headers=headers,
        data=uniswap_payload,
        timeout=30,
    )
    if response.status_code == 200:
        data = json.loads(response.text)
        print('Response', data)
        data = data['data']
        return float(data['pair']['reserveUSD'])
    else:
        print('Error fetching data from uniswap THE GRAPH %s', response)
        return 0


async def compare_liquidity():
    """

    Compares the liquidity of multiple contracts using two different methods: `fetch_liquidityUSD_graph` and `fetch_liquidityUSD_rpc`.

    Parameters:
        None

    Returns:
        None

    Example Usage:
        compare_liquidity()

    This function initializes two variables, `total_liquidity_usd_graph` and `total_liquidity_usd_rpc`, to keep track of the total liquidity in USD for each method. It also sets the `block_num` variable to a specific block number.

    The function then creates a list of contracts and appends a contract address to it. For each contract in the list, it fetches the liquidity in USD using the `fetch_liquidityUSD_graph` function and assigns it to the `liquidity_usd_graph` variable. It also fetches the liquidity in USD using the `fetch_liquidityUSD_rpc` function asynchronously and assigns it to the `liquidity_usd_rpc` variable.

    The function then prints the contract address, `liquidity_usd_graph`, `liquidity_usd_rpc`, and the difference between the two. It also updates the `total_liquidity_usd_graph` and `total_liquidity_usd_rpc` variables by adding the respective liquidity values.

    Finally, the function prints the total number of contracts compared, `total_liquidity_usd_rpc`, and `total_liquidity_usd_graph`.

    Note: This function assumes that the `fetch_liquidityUSD_graph` and `fetch_liquidityUSD_rpc` functions are defined elsewhere.

    """
    total_liquidity_usd_graph = 0
    total_liquidity_usd_rpc = 0
    block_num = 16046250

    contracts = list()
    contracts.append('0xae461ca67b15dc8dc81ce7615e0320da1a9ab8d5')
    for contract in contracts:
        liquidity_usd_graph = fetch_liquidityUSD_graph(contract, block_num)
        liquidity_usd_rpc = await fetch_liquidityUSD_rpc(contract, block_num)
        print(
            f'Contract {contract}, liquidityUSD_graph is'
            f' {liquidity_usd_graph} , liquidityUSD_rpc {liquidity_usd_rpc},'
            ' liquidityUSD difference'
            f' {(liquidity_usd_rpc - liquidity_usd_graph)}',
        )
        total_liquidity_usd_graph += liquidity_usd_graph
        total_liquidity_usd_rpc += liquidity_usd_rpc

    print(
        f'{len(contracts)} contracts compared, liquidityUSD_rpc_total is'
        f' {total_liquidity_usd_rpc}, liquidityUSD_graph_total is'
        f' {total_liquidity_usd_graph}',
    )


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(compare_liquidity())
