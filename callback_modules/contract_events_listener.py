from urllib.parse import urljoin
from fastapi import FastAPI, Request, Response, Header, BackgroundTasks
from redis_conn import provide_async_redis_conn
import aioredis
import time
import json
from dynaconf import settings
import logging
import sys
import hmac
import asyncio
from setproctitle import setproctitle


trade_logger = logging.getLogger(__name__)
socket_log_handler = logging.handlers.SocketHandler('localhost', logging.handlers.DEFAULT_TCP_LOGGING_PORT)
trade_logger.addHandler(socket_log_handler)
app = FastAPI()
setproctitle(f'PowerLoom|ContractEventListener') 


# TODO: if we support the callback confirmation call feature from AuditProtocol, this module would need a rewrite
REDIS_CONN_CONF = {
    "host": settings['REDIS']['HOST'],
    "port": settings['REDIS']['PORT'],
    "password": settings['REDIS']['PASSWORD'],
    "db": settings['REDIS']['DB']
}

COMMIT_ID_CONFIRMATION_CB_PATH = f'/{settings.WEBHOOK_LISTENER.COMMIT_CONFIRMATION_CALLBACK_PATH}'


@app.on_event('startup')
async def startup_boilerplate():
    app.redis_pool = await aioredis.create_pool(
        address=(REDIS_CONN_CONF['host'], REDIS_CONN_CONF['port']),
        db=REDIS_CONN_CONF['db'],
        password=REDIS_CONN_CONF['password'],
        maxsize=5
    )
    app.semaphore = asyncio.BoundedSemaphore(1)
    app.redis_semaphore_locks = dict()


def check_signature(core_payload, signature):
    """
    Given the core_payload, check the signature generated by the webhook listener
    """
    _sign_rebuilt = hmac.new(
        key=settings.MATIC_VIGIL_KEYS.API_KEY.encode('utf-8'),
        msg=json.dumps(core_payload).encode('utf-8'),
        digestmod='sha256'
    ).hexdigest()

    trade_logger.debug("Signature the came in the header: ")
    trade_logger.debug(signature)
    trade_logger.debug("Signature rebuilt from core payload: ")
    trade_logger.debug(_sign_rebuilt)

    return _sign_rebuilt == signature


@app.post("/hooks")
@provide_async_redis_conn
async def event_listener(
        request: Request,
        response: Response,
        bg_tasks: BackgroundTasks,
        redis_conn=None,
        x_hook_signature: str = Header(None)
):
    req_json = await request.json()
    trade_logger.debug(req_json)
    if x_hook_signature:
        is_safe = check_signature(req_json, x_hook_signature)
        if is_safe:
            trade_logger.debug("Signature in the payload event is verified")
        else:
            trade_logger.debug("The signature in the payload event is invalid")
            return dict()
    if 'event_name' in req_json.keys():
        trade_logger.debug("Got an event")
        trade_logger.debug(req_json)
        if req_json['event_name'] == "FPMMFundingAdded":
            funding_added_key = f"{req_json['contract']}:fundingAdded"
            funding_added_data = {}
            funding_added_data.update({'event_data': req_json['event_data']})
            funding_added_data['txHash'] = req_json['txHash']
            funding_added_data['chainHeight'] = req_json['blockNumber']
            trade_logger.debug("Saving fundingAdded data")
            trade_logger.debug(req_json['event_data'])
            json_funding_added_data = json.dumps(funding_added_data)
            # add to event data sink
            _ = await redis_conn.zadd(
                key=funding_added_key,
                member=json_funding_added_data,
                score=req_json['blockNumber']
            )

        if req_json['event_name'] == "FPMMFundingRemoved":
            timestamp = int(time.time())
            funding_removed_key = f"{req_json['contract']}:fundingRemoved"
            trade_logger.debug(req_json['event_data'])
            funding_removed_data = {}
            funding_removed_data.update({'event_data': req_json['event_data']})
            funding_removed_data['txHash'] = req_json['txHash']
            funding_removed_data['chainHeight'] = req_json['blockNumber']
            trade_logger.debug("Saving fundingRemoved data")
            json_funding_removed_data = json.dumps(funding_removed_data)
            # add to event data sink
            _ = await redis_conn.zadd(
                key=funding_removed_key,
                member=json_funding_removed_data,
                score=req_json['blockNumber']
            )

        if req_json['event_name'] == "FPMMBuy":
            """ It is a Buy Event """
            """ Get the investmentAmount from the request JSON """
            investment_amount = req_json['event_data']['investmentAmount']
            trade_logger.debug("Got the investment amount: ")
            trade_logger.debug(investment_amount / 1000000)

            # Save the investment amount on redis against the event chain height
            trade_logger.debug("Saving the investment Amount: ")
            investment_amount_key = f"{req_json['contract']}:investmentAmount"
            buy_event_data = dict()
            buy_event_data.update({'event_data': req_json['event_data']})
            buy_event_data['txHash'] = req_json['txHash']
            buy_event_data['chainHeight'] = req_json['blockNumber']
            json_buy_event_data = json.dumps(buy_event_data)
            # add to event data sink
            _ = await redis_conn.zadd(
                key=investment_amount_key,
                member=json_buy_event_data,
                score=req_json['blockNumber']
            )
        if req_json['event_name'] == "FPMMSell":
            return_amount = req_json['event_data']['returnAmount']
            trade_logger.debug("Got the return amount: ")
            trade_logger.debug(return_amount / 1000000)

            # Save the investment amount on redis against the event chain height
            timestamp = int(time.time())
            trade_logger.debug("Saving the Return Amount: ")
            return_amount_key = f"{req_json['contract']}:returnAmount"
            return_amount_data = {}
            return_amount_data.update({'event_data': req_json['event_data']})
            return_amount_data['txHash'] = req_json['txHash']
            return_amount_data['chainHeight'] = req_json['blockNumber']
            json_return_amount_data = json.dumps(return_amount_data)
            # add to event data sink
            _ = await redis_conn.zadd(
                key=return_amount_key,
                member=json_return_amount_data,
                score=req_json['blockNumber']
            )
    else:
        trade_logger.debug("Not an event")
    response.status_code = 200
    return dict()


@app.post(COMMIT_ID_CONFIRMATION_CB_PATH)
@provide_async_redis_conn
async def commit_confirmation_cb(
        request: Request,
        response: Response,
        redis_conn=None
):
    trade_logger.debug('Got new commit confirmation CB')
    req_json = await request.json()
    trade_logger.debug(req_json)
    if 'commitID' not in req_json.keys() or 'projectID' not in req_json.keys():
        response.status_code = 200
        return {}
    commit_id = req_json['commitID']
    project_id = req_json['projectID']
    market_id = int(project_id.split('_')[2])  # eg: polymarket_onChain_73
    commit_status = req_json['status']
    # TODO: Future use, deal with commit status flag
    pending_commit_local_key = f'polymarket:pendingLastCommitConfirmation:{market_id}'
    pending_info_r = await redis_conn.get(pending_commit_local_key)
    if not pending_info_r:
        trade_logger.error('Could not find pending last commit information for market ID')
        trade_logger.error({'commitID': commit_id, 'marketID': market_id})
        response.status_code = 200
        return {}
    pending_info = json.loads(pending_info_r)
    if pending_info['commitId'] != commit_id:
        trade_logger.error('Callback of pending commit confirmation did not match pending last commit information for market ID')
        trade_logger.error({'commitID': {'callback': commit_id, 'pending': pending_info['commitId']}, 'marketID': market_id})
        response.status_code = 200
        return {}

    contract_address = None
    if not contract_address:
        response.status_code = 200
        return {}
    contract_address = contract_address.lower()
    # set ahead trade vol and liquidity last aggregated timestamp
    set_timestamp = pending_info['timestamp']
    liquidity_event_stream_range = pending_info['event_stream_range']['liquidity']
    trades_event_stream_range = pending_info['event_stream_range']['trades']
    last_liquidity_aggregation_key = f"polymarket:marketMaker:{contract_address}:lastLiquidityAggregation"
    last_checked_trade_chain_height_key = f'polymarket:marketMaker:{contract_address}:lastTradeVolAggregation'
    # set marker of last checked chain height for trade and liquidity events
    if trades_event_stream_range['max'] != -1:
        await redis_conn.set(last_checked_trade_chain_height_key, trades_event_stream_range['max'])
    if liquidity_event_stream_range['max'] != -1:
        await redis_conn.set(last_liquidity_aggregation_key, liquidity_event_stream_range['max'])
    # delete events piled up in zset ranges
    # liquidity events
    funding_added_events_key = f"{contract_address}:fundingAdded"
    funding_removed_events_key = f"{contract_address}:fundingRemoved"
    if not (liquidity_event_stream_range['min'] == -1 and liquidity_event_stream_range['max'] == -1):
        await redis_conn.zremrangebyscore(
            key=funding_added_events_key,
            min=liquidity_event_stream_range['min'],
            max=liquidity_event_stream_range['max']
        )
        redis_conn.zremrangebyscore(
            key=funding_removed_events_key,
            min=liquidity_event_stream_range['min'],
            max=liquidity_event_stream_range['max']
        )
    # trade events
    buy_events_key = f"{contract_address}:investmentAmount"
    sell_events_key = f"{contract_address}:returnAmount"
    if not(trades_event_stream_range['min'] == -1 and trades_event_stream_range['max'] == -1):
        await redis_conn.zremrangebyscore(
            key=buy_events_key,
            min=trades_event_stream_range['min'],
            max=trades_event_stream_range['max']
        )
        redis_conn.zremrangebyscore(
            key=sell_events_key,
            min=trades_event_stream_range['min'],
            max=trades_event_stream_range['max']
        )
    await redis_conn.delete(pending_commit_local_key)
    response.status_code = 200
    return {}
