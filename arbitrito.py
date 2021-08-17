#!/usr/bin/env python3
from __future__ import print_function
import asyncio
import time
import logging
import yaml
import sys
import traceback

import requests
from os.path import exists
import cryptocom.exchange as cro
from cryptocom.exchange.structs import Pair
from cryptocom.exchange.structs import PrivateTrade
import krakenex
from binance.client import Client as Client
from binance.exceptions import *

import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Wrapper for Binance API (helps getting through the recvWindow issue)
class Binance:
    def __init__(self, public_key = '', secret_key = '', sync = False):
        self.time_offset = 0
        self.b = Client(public_key, secret_key)

        if sync:
            self.time_offset = self._get_time_offset()

    def _get_time_offset(self):
        res = self.b.get_server_time()
        return res['serverTime'] - int(time.time() * 1000)

    def synced(self, fn_name, **args):
        args['timestamp'] = int(time.time() - self.time_offset)

async def main():
    iteration = 0

    opportunities_bnb_krk_count = 0
    opportunities_bnb_cdc_count = 0
    opportunities_krk_bnb_count = 0
    opportunities_krk_cdc_count = 0
    opportunities_cdc_bnb_count = 0
    opportunities_cdc_krk_count = 0

    max_buy_price_avg = 0.0
    max_buy_trend = 0
    min_sell_price_avg = 0.0
    min_sell_trend = 0
    spread_avg = 0.0
    spread_avg_trend = 0

    config = get_config()
    logger = setupLogger('logfile.log')

    ticker_pairs = ['XXLMZEUR', 'DOTEUR', 'LINKEUR', 'ADAEUR']
    pair_coins = {'XXLMZEUR': {'bnb_pair': 'XLMEUR', 'bnb_base': 'XLM', 'base': 'XXLM', 'quote': 'ZEUR', 'krk_address': config['krk_xlm_address'], 'krk_address_memo': config['krk_xlm_address_memo']},
                  'DOTEUR': {'bnb_pair': 'DOTEUR', 'bnb_base': 'DOT', 'base': 'DOT', 'quote': 'ZEUR', 'krk_address': config['krk_dot_address']},
                  'LINKEUR': {'bnb_pair': 'LINKEUR', 'bnb_base': 'LINK', 'base': 'LINK', 'quote': 'ZEUR', 'krk_address': config['krk_link_address']},
                  'ADAEUR': {'bnb_pair': 'ADAEUR', 'bnb_base': 'ADA', 'base': 'ADA', 'quote': 'ZEUR', 'krk_address': config['krk_ada_address']},
                  'EURUSDT': {'bnb_pair': 'EURUSDT', 'bnb_base': 'EUR', 'base': 'ZEUR', 'quote': 'USDT', 'krk_address': config['krk_usdt_address']}}

    pairs = {'DOTUSDT': {'bnb_krk': 0, 'krk_bnb': 0},
             'ADAUSDT': {'bnb_krk': 0, 'krk_bnb': 0},
             'LTCUSDT': {'bnb_krk': 0, 'krk_bnb': 0},
             'LINKUSDT': {'bnb_krk': 0, 'krk_bnb': 0},
             'XRPEUR': {'bnb_krk': 0, 'krk_bnb': 0},
             'ADAEUR': {'bnb_krk': 0, 'krk_bnb': 0}}
    opportunities_DOTUSDT_count = 0
    opportunities_ADAUSDT_count = 0
    opportunities_LTCUSDT_count = 0
    opportunities_LINKUSDT_count = 0

    #Crypto.com API setup
    # cdc_exchange = cro.Exchange()
    # cdc_account = cro.Account(api_key=config['cdc_api_key'], api_secret=config['cdc_api_secret'])
    # cdc_pair = eval('cro.pairs.' + config['cdc_trading_pair'])

    # Kraken API setup
    krk_exchange = krakenex.API(key=config['krk_api_key'], secret=config['krk_api_secret'])
    for _ in range(10):
        try:
            krk_assets = krk_exchange.query_public('Assets')
            break
        except:
            await asyncio.sleep(10)
            continue

    # Binance API setup
    binance = Binance(public_key=config['bnb_api_key'], secret_key=config['bnb_api_secret'], sync=True)
    bnb_exchange = binance.b

    while True:
        try:
            iteration += 1
            logger.info(f'------------ Iteration {iteration} ------------')

            trades = []
            # Check first if exchanges are both up
            exchanges_are_up = exchanges_up(krk_exchange, bnb_exchange)

            if exchanges_are_up:

                # Check if there are trades to cancel (if trades are still open after 20 mins)
                krk_open_orders = krk_exchange.query_private('OpenOrders')['result']['open']
                logger.info(f'Kraken open trades: {krk_open_orders}')
                bnb_open_orders = bnb_exchange.get_open_orders(symbol=config['bnb_trading_pair'])
                logger.info(f'Binance open trades: {bnb_open_orders}')

                # Kraken: Get my balances
                kraken_balances = get_kraken_balances(krk_exchange, config)
                logger.info(f"Kraken's Balances\n(Base) {config['krk_base_currency']} balance:{kraken_balances['krk_base_currency_available']} \n(Quote) {config['krk_target_currency']} balance:{kraken_balances['krk_target_currency_available']}\n")

                # Binance: Get my balances
                binance_balances = get_binance_balances(bnb_exchange, config)
                logger.info(f"Binance's Balances\n(Base) {config['bnb_base_currency']} balance:{binance_balances['bnb_base_currency_available']} \n(Quote) {config['bnb_target_currency']} balance:{binance_balances['bnb_target_currency_available']}\n")

                # Log total balances
                total_base = round(float(kraken_balances['krk_base_currency_available']) + float(binance_balances['bnb_base_currency_available']), 8)
                total_quote = round(float(kraken_balances['krk_target_currency_available']) + float(binance_balances['bnb_target_currency_available']), 2)
                logger.info(f"Total balances: {config['bnb_base_currency']}={str(total_base)} | {config['bnb_target_currency']}={str(total_quote)}")

                # # Check target currency price spreaderences in exchanges
                # # Crypto.com target currency ticker
                # cdc_tickers = await cdc_exchange.get_tickers()
                # cdc_ticker = cdc_tickers[cdc_pair]
                # cdc_buy_price = cdc_ticker.buy_price
                # cdc_sell_price = cdc_ticker.sell_price
                # # cdc_high = cdc_ticker.high
                # # cdc_low = cdc_ticker.low
                # logger.info(f'\nCRYPTO.COM => Market {cdc_pair.name}\nbuy price: {cdc_buy_price} - sell price: {cdc_sell_price}\n\n')

                # Kraken trading pair ticker
                krk_tickers = krk_exchange.query_public("Ticker", {'pair': config['krk_trading_pair']})['result'][config['krk_trading_pair']]
                krk_buy_price = krk_tickers['b'][0]
                krk_sell_price = krk_tickers['a'][0]
                # logger.info(f"\nKRAKEN => Market {config['krk_trading_pair']}\nbuy price: {krk_buy_price} - sell price: {krk_sell_price}\n")

                # Binance trading pair ticker
                bnb_tickers = bnb_exchange.get_orderbook_tickers()
                bnb_ticker = next(item for item in bnb_tickers if item['symbol'] == config['bnb_trading_pair'])
                bnb_buy_price = bnb_ticker['bidPrice']
                bnb_sell_price = bnb_ticker['askPrice']
                # logger.info(f"\nBINANCE => Market {config['bnb_trading_pair']}\nbuy price: {bnb_buy_price} - sell price: {bnb_sell_price}\n")

                # If balances not enough, raise exception
                if float(kraken_balances['krk_target_currency_available']) < config['trade_amount'] or float(binance_balances['bnb_base_currency_available']) < (float(config['trade_amount_bnb']) / float(bnb_buy_price)):
                    if config['telegram_notifications_on']:
                        telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Not enough funds to start! (trade amount {config['trade_amount']})")
                    raise Exception(f"Not enough funds to start! (trade amount {config['trade_amount']})")

                # Kraken trading pair ticker
                krk_tickers = krk_exchange.query_public("Ticker", {'pair': config['krk_trading_pair']})['result'][config['krk_trading_pair']]
                krk_buy_price = krk_tickers['b'][0]
                krk_sell_price = krk_tickers['a'][0]
                # logger.info(f"\nKRAKEN => Market {config['krk_trading_pair']}\nbuy price: {krk_buy_price} - sell price: {krk_sell_price}\n")

                # Binance trading pair ticker
                bnb_tickers = bnb_exchange.get_orderbook_tickers()
                bnb_ticker = next(item for item in bnb_tickers if item['symbol'] == config['bnb_trading_pair'])
                bnb_buy_price = bnb_ticker['bidPrice']
                bnb_sell_price = bnb_ticker['askPrice']

                buy_prices = {'krk': krk_buy_price, 'bnb': bnb_buy_price}
                # buy_prices = {'cdc': cdc_buy_price, 'krk': krk_buy_price, 'bnb': bnb_buy_price}
                max_buy_price_key = max(buy_prices, key=buy_prices.get)
                max_buy_price = buy_prices[max_buy_price_key]
                sell_prices = {'krk': krk_sell_price, 'bnb': bnb_sell_price}
                # sell_prices = {'cdc': cdc_sell_price, 'krk': krk_sell_price, 'bnb': bnb_sell_price}
                min_sell_price_key = min(sell_prices, key=sell_prices.get)
                min_sell_price = sell_prices[min_sell_price_key]
                # logger.info(f"Max buy price -> {max_buy_price_key} = {max_buy_price}")
                # logger.info(f"Min sell price -> {min_sell_price_key} = {min_sell_price}")
                spread = round(float(max_buy_price) / float(min_sell_price), 8)
                logger.info(f"Max(buy price {max_buy_price_key}) / Min(sell price {min_sell_price_key}) = {spread}\n")

                item = {'spread': spread, 'trading_pair_config_suffix': '', 'max_buy_price_key': max_buy_price_key, 'min_sell_price_key': min_sell_price_key}
                # Create list of potential opportunities
                # opportunity_list = {'spread': spread, 'trading_pair_config_suffix': '', 'max_buy_price_key': max_buy_price_key, 'min_sell_price_key': min_sell_price_key}
                                    # {'spread': spread2, 'trading_pair_config_suffix': '2', 'max_buy_price_key': max_buy_price_key2, 'min_sell_price_key': min_sell_price_key2}]
                # Sort list by spread descending
                # sorted_opportunity_list = sorted(opportunity_list, key=lambda k: k['spread'], reverse=True)

                # Prnt sorted_opportunity_list for reference
                # logger.info("Sorted Opportunity list:\n")
                # for item in sorted_opportunity_list:
                #     logger.info(f'{item}')

                # for item in sorted_opportunity_list:
                if (item['spread'] > config['minimum_spread_krk'] and item['max_buy_price_key'] == 'krk') or (item['spread'] >= config['minimum_spread_bnb'] and item['max_buy_price_key'] == 'bnb'):

                    if not config['safe_mode_on']:
                        try:
                            # Set trading pair accordingly
                            # bnb_trading_pair = config['bnb_trading_pair' + item['trading_pair_config_suffix']]
                            # krk_trading_pair = config['krk_trading_pair' + item['trading_pair_config_suffix']]

                            if item['max_buy_price_key'] == 'bnb' and item['min_sell_price_key'] == 'krk':
                                # Make orders only if there are enough funds in both exchanges to perform both trades
                                # TODO:

                                ################################################################################################################################################
                                # Step 1:
                                # Limit orders (sell bnb_trading_pair in Binance and buy krk_trading_pair in Kraken, but first in Kraken since the exchange is slower)
                                ################################################################################################################################################
                                try:
                                    quantity = str(round(float(config['trade_amount_bnb']) / float(max_buy_price), 1))
                                    # result_krk = krk_exchange.query_private('AddOrder', {'pair': config['krk_trading_pair'], 'type': 'buy', 'ordertype': 'market', 'oflags': 'fciq', 'volume': quantity})
                                    result_krk = krk_exchange.query_private('AddOrder', {'pair': config['krk_trading_pair'], 'type': 'buy', 'ordertype': 'limit', 'oflags': 'fciq', 'price': min_sell_price, 'volume': quantity})
                                    if result_krk['error']:
                                        if config['telegram_notifications_on']:
                                            telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Opportunity of {str(round(float(item['spread']), 5))} found! (Max buy {item['max_buy_price_key']} | Min sell {item['min_sell_price_key']} ). Error when placing limit order in {item['min_sell_price_key']}")
                                        raise Exception("Could not sell '{}' in pair '{}' for '{}' in Kraken: {}".format(config['trade_amount_bnb'], config['krk_trading_pair'], str(round(float(min_sell_price) - 0.0001, 5)), result_krk['error']))
                                    result_bnb = bnb_exchange.order_limit_sell(symbol=config['bnb_trading_pair'], quantity=quantity, price=str(round(float(max_buy_price) + 0.001, 5)))
                                    logger.info(result_krk)
                                    logger.info(result_bnb)
                                    trades.append({'exchange': 'krk', 'orderid': result_krk['result']['txid'][0], 'time': time.time(), 'spread': item['spread']})
                                    trades.append({'exchange': 'bnb', 'orderid': result_bnb['orderId'], 'time': time.time(), 'spread': item['spread']})

                                    if not result_bnb:
                                        if config['telegram_notifications_on']:
                                            telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Opportunity of {str(round(float(item['spread']), 5))} found! (Max buy {item['max_buy_price_key']} | Min sell {item['min_sell_price_key']} ). Error when placing limit order in {item['max_buy_price_key']}")
                                        raise Exception("Could not sell '{}' in pair '{}' for '{}' in Binance.".format(config['trade_amount_bnb'], config['bnb_trading_pair'], str(float(max_buy_price))))
                                    # TODO:
                                    # sometimes kraken returns an error but the trade was made so we need to find a proper solution for this
                                    # if result_krk['error']:
                                    #     if config['telegram_notifications_on']:
                                    #         telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Opportunity of {str(round(float(item['spread']), 5))} found! (Max buy {item['max_buy_price_key']} | Min sell {item['min_sell_price_key']} ). Error when placing limit order in {item['min_sell_price_key']}")
                                    #         raise Exception("Could not buy '{}' in pair '{}' for '{}' in Kraken: {}".format(config['trade_amount_bnb'], config['krk_trading_pair'], trade_price, result_krk['error']))
                                except:
                                    if config['telegram_notifications_on']:
                                        telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Opportunity of {str(round(float(item['spread']), 5))} found! (Max buy {item['max_buy_price_key']} | Min sell {item['min_sell_price_key']} ). Error when placing limit orders.")
                                    logger.info(traceback.format_exc())
                                    raise Exception("Could not sell '{}' in pair '{}' in Binance.".format(config['trade_amount_bnb'], config['bnb_trading_pair']))

                                # Wait 10 seconds to give exchanges time to process orders
                                logger.info('Waiting 10 seconds to give exchanges time to process orders...')
                                if config['telegram_notifications_on']:
                                    telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Opportunity of {str(round(float(item['spread']), 5))} found! (Max buy {item['max_buy_price_key']} | Min sell {item['min_sell_price_key']} ): limit orders sent and waiting for fulfillment...")
                                await asyncio.sleep(10)

                                ################################################################################################################################################
                                # Step 2:
                                # Wait until limit orders have been fulfilled
                                ################################################################################################################################################
                                limit_orders_closed = await wait_for_orders(trades, config, krk_exchange, bnb_exchange, logger)
                                if not limit_orders_closed:
                                    if config['telegram_notifications_on']:
                                        telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Opportunity of {str(round(float(item['spread']), 5))} found! (Max buy {item['max_buy_price_key']} | Min sell {item['min_sell_price_key']} ): At least 1 limit order could not be fulfilled after {config['limit_order_time']} seconds")
                                    raise Exception("At least 1 limit order could not be fulfilled")
                                trades = []

                                await asyncio.sleep(20)
                                ################################################################################################################################################
                                # Step 3:
                                # Send bought crypto from Kraken to Binance
                                ################################################################################################################################################
                                tries = 100
                                success = False
                                krk_withdraw_refid = ''
                                while tries >= 0 and not success:
                                    try:
                                        tries -= 1
                                        kraken_balances_2 = get_kraken_balances(krk_exchange, config)
                                        withdrawal_result_krk = krk_exchange.query_private('Withdraw', {'asset': config['krk_base_currency'], 'key': config['krk_bnb_xrp_address_key'], 'amount': kraken_balances_2['krk_base_currency_available']})
                                        if withdrawal_result_krk['result']:
                                            logger.info(f"Withdraw info from Kraken -> {withdrawal_result_krk['result']}")
                                            krk_withdraw_refid = withdrawal_result_krk['result']['refid']
                                            success = True
                                            # Withdrawal id (in order to check status of withdrawal later)
                                    except:
                                        logger.info(traceback.format_exc())
                                        # wait a few seconds before trying again
                                        await asyncio.sleep(5)
                                        continue
                                ################################################################################################################################################
                                # Step 4:
                                # Trade fiat for a fast crypto currency in order to send it in Binance
                                ################################################################################################################################################
                                # First check which fast coin is better today
                                candidates = []
                                # for _ in range(50):
                                #     candidates = []
                                #     try:
                                #         candidates = get_candidates(krk_exchange, ticker_pairs)
                                #         print(f"Candidates -> {candidates}")
                                #         break
                                #     except:
                                #         logger.info(traceback.format_exc())
                                #         await asyncio.sleep(10)
                                #         continue
                                #
                                # if candidates:
                                #     candidate = {'pair': pair_coins[candidates[0]['pair']]['bnb_pair'], 'spread': candidates[0]['spread']}
                                # else:
                                #     candidate = {'pair': 'DOTEUR', 'spread': 1.0} # let's choose DOT as it's the one with highest market cap for now
                                #     candidates.append(candidate)

                                # candidate = {'pair': 'DOTEUR', 'spread': 1.0} # let's choose DOT as it's the one with highest market cap for now
                                # candidates.append(candidate)

                                # USDT mode
                                candidate = {'pair': 'EURUSDT', 'spread': 1.0}
                                candidates.append(candidate)

                                tries = 90
                                success = False
                                result_bnb = None
                                trades = []
                                first_try = True
                                while tries >= 0 and not success:
                                    tries -= 1
                                    try:
                                        # Calculate difference of fiat to be used in the next order
                                        tries_inside = 100
                                        success_inside = False
                                        while tries_inside >= 0 and not success_inside:
                                            try:
                                                tries_inside -= 1
                                                bnb_fiat_balance_result = bnb_exchange.get_asset_balance(asset=config['bnb_target_currency'])
                                                if bnb_fiat_balance_result:
                                                    bnb_fiat_currency_available = bnb_fiat_balance_result['free']
                                                else:
                                                    bnb_fiat_currency_available = "0.0"
                                                # bnb_candidate_balance_result = bnb_exchange.get_asset_balance(asset=pair_coins[candidate['pair']]['base'])
                                                # if bnb_candidate_balance_result:
                                                #     bnb_base_currency_available = float(bnb_candidate_balance_result['free'])
                                                # else:
                                                #     bnb_base_currency_available = 0.0
                                                success_inside = True
                                            except:
                                                logger.info(traceback.format_exc())
                                                # wait a few seconds before trying again
                                                await asyncio.sleep(5)
                                                continue

                                        fiat_amount = round(float(bnb_fiat_currency_available) - 0.1, 1)
                                        logger.info("Trying 2nd limit order...")
                                        # Get pair info
                                        info = bnb_exchange.get_symbol_info(candidate['pair'])
                                        # Binance trading pair ticker
                                        bnb_tickers = bnb_exchange.get_orderbook_tickers()
                                        bnb_ticker = next(item for item in bnb_tickers if item['symbol'] == candidate['pair'])
                                        bnb_buy_price = bnb_ticker['bidPrice']
                                        # quantity = round((fiat_amount / float(bnb_buy_price) - 0.005), info['filters'][2]['stepSize'].find('1') - 1) # get decimals from API
                                        # quantity = round(fiat_amount / (float(bnb_buy_price) * 0.999995), info['filters'][2]['stepSize'].find('1') - 1) # get decimals from API
                                        quantity = bnb_fiat_currency_available[0:bnb_fiat_currency_available.find('.') + info['filters'][2]['stepSize'].find('1')] # get decimals from API
                                        price = str(float(bnb_buy_price) + 0.0002)[0:str(float(bnb_buy_price) + 0.0002).find('.') + info['filters'][0]['tickSize'].find('1')]
                                        result_bnb = bnb_exchange.order_limit_sell(symbol=candidate['pair'], quantity=quantity, price=price)
                                        # result_bnb = bnb_exchange.order_limit_buy(symbol=candidate['pair'], quantity=quantity, price=str(round(float(bnb_buy_price) * 0.999995, info['filters'][0]['tickSize'].find('1') - 1)))
                                        # result_bnb = bnb_exchange.order_market_buy(symbol=candidate['pair'], quantity=quantity)
                                        logger.info(result_bnb)
                                        first_try = False
                                        trades.append({'exchange': 'bnb', 'orderid': result_bnb['orderId'], 'time': time.time(), 'spread': item['spread'], 'pair': candidate['pair']})
                                        limit_order_successful = await short_wait_for_bnb_order(trades, config, bnb_exchange, logger)
                                        # Calculate difference of fiat to be used in the next order
                                        tries_inside = 100
                                        success_inside = False
                                        while tries_inside >= 0 and not success_inside:
                                            try:
                                                tries_inside -= 1
                                                bnb_fiat_balance_result = bnb_exchange.get_asset_balance(asset=config['bnb_target_currency'])
                                                if bnb_fiat_balance_result:
                                                    bnb_fiat_currency_available = bnb_fiat_balance_result['free']
                                                else:
                                                    bnb_fiat_currency_available = "0.0"
                                                # bnb_candidate_balance_result = bnb_exchange.get_asset_balance(asset=pair_coins[candidate['pair']]['base'])
                                                # if bnb_candidate_balance_result:
                                                #     bnb_base_currency_available = float(bnb_candidate_balance_result['free'])
                                                # else:
                                                #     bnb_base_currency_available = 0.0
                                                success_inside = True
                                            except:
                                                logger.info(traceback.format_exc())
                                                # wait a few seconds before trying again
                                                await asyncio.sleep(5)
                                                continue
                                        if limit_order_successful and float(bnb_fiat_currency_available) < 10.0:
                                            success = True
                                            trades = []
                                    except:
                                        logger.info(traceback.format_exc())
                                        # wait a few seconds before trying again
                                        await asyncio.sleep(10)
                                        continue
                                if not success:
                                    if config['telegram_notifications_on']:
                                        telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{candidate['pair']}]. Error when placing limit buy order in Binance")
                                    logger.info(traceback.format_exc())
                                    raise Exception("Could not limit sell '{}' for '{}' in pair '{}' in Binance.".format(quantity, price, candidate['pair']))
                                else:
                                    if config['telegram_notifications_on']:
                                        telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{candidate['pair']}] 2nd limit order successful")
                                    logger.info(f"[{candidate['pair']}] 2nd limit order successful")

                                await asyncio.sleep(20)

                                ################################################################################################################################################
                                # Step 5:
                                # Send fast coin from Binance to Kraken
                                ################################################################################################################################################
                                # Get fast coin balance in Binance
                                # TODO: Wait until the trade was completed and there are funds to withdraw
                                tries = 100
                                success = False
                                bnb_withdraw_id = ''
                                withdrawal_result_bnb = None
                                while tries >= 0 and not success:
                                    try:
                                        info = bnb_exchange.get_symbol_info(candidate['pair'])
                                        logger.info("Trying to withdraw 2nd pair from Binance...")
                                        tries -= 1
                                        bnb_balance_result = bnb_exchange.get_asset_balance(asset=pair_coins[candidates[0]['pair']]['quote'])
                                        # bnb_balance_result = bnb_exchange.get_asset_balance(asset=pair_coins[candidates[0]['pair']]['bnb_base'])
                                        if bnb_balance_result:
                                            bnb_base_currency_available = bnb_balance_result['free'][0:bnb_balance_result['free'].find('.') + info['filters'][2]['stepSize'].find('1')]
                                        else:
                                            bnb_base_currency_available = 0.0
                                        if 'krk_address_memo' in pair_coins[candidates[0]['pair']]:
                                            withdrawal_result_bnb = bnb_exchange.withdraw(asset=pair_coins[candidates[0]['pair']]['bnb_base'], address=pair_coins[candidates[0]['pair']]['krk_address'], addressTag=pair_coins[candidates[0]['pair']]['krk_address_memo'], amount=bnb_base_currency_available)
                                        else:
                                            withdrawal_result_bnb = bnb_exchange.withdraw(asset=pair_coins[candidates[0]['pair']]['quote'], address=pair_coins[candidates[0]['pair']]['krk_address'], amount=bnb_base_currency_available)
                                            # withdrawal_result_bnb = bnb_exchange.withdraw(asset=pair_coins[candidates[0]['pair']]['bnb_base'], address=pair_coins[candidates[0]['pair']]['krk_address'], amount=bnb_base_currency_available)

                                        if withdrawal_result_bnb['success']:
                                            logger.info(withdrawal_result_bnb)
                                            bnb_withdraw_id = withdrawal_result_bnb['id']
                                            success = True
                                    except:
                                        logger.info(traceback.format_exc())
                                        # wait a few seconds before trying again
                                        await asyncio.sleep(10)
                                        continue

                                if not success:
                                    if config['telegram_notifications_on']:
                                        telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{pair_coins[candidates[0]['pair']]['quote']}] Error when withdrawing from Binance")
                                        # telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{pair_coins[candidates[0]['pair']]['bnb_base']}] Error when withdrawing from Binance")
                                    logger.info(traceback.format_exc())
                                    raise Exception(f"[{pair_coins[candidates[0]['pair']]['quote']}] Error when withdrawing from Binance")

                                ################################################################################################################################################
                                # Step 6:
                                # Wait for withdrawals
                                ################################################################################################################################################
                                withdrawals_processed = await wait_for_withdrawals(krk_withdraw_refid, bnb_withdraw_id, config, krk_exchange, bnb_exchange, config['krk_base_currency'], pair_coins[candidates[0]['pair']]['quote'], logger)
                                # withdrawals_processed = await wait_for_withdrawals(krk_withdraw_refid, bnb_withdraw_id, config, krk_exchange, bnb_exchange, config['krk_base_currency'], pair_coins[candidates[0]['pair']]['base'], logger)
                                if not withdrawals_processed:
                                    logger.info(f"Waited too long for Withdrawals/deposits\n")
                                    if config['telegram_notifications_on']:
                                        telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Opportunity of {str(round(float(item['spread']), 5))}: waited too long for Withdrawals/Deposits!")
                                else:
                                    logger.info(f"Withdrawals/deposits completed\n")
                                    if config['telegram_notifications_on']:
                                        telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Opportunity of {str(round(float(item['spread']), 5))}: Withdrawals/Deposits completed")

                                ################################################################################################################################################
                                # Step 7:
                                # Limit sell stable coin for fiat in Kraken
                                ################################################################################################################################################
                                # Check 60 minute candel to try to set the best sell price
                                # trend = 0
                                # try:
                                #     for _ in range(3):
                                #         ohlc_id = int(krk_exchange.query_public('OHLC', {'pair': candidates[0]['pair'], 'interval': '1'})['result']['last']) - 1
                                #         ohlc = krk_exchange.query_public('OHLC', {'pair': candidates[0]['pair'], 'interval': '1', 'since': ohlc_id})
                                #         # ohlc_o1 = ohlc['result'][pair][0][1]
                                #         # ohlc_c1 = ohlc['result'][pair][0][4]
                                #         if ohlc['result'][candidates[0]['pair']][0][4] > ohlc['result'][candidates[0]['pair']][0][1]:
                                #             trend += 1
                                #         elif ohlc['result'][candidates[0]['pair']][0][4] < ohlc['result'][candidates[0]['pair']][0][1]:
                                #             trend -= 1
                                # except:
                                #     raise("Could not calculate trend in last step")

                                tries = 90
                                success = False
                                trades = []
                                first_try = True
                                while tries >= 0 and not success:
                                    try:
                                        logger.info("Trying to limit sell in Kraken...")
                                        tries -= 1
                                        # Get decimals info
                                        krk_info = krk_exchange.query_public('AssetPairs', {'pair': 'USDTEUR'})
                                        decimals = krk_info['result']['USDTEUR']['pair_decimals']
                                        # krk_info = krk_exchange.query_public('AssetPairs', {'pair': candidates[0]['pair']})
                                        # decimals = krk_info['result'][candidates[0]['pair']]['pair_decimals']

                                        krk_balance = krk_exchange.query_private('Balance')
                                        krk_base_currency_available = 0.0
                                        # if pair_coins[candidates[0]['pair']]['base'] in krk_balance['result']:
                                        if 'USDT' in krk_balance['result']:
                                            krk_base_currency_available = krk_balance['result']['USDT']
                                            # krk_base_currency_available = krk_balance['result'][pair_coins[candidates[0]['pair']]['base']]
                                        if float(krk_base_currency_available) < 5.0 and not first_try:
                                            success = True
                                            trades = []
                                        else:
                                            krk_tickers = krk_exchange.query_public("Ticker", {'pair': 'USDTEUR'})['result']['USDTEUR']
                                            # krk_tickers = krk_exchange.query_public("Ticker", {'pair': candidates[0]['pair']})['result'][candidates[0]['pair']]
                                            krk_buy_price = krk_tickers['b'][0]
                                            price = (float(krk_buy_price) + 0.0001)
                                            # price = round(float(krk_buy_price) * 1.0004, decimals)
                                            # krk_sell_price = krk_tickers['a'][0]
                                            # if trend >= 0:
                                            #     price = round(float(bnb_buy_price) * 1.0005, 5)
                                            # else:
                                            #     price = round(float(bnb_buy_price) * 0.99982, 5)
                                            # if float(bnb_buy_price) > float(krk_sell_price):
                                            #     price = round(float(bnb_buy_price) * 1.0005, 5)
                                            # else:
                                            #     price = round(float(krk_sell_price) * 1.0005, 5)
                                            result_krk = krk_exchange.query_private('AddOrder', {'pair': 'USDTEUR', 'type': 'sell', 'ordertype': 'limit', 'oflags': 'fciq', 'price': price, 'volume': krk_base_currency_available})
                                            # result_krk = krk_exchange.query_private('AddOrder', {'pair': candidates[0]['pair'], 'type': 'sell', 'ordertype': 'limit', 'oflags': 'fciq', 'price': price, 'volume': krk_base_currency_available})
                                            logger.info(result_krk)
                                            if result_krk['result']:
                                                first_try = False
                                            trades.append({'exchange': 'krk', 'orderid': result_krk['result']['txid'][0], 'time': time.time(), 'spread': item['spread'], 'pair': 'USDTEUR'})
                                            # trades.append({'exchange': 'krk', 'orderid': result_krk['result']['txid'][0], 'time': time.time(), 'spread': item['spread'], 'pair': candidate['pair']})
                                            limit_order_successful = await short_wait_for_krk_order(trades, config, krk_exchange, logger)
                                            success = limit_order_successful
                                            trades = []
                                            # if result_krk['error']:
                                            #     if config['telegram_notifications_on']:
                                            #         telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{candidates[0]['pair']}] Error when placing limit order in Kraken ({krk_base_currency_available} @ {price})")
                                            #     raise Exception(f"Error when placing sell limit order in Kraken ({krk_base_currency_available} {candidates[0]['pair']} @ {price})")

                                            # if result_krk['error']:
                                            #     if config['telegram_notifications_on']:
                                            #         telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{candidates[0]['pair']}] Error when placing limit order in Kraken ({krk_base_currency_available} @ {price})")
                                            #     raise Exception(f"Error when placing sell limit order in Kraken ({krk_base_currency_available} {candidates[0]['pair']} @ {price})")
                                    except:
                                        logger.info(traceback.format_exc())
                                        # wait a few seconds before trying again
                                        await asyncio.sleep(10)
                                        continue

                                if not success:
                                    if config['telegram_notifications_on']:
                                        telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [USDTEUR] Error when selling in Kraken")
                                        # telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{pair_coins[candidates[0]['pair']]['bnb_base']}] Error when selling in Kraken")
                                    raise Exception(f"[USDTEUR] Error when selling in Kraken")
                                    # raise Exception(f"[{pair_coins[candidates[0]['pair']]['bnb_base']}] Error when selling in Kraken")

                            # elif item['max_buy_price_key'] == 'krk' and item['min_sell_price_key'] == 'bnb':
                            #     # Make orders only if there are enough funds in both exchanges to perform both trades
                            #     # TODO:
                            #
                            #     # Limit order to sell pair in Kraken
                            #     # First reduce the price a bit if the spread is greater than maximum_spread_krk (in order to make sure the trade will be fulfilled in Kraken)
                            #     if config['limit_spread_krk'] and item['spread'] > config['maximum_spread_krk']:
                            #         trade_price = str(round(float(min_sell_price) * item['spread'] * config['reduction_rate_krk'], 1))
                            #     else:
                            #         trade_price = str(round(float(max_buy_price), 1))
                            #
                            #     result = krk_exchange.query_private('AddOrder', {'pair': config['krk_trading_pair'], 'type': 'sell', 'ordertype': 'limit', 'oflags': 'fciq', 'price': trade_price, 'volume': config['trade_amount_krk']})
                            #     if result['error']:
                            #         if config['telegram_notifications_on']:
                            #             telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Opportunity of {str(round(float(item['spread']), 5))} found! (Max buy {item['max_buy_price_key']} | Min sell {item['min_sell_price_key']} ). Error when placing limit order in {item['max_buy_price_key']}")
                            #         raise Exception("Could not sell '{}' in pair '{}' for '{}' in Kraken: {}".format(config['trade_amount_krk'], config['krk_trading_pair'], trade_price, result['error']))
                            #     logger.info(result)
                            #     trades.append({'exchange': 'krk', 'orderid': result['result']['txid'][0], 'time': time.time(), 'spread': item['spread']})
                            #
                            #     # Limit order to buy the same amount of pair in Binance
                            #     try:
                            #         result = bnb_exchange.order_limit_buy(symbol=config['bnb_trading_pair'], quantity=config['trade_amount_krk'], price=str(float(min_sell_price)))
                            #         if not result:
                            #             if config['telegram_notifications_on']:
                            #                 telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Opportunity of {str(round(float(item['spread']), 5))} found! (Max buy {item['max_buy_price_key']} | Min sell {item['min_sell_price_key']} ). Error when placing limit order in {item['min_sell_price_key']}")
                            #             raise Exception("Could not buy '{}' in pair '{}' for '{}' in Binance.".format(config['trade_amount_krk'], config['bnb_trading_pair'], str(float(min_sell_price))))
                            #     except:
                            #         if config['telegram_notifications_on']:
                            #             telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Opportunity of {str(round(float(item['spread']), 5))} found! (Max buy {item['max_buy_price_key']} | Min sell {item['min_sell_price_key']} ). Error when placing limit order in {item['min_sell_price_key']}")
                            #         logger.info(traceback.format_exc())
                            #         raise Exception("Could not buy '{}' in pair '{}' in Binance.".format(config['trade_amount_krk'], config['bnb_trading_pair']))
                            #     logger.info(result)
                            #     trades.append({'exchange': 'bnb', 'orderid': result['orderId'], 'time': time.time(), 'spread': item['spread']})


                            # Notify Volume in Kraken
                            if config['telegram_notifications_on']:
                                fee_volume = 0
                                for _ in range(5):
                                    try:
                                        fee_volume = round(float(krk_exchange.query_private('TradeVolume')['result']['volume']), 2)
                                        await asyncio.sleep(5)
                                        break
                                    except:
                                        logger.info(traceback.format_exc())
                                        continue
                                telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> Current Volume: {str(fee_volume)} USD")

                            # Notify totals
                            # Kraken: Get my balances
                            kraken_balances = get_kraken_balances(krk_exchange, config)
                            logger.info(f"Kraken's Balances\n(Base) {config['krk_base_currency']} balance:{kraken_balances['krk_base_currency_available']} \n(Quote) {config['krk_target_currency']} balance:{kraken_balances['krk_target_currency_available']}\n")


                            # Binance: Get my balances
                            binance_balances = get_binance_balances(bnb_exchange, config)
                            logger.info(f"Binance's Balances\n(Base) {config['bnb_base_currency']} balance:{binance_balances['bnb_base_currency_available']} \n(Quote) {config['bnb_target_currency']} balance:{binance_balances['bnb_target_currency_available']}\n")

                            # Log total balances
                            total_base_after_trades = round(float(kraken_balances['krk_base_currency_available']) + float(binance_balances['bnb_base_currency_available']), 8)
                            total_quote_after_trades = round(float(kraken_balances['krk_target_currency_available']) + float(binance_balances['bnb_target_currency_available']), 2)
                            logger.info(f"Total Balances\n(Base) {config['bnb_base_currency']} balance:{str(total_base_after_trades)} \n(Quote) {config['bnb_target_currency']} balance:{str(total_quote_after_trades)}\n")

                            # Compute total diff after trades
                            base_diff = round(total_base_after_trades - total_base, 8)
                            quote_diff = round(total_quote_after_trades - total_quote, 2)

                            # Convert base to quote
                            # total_quote_before_trades = round(((float(max_buy_price) - float(min_sell_price)) * total_base) + total_quote, 2)
                            # total_quote_after_trades = round(((float(max_buy_price) - float(min_sell_price)) * total_base_after_trades) + total_quote_after_trades, 2)
                            # diff = round(total_quote_after_trades - total_quote, 2)
                            if quote_diff >= 0.0:
                                logger.info(f"You won {str(abs(quote_diff))} {config['bnb_target_currency']} after last operation")
                                if config['telegram_notifications_on']:
                                    telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Total balances after trades: {config['bnb_base_currency']}={str(total_base_after_trades)} | {config['bnb_target_currency']}={str(total_quote_after_trades)}. | You won {str(abs(quote_diff))} {config['bnb_target_currency']} after last operation")
                            else:
                                logger.info(f"You lost {str(abs(quote_diff))} {config['bnb_target_currency']} after last operation")
                                if config['telegram_notifications_on']:
                                    telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Total balances after trades: {config['bnb_base_currency']}={str(total_base_after_trades)} | {config['bnb_target_currency']}={str(total_quote_after_trades)}. | You lost {str(abs(quote_diff))} {config['bnb_target_currency']} after last operation")

                            if item['max_buy_price_key'] == 'bnb' and item['min_sell_price_key'] == 'krk':
                                opportunities_bnb_krk_count += 1
                            elif item['max_buy_price_key'] == 'krk' and item['min_sell_price_key'] == 'bnb':
                                opportunities_krk_bnb_count += 1

                            fee_volume = 0
                            for _ in range(5):
                                try:
                                    fee_volume = round(float(krk_exchange.query_private('TradeVolume')['result']['volume']), 2)
                                    await asyncio.sleep(5)
                                    break
                                except:
                                    continue

                            # Update google sheet
                            update_google_sheet(config['sheet_id'], config['range_name'], kraken_balances['krk_target_currency_available'], fee_volume)

                        except Exception as e:
                            logger.info(traceback.format_exc())

                            if item['max_buy_price_key'] == 'bnb' and item['min_sell_price_key'] == 'krk':
                                opportunities_bnb_krk_count += 1
                            elif item['max_buy_price_key'] == 'krk' and item['min_sell_price_key'] == 'bnb':
                                opportunities_krk_bnb_count += 1

                            print("\n")
                            opportunities = {'Opportunities_BNB_KRK': opportunities_bnb_krk_count,
                                             'Opportunities_BNB_CDC': opportunities_bnb_cdc_count,
                                             'Opportunities_KRK_BNB': opportunities_krk_bnb_count,
                                             'Opportunities_KRK_CDC': opportunities_krk_cdc_count,
                                             'Opportunities_CDC_BNB': opportunities_cdc_bnb_count,
                                             'Opportunities_CDC_KRK': opportunities_cdc_krk_count}
                            for key, value in opportunities.items():
                                logger.info(f'{key} = {value}')

                            fee_volume = 0
                            for _ in range(5):
                                try:
                                    fee_volume = round(float(krk_exchange.query_private('TradeVolume')['result']['volume']), 2)
                                    await asyncio.sleep(5)
                                    break
                                except:
                                    continue
                            logger.info(f'New Volume -> {str(fee_volume)}')

                            logger.info("Exception occurred: Waiting for next iteration... ({} seconds)\n\n\n".format(config['seconds_between_iterations']))
                            await asyncio.sleep(config['seconds_between_iterations'])
                            continue

                    else: # if not config['safe_mode_on']:
                        if config['telegram_notifications_on']:
                            telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Opportunity of {str(round(float(item['spread']), 5))} found! (Max buy {item['max_buy_price_key']} | Min sell {item['min_sell_price_key']})")

                print("\n")
                opportunities = {'Opportunities_BNB_KRK': opportunities_bnb_krk_count,
                                 'Opportunities_BNB_CDC': opportunities_bnb_cdc_count,
                                 'Opportunities_KRK_BNB': opportunities_krk_bnb_count,
                                 'Opportunities_KRK_CDC': opportunities_krk_cdc_count,
                                 'Opportunities_CDC_BNB': opportunities_cdc_bnb_count,
                                 'Opportunities_CDC_KRK': opportunities_cdc_krk_count}

                for key, value in opportunities.items():
                    logger.info(f'{key} = {value}')

                # Averages
                max_buy_price_avg = max_buy_price_avg or float(max_buy_price)
                max_buy_price_avg = round((max_buy_price_avg + float(max_buy_price))/2, 5)
                logger.info(f'Max_buy_price_avg = {str(max_buy_price_avg)}')
                factor = float(max_buy_price)/max_buy_price_avg
                if factor > 1.0006:
                    max_buy_trend += 1
                    logger.info(f'Max buy trend = {max_buy_trend} | trending up hard')
                elif factor > 1.0:
                    max_buy_trend += 1
                    logger.info(f'Max buy trend = {max_buy_trend} | trending up')
                elif factor < 0.9994:
                    max_buy_trend -= 1
                    logger.info(f'Max buy trend = {max_buy_trend} | trending down hard')
                elif factor < 1.0:
                    max_buy_trend -= 1
                    logger.info(f'Max buy trend = {max_buy_trend} | trending down')
                else:
                    pass
                min_sell_price_avg = min_sell_price_avg or float(min_sell_price)
                min_sell_price_avg = round((min_sell_price_avg + float(min_sell_price))/2, 5)
                logger.info(f'Min_sell_price_avg = {str(min_sell_price_avg)}')
                factor = float(min_sell_price)/min_sell_price_avg
                if factor > 1.0006:
                    min_sell_trend += 1
                    logger.info(f'Min sell trend = {min_sell_trend} | trending up hard')
                elif factor > 1.0:
                    min_sell_trend += 1
                    logger.info(f'Min sell trend = {min_sell_trend} | trending up')
                elif factor < 0.9994:
                    min_sell_trend -= 1
                    logger.info(f'Min sell trend = {min_sell_trend} | trending down hard')
                elif factor < 1.0:
                    min_sell_trend -= 1
                    logger.info(f'Min sell trend = {min_sell_trend} | trending down')
                else:
                    pass

                spread_avg = spread_avg or float(item['spread'])
                spread_avg = round((spread_avg + float(item['spread']))/2, 6)
                logger.info(f'Spread_avg = {str(spread_avg)}')
                factor = float(float(item['spread']))/spread_avg
                if factor > 1.002297:
                    spread_avg_trend += 1
                    logger.info(f'Spread avg trend = {spread_avg_trend} | trending up hard')
                elif factor > 1.0:
                    spread_avg_trend += 1
                    logger.info(f'Spread avg trend = {spread_avg_trend} | trending up')
                elif factor <= 0.998005:
                    spread_avg_trend -= 1
                    logger.info(f'Spread avg trend = {spread_avg_trend} | trending down hard')
                elif factor < 1.0:
                    spread_avg_trend -= 1
                    logger.info(f'Spread avg trend = {spread_avg_trend} | trending down')
                else:
                    pass

                # Volume in Kraken
                fee_volume = 0
                for _ in range(5):
                    try:
                        fee_volume = round(float(krk_exchange.query_private('TradeVolume')['result']['volume']), 2)
                        await asyncio.sleep(5)
                        break
                    except:
                        continue
                logger.info(f'New Volume -> {str(fee_volume)}')

                if spread > 1.001:
                    if config['telegram_notifications_on']:
                        telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Max(buy price {max_buy_price_key}) / Min(sell price {min_sell_price_key}) = {spread}\n")

                #Analize other pairs
                for cpair in pairs.keys():
                    # Kraken trading pair ticker
                    if cpair == 'XRPEUR':
                        the_pair = "XXRPZEUR"
                    else:
                        the_pair = cpair
                    krk_tickers = krk_exchange.query_public("Ticker", {'pair': the_pair})['result'][the_pair]
                    krk_buy_price = krk_tickers['b'][0]
                    krk_sell_price = krk_tickers['a'][0]
                    # logger.info(f"\nKRAKEN => Market {config['krk_trading_pair']}\nbuy price: {krk_buy_price} - sell price: {krk_sell_price}\n")

                    # Binance trading pair ticker
                    bnb_tickers = bnb_exchange.get_orderbook_tickers()
                    bnb_ticker = next(item for item in bnb_tickers if item['symbol'] == cpair)
                    bnb_buy_price = bnb_ticker['bidPrice']
                    bnb_sell_price = bnb_ticker['askPrice']
                    # logger.info(f"\nBINANCE => Market {config['cpair']}\nbuy price: {bnb_buy_price} - sell price: {bnb_sell_price}\n")

                    buy_prices = {'krk': krk_buy_price, 'bnb': bnb_buy_price}
                    # buy_prices = {'cdc': cdc_buy_price, 'krk': krk_buy_price, 'bnb': bnb_buy_price}
                    max_buy_price_key = max(buy_prices, key=buy_prices.get)
                    max_buy_price = buy_prices[max_buy_price_key]
                    sell_prices = {'krk': krk_sell_price, 'bnb': bnb_sell_price}
                    # sell_prices = {'cdc': cdc_sell_price, 'krk': krk_sell_price, 'bnb': bnb_sell_price}
                    min_sell_price_key = min(sell_prices, key=sell_prices.get)
                    min_sell_price = sell_prices[min_sell_price_key]
                    # logger.info(f"Max buy price -> {max_buy_price_key} = {max_buy_price}")
                    # logger.info(f"Min sell price -> {min_sell_price_key} = {min_sell_price}")
                    spread = round(float(max_buy_price) / float(min_sell_price), 8)
                    logger.info(f"[{cpair}] Max(buy price {max_buy_price_key}) / Min(sell price {min_sell_price_key}) = {spread}\n")
                    if spread > 1.0007:
                        if max_buy_price_key == 'bnb':
                            pairs[cpair]['bnb_krk'] += 1
                        else:
                            pairs[cpair]['krk_bnb'] += 1
                            if config['telegram_notifications_on']:
                                telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{cpair}] Max(buy price {max_buy_price_key}) / Min(sell price {min_sell_price_key}) = {spread}\n")

                for key, value in pairs.items():
                    logger.info(f"{key} BNB_KRK = {value['bnb_krk']}")
                    logger.info(f"{key} KRK_BNB = {value['krk_bnb']}")
            else: # if exchanges_are_up:
                logger.info("One of the exchanges was down or under maintenance!")


            logger.info(f'------------ Iteration {iteration} ------------\n')

            if config['test_mode_on']:
                await asyncio.sleep(1)
                break
            else:
                # Wait given seconds until next poll
                logger.info("Waiting for next iteration... ({} seconds)\n\n\n".format(config['seconds_between_iterations']))
                await asyncio.sleep(config['seconds_between_iterations'])

        except Exception as e:
            buy_prices = {'krk': krk_buy_price, 'bnb': bnb_buy_price}
            max_buy_price_key = max(buy_prices, key=buy_prices.get)
            max_buy_price = buy_prices[max_buy_price_key]
            sell_prices = {'krk': krk_sell_price, 'bnb': bnb_sell_price}
            min_sell_price_key = min(sell_prices, key=sell_prices.get)
            min_sell_price = sell_prices[min_sell_price_key]
            spread = round(float(max_buy_price) / float(min_sell_price), 8)
            logger.info(f"Max(buy price {max_buy_price_key}) / Min(sell price {min_sell_price_key}) = {spread}\n")

            if spread > 1.001:
                if config['telegram_notifications_on']:
                    telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Max(buy price {max_buy_price_key}) / Min(sell price {min_sell_price_key}) = {spread}\n")

            #Analize other pairs
            try:
                for cpair in pairs.keys():
                    # Kraken trading pair ticker
                    if cpair == 'XRPEUR':
                        the_pair = "XXRPZEUR"
                    else:
                        the_pair = cpair
                    krk_tickers = krk_exchange.query_public("Ticker", {'pair': the_pair})['result'][the_pair]
                    krk_buy_price = krk_tickers['b'][0]
                    krk_sell_price = krk_tickers['a'][0]
                    # logger.info(f"\nKRAKEN => Market {config['krk_trading_pair']}\nbuy price: {krk_buy_price} - sell price: {krk_sell_price}\n")

                    # Binance trading pair ticker
                    bnb_tickers = bnb_exchange.get_orderbook_tickers()
                    bnb_ticker = next(item for item in bnb_tickers if item['symbol'] == cpair)
                    bnb_buy_price = bnb_ticker['bidPrice']
                    bnb_sell_price = bnb_ticker['askPrice']
                    # logger.info(f"\nBINANCE => Market {config['cpair']}\nbuy price: {bnb_buy_price} - sell price: {bnb_sell_price}\n")

                    buy_prices = {'krk': krk_buy_price, 'bnb': bnb_buy_price}
                    # buy_prices = {'cdc': cdc_buy_price, 'krk': krk_buy_price, 'bnb': bnb_buy_price}
                    max_buy_price_key = max(buy_prices, key=buy_prices.get)
                    max_buy_price = buy_prices[max_buy_price_key]
                    sell_prices = {'krk': krk_sell_price, 'bnb': bnb_sell_price}
                    # sell_prices = {'cdc': cdc_sell_price, 'krk': krk_sell_price, 'bnb': bnb_sell_price}
                    min_sell_price_key = min(sell_prices, key=sell_prices.get)
                    min_sell_price = sell_prices[min_sell_price_key]
                    # logger.info(f"Max buy price -> {max_buy_price_key} = {max_buy_price}")
                    # logger.info(f"Min sell price -> {min_sell_price_key} = {min_sell_price}")
                    spread = round(float(max_buy_price) / float(min_sell_price), 8)
                    logger.info(f"[{cpair}] Max(buy price {max_buy_price_key}) / Min(sell price {min_sell_price_key}) = {spread}\n")
                    if spread > 1.0007:
                        if max_buy_price_key == 'bnb':
                            pairs[cpair]['bnb_krk'] += 1
                        else:
                            pairs[cpair]['krk_bnb'] += 1
                            if config['telegram_notifications_on']:
                                telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{cpair}] Max(buy price {max_buy_price_key}) / Min(sell price {min_sell_price_key}) = {spread}\n")

                for key, value in pairs.items():
                    logger.info(f"{key} BNB_KRK = {value['bnb_krk']}")
                    logger.info(f"{key} KRK_BNB = {value['krk_bnb']}")
            except:
                continue

            logger.info(traceback.format_exc())
            # Network issue(s) occurred (most probably). Jumping to next iteration
            logger.info("Exception occurred -> '{}'. Waiting for next iteration... ({} seconds)\n\n\n".format(e, config['seconds_between_iterations']))
            await asyncio.sleep(config['seconds_between_iterations'])



def get_config():
    config_path = "config/default_config.yaml"
    if exists("config/user_config.yaml"):
        config_path = "config/user_config.yaml"
        print('\n\nUser config detected... checking options ...\n')
    else:
        print('Loading default configuration...')
    config_file = open(config_path)
    data = yaml.load(config_file, Loader=yaml.FullLoader)
    config_file.close()
    check_config(data)
    return data

def check_config(data):
    # Check Crypto.com trading pair and coins
    try:
        eval('cro.pairs.' + data['cdc_trading_pair'])
    except AttributeError:
        print("Crypto.com's trading pair '{}' does not exist (check your config_file)".format(data['cdc_trading_pair']))
        sys.exit(1)
    try:
        eval('cro.coins.' + data['cdc_target_currency'])
    except AttributeError:
        print('Currency "{}" does not exist (check your config_file)'.format(data['cdc_target_currency']))
        sys.exit(1)
    try:
        eval('cro.coins.' + data['cdc_base_currency'])
    except AttributeError:
        print('Currency "{}" does not exist (check your config_file)'.format(data['cdc_base_currency']))
        sys.exit(1)

    # Check kraken trading pair and coins
    try:
        krk_x = krakenex.API(key='', secret='')
        result = krk_x.query_public("Ticker", {'pair': data['krk_trading_pair']})
        if result['error'] != [] and result['error'][0] == 'EQuery:Unknown asset pair':
            raise AttributeError
    except AttributeError:
        print("Kraken's Trading pair '{}' does not exist (check your config_file)".format(data['krk_trading_pair']))
        sys.exit(1)
    print('All options looking good\n')

def get_kraken_balances(exchange, config):
    krk_balance = exchange.query_private('Balance')
    krk_base_currency_available = 0.0
    if config['krk_base_currency'] in krk_balance['result']:
        krk_base_currency_available = krk_balance['result'][config['krk_base_currency']]
    # Kraken: Get my target currency balance
    krk_target_currency_available = 0.0
    if config['krk_target_currency'] in krk_balance['result']:
        krk_target_currency_available = krk_balance['result'][config['krk_target_currency']]
    return ({'krk_base_currency_available': krk_base_currency_available, 'krk_target_currency_available': krk_target_currency_available})

def get_binance_balances(exchange, config):
    bnb_balance_result = exchange.get_asset_balance(asset=config['bnb_base_currency'])
    if bnb_balance_result:
        bnb_base_currency_available = round(float(bnb_balance_result['free']) + float(bnb_balance_result['locked']), 8)
    else:
        bnb_base_currency_available = 0.0
    bnb_balance_result = exchange.get_asset_balance(asset=config['bnb_target_currency'])
    if bnb_balance_result:
        bnb_target_currency_available = round(float(bnb_balance_result['free']) + float(bnb_balance_result['locked']), 2)
    else:
        bnb_target_currency_available = 0.0
    return ({'bnb_base_currency_available': bnb_base_currency_available, 'bnb_target_currency_available': bnb_target_currency_available})

def exchanges_up(krk, bnb):
    krk_up_result = krk.query_public('SystemStatus')
    krk_up = krk_up_result['result'] and krk_up_result['result']['status'] == 'online'

    bnb_up_result = bnb.get_system_status()
    bnb_up = bnb_up_result and bnb_up_result['status'] == 0 # binance api docs -> 0=normal; 1=system maintenance

    return krk_up and bnb_up


def setupLogger(log_filename):
    logger = logging.getLogger('CN')

    file_log_handler = logging.FileHandler(log_filename)
    logger.addHandler(file_log_handler)

    stderr_log_handler = logging.StreamHandler()
    logger.addHandler(stderr_log_handler)

    # nice output format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_log_handler.setFormatter(formatter)
    stderr_log_handler.setFormatter(formatter)

    logger.setLevel('DEBUG')
    return logger

def telegram_bot_sendtext(bot_token, bot_chatID, bot_message):
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + str(bot_chatID) + '&parse_mode=Markdown&text=' + bot_message
    response = requests.get(send_text)

    return response.json()

async def wait_for_orders(trades, config, krk_exchange, bnb_exchange, logger):
    logger.info("Waiting for limit orders...")
    if config['telegram_notifications_on']:
        telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Waiting for limit orders...")
    # Check if there are trades to cancel
    krk_open_orders = krk_exchange.query_private('OpenOrders')['result']['open']
    logger.info(f'Kraken open trades: {krk_open_orders}')
    bnb_open_orders = bnb_exchange.get_open_orders(symbol=config['bnb_trading_pair'])
    logger.info(f'Binance open trades: {bnb_open_orders}')
    now = time.time()
    start_time = now
    while now - start_time < config['limit_order_time'] and (krk_open_orders or bnb_open_orders):
        logger.info(f"Waiting now {str(int((now - start_time)))} seconds")
        await asyncio.sleep(10)
        tries = 10
        success = False
        while tries >= 0 and not success:
            try:
                tries = tries - 1
                krk_open_orders = krk_exchange.query_private('OpenOrders')['result']['open']
                bnb_open_orders = bnb_exchange.get_open_orders(symbol=config['bnb_trading_pair'])
                logger.info(f'Kraken open trades: {krk_open_orders}')
                logger.info(f'Binance open trades: {bnb_open_orders}')
                success = True
                await asyncio.sleep(5)
            except:
                logger.info(traceback.format_exc())
                # wait a few seconds before trying again
                await asyncio.sleep(5)
                continue

        now = time.time()

    if not krk_open_orders and not bnb_open_orders:
        logger.info("Limit orders fulfilled")
        if config['telegram_notifications_on']:
            telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Limit orders fulfilled")
        return True
    else:
        logger.info(f"Waited more than {config['limit_order_time']} seconds for limit orders...")
        if config['telegram_notifications_on']:
            telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{config['bnb_trading_pair']}] Waited more than {config['limit_order_time']} seconds for limit orders unsuccessfully")
        return False

async def wait_for_bnb_order(trades, config, bnb_exchange, logger):
    logger.info("Waiting for limit orders...")
    if config['telegram_notifications_on']:
        telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> [{trades[0]['pair']}] Waiting for 2nd limit order...")
    # Check if there are trades to cancel
    bnb_open_orders = bnb_exchange.get_open_orders()
    logger.info(f'Binance open trades: {bnb_open_orders}')
    success = False
    now = time.time()
    start_time = now
    while now - start_time < config['limit_order_time'] and bnb_open_orders:
        logger.info(f"Waiting now {str(int((now - start_time)))} seconds")
        await asyncio.sleep(10)
        tries = 10
        success = False
        while tries >= 0 and not success:
            try:
                tries = tries - 1
                bnb_open_orders = bnb_exchange.get_open_orders()
                logger.info(f'Binance open trades: {bnb_open_orders}')
                success = True
                await asyncio.sleep(5)
            except:
                logger.info(traceback.format_exc())
                # wait a few seconds before trying again
                await asyncio.sleep(5)
                continue

        now = time.time()
    return success

async def short_wait_for_bnb_order(trades, config, bnb_exchange, logger):
    logger.info("Waiting for limit order (Binance)...")
    await asyncio.sleep(30)
    # Check if there are trades to cancel
    bnb_open_orders = bnb_exchange.get_open_orders()
    logger.info(f'Binance open trades: {bnb_open_orders}')
    success = True
    if bnb_open_orders:
        for _ in range(50):
            try:
                result_bnb = bnb_exchange.cancel_order(symbol=trades[0]['pair'], orderId=trades[0]['orderid'])
                logger.info(f"Cancelled order {trades[0]['orderid']}")
                success = False
                break
            except BinanceAPIException as e:
                logger.info(e.message)
                if "Unknown order sent" in e.message:
                    break
            except:
                logger.info(traceback.format_exc())
                await asyncio.sleep(5)
                continue
    return success

async def short_wait_for_krk_order(trades, config, krk_exchange, logger):
    logger.info("Waiting for limit order (Kraken)...")
    await asyncio.sleep(40)
    # Check if there are trades to cancel
    krk_open_orders = krk_exchange.query_private('OpenOrders')['result']['open']
    logger.info(f'Kraken open trades: {krk_open_orders}')
    success = True
    if krk_open_orders:
        logger.info(f"Cancelling order {trades[0]['orderid']}")
        for _ in range(50):
            try:
                krk_result = krk_exchange.query_private('CancelAll')
                success = False
                break
            except:
                logger.info(traceback.format_exc())
                await asyncio.sleep(5)
                continue
    return success


async def wait_for_withdrawals(withdrawal_id_krk, withdrawal_id_bnb, config, krk_exchange, bnb_exchange, krk_asset1, krk_asset2, logger):
    logger.info("Waiting for withdrawals...")
    if config['telegram_notifications_on']:
        telegram_bot_sendtext(config['telegram_bot_token'], config['telegram_user_id'], f"<Arbitrito> Waiting for withdrawals...")

    awaiting_withdrawals = True
    now = time.time()
    start_time = now
    while now - start_time < config['withdrawals_max_wait'] and awaiting_withdrawals:
        logger.info(f"Waiting for withdrawals {str(int(now - start_time))} seconds")
        tries = 10
        success = False
        while tries >= 0 and not success:
            try:
                tries = tries - 1
                # Wait until withdrawals are completed
                # Get withdrawals ids from Withdrawal histories and check its statuses
                krk_result = krk_exchange.query_private('WithdrawStatus', {'asset': krk_asset1})
                krk_success = [item for item in krk_result['result'] if item.get('refid') == withdrawal_id_krk]
                logger.info(f"krk_success -> {krk_success}")
                bnb_result = bnb_exchange.get_withdraw_history()
                bnb_success = [item for item in bnb_result['withdrawList'] if item['id'] == withdrawal_id_bnb]
                logger.info(f"bnb_success -> {bnb_success}")
                logger.info(f"Statuses -> krk: {krk_success[0]['status']} | bnb: {bnb_success[0]['status']}")
                success = True
            except:
                logger.info(traceback.format_exc())
                # wait a few seconds before trying again
                await asyncio.sleep(5)

        # Binance -> status=6 means 'success' in withdrawals from Binance API docs
        if krk_success[0]['status'] == 'Success' and bnb_success[0]['status'] == 6:
            awaiting_withdrawals = False

        if awaiting_withdrawals:
            await asyncio.sleep(20)
            logger.info("Waiting for withdrawals completion...")
        else:
            logger.info("Withdrawals completed.")

        now = time.time()

    # Get deposit tx ids
    krk_deposit_txid = bnb_success[0]['txId']
    logger.info(f"Kraken txid =>  {str(krk_deposit_txid)}")
    bnb_deposit_txid = krk_success[0]['txid']
    logger.info(f"Binance txid =>  {str(bnb_deposit_txid)}")

    # Wait until deposits are completed
    awaiting_deposits = True
    now = time.time()
    start_time = now
    krk_success = False
    bnb_success = []
    while now - start_time < config['deposits_max_wait'] and awaiting_deposits:
        logger.info(f"Waiting for deposits {str(int(now - start_time))} seconds")
        tries = 50
        success = False
        while tries >= 0 and not success:
            try:
                tries = tries - 1
                # Get Deposit histories
                # krk_deposit_history = krk_exchange.query_private('DepositStatus', {'asset': krk_asset2})
                # krk_success = [item for item in krk_deposit_history['result'] if item.get('txid') == krk_deposit_txid]
                # logger.info(f"krk_success -> {krk_success}")
                krk_balance = krk_exchange.query_private('Balance')
                krk_available = 0.0
                if 'USDT' in krk_balance['result']:
                    krk_available = krk_balance['result']['USDT']
                if float(krk_available) > 100.0:
                    krk_success = True
                bnb_deposit_history = bnb_exchange.get_deposit_history()
                bnb_success = [item for item in bnb_deposit_history['depositList'] if item['txId'] == bnb_deposit_txid]
                logger.info(f"bnb_success -> {bnb_success}")
                success = True
            except:
                logger.info(traceback.format_exc())
                # wait a few seconds before trying again
                await asyncio.sleep(5)

        if krk_success and len(bnb_success) > 0:
            if bnb_success[0]['status'] == 1:
                awaiting_deposits = False

        if awaiting_deposits:
            await asyncio.sleep(20)
            logger.info("Waiting for deposits completion...")
        else:
            logger.info("Deposits completed.")
        now = time.time()

    # Wait 20 seconds more to exchanges to properly set deposits to our accounts...
    await asyncio.sleep(10)

    return not awaiting_deposits


def get_candidates(krk_exchange, ticker_pairs):
    candidates = []
    for pair in ticker_pairs:
        ohlc_id = int(krk_exchange.query_public('OHLC', {'pair': pair, 'interval': '1440'})['result']['last'])
        ohlc = krk_exchange.query_public('OHLC', {'pair': pair, 'interval': '1440', 'since': ohlc_id})
        ohlc_o1 = ohlc['result'][pair][0][1]
        ohlc_c1 = ohlc['result'][pair][0][4]
        spread = float(ohlc_c1) / float(ohlc_o1)
        if spread > 1.0:
            candidates.append({'pair': pair, 'spread': spread})

    # Sort list by vwap ascending
    sorted_candidates = sorted(candidates, key=lambda k: k['spread'], reverse=True)
    return sorted_candidates


def update_google_sheet(sheet_id, data_range, balance, volume):
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

            service = build('sheets', 'v4', credentials=creds)

            # Call the Sheets API
            sheet = service.spreadsheets()

    # How the input data should be interpreted.
    value_input_option = 'USER_ENTERED'

    # How the input data should be inserted.
    insert_data_option = 'OVERWRITE'

    data_value = str(time.localtime(time.time())[1]) + '/' + str(time.localtime(time.time())[2]) + '/' + str(time.localtime(time.time())[0])

    value_range_body = {
        "range": data_range,
        "majorDimension": "ROWS",
        "values": [
            [data_value, balance],
        ],
    }

    # append new balance and date
    result = sheet.values().append(spreadsheetId=sheet_id,
                                   range=data_range,
                                   valueInputOption=value_input_option,
                                   insertDataOption=insert_data_option,
                                   body=value_range_body).execute()

    value_range_body = {
        "range": "Start_17_02_2021!F3:F3",
        "majorDimension": "ROWS",
        "values": [
            [volume],
        ],
    }

    # update volume
    result = sheet.values().update(spreadsheetId=sheet_id,
                                   range="Start_17_02_2021!F3:F3",
                                   valueInputOption=value_input_option,
                                   body=value_range_body).execute()

loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(main())
except KeyboardInterrupt:
    pass
finally:
    print("Stopping Arbitrito...")
    loop.close()
