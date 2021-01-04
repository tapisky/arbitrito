#!/usr/bin/env python3
import asyncio
import time
import logging
import yaml
import sys
import traceback
from os.path import exists
import cryptocom.exchange as cro
from cryptocom.exchange.structs import Pair
from cryptocom.exchange.structs import PrivateTrade
import krakenex
from binance.client import Client as Client

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
    opportunities175 = 0
    opportunities120 = 0
    config = get_config()
    logger = setupLogger('logfile.log')

    #Crypto.com API setup
    cdc_exchange = cro.Exchange()
    cdc_account = cro.Account(api_key=config['cdc_api_key'], api_secret=config['cdc_api_secret'])
    cdc_pair = eval('cro.pairs.' + config['cdc_trading_pair'])

    # Kraken API setup
    krk_exchange = krakenex.API(key=config['krk_api_key'], secret=config['krk_api_secret'])

    # Binance API setup
    binance = Binance(public_key=config['bnb_api_key'], secret_key=config['bnb_api_secret'], sync=True)
    bnb_exchange = binance.b

    while True:
        try:
            iteration += 1
            print(f'------------ Iteration {iteration} ------------')
            # Check Balances
            # cdc_coin_base_currency = eval('cro.coins.' + config['cdc_base_currency'])
            # cdc_target_currency = eval('cro.coins.' + config['cdc_target_currency'])
            # cdc_balances = await cdc_account.get_balance()
            # Crypto.com: Get my base currency balance
            # cdc_base_currency_balance = cdc_balances[cdc_coin_base_currency]
            # cdc_base_currency_available = cdc_base_currency_balance.available
            # Get my Target currency balance
            # cdc_target_currency_balance = cdc_balances[cdc_target_currency]
            # EXAMPLE BTC_balance:Balance(total=0.04140678, available=3.243e-05, in_orders=0.04137435, in_stake=0, coin=Coin(name='BTC'))
            # logger.info(f"Crypto.com's Balances\n(Base) {config['cdc_base_currency']} balance:{cdc_base_currency_balance} \n(Target) {config['cdc_target_currency']} balance:{cdc_target_currency_balance}\n\n")

            # Kraken: Get my base currency balance
            krk_balance = krk_exchange.query_private('Balance')
            krk_base_currency_available = 0
            if config['krk_base_currency'] in krk_balance['result']:
                krk_base_currency_available = krk_balance['result'][config['krk_base_currency']]
            # Kraken: Get my target currency balance
            krk_target_currency_available = 0
            if config['krk_target_currency'] in krk_balance['result']:
                krk_target_currency_available = krk_balance['result'][config['krk_target_currency']]
            logger.info(f"Kraken's Balances\n(Base) {config['krk_base_currency']} balance:{krk_base_currency_available} \n(Target) {config['krk_target_currency']} balance:{krk_target_currency_available}\n")

            # Binance: Get my base currency balance
            bnb_btc_balance_result = bnb_exchange.get_asset_balance(asset=config['bnb_base_currency'])
            if bnb_btc_balance_result:
                bnb_btc_balance = float(bnb_btc_balance_result['free'])
            else:
                bnb_btc_balance = 0.0
            logger.info(f"Binance's Balances\nBTC balance:{bnb_btc_balance} \n")

            # Check target currency price differences in exchanges
            # Crypto.com target currency ticker
            # cdc_tickers = await cdc_exchange.get_tickers()
            # cdc_ticker = cdc_tickers[cdc_pair]
            # cdc_buy_price = cdc_ticker.buy_price
            # cdc_sell_price = cdc_ticker.sell_price
            # cdc_high = cdc_ticker.high
            # cdc_low = cdc_ticker.low
            # logger.info(f'\nCRYPTO.COM => Market {cdc_pair.name}\nbuy price: {cdc_buy_price} - sell price: {cdc_sell_price} <> low: {cdc_low} - high: {cdc_high}\n\n')

            # Kraken trading pair ticker
            krk_tickers = krk_exchange.query_public("Ticker", {'pair': config['krk_trading_pair']})['result'][config['krk_trading_pair']]
            krk_buy_price = krk_tickers['b'][0]
            krk_sell_price = krk_tickers['a'][0]
            krk_high = krk_tickers['h'][0]
            krk_low = krk_tickers['l'][0]
            logger.info(f"\nKRAKEN => Market {config['krk_trading_pair']}\nbuy price: {krk_buy_price} - sell price: {krk_sell_price} <> low: {krk_low} - high: {krk_high}\n")

            # Binance trading pair ticker
            bnb_tickers = bnb_exchange.get_orderbook_tickers()
            bnb_ticker = next(item for item in bnb_tickers if item['symbol'] == config['bnb_trading_pair'])
            bnb_buy_price = bnb_ticker['bidPrice']
            bnb_sell_price = bnb_ticker['askPrice']
            logger.info(f"\nBINANCE => Market {config['bnb_trading_pair']}\nbuy price: {bnb_buy_price} - sell price: {bnb_sell_price}\n")

            buy_prices = {'krk': krk_buy_price, 'bnb': bnb_buy_price}
            # buy_prices = {'cdc': cdc_buy_price, 'krk': krk_buy_price, 'bnb': bnb_buy_price}
            max_buy_price_key = max(buy_prices, key=buy_prices.get)
            max_buy_price = buy_prices[max_buy_price_key]
            sell_prices = {'krk': krk_sell_price, 'bnb': bnb_sell_price}
            # sell_prices = {'cdc': cdc_sell_price, 'krk': krk_sell_price, 'bnb': bnb_sell_price}
            min_sell_price_key = min(sell_prices, key=sell_prices.get)
            min_sell_price = sell_prices[min_sell_price_key]
            logger.info(f"Max buy price -> {max_buy_price_key} = {max_buy_price}")
            logger.info(f"Min sell price -> {min_sell_price_key} = {min_sell_price}")
            logger.info(f"Max(buy price) - Min(sell price) = {float(max_buy_price) - float(min_sell_price)}\n")

            diff = (float(max_buy_price) - float(min_sell_price))
            if diff >= 175.0:
                opportunities175 += 1
                if max_buy_price_key == 'bnb' and min_sell_price_key == 'krk' and not config['safe_mode_on']:
                    try:
                        # Market order to buy XRP with BTC in Binance
                        result = bnb_exchange.order_market_buy(symbol=config['bnb_buy_trading_pair'], quoteOrderQty=bnb_btc_balance)
                        if result:
                            if result['status'] != "FILLED":
                                raise Exception("Could not sell BTC for XRP in Binance: {}".format(result))
                        else:
                            raise Exception("Could not sell BTC for XRP in Binance. 'result' is empty!")
                        logger.info(result)

                        # Market order to buy the same amount of BTC with EUR in Kraken
                        result = krk_exchange.query_private('AddOrder', {'pair': config['krk_buy_trading_pair'], 'type': 'buy', 'ordertype': 'market', 'oflags': 'fciq', 'volume': bnb_btc_balance})
                        if result['error']:
                            raise Exception("Could not perform 'AddOrder' of type 'buy' in Kraken")
                        logger.info(result)

                        # In Binance: Send XRP to Kraken
                        # First get XRP balance
                        bnb_xrp_balance_result = bnb_exchange.get_asset_balance(asset=config['bnb_base_currency'])
                        if bnb_xrp_balance_result:
                            bnb_xrp_balance = float(bnb_xrp_balance_result['free'])
                        else:
                            bnb_xrp_balance = 0.0
                        logger.info(f"Binance's Balances\nXRP balance:{bnb_xrp_balance} \n")
                        # Send XRP from Binance to Kraken
                        result = bnb_exchange.withdraw(asset='XRP', address=config['krk_xrp_address'], addressTag=config['krk_xrp_address_tag'], amount=bnb_xrp_balance)
                        if result:
                            if not result['success']:
                                raise Exception("Could not send XRP to Kraken")
                        else:
                            raise Exception("Could not send XRP to Kraken")
                        logger.info(result)

                        # In Kraken: buy XRP with BTC
                        # First get BTC Balance
                        krk_balance = krk_exchange.query_private('Balance')
                        krk_base_currency_available = 0
                        if config['krk_base_currency'] in krk_balance['result']:
                            krk_base_currency_available = krk_balance['result'][config['krk_base_currency']]
                        krk_tickers = krk_exchange.query_public("Ticker", {'pair': config['krk_buy_trading_pair_step2']})['result'][config['krk_buy_trading_pair_step2']]
                        krk_buy_price = krk_tickers['b'][0]
                        krk_xrp_volume = round(float(krk_base_currency_available) / float(krk_buy_price), 8)

                        result = krk_exchange.query_private('AddOrder', {'pair': config['krk_buy_trading_pair_step2'], 'type': 'buy', 'ordertype': 'market', 'oflags': 'fciq', 'volume': krk_xrp_volume})
                        if result['error']:
                            raise Exception("Could not buy XRP with BTC in Kraken: {}".format(result['error']))
                        logger.info(result)

                        #  Send XRP amount from Kraken to Binance
                        krk_balance = krk_exchange.query_private('Balance')
                        krk_xrp_volume = 0
                        if 'XXRP' in krk_balance['result']:
                            krk_xrp_volume = krk_balance['result']['XXRP']
                        result = krk_exchange.query_private('Withdraw', {'asset': 'XXRP', 'key': config['krk_bnb_xrp_address_key'], 'amount': krk_xrp_volume})

                        # Wait until withdrawals are complete (should be max 4 mins for XRP transfers)
                        # Give withdrawals some time to do their thing
                        await asyncio.sleep(10)
                        tries = 0
                        waiting = True
                        while tries < 50 and waiting:
                            tries =+ 1
                            # Get XRP balance in Kraken
                            krk_balance = krk_exchange.query_private('Balance')
                            krk_xrp_volume_from_bnb = 0
                            if 'XXRP' in krk_balance['result']:
                                krk_xrp_volume_from_bnb = krk_balance['result']['XXRP']
                            # Get XRP balance in Binance
                            bnb_xrp_balance_result = bnb_exchange.get_asset_balance(asset='XRP')
                            if bnb_xrp_balance_result:
                                bnb_xrp_balance_from_krk = float(bnb_xrp_balance_result['free'])
                            else:
                                bnb_xrp_balance_from_krk = 0.0
                            Waiting = (krk_xrp_volume_from_bnb + 100.0) > bnb_xrp_balance and (bnb_xrp_balance_from_krk + 100.0) > krk_xrp_volume
                            await asyncio.sleep(10)

                        # in Binance sell XRP to buy BTC
                        bnb_xrp_balance_result = bnb_exchange.get_asset_balance(asset='XRP')
                        if bnb_xrp_balance_result:
                            bnb_xrp_balance = float(bnb_xrp_balance_result['free'])
                        else:
                            bnb_xrp_balance = 0.0
                        # bnb_xrp_balance = round(bnb_xrp_balance, 3)
                        logger.info(f"Binance's Balances\nXRP balance:{bnb_xrp_balance} \n")

                        # Market order to buy XRP with BTC in Binance
                        result = bnb_exchange.order_market_sell(symbol=config['bnb_buy_trading_pair'], quantity=int(bnb_xrp_balance))
                        if result:
                            if result['status'] != "FILLED":
                                raise Exception("Could not sell BTC for XRP in Binance: {}".format(result))
                        else:
                            raise Exception("Could not sell BTC for XRP in Binance")
                        logger.info(result)

                        # In Kraken: sell XRP to buy EUR
                        krk_balance = krk_exchange.query_private('Balance')
                        krk_xrp_volume = 0
                        if 'XXRP' in krk_balance['result']:
                            krk_xrp_volume = krk_balance['result']['XXRP']
                        result = krk_exchange.query_private('AddOrder', {'pair': config['krk_buy_trading_pair_step3'], 'type': 'sell', 'ordertype': 'market', 'oflags': 'fciq', 'volume': krk_xrp_volume})
                        if result['error']:
                            raise Exception("Could not buy BTC with EUR in Kraken: {}".format(result))
                        logger.info(result)
                    except Exception as e:
                        logger.info(traceback.format_exc())
                        logger.info("\nException occurred -> '{}'. Waiting for next iteration... ({} seconds)\n\n\n".format(e, config['seconds_between_iterations']))



            elif 120.0 <= diff < 175.0:
                opportunities120 += 1

            opportunities = {'Opportunities175': opportunities175,
                             'Opportunities120': opportunities120}

            for key, value in opportunities.items():
                logger.info(f'{key} = {value}')

            print(f'------------ Iteration {iteration} ------------\n')
            if config['test_mode_on']:
                await asyncio.sleep(1)
                break
            else:
                # Wait given seconds until next poll
                logger.info("Waiting for next iteration... ({} seconds)\n\n\n".format(config['seconds_between_iterations']))
                await asyncio.sleep(config['seconds_between_iterations'])
        except Exception as e:
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



loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(main())
except KeyboardInterrupt:
    pass
finally:
    print("Stopping Arbitrito...")
    loop.close()
