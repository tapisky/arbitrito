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
    opportunities_BTCEUR_250_BNB_KRK = 0
    opportunities_BTCEUR_250_KRK_BNB = 0
    opportunities_BTCEUR_50_BNB_KRK = 0
    opportunities_BTCEUR_50_KRK_BNB = 0
    opportunities_BTCDAI_250_BNB_KRK = 0
    opportunities_BTCDAI_250_KRK_BNB = 0
    opportunities_BTCDAI_50_BNB_KRK = 0
    opportunities_BTCDAI_50_KRK_BNB = 0
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
            logger.info(f'------------ Iteration {iteration} ------------')

            # Check first if exchanges are both up
            exchanges_are_up = exchanges_up(krk_exchange, bnb_exchange)

            if exchanges_are_up:
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

                # Kraken: Get my balances
                kraken_balances = get_kraken_balances(krk_exchange, config)
                logger.info(f"Kraken's Balances\n(Base) {config['krk_base_currency']} balance:{kraken_balances['krk_base_currency_available']} \n(Target) {config['krk_target_currency']} balance:{kraken_balances['krk_target_currency_available']}\n")

                # Binance: Get my balances
                binance_balances = get_binance_balances(bnb_exchange, config)
                logger.info(f"Binance's Balances\n(Base) {config['bnb_base_currency']} balance:{binance_balances['bnb_base_currency_available']} \n(Target) {config['bnb_target_currency']} balance:{binance_balances['bnb_target_currency_available']}\n")

                # Log total balances
                total_BTC = float(kraken_balances['krk_base_currency_available']) + float(binance_balances['bnb_base_currency_available'])
                total_EUR = float(kraken_balances['krk_target_currency_available']) + float(binance_balances['bnb_target_currency_available'])
                logger.info(f"Total balances: BTC={str(total_BTC)}  |  EUR={str(total_EUR)}")

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
                logger.info(f"{config['bnb_trading_pair']} Max(buy price) - Min(sell price) = {float(max_buy_price) - float(min_sell_price)}\n")

                # Pair2
                # Kraken trading pair2 ticker
                # krk_tickers = krk_exchange.query_public("Ticker", {'pair': config['krk_trading_pair2']})['result'][config['krk_trading_pair2']]
                # krk_buy_price2 = krk_tickers['b'][0]
                # krk_sell_price2 = krk_tickers['a'][0]
                # krk_high2 = krk_tickers['h'][0]
                # krk_low2 = krk_tickers['l'][0]
                # logger.info(f"\nKRAKEN => Market {config['krk_trading_pair2']}\nbuy price: {krk_buy_price2} - sell price: {krk_sell_price2} <> low: {krk_low2} - high: {krk_high2}\n")
                #
                # # Binance trading pair2 ticker
                # bnb_tickers = bnb_exchange.get_orderbook_tickers()
                # bnb_ticker = next(item for item in bnb_tickers if item['symbol'] == config['bnb_trading_pair2'])
                # bnb_buy_price2 = bnb_ticker['bidPrice']
                # bnb_sell_price2 = bnb_ticker['askPrice']
                # logger.info(f"\nBINANCE => Market {config['bnb_trading_pair2']}\nbuy price: {bnb_buy_price2} - sell price: {bnb_sell_price2}\n")
                #
                # buy_prices2 = {'krk': krk_buy_price2, 'bnb': bnb_buy_price2}
                # # buy_prices = {'cdc': cdc_buy_price, 'krk': krk_buy_price, 'bnb': bnb_buy_price}
                # max_buy_price_key2 = max(buy_prices2, key=buy_prices2.get)
                # max_buy_price2 = buy_prices2[max_buy_price_key2]
                # sell_prices2 = {'krk': krk_sell_price2, 'bnb': bnb_sell_price2}
                # # sell_prices = {'cdc': cdc_sell_price, 'krk': krk_sell_price, 'bnb': bnb_sell_price}
                # min_sell_price_key2 = min(sell_prices2, key=sell_prices2.get)
                # min_sell_price2 = sell_prices2[min_sell_price_key2]
                # logger.info(f"Max buy price -> {max_buy_price_key2} = {max_buy_price2}")
                # logger.info(f"Min sell price -> {min_sell_price_key2} = {min_sell_price2}")
                # logger.info(f"BTCDAI Max(buy price) - Min(sell price) = {float(max_buy_price2) - float(min_sell_price2)}\n")

                diff = (float(max_buy_price) - float(min_sell_price))
                # diff2 = (float(max_buy_price2) - float(min_sell_price2))

                # Create list of potential opportunities
                opportunity_list = [{'diff': diff, 'trading_pair_config_suffix': '', 'max_buy_price_key': max_buy_price_key, 'min_sell_price_key': min_sell_price_key}]
                                    # {'diff': diff2, 'trading_pair_config_suffix': '2', 'max_buy_price_key': max_buy_price_key2, 'min_sell_price_key': min_sell_price_key2}]
                # Sort list by diff descending
                sorted_opportunity_list = sorted(opportunity_list, key=lambda k: k['diff'], reverse=True)

                # Prnt sorted_opportunity_list for reference
                logger.info("Sorted Opportunity list:\n")
                for item in sorted_opportunity_list:
                    logger.info(f'{item}')

                for item in sorted_opportunity_list:
                    if item['diff'] >= 250.0:
                        if not config['safe_mode_on']:
                            try:
                                # Set trading pair accordingly
                                bnb_trading_pair = config['bnb_trading_pair' + item['trading_pair_config_suffix']]
                                krk_trading_pair = config['krk_trading_pair' + item['trading_pair_config_suffix']]

                                # Make orders
                                if item['max_buy_price_key'] == 'bnb' and item['min_sell_price_key'] == 'krk':
                                    if item['trading_pair_config_suffix'] == '':
                                        opportunities_BTCEUR_250_BNB_KRK += 1
                                    elif item['trading_pair_config_suffix'] == '2':
                                        opportunities_BTCDAI_250_BNB_KRK += 1
                                    # Market order to sell BTC in Binance
                                    result = bnb_exchange.order_market_sell(symbol=bnb_trading_pair, quantity=config['trade_amount'])
                                    if result:
                                        if result['status'] != "FILLED":
                                            raise Exception("Could not sell '{}' in pair '{}' in Binance. Status => {}".format(config['trade_amount'], bnb_trading_pair, result['status']))
                                    else:
                                        raise Exception("Could not sell '{}' in pair '{}' in Binance.".format(config['trade_amount'], bnb_trading_pair))
                                    logger.info(result)

                                    # Market order to buy the same amount of pair in Kraken
                                    result = krk_exchange.query_private('AddOrder', {'pair': krk_trading_pair, 'type': 'buy', 'ordertype': 'market', 'oflags': 'fciq', 'volume': config['trade_amount']})
                                    if result['error']:
                                        raise Exception("Could not buy '{}' in pair '{}' in Kraken: {}".format(config['trade_amount'], krk_trading_pair, result['error']))
                                    logger.info(result)

                                    # Kraken: Get my balances
                                    kraken_balances = get_kraken_balances(krk_exchange, config)
                                    logger.info(f"Kraken's Balances\n(Base) {config['krk_base_currency']} balance:{kraken_balances['krk_base_currency_available']} \n(Target) {config['krk_target_currency']} balance:{kraken_balances['krk_target_currency_available']}\n")

                                    # Binance: Get my balances
                                    binance_balances = get_binance_balances(bnb_exchange, config)
                                    logger.info(f"Binance's Balances\n(Base) {config['bnb_base_currency']} balance:{binance_balances['bnb_base_currency_available']} \n(Target) {config['bnb_target_currency']} balance:{binance_balances['bnb_target_currency_available']}\n")

                                elif item['max_buy_price_key'] == 'krk' and item['min_sell_price_key'] == 'bnb':
                                    # krk_balance = krk_exchange.query_private('Balance')
                                    # krk_base_currency_available = 0
                                    # if config['krk_base_currency'] in krk_balance['result']:
                                    #     krk_base_currency_available = krk_balance['result'][config['krk_base_currency']]
                                    # krk_base_currency_available = 0.001
                                    # krk_tickers = krk_exchange.query_public("Ticker", {'pair': config['krk_buy_trading_pair_step2']})['result'][config['krk_buy_trading_pair_step2']]
                                    # krk_buy_price = krk_tickers['b'][0]
                                    # krk_xrp_volume = round(float(krk_base_currency_available) / float(krk_buy_price), 8)

                                    if item['trading_pair_config_suffix'] == '':
                                        opportunities_BTCEUR_250_KRK_BNB += 1
                                    elif item['trading_pair_config_suffix'] == '2':
                                        opportunities_BTCDAI_250_KRK_BNB += 1

                                    # Market order to sell pair in Kraken
                                    result = krk_exchange.query_private('AddOrder', {'pair': krk_trading_pair, 'type': 'sell', 'ordertype': 'market', 'oflags': 'fciq', 'volume': config['trade_amount']})
                                    if result['error']:
                                        raise Exception("Could not sell '{}' in pair '{}' in Kraken: {}".format(config['trade_amount'], krk_trading_pair, result['error']))
                                    logger.info(result)

                                    # Market order to buy the same amount of pair in Binance
                                    result = bnb_exchange.order_market_buy(symbol=bnb_trading_pair, quantity=config['trade_amount'])
                                    if result:
                                        if result['status'] != "FILLED":
                                            raise Exception("Could not buy '{}' in pair '{}' in Binance. Status => {}".format(config['trade_amount'], bnb_trading_pair, result['status']))
                                    else:
                                        raise Exception("Could not buy '{}' in pair '{}' in Binance.".format(config['trade_amount'], bnb_trading_pair))
                                    logger.info(result)

                                    # Kraken: Get my balances
                                    kraken_balances = get_kraken_balances(krk_exchange, config)
                                    logger.info(f"Kraken's Balances\n(Base) {config['krk_base_currency']} balance:{kraken_balances['krk_base_currency_available']} \n(Target) {config['krk_target_currency']} balance:{kraken_balances['krk_target_currency_available']}\n")

                                    # Binance: Get my balances
                                    binance_balances = get_binance_balances(bnb_exchange, config)
                                    logger.info(f"Binance's Balances\n(Base) {config['bnb_base_currency']} balance:{binance_balances['bnb_base_currency_available']} \n(Target) {config['bnb_target_currency']} balance:{binance_balances['bnb_target_currency_available']}\n")


                                # Wait 20 seconds more to exchanges to properly complete trades...
                                # await asyncio.sleep(20)

                            except Exception as e:
                                logger.info(traceback.format_exc())
                                # logger.info("\nException occurred -> '{}'. Waiting for next iteration... ({} seconds)\n\n\n".format(e, config['seconds_between_iterations']))

                                # Kraken: Get my balances
                                kraken_balances = get_kraken_balances(krk_exchange, config)
                                logger.info(f"Kraken's Balances\n(Base) {config['krk_base_currency']} balance:{kraken_balances['krk_base_currency_available']} \n(Target) {config['krk_target_currency']} balance:{kraken_balances['krk_target_currency_available']}\n")

                                # Binance: Get my balances
                                binance_balances = get_binance_balances(bnb_exchange, config)
                                logger.info(f"Binance's Balances\n(Base) {config['bnb_base_currency']} balance:{binance_balances['bnb_base_currency_available']} \n(Target) {config['bnb_target_currency']} balance:{binance_balances['bnb_target_currency_available']}\n")

                                # Continue to next opportunity
                                continue


                    elif 50.0 <= diff < 250.0:
                        if item['max_buy_price_key'] == 'bnb' and item['min_sell_price_key'] == 'krk':
                            if item['trading_pair_config_suffix'] == '':
                                opportunities_BTCEUR_50_BNB_KRK += 1
                            elif item['trading_pair_config_suffix'] == '2':
                                opportunities_BTCDAI_50_BNB_KRK += 1
                        elif item['max_buy_price_key'] == 'krk' and item['min_sell_price_key'] == 'bnb':
                            if item['trading_pair_config_suffix'] == '':
                                opportunities_BTCEUR_50_KRK_BNB += 1
                            elif item['trading_pair_config_suffix'] == '2':
                                opportunities_BTCDAI_50_KRK_BNB += 1

                opportunities = {'opportunities_BTCEUR_250_BNB-KRK': opportunities_BTCEUR_250_BNB_KRK,
                                 'opportunities_BTCEUR_250_KRK_KRK': opportunities_BTCEUR_250_KRK_BNB,
                                 'opportunities_BTCEUR_50_BNB_KRK': opportunities_BTCEUR_50_BNB_KRK,
                                 'opportunities_BTCEUR_50_KRK_BNB': opportunities_BTCEUR_50_KRK_BNB}
                                 # 'opportunities_BTCDAI_250_BNB_KRK': opportunities_BTCDAI_250_BNB_KRK,
                                 # 'opportunities_BTCDAI_250_KRK_BNB': opportunities_BTCDAI_250_KRK_BNB,
                                 # 'opportunities_BTCDAI_50_BNB_KRK': opportunities_BTCDAI_50_BNB_KRK,
                                 # 'opportunities_BTCDAI_50_KRK_BNB': opportunities_BTCDAI_50_KRK_BNB}

                for key, value in opportunities.items():
                    logger.info(f'{key} = {value}')

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
            # logger.info(traceback.format_exc())
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
    krk_base_currency_available = 0
    if config['krk_base_currency'] in krk_balance['result']:
        krk_base_currency_available = krk_balance['result'][config['krk_base_currency']]
    # Kraken: Get my target currency balance
    krk_target_currency_available = 0
    if config['krk_target_currency'] in krk_balance['result']:
        krk_target_currency_available = krk_balance['result'][config['krk_target_currency']]
    return ({'krk_base_currency_available': krk_base_currency_available, 'krk_target_currency_available': krk_target_currency_available})

def get_binance_balances(exchange, config):
    bnb_balance_result = exchange.get_asset_balance(asset=config['bnb_base_currency'])
    if bnb_balance_result:
        bnb_base_currency_available = float(bnb_balance_result['free'])
    else:
        bnb_base_currency_available = 0.0
    bnb_balance_result = exchange.get_asset_balance(asset=config['bnb_target_currency'])
    if bnb_balance_result:
        bnb_target_currency_available = float(bnb_balance_result['free'])
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



loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(main())
except KeyboardInterrupt:
    pass
finally:
    print("Stopping Arbitrito...")
    loop.close()
