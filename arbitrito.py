#!/usr/bin/env python3
import asyncio
import time
import logging
import yaml
import sys
from os.path import exists
import cryptocom.exchange as cro
from cryptocom.exchange.structs import Pair
from cryptocom.exchange.structs import PrivateTrade
import krakenex
from binance.client import Client as BNBClient

async def main():
    config = get_config()
    logger = setupLogger('logfile.log')

    #Crypto.com API setup
    cdc_exchange = cro.Exchange()
    cdc_account = cro.Account(api_key=config['cdc_api_key'], api_secret=config['cdc_api_secret'])
    cdc_pair = eval('cro.pairs.' + config['cdc_trading_pair'])

    # Kraken API setup
    krk_exchange = krakenex.API(key=config['krk_api_key'], secret=config['krk_api_secret'])

    # Binance API setup
    bnb_exchange = BNBClient('', '')

    # Check Balances
    cdc_coin_base_currency = eval('cro.coins.' + config['cdc_base_currency'])
    cdc_target_currency = eval('cro.coins.' + config['cdc_target_currency'])
    cdc_balances = await cdc_account.get_balance()
    # Crypto.com: Get my base currency balance
    cdc_base_currency_balance = cdc_balances[cdc_coin_base_currency]
    cdc_base_currency_available = cdc_base_currency_balance.available
    # Get my Target currency balance
    cdc_target_currency_balance = cdc_balances[cdc_target_currency]
    # EXAMPLE BTC_balance:Balance(total=0.04140678, available=3.243e-05, in_orders=0.04137435, in_stake=0, coin=Coin(name='BTC'))
    logger.info(f"Crypto.com's Balances\n(Base) {config['cdc_base_currency']} balance:{cdc_base_currency_balance} \n(Target) {config['cdc_target_currency']} balance:{cdc_target_currency_balance}\n\n")

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

    iteration = 0

    # Check target currency price differences in exchanges
    # Crypto.com target currency ticker
    cdc_tickers = await cdc_exchange.get_tickers()
    cdc_ticker = cdc_tickers[cdc_pair]
    cdc_buy_price = cdc_ticker.buy_price
    cdc_sell_price = cdc_ticker.sell_price
    cdc_high = cdc_ticker.high
    cdc_low = cdc_ticker.low
    logger.info(f'\nCRYPTO.COM => Market {cdc_pair.name}\nbuy price: {cdc_buy_price} - sell price: {cdc_sell_price} <> low: {cdc_low} - high: {cdc_high}\n\n')

    # Kraken target currency ticker
    krk_tickers = krk_exchange.query_public("Ticker", {'pair': config['krk_trading_pair']})['result'][config['krk_trading_pair']]
    krk_buy_price = krk_tickers['b'][0]
    krk_sell_price = krk_tickers['a'][0]
    krk_high = krk_tickers['h'][0]
    krk_low = krk_tickers['l'][0]
    logger.info(f"\nKRAKEN => Market {config['krk_trading_pair']}\nbuy price: {krk_buy_price} - sell price: {krk_sell_price} <> low: {krk_low} - high: {krk_high}\n")

    # Check target currency price differences in exchanges
    # Binance target currency ticker
    bnb_tickers = bnb_exchange.get_orderbook_tickers()
    bnb_ticker = next(item for item in bnb_tickers if item['symbol'] == config['bnb_trading_pair'])
    bnb_buy_price = bnb_ticker['bidPrice']
    bnb_sell_price = bnb_ticker['askPrice']
    logger.info(f"\nBINANCE => Market {config['bnb_trading_pair']}\nbuy price: {bnb_buy_price} - sell price: {bnb_sell_price}\n")

    buy_prices = {'cdc': cdc_buy_price, 'krk': krk_buy_price, 'bnb': bnb_buy_price}
    max_buy_price = buy_prices[max(buy_prices)]
    sell_prices = {'cdc': cdc_sell_price, 'krk': krk_sell_price, 'bnb': bnb_sell_price}
    min_sell_price = sell_prices[min(sell_prices)]
    logger.info(f"Max buy price -> {max(buy_prices)} = {max_buy_price}")
    logger.info(f"Min sell price -> {min(sell_prices)} = {min_sell_price}")
    logger.info(f"Max(buy price) - Min(sell price) = {float(max_buy_price) -float(min_sell_price)}")

    await asyncio.sleep(1)

  # while True:
  #   try:
  #     iteration += 1
  #     print(f'------------ Iteration {iteration} ------------')
  #     # Get selected trading pair ticker info on Crypto.Com
  #     cdc_tickers = await cdc_exchange.get_tickers()
  #     cdc_ticker = cdc_tickers[pair]
  #     cdc_buy_price = ticker.buy_price
  #     cdc_target_price = ticker.target_price
  #     cdc_high = ticker.high
  #     cdc_low = ticker.low
  #     logger.info(f'\nCRYPTO.COM => Market {pair.name}\nbuy price: {buy_price} - target price: {target_price} <> low: {low} - high: {high}\n')
  #
  #
  #
  #
  #     # EXAMPLE ticker = MarketTicker(pair=Pair(name='ETH_BTC'),
  #     # buy_price=0.027638, target_price=0.027641, trade_price=0.027641,
  #     # time=1608466157, volume=10596.948,
  #     # high=0.028549, low=0.027439, change=-0.001)
  #
  #     # Get my base currency balance
  #     balances = await account.get_balance()
  #     base_currency_balance = balances[cro_coin_base_currency]
  #     base_currency_available = base_currency_balance.available
  #     # EXAMPLE BTC_balance:Balance(total=0.04140678, available=3.243e-05, in_orders=0.04137435, in_stake=0, coin=Coin(name='BTC'))
  #
  #     # Get my Core Number currency balance
  #     core_number_currency_balance = balances[cro_coin_core_number_currency]
  #
  #     logger.info(f'Balances\n(Base) {base_currency} balance:{base_currency_balance} \n(Core) {core_number_currency} balance:{core_number_currency_balance}\n')
  #
  #     ###########################
  #     # Core Number Adjustments #
  #     ###########################
  #     deviated_core_number = base_currency_available / buy_price
  #     logger.info(f'Core number adjustments')
  #     logger.info(f'Core number: {core_number} {core_number_currency}')
  #     logger.info(f'Deviated Core number:{deviated_core_number:.6f} {core_number_currency}')
  #     excess = round(deviated_core_number - core_number, config['max_decimals_buy'])
  #     increase_percentage = excess * 100 / core_number
  #     missing = round(core_number - deviated_core_number, config['max_decimals_target'])
  #     decrease_percentage = missing * 100 / core_number
  #
  #     if coreNumberExploded(core_number, deviated_core_number, config['max_core_number_increase_percentage']):
  #       logger.info(f'> Exploded {increase_percentage:.2f}%\nConsider updating CoreNumber to {deviated_core_number:.6f}')
  #
  #     elif coreNumberIncreased(core_number, deviated_core_number, config['min_core_number_increase_percentage'], config['max_core_number_increase_percentage']):
  #       logger.info(f'Increased {increase_percentage:.2f}% - excess of {excess:.6f} {core_number_currency} denominated in {base_currency}')
  #       tx_result = round(excess * buy_price, config['max_decimals_buy'])
  #       logger.info(f'\n\n>>> Selling: {tx_result:.6f} {base_currency} at {buy_price} to park an excess of {excess:.6f} {core_number_currency}\n')
  #       # Sell excess of base currency ie. => Market Sell for ETH_BTC is denominated in BTC!!!
  #       if (not config['safe_mode_on']):
  #         await account.buy_market(pair, tx_result)
  #
  #     elif coreNumberDecreased(core_number, deviated_core_number, config['min_core_number_decrease_percentage'], config['max_core_number_decrease_percentage']):
  #       logger.info(f'Decreased {decrease_percentage:.2f}% - missing {missing:.6f} {core_number_currency} denominated in {base_currency}')
  #       tx_result = missing * target_price
  #       logger.info(f'\n\n>>> Buying: {tx_result:.6f} {base_currency} at {buy_price} taking {missing:.6f} {core_number_currency} from reserves\n')
  #       # Buy missing base currency; ie. => in ETH_BTC pair, buy missing BTC => Sell ETH
  #       if (not config['safe_mode_on']):
  #         await account.target_market(pair, missing)
  #
  #     elif coreNumberPlummeted(core_number, deviated_core_number, config['max_core_number_decrease_percentage']):
  #       logger.info(f'> Plummeted {decrease_percentage:.2f}%\nConsider updating CoreNumber to {deviated_core_number:.6f}')
  #
  #     else:
  #       logger.info(f'> Price is rock-solid stable ({increase_percentage:.2f}%)')
  #
  #     # Update balances after adjusting to core number
  #     balances = await account.get_balance()
  #     logger.info(f'Final {base_currency} available:{balances[cro_coin_base_currency].available} - {core_number_currency} available:{balances[cro_coin_core_number_currency].available}')
  #
  #     # Loop end
  #     print(f'------------ Iteration {iteration} ------------\n')
  #     if config['test_mode_on']:
  #       await asyncio.sleep(1)
  #       break
  #     else:
  #       # Wait given seconds until next poll
  #       logger.info("Waiting for next iteration... ({} seconds)\n\n\n".format(config['seconds_between_iterations']))
  #       await asyncio.sleep(config['seconds_between_iterations'])
  #
  #   except Exception as e:
  #       # Network issue(s) occurred (most probably). Jumping to next iteration
  #       logger.info("Exception occurred -> '{}'. Waiting for next iteration... ({} seconds)\n\n\n".format(e, config['seconds_between_iterations']))
  #       await asyncio.sleep(config['seconds_between_iterations'])


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

    # try:
    #   eval('cro.coins.' + data['core_number_currency'])
    # except AttributeError:
    #   print('Currency "{}" does not exist (check your config_file)'.format(data['core_number_currency']))
    #   sys.exit(1)
    # try:
    #   eval('cro.coins.' + data['base_currency'])
    # except AttributeError:
    #   print('Currency "{}" does not exist (check your config_file)'.format(data['base_currency']))
    #   sys.exit(1)


def coreNumberIncreased(core_number, deviated_core_number, min_core_number_increase_percentage, max_core_number_increase_percentage):
    min_core_number_increase = core_number * (1 + (min_core_number_increase_percentage/100))
    max_core_number_increase = core_number * (1 + (max_core_number_increase_percentage/100))
    return deviated_core_number >= min_core_number_increase and deviated_core_number <= max_core_number_increase

def coreNumberExploded(core_number, deviated_core_number, max_core_number_increase_percentage):
    max_core_number_increase = core_number * (1 + (max_core_number_increase_percentage/100))
    return deviated_core_number > max_core_number_increase

def coreNumberDecreased(core_number, deviated_core_number, min_core_number_decrease_percentage, max_core_number_decrease_percentage):
    min_core_number_decrease = core_number * (1 - (min_core_number_decrease_percentage/100))
    max_core_number_decrease = core_number * (1 - (max_core_number_decrease_percentage/100))
    return deviated_core_number <= min_core_number_decrease and deviated_core_number >= max_core_number_decrease

def coreNumberPlummeted(core_number, deviated_core_number, max_core_number_decrease_percentage):
    max_core_number_decrease = core_number * (1 - (max_core_number_decrease_percentage/100))
    return deviated_core_number < max_core_number_decrease

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
