# arbitrito ![Alt text](other/readme_header.png?raw=true "Title")
A crypto arbitrage bot

“Buy low, sell high” (Wall Street proverb)

## Setting up

*python3
*To run this project, you may need to install the following python modules:

```
pip3 install cryptocom.exchange
pip3 install pyyaml
pip3 install krakenex # it's recommended to install for the user -> pip3 install --user krakenex
pip3 install python-binance
```

## Config

1. Duplicate "config/default_config.yaml" file
2. Rename it as "user_config.yaml" and add there your API keys and set the other values as you wish

Be careful! -> Do not set your API keys in default_config.yaml and push the file to git!

## Disclaimer
This project is for informational purposes only. You should not construe any such information or other material as legal, tax, investment, financial, or other advice. Nothing contained here constitutes a solicitation, recommendation, endorsement, or offer by me or any third party service provider to buy or sell any securities or other financial instruments in this or in any other jurisdiction in which such solicitation or offer would be unlawful under the securities laws of such jurisdiction.

If you plan to use real money, USE AT YOUR OWN RISK.

Under no circumstances will I be held responsible or liable in any way for any claims, damages, losses, expenses, costs, or liabilities whatsoever, including, without limitation, any direct or indirect damages for loss of profits.
