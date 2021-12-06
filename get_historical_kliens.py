import os, time
import pandas as pd
from unicorn_binance_rest_api.unicorn_binance_rest_api_manager import BinanceRestApiManager


def get_futures_historical_klines_ubra(lst_coin, exchange, start_time, end_time=None):
    """
    :param lst_coin: list of coin names
    :param exchange: Select binance.com, binance.com-testnet, binance.com-margin, binance.com-margin-testnet,
         binance.com-isolated_margin, binance.com-isolated_margin-testnet, binance.com-futures,
         binance.com-futures-testnet, binance.com-coin-futures, binance.us, trbinance.com
         or jex.com (default: binance.com) This overules parameter `tld`.
    :param start_time: eg. "2021/11/5"
    :param end_time: eg. "2021/11/5"

    """
    # set api key and secret key
    api_key = ""
    api_secret = ""

    # create connections
    ubra = BinanceRestApiManager(api_key, api_secret, exchange=exchange)

    columns_name = ["Time", "Open", "High", "Low", "Close", "Volume", "CloseTime",
                    "Volume quote", "Number of trades", "Taker buy base asset volume", "Taker buy quote asset volume",
                    "Ignore"]

    # set data_path to save the file
    # if not exist, create one
    if not os.path.isdir("data"):
        os.mkdir("data")
    data_path = os.path.join(os.getcwd(), "data")

    # make sure all coin names are capitalized
    lst_coin = [i.upper() for i in lst_coin]

    for coin_name in lst_coin:
        print("-"*80)
        print(coin_name)
        print("working on")

        # get historical klines
        klines = ubra.get_futures_historical_klines(symbol=coin_name,interval="1m", start_str=start_time, end_str=end_time)

        # change it to pandas dataframe
        df = pd.DataFrame(klines, columns=columns_name)
        df = df.loc[:, ["Time", "Open", "High", "Low", "Close", "Volume"]]
        df.loc[:, "Time"] = pd.to_datetime(df.Time, unit="ms")
        df.set_index("Time", inplace=True)

        # set file_name
        file_name = f"binance_futures_{coin_name}_{start_time}_{end_time}.csv".replace("/", "")
        # save as csv
        df.to_csv(os.path.join(data_path, file_name))
        print("finished")
    print("All finished!")


if __name__ == "__main__":
    # coin name
    lst_coin = ["adausdt", "bnbusdt", "xrpusdt", "ethusdt", "btcusdt"]

    # dates
    start_time = "2021/11/5"
    end_time = "2021/11/26"

    # get klines data and save them
    get_futures_historical_klines_ubra(lst_coin, "binance.com", start_time, end_time)