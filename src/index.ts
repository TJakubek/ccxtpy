import { spawnPythonProcess } from "./initiateCcxtPython";

const exchanges = [
  "ascendex",
  //   "bequant",
  //   "binance",
  //   "binanceus",
  //   "binanceusdm",
  //   "bingx",
  //   "bitfinex",
  //   "bitfinex1",
  //   "bitget",
  //   "bitmart",
  //   "bitmex",
  //   // # 'bitopro', this one fucked up
  //   "bitrue",
  //   "bitstamp",
  //   "blockchaincom",
  //   // # 'blofin', no markets
  //   "bybit",
  //   "cryptocom",
  //   "deribit",
  //   "exmo",
  //   "gate",
  //   "gateio",
  //   // # 'gemini', something fucked up with market loading
  //   "hashkey",
  //   "hitbtc",
  //   "kraken",
  //   // # 'krakenfutures', no markets
  //   "kucoin",
  //   "myokx",
  //   // # 'okx', same market error as gemini
  //   "p2b",
  //   "phemex",
  //   "poloniex",
  //   // # 'poloniexfutures', no markets
  //   "probit",
  //   "upbit",
  //   "whitebit",
  //   "woo",
  //   "xt",
];
const runCcxtPython = async () => {
  for (let exchange of exchanges) spawnPythonProcess(exchange);
};

runCcxtPython();
