import { spawnPythonProcess } from "./initiateCcxtPython";
import { MongoClient } from "mongodb";
import * as dotenv from "dotenv";

dotenv.config();

const exchanges = [
  // "ascendex",
  // "bequant",
  // "binance",
  // "binanceus",
  // "binanceusdm",
  // "bingx",
  // "bitfinex",
  // "bitfinex1",
  "bitget",
  // "bitmart",
  "bitmex",
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

const fetchTokensFromMongo = async (): Promise<string[]> => {
  const mongoUrl =
    process.env.MONGO_SERVER ||
    "mongodb://tsoData:naefeed0@65.109.52.201:27017";
  const client = new MongoClient(mongoUrl);

  try {
    await client.connect();
    const db = client.db("tso");
    const collection = db.collection("symbols");

    const documents = await collection.find({}).toArray();
    const tokens = [
      ...new Set(
        documents.map((doc: any) => doc.symbol).filter((symbol: any) => symbol)
      ),
    ] as string[];

    console.log(`✅ Fetched ${tokens.length} unique tokens from MongoDB`);
    return tokens;
  } catch (error) {
    console.error("❌ Failed to fetch tokens from MongoDB:", error);
    // Fallback to hardcoded tokens
    return [
      "BTC",
      "ETH",
      "BNB",
      "XRP",
      "ADA",
      "SOL",
      "DOGE",
      "DOT",
      "AVAX",
      "SHIB",
      "MATIC",
      "LTC",
      "UNI",
      "LINK",
      "ATOM",
      "ETC",
      "XLM",
      "BCH",
      "ALGO",
      "VET",
      "ICP",
      "FIL",
      "TRX",
      "APE",
      "NEAR",
      "MANA",
      "SAND",
      "AXS",
      "THETA",
      "AAVE",
    ];
  } finally {
    await client.close();
  }
};

const runCcxtPython = async () => {
  const tokens = await fetchTokensFromMongo();
  for (let exchange of exchanges) {
    spawnPythonProcess(exchange, tokens);
  }
};

runCcxtPython();
