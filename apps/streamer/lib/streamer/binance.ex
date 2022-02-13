defmodule Streamer.Binance do
  use WebSockex
  require Logger

  def start_link(symbol) do
    WebSockex.start_link(stream_endpoint(symbol), __MODULE__, nil)
  end

  defp stream_endpoint(symbol) do
    symbol = String.downcase(symbol)
    "wss://stream.binance.com:9443/ws/#{symbol}@trade"
  end

  def handle_frame({type, msg}, state) do
    case Jason.decode(msg) do
      {:ok, event} -> process_event(event)
      {:error, _rest} -> Logger.error("Unable to parse message #{msg}")
    end

    {:ok, state}
  end

  defp process_event(%{"e" => "trade"} = event) do
    trade_event = %Streamer.Binance.TradeEvent{
      :event_type => event["e"],
      :event_time => event["E"],
      :symbol => event["s"],
      :trade_id => event["t"],
      :price => event["p"],
      :quantity => event["q"],
      :buyer_order_id => event["a"],
      :seller_order_id => event["a"],
      :trade_time => event["T"],
      :buyer_market_marker => event["m"]
    }

    Logger.debug("Trade event received " <> "#{trade_event.symbol}@#{trade_event.price}")
  end

  def handle_cast({send, {type, msg} = frame}, state) do
    IO.puts("Sending #{type} frame with payload: #{msg}")
    {:reply, frame, state}
  end
end
