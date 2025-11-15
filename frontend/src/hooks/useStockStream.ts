import { useEffect, useReducer, useRef } from "react";

export type StockAggregate = {
  symbol: string;
  avgPrice: number;
  minPrice: number;
  maxPrice: number;
  count: number;
  windowStart: string;
  windowEnd: string;
};

type State = {
  stocks: Record<string, StockAggregate>;
  connection: "connected" | "disconnected" | "reconnecting";
  error: string | null;
};

type Action =
  | { type: "update"; stock: StockAggregate }
  | { type: "connected" }
  | { type: "disconnected"; error?: string }
  | { type: "reconnecting" };

function reducer(state: State, action: Action): State {
  switch (action.type) {
    case "update":
      return {
        ...state,
        stocks: { ...state.stocks, [action.stock.symbol]: action.stock },
      };
    case "connected":
      return { ...state, connection: "connected", error: null };
    case "disconnected":
      return { ...state, connection: "disconnected", error: action.error || null };
    case "reconnecting":
      return { ...state, connection: "reconnecting" };
    default:
      return state;
  }
}

export default function useStockStream() {
  const [state, dispatch] = useReducer(reducer, {
    stocks: {},
    connection: "disconnected",
    error: null,
  });

  const eventSourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    let reconnectAttempt = 0;
    let stop = false;

    function connect() {
      dispatch({ type: reconnectAttempt === 0 ? "disconnected" : "reconnecting" });
      const es = new EventSource("/api/stocks/stream");
      eventSourceRef.current = es;

      es.onopen = () => {
        dispatch({ type: "connected" });
        reconnectAttempt = 0;
      };

      es.onmessage = (event) => {
        try {
          const stock: StockAggregate = JSON.parse(event.data);
          dispatch({ type: "update", stock });
        } catch (e) { /* ignore parse errors */ }
      };

      es.onerror = () => {
        dispatch({ type: "disconnected", error: "Lost connection" });
        es.close();
        if (!stop) {
          reconnectAttempt += 1;
          setTimeout(connect, Math.min(reconnectAttempt * 1000, 5000));
        }
      };
    }

    connect();
    return () => {
      stop = true;
      eventSourceRef.current?.close();
    };
  }, []);

  return {
    stocks: Object.values(state.stocks),
    connection: state.connection,
    error: state.error,
  };
}