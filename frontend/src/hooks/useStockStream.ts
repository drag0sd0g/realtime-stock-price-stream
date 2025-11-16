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

      // Handler for named SSE events from backend
      es.addEventListener("stock-update", (event) => {
        try {
          const stock: StockAggregate = JSON.parse((event as MessageEvent).data);
          dispatch({ type: "update", stock });
        } catch (e) {
          // Optionally log parse errors
          // console.error("Parse error on stock-update SSE", e);
        }
      });

      // Optionally, handler for generic messages (if backend sends those)
      // es.onmessage = (event) => {
      //   try {
      //     const stock: StockAggregate = JSON.parse(event.data);
      //     dispatch({ type: "update", stock });
      //   } catch (e) { }
      // };

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

// (Optional) helper for alternate EventSource, not needed by default usage
export function createStockEventSource(onMessage: (data: any) => void, onError?: (err: any) => void) {
  const es = new EventSource("/api/stocks/stream");
  es.addEventListener("stock-update", (event) => onMessage(JSON.parse((event as MessageEvent).data)));
  es.onerror = (err) => {
    if (onError) onError(err);
    es.close();
  };
  return es;
}

export function formatPrice(price: number): string {
  return "$" + price.toFixed(2);
}

export function formatPercent(change: number): string {
  return (change > 0 ? "+" : "") + (change * 100).toFixed(2) + "%";
}