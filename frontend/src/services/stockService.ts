// Stub for EventSource & data fetching. Actual logic will stream from /api/stocks/stream later.
export function createStockEventSource(onMessage: (data: any) => void, onError?: (err: any) => void) {
  const es = new EventSource("/api/stocks/stream");
  es.onmessage = (event) => onMessage(JSON.parse(event.data));
  es.onerror = (err) => {
    if (onError) onError(err);
    es.close();
  };
  return es;
}