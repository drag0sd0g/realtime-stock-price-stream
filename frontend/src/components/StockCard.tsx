import React from "react";
import type { StockAggregate } from "../hooks/useStockStream";

const StockCard: React.FC<{ stock: StockAggregate }> = ({ stock }) => (
  <div className="stock-card">
    <h2>{stock.symbol}</h2>
    <div className="price">Avg: ${stock.avgPrice.toFixed(2)}</div>
    <div className="range">
      <span>Min: ${stock.minPrice.toFixed(2)}</span>&nbsp;|&nbsp;
      <span>Max: ${stock.maxPrice.toFixed(2)}</span>
    </div>
    <div className="count">Updates: {stock.count}</div>
    <span className="timestamp">
      Window: {new Date(stock.windowStart).toLocaleTimeString()} - {new Date(stock.windowEnd).toLocaleTimeString()}
    </span>
  </div>
);

export default StockCard;