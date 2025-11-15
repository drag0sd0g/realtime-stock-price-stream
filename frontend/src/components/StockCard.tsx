import React from "react";

type Stock = {
  symbol: string;
  price: number;
  change: number;
  lastUpdate: string;
};

const StockCard: React.FC<{ stock: Stock }> = ({ stock }) => (
  <div className="stock-card">
    <h2>{stock.symbol}</h2>
    <div className="price">${stock.price.toFixed(2)}</div>
    <div className="change">{stock.change > 0 ? "↑" : stock.change < 0 ? "↓" : "→"}</div>
    <span className="timestamp">{new Date(stock.lastUpdate).toLocaleTimeString()}</span>
  </div>
);

export default StockCard;