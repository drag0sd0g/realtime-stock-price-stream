import React from "react";
import StockCard from "./StockCard";

// Temporary: placeholder data until stream is hooked up
const dummyStocks = Array.from({ length: 50 }, (_, i) => ({
  symbol: `TECH${i + 1}`,
  price: 100 + i,
  change: 0,
  lastUpdate: new Date().toISOString(),
}));

const StockGrid: React.FC = () => (
  <div className="stock-grid">
    {dummyStocks.map((stock) => (
      <StockCard key={stock.symbol} stock={stock} />
    ))}
  </div>
);

export default StockGrid;