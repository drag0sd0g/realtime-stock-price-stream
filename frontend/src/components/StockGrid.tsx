import React from "react";
import StockCard from "./StockCard";
import useStockStream from "../hooks/useStockStream";

const StockGrid: React.FC = () => {
  const { stocks } = useStockStream();

  return (
    <div className="stock-grid">
      {stocks.length ? (
        stocks.map((stock) => <StockCard key={stock.symbol} stock={stock} />)
      ) : (
        <div className="loading">Loading stocks...</div>
      )}
    </div>
  );
};

export default StockGrid;