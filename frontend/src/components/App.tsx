import React from "react";
import StockGrid from "./StockGrid";
import ConnectionStatus from "./ConnectionStatus";
import "../styles/theme.css";

const App: React.FC = () => (
  <div className="app">
    <header>
      <h1>📈 Real-Time Tech Stocks Dashboard</h1>
      <ConnectionStatus />
    </header>
    <main>
      <StockGrid />
    </main>
    <footer>
      <span>Built with Vite + React + TypeScript</span>
    </footer>
  </div>
);

export default App;