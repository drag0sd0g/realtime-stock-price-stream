import React from "react";
import useStockStream from "../hooks/useStockStream";

const statusMap = {
  connected: { color: "connection-status--connected", text: "Live" },
  disconnected: { color: "connection-status--disconnected", text: "Offline" },
  reconnecting: { color: "connection-status--reconnecting", text: "Reconnecting" },
};

const ConnectionStatus: React.FC = () => {
  const { connection } = useStockStream();
  const { color, text } = statusMap[connection] || statusMap.disconnected;

  return (
    <div className={`connection-status ${color}`} title={text}>
      ● {text}
    </div>
  );
};

export default ConnectionStatus;