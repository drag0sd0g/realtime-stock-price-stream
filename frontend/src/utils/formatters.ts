export function formatPrice(price: number): string {
  return "$" + price.toFixed(2);
}

export function formatPercent(change: number): string {
  return (change > 0 ? "+" : "") + (change * 100).toFixed(2) + "%";
}