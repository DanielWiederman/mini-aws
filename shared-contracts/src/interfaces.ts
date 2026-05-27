// shared-contracts/src/interfaces.ts

// 1. Customer Event (Streamed on registration or profile updates)
export interface CustomerEvent {
  customerId: string;
  fullName: string;
  email: string;
  tier: 'STANDARD' | 'PREMIUM'; // Affects order calculations
}

// 2. Catalog/Product Event (Streamed when inventory changes or prices update)
export interface CatalogEvent {
  productId: string;
  title: string;
  price: number;
  stockCount: number;
}

// 3. Order Event (Streamed when a checkout happens)
export interface OrderItem {
  productId: string;
  quantity: number;
}

export interface OrderEvent {
  orderId: string;
  customerId: string;
  items: OrderItem[];
  status: 'PENDING' | 'COMPLETED' | 'CANCELLED';
  timestamp: string;
}
