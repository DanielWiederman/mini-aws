// shared-contracts/src/interfaces.ts

// --- COMMANDS (Published by API Gateway, Consumed by Services) ---
export type CustomerCommandType = 'CREATE_CUSTOMER_COMMAND' | 'UPGRADE_TIER_COMMAND';

export interface CustomerCommand {
  commandType: CustomerCommandType;
  payload: any;
}

export interface CreateCustomerCommandPayload {
  customerId: string;
  firstName: string;
  lastName: string;
  email: string;
}

export interface UpgradeTierCommandPayload {
  customerId: string;
  newTier: 'STANDARD' | 'PREMIUM';
}

// --- EVENTS (Published by Services after state changes) ---
export type CustomerEventType = 'CUSTOMER_CREATE_START' | 'CUSTOMER_CREATE_END' | 'CUSTOMER_UPDATE_START' | 'CUSTOMER_UPDATE_END';

export interface CustomerEvent {
  eventType?: CustomerEventType; // optional for backward compatibility or pure state events
  customerId: string;
  firstName: string;
  lastName: string;
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
