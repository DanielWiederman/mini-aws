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
  password?: string;
  role?: 'CUSTOMER' | 'ADMIN' | 'SUPER_ADMIN';
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
  passwordHash?: string;
  role?: 'CUSTOMER' | 'ADMIN' | 'SUPER_ADMIN';
}

// 2. Catalog/Product Event (Streamed when inventory changes or prices update)
export type CatalogCommandType = 
  | 'CREATE_PRODUCT_START' 
  | 'UPDATE_PRICE_START' 
  | 'UPDATE_PRODUCT_START' 
  | 'DELETE_PRODUCT_START' 
  | 'SCHEDULE_PRICE_UPDATE_COMMAND'
  | 'RESTORE_STOCK_COMMAND';

export interface CatalogCommand {
  commandType: CatalogCommandType;
  payload: any;
}

export interface CreateProductCommandPayload {
  productId: string;
  title: string;
  price: number;
  stockCount: number;
  description?: string;
  thumbnail?: string;
  image?: string;
}

export interface RestoreStockCommandPayload {
  orderId: string;
  items: { productId: string; quantity: number }[];
}

export interface UpdatePriceCommandPayload {
  productId: string;
  newPrice: number;
}

export interface UpdateProductCommandPayload {
  productId: string;
  title?: string;
  description?: string;
  thumbnail?: string;
  image?: string;
}

export interface DeleteProductCommandPayload {
  productId: string;
}

export interface SchedulePriceUpdateCommandPayload {
  productId: string;
  newPrice: number;
  triggerAt: string; // ISO timestamp
}

export type CatalogEventType = 'CATALOG_CREATE_END' | 'CATALOG_UPDATE_END';

export interface CatalogEvent {
  eventType?: CatalogEventType;
  productId: string;
  title: string;
  price: number;
  stockCount: number;
  description?: string;
  thumbnail?: string;
  image?: string;
  isDeleted?: boolean;
}

// 3. Order Event (Streamed when a checkout happens)
export type OrderCommandType = 'CREATE_ORDER_START';

export interface OrderCommand {
  commandType: OrderCommandType;
  payload: any;
}

export interface CreateOrderCommandPayload {
  orderId: string;
  customerId: string;
  items: OrderItem[];
}

export type OrderEventType = 
  | 'ORDER_PENDING_END' 
  | 'ORDER_COMPLETED_END' 
  | 'ORDER_CANCELLED_END'
  | 'STOCK_RESERVED_END'
  | 'STOCK_DENIED_END'
  | 'CUSTOMER_VALIDATED_END'
  | 'CUSTOMER_INVALID_END';

export interface OrderItem {
  productId: string;
  quantity: number;
}

export interface OrderEvent {
  eventType?: OrderEventType;
  orderId: string;
  customerId?: string;
  items?: OrderItem[];
  status?: 'PENDING' | 'COMPLETED' | 'CANCELLED';
  reason?: string; // used for cancellation reason (e.g. out of stock)
  timestamp: string;
}
