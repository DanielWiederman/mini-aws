import { db } from './db.js';
import { Producer } from 'kafkajs';
import { CatalogEvent, sendTraced } from 'shared-contracts';

export class CatalogModel {
  constructor(private producer: Producer) {}

  async createProduct(product: Omit<CatalogEvent, 'eventType'>) {
    // 1. Emit START event
    await this.emitEvent({ ...product, eventType: 'CREATE_PRODUCT_START' as any });

    try {
      await db.transaction().execute(async (trx) => {
        // 2. Perform DB insert
        await trx.insertInto('product')
          .values({
            product_id: product.productId,
            title: product.title,
            price: product.price,
            stock_count: product.stockCount
          })
          .execute();
      });
      console.log(`[CatalogModel] Inserted ${product.title} into DB via Kysely`);
    } catch (e) {
      console.error('Failed to create product', e);
      throw e;
    }

    // 3. Emit END event
    await this.emitEvent({ ...product, eventType: 'CATALOG_CREATE_END' });
  }

  async updatePrice(productId: string, newPrice: number) {
    let eventPayload: CatalogEvent | null = null;

    try {
      await db.transaction().execute(async (trx) => {
        const productRow = await trx.selectFrom('product')
          .selectAll()
          .where('product_id', '=', productId)
          .executeTakeFirst();
          
        if (!productRow) throw new Error('Product not found');
        
        eventPayload = {
          productId,
          title: productRow.title,
          price: newPrice,
          stockCount: productRow.stock_count
        };

        // 1. Emit START event
        await this.emitEvent({ ...(eventPayload as any), eventType: 'UPDATE_PRICE_START' as any });

        // 2. Perform DB update
        await trx.updateTable('product')
          .set({ price: newPrice })
          .where('product_id', '=', productId)
          .execute();
      });
      
      console.log(`[CatalogModel] Updated ${productId} price to ${newPrice} via Kysely`);
      
      // 3. Emit END event
      if (eventPayload) {
        await this.emitEvent({ ...(eventPayload as any), eventType: 'CATALOG_UPDATE_END' });
      }
    } catch (e) {
      console.error('Failed to update price', e);
      throw e;
    }
  }

  async handleOrderPending(orderEvent: any) {
    let success = true;
    try {
      await db.transaction().execute(async (trx) => {
        for (const item of orderEvent.items) {
          const res = await trx.updateTable('product')
            .set((eb) => ({ stock_count: eb('stock_count', '-', item.quantity) }))
            .where('product_id', '=', item.productId)
            .where('stock_count', '>=', item.quantity)
            .executeTakeFirst();
            
          if (res.numUpdatedRows === 0n) {
            throw new Error(`Insufficient stock for ${item.productId}`);
          }
        }
      });
      console.log(`[CatalogModel] Reserved stock for order ${orderEvent.orderId}`);
    } catch (e: any) {
      success = false;
      console.log(`[CatalogModel] Stock reservation denied for ${orderEvent.orderId}: ${e.message}`);
    }
    
    const sagaResponse = {
      eventType: success ? 'STOCK_RESERVED_END' : 'STOCK_DENIED_END',
      orderId: orderEvent.orderId,
      timestamp: new Date().toISOString()
    };
    await sendTraced(this.producer, 'orders-topic', [
      { key: orderEvent.orderId, value: JSON.stringify(sagaResponse) }
    ]);
  }

  private async emitEvent(event: CatalogEvent) {
    await sendTraced(this.producer, 'catalog-topic', [
      { key: event.productId, value: JSON.stringify(event) }
    ]);
    console.log(`[Kafka] Emitted ${event.eventType} for ${event.productId}`);
  }
}
