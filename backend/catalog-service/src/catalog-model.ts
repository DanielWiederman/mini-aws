import { db } from './db.js';
import { sql } from 'kysely';
import { Producer } from 'kafkajs';
import { CatalogEvent, sendTraced, KafkaLogger } from 'shared-contracts';
import { priceUpdateQueue } from './scheduler.js';

export class CatalogModel {
  constructor(private producer: Producer, private sysLogger: KafkaLogger) {}

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
            stock_count: product.stockCount,
            description: product.description || null,
            thumbnail: product.thumbnail || '',
            image: product.image || ''
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
          stockCount: productRow.stock_count,
          description: productRow.description ?? undefined,
          thumbnail: productRow.thumbnail,
          image: productRow.image,
          isDeleted: !!productRow.deleted_at
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

  async updateProduct(payload: any) {
    let eventPayload: CatalogEvent | null = null;
    try {
      await db.transaction().execute(async (trx) => {
        const productRow = await trx.selectFrom('product').selectAll().where('product_id', '=', payload.productId).executeTakeFirst();
        if (!productRow) throw new Error('Product not found');

        const newTitle = payload.title || productRow.title;
        const newDesc = payload.description !== undefined ? payload.description : productRow.description;
        const newThumb = payload.thumbnail || productRow.thumbnail;
        const newImage = payload.image || productRow.image;
        const newPrice = payload.price !== undefined ? parseFloat(payload.price) : productRow.price;
        const newStock = payload.stockCount !== undefined ? parseInt(payload.stockCount, 10) : productRow.stock_count;

        eventPayload = {
          productId: payload.productId,
          title: newTitle,
          price: newPrice,
          stockCount: newStock,
          description: newDesc ?? undefined,
          thumbnail: newThumb,
          image: newImage,
          isDeleted: !!productRow.deleted_at
        };

        await this.emitEvent({ ...(eventPayload as any), eventType: 'UPDATE_PRODUCT_START' as any });

        await trx.updateTable('product')
          .set({ 
            title: newTitle, 
            description: newDesc, 
            thumbnail: newThumb, 
            image: newImage, 
            price: newPrice,
            stock_count: newStock,
            updated_at: new Date() as any 
          })
          .where('product_id', '=', payload.productId)
          .execute();
      });
      console.log(`[CatalogModel] Updated product ${payload.productId}`);
      if (eventPayload) await this.emitEvent({ ...(eventPayload as any), eventType: 'CATALOG_UPDATE_END' });
    } catch (e) {
      console.error('Failed to update product', e);
      throw e;
    }
  }

  async deleteProduct(productId: string) {
    let eventPayload: CatalogEvent | null = null;
    try {
      await db.transaction().execute(async (trx) => {
        const productRow = await trx.selectFrom('product').selectAll().where('product_id', '=', productId).executeTakeFirst();
        if (!productRow) throw new Error('Product not found');

        eventPayload = {
          productId,
          title: productRow.title,
          price: productRow.price,
          stockCount: productRow.stock_count,
          description: productRow.description ?? undefined,
          thumbnail: productRow.thumbnail,
          image: productRow.image,
          isDeleted: true
        };

        await this.emitEvent({ ...(eventPayload as any), eventType: 'DELETE_PRODUCT_START' as any });

        await trx.updateTable('product')
          .set({ deleted_at: new Date() as any })
          .where('product_id', '=', productId)
          .execute();
      });
      console.log(`[CatalogModel] Soft deleted product ${productId}`);
      if (eventPayload) await this.emitEvent({ ...(eventPayload as any), eventType: 'CATALOG_UPDATE_END' });
    } catch (e) {
      console.error('Failed to delete product', e);
      throw e;
    }
  }

  async schedulePriceUpdate(payload: any) {
    try {
      const res = await db.insertInto('scheduled_price_update')
        .values({
          product_id: payload.productId,
          new_price: payload.newPrice,
          trigger_at: payload.triggerAt
        })
        .returning('id')
        .executeTakeFirstOrThrow();
        
      const delay = new Date(payload.triggerAt).getTime() - Date.now();
      await priceUpdateQueue.add('updatePrice', {
        dbRowId: res.id,
        productId: payload.productId,
        newPrice: payload.newPrice
      }, { delay: Math.max(0, delay) });

      console.log(`[CatalogModel] Scheduled price update for ${payload.productId} at ${payload.triggerAt}`);
    } catch (e) {
      console.error('Failed to schedule price update', e);
      throw e;
    }
  }

  async handleOrderPending(orderEvent: any) {
    let success = true;
    let updatedProducts: CatalogEvent[] = [];
    try {
      await db.transaction().execute(async (trx) => {
        for (const item of orderEvent.items) {
          const res = await trx.updateTable('product')
            .set((eb) => ({ stock_count: eb('stock_count', '-', item.quantity) }))
            .where('product_id', '=', item.productId)
            .where('stock_count', '>=', item.quantity)
            .returningAll()
            .executeTakeFirst();
            
          if (!res) {
            throw new Error(`Insufficient stock for ${item.productId}`);
          }
          
          updatedProducts.push({
            productId: res.product_id,
            title: res.title,
            price: parseFloat(res.price as any),
            stockCount: parseInt(res.stock_count as any, 10),
            description: res.description ?? undefined,
            thumbnail: res.thumbnail,
            image: res.image,
            isDeleted: !!res.deleted_at,
            eventType: 'CATALOG_UPDATE_END'
          });
        }
      });
      this.sysLogger.info(`Stock Reserved: Order ${orderEvent.orderId} reserved items successfully`).catch(() => {});
      console.log(`[CatalogModel] Reserved stock for order ${orderEvent.orderId}`);
      
      // Emit updates to sync Redis view
      for (const prod of updatedProducts) {
        await this.emitEvent(prod);
      }
    } catch (e: any) {
      success = false;
      this.sysLogger.error(`Stock Denied: Order ${orderEvent.orderId} failed due to ${e.message}`, e).catch(() => {});
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

  async restoreStock(payload: any) {
    const { orderId, items } = payload;
    if (!items || items.length === 0) return;

    try {
      const updatedProducts: CatalogEvent[] = [];
      await db.transaction().execute(async (trx) => {
        for (const item of items) {
          const res = await trx.updateTable('product')
            .set((eb) => ({
              stock_count: sql`${eb.ref('stock_count')} + ${item.quantity}`
            }))
            .where('product_id', '=', item.productId)
            .returningAll()
            .executeTakeFirst();
            
          if (res) {
            updatedProducts.push({
              productId: res.product_id,
              title: res.title,
              price: parseFloat(res.price as any),
              stockCount: parseInt(res.stock_count as any, 10),
              description: res.description ?? undefined,
              thumbnail: res.thumbnail,
              image: res.image,
              isDeleted: !!res.deleted_at,
              eventType: 'CATALOG_UPDATE_END'
            });
          }
        }
      });
      this.sysLogger.warn(`Compensating Transaction: Restored stock for cancelled order ${orderId}`).catch(() => {});
      console.log(`[CatalogModel] Compensating Transaction: Restored stock for cancelled order ${orderId}`);
      
      // Emit updates to sync Redis view
      for (const prod of updatedProducts) {
        await this.emitEvent(prod);
      }
    } catch (e: any) {
      console.error(`[CatalogModel] Failed to restore stock for order ${orderId}`, e);
    }
  }

  private async emitEvent(event: CatalogEvent) {
    await sendTraced(this.producer, 'catalog-topic', [
      { key: event.productId, value: JSON.stringify(event) }
    ]);
    console.log(`[Kafka] Emitted ${event.eventType} for ${event.productId}`);
  }
}
