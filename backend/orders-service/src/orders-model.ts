import { db } from './db.js';
import { sql } from 'kysely';
import { Producer } from 'kafkajs';
import { OrderEvent, CreateOrderCommandPayload, sendTraced } from 'shared-contracts';

export class OrdersModel {
  constructor(private producer: Producer) {}

  async createPendingOrder(payload: CreateOrderCommandPayload) {
    try {
      await db.transaction().execute(async (trx) => {
        await trx.insertInto('order').values({
          order_id: payload.orderId,
          customer_id: payload.customerId,
          status: 'PENDING',
          stock_status: 'PENDING',
          customer_status: 'PENDING'
        }).execute();

        for (const item of payload.items) {
          await trx.insertInto('order_item').values({
            order_id: payload.orderId,
            product_id: item.productId,
            quantity: item.quantity
          }).execute();
        }
        
        const event: OrderEvent = {
          eventType: 'ORDER_PENDING_END',
          orderId: payload.orderId,
          customerId: payload.customerId,
          items: payload.items,
          status: 'PENDING',
          timestamp: new Date().toISOString()
        };
        
        await trx.insertInto('outbox').values({
          topic: 'orders-topic',
          key: payload.orderId,
          payload: JSON.stringify(event),
          event_id: `${event.eventType}:${payload.orderId}:${event.timestamp}`
        }).execute();
      });
      
      await this.flushOutbox(payload.orderId);
      console.log(`[OrdersModel] Created PENDING order ${payload.orderId}. Flushed ORDER_PENDING_END to outbox`);
    } catch (e) {
      console.error('Failed to create pending order', e);
      throw e;
    }
  }

  async handleSagaResponse(orderId: string, eventType: 'STOCK_RESERVED_END' | 'STOCK_DENIED_END' | 'CUSTOMER_VALIDATED_END' | 'CUSTOMER_INVALID_END') {

    try {
      await db.transaction().execute(async (trx) => {
        const order = await trx.selectFrom('order').selectAll().where('order_id', '=', orderId).executeTakeFirst();
        if (!order) return;
        
        // RACE CONDITION FIX: If order is already CANCELLED but STOCK_RESERVED just arrived, we must restore it!
        if (order.status === 'CANCELLED' && eventType === 'STOCK_RESERVED_END') {
          const items = await trx.selectFrom('order_item').selectAll().where('order_id', '=', orderId).execute();
          const orderItems = items.map(i => ({ productId: i.product_id, quantity: i.quantity }));
          
          const restoreCommandPayload = {
            commandType: 'RESTORE_STOCK_COMMAND',
            payload: { orderId, items: orderItems }
          };
          
          await trx.insertInto('outbox').values({
            topic: 'catalog-commands-topic',
            key: orderId,
            payload: JSON.stringify(restoreCommandPayload),
            event_id: `${restoreCommandPayload.commandType}:${orderId}:${new Date().toISOString()}`
          }).execute();
          return;
        }

        if (order.status !== 'PENDING') return; // State machine already finalized

        let updateData: any = {};
        
        if (eventType === 'STOCK_RESERVED_END') updateData.stock_status = 'RESERVED';
        if (eventType === 'STOCK_DENIED_END') updateData.stock_status = 'DENIED';
        if (eventType === 'CUSTOMER_VALIDATED_END') updateData.customer_status = 'VALID';
        if (eventType === 'CUSTOMER_INVALID_END') updateData.customer_status = 'INVALID';

        const nextStockStatus = updateData.stock_status || order.stock_status;
        const nextCustomerStatus = updateData.customer_status || order.customer_status;

        if (nextStockStatus === 'DENIED' || nextCustomerStatus === 'INVALID') {
          updateData.status = 'CANCELLED';
        } else if (nextStockStatus === 'RESERVED' && nextCustomerStatus === 'VALID') {
          updateData.status = 'COMPLETED';
        }

        if (Object.keys(updateData).length > 0) {
          await trx.updateTable('order').set(updateData).where('order_id', '=', orderId).execute();
          
          const items = await trx.selectFrom('order_item').selectAll().where('order_id', '=', orderId).execute();
          const orderItems = items.map(i => ({ productId: i.product_id, quantity: i.quantity }));
          
          if (updateData.status === 'COMPLETED') {
             const orderEventToEmit = {
               eventType: 'ORDER_COMPLETED_END',
               orderId,
               customerId: order.customer_id,
               items: orderItems,
               status: 'COMPLETED',
               timestamp: new Date().toISOString()
             };
             
             await trx.insertInto('outbox').values({
               topic: 'orders-topic',
               key: orderId,
               payload: JSON.stringify(orderEventToEmit),
               event_id: `${orderEventToEmit.eventType}:${orderId}:${orderEventToEmit.timestamp}`
             }).execute();
             
          } else if (updateData.status === 'CANCELLED') {
             const orderEventToEmit = {
               eventType: 'ORDER_CANCELLED_END',
               orderId,
               customerId: order.customer_id,
               items: orderItems,
               status: 'CANCELLED',
               reason: `Saga failed at ${eventType}`,
               timestamp: new Date().toISOString()
             };
             
             await trx.insertInto('outbox').values({
               topic: 'orders-topic',
               key: orderId,
               payload: JSON.stringify(orderEventToEmit),
               event_id: `${orderEventToEmit.eventType}:${orderId}:${orderEventToEmit.timestamp}`
             }).execute();
             
             // COMPENSATING TRANSACTION: If stock was previously reserved, we must un-reserve it!
             if (nextStockStatus === 'RESERVED') {
               const restoreCommandPayload = {
                 commandType: 'RESTORE_STOCK_COMMAND',
                 payload: { orderId, items: orderItems }
               };
               
               await trx.insertInto('outbox').values({
                 topic: 'catalog-commands-topic',
                 key: orderId,
                 payload: JSON.stringify(restoreCommandPayload),
                 event_id: `${restoreCommandPayload.commandType}:${orderId}:${new Date().toISOString()}`
               }).execute();
             }
          }
        }
      });

      // Transaction committed. Flush the outbox for this order immediately!
      await this.flushOutbox(orderId);

    } catch (e) {
      console.error(`Failed to handle saga response ${eventType} for ${orderId}`, e);
    }
  }

  async flushOutbox(orderId?: string) {
    try {
      let query = db.selectFrom('outbox')
        .selectAll()
        .where('processed_at', 'is', null)
        .orderBy('id', 'asc');

      if (orderId) {
        // If an orderId is provided, just process rows for that key
        query = query.where('key', '=', orderId);
      } else {
        // For polling relay: pick rows older than 10 seconds
        query = query.where('created_at', '<', sql<Date>`now() - interval '10 seconds'`);
      }

      const rows = await query.execute();
      
      for (const row of rows) {
        try {
          const payloadStr = typeof row.payload === 'string' ? row.payload : JSON.stringify(row.payload);
          await sendTraced(this.producer, row.topic as string, [
            { key: row.key as string, value: payloadStr }
          ]);
          
          await db.updateTable('outbox')
            .set({ processed_at: sql<Date>`now()` })
            .where('id', '=', row.id)
            .execute();
            
          console.log(`[Outbox] Flushed message ${row.id} to ${row.topic}`);
        } catch (err) {
          console.error(`[Outbox] Failed to flush message ${row.id} to ${row.topic}`, err);
        }
      }
    } catch (e) {
      console.error(`[Outbox] Flush operation failed`, e);
    }
  }



}
