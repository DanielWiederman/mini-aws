import { db } from './db.js';
import { Producer } from 'kafkajs';
import { OrderEvent, CreateOrderCommandPayload } from 'shared-contracts';

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
      });
      
      const event: OrderEvent = {
        eventType: 'ORDER_PENDING_END',
        orderId: payload.orderId,
        customerId: payload.customerId,
        items: payload.items,
        status: 'PENDING',
        timestamp: new Date().toISOString()
      };
      await this.emitEvent(event);
      console.log(`[OrdersModel] Created PENDING order ${payload.orderId}. Emitted ORDER_PENDING_END`);
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
             await this.emitEvent({
               eventType: 'ORDER_COMPLETED_END',
               orderId,
               customerId: order.customer_id,
               items: orderItems,
               status: 'COMPLETED',
               timestamp: new Date().toISOString()
             });
             console.log(`[OrdersModel] Saga Complete: Order ${orderId} is now COMPLETED`);
          } else if (updateData.status === 'CANCELLED') {
             await this.emitEvent({
               eventType: 'ORDER_CANCELLED_END',
               orderId,
               customerId: order.customer_id,
               items: orderItems,
               status: 'CANCELLED',
               reason: `Saga failed at ${eventType}`,
               timestamp: new Date().toISOString()
             });
             console.log(`[OrdersModel] Saga Failed: Order ${orderId} CANCELLED (${eventType})`);
          }
        }
      });
    } catch (e) {
      console.error(`Failed to handle saga response ${eventType} for ${orderId}`, e);
    }
  }

  private async emitEvent(event: OrderEvent) {
    await this.producer.send({
      topic: 'orders-topic',
      messages: [{ key: event.orderId, value: JSON.stringify(event) }]
    });
  }
}
