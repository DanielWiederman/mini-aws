import { db } from './db.js';
import { Producer } from 'kafkajs';
import { CustomerEvent } from 'shared-contracts';

export class CustomerModel {
  constructor(private producer: Producer) {}

  async createCustomer(customer: Omit<CustomerEvent, 'eventType' | 'tier'>) {
    // 1. Emit START event
    await this.emitEvent({
      ...customer,
      tier: 'STANDARD',
      eventType: 'CUSTOMER_CREATE_START',
    });

    try {
      await db.transaction().execute(async (trx) => {
        // 2. Perform DB insert
        const newCustomer = await trx.insertInto('customer')
          .values({
            customer_id: customer.customerId,
            first_name: customer.firstName,
            last_name: customer.lastName,
            email: customer.email
          })
          .returning('id')
          .executeTakeFirstOrThrow();
        
        // Insert default tier index
        await trx.insertInto('customer_tier_index')
          .values({
            customer_id: newCustomer.id,
            tier_name: 'STANDARD'
          })
          .execute();
      });
      console.log(`[CustomerModel] Inserted ${customer.firstName} into DB via Kysely`);
    } catch (e) {
      console.error('Failed to create customer', e);
      throw e;
    }

    // 3. Emit END event
    await this.emitEvent({
      ...customer,
      tier: 'STANDARD',
      eventType: 'CUSTOMER_CREATE_END',
    });
  }

  async upgradeTier(customerId: string, newTier: 'STANDARD' | 'PREMIUM') {
    let eventPayload: CustomerEvent | null = null;

    try {
      await db.transaction().execute(async (trx) => {
        const customerRow = await trx.selectFrom('customer')
          .selectAll()
          .where('customer_id', '=', customerId)
          .executeTakeFirst();
          
        if (!customerRow) throw new Error('Customer not found');
        
        eventPayload = {
          customerId,
          firstName: customerRow.first_name,
          lastName: customerRow.last_name,
          email: customerRow.email,
          tier: newTier,
        };

        // 1. Emit START event
        await this.emitEvent({ ...eventPayload, eventType: 'CUSTOMER_UPDATE_START' });

        // 2. Perform DB update
        await trx.updateTable('customer_tier_index')
          .set({ tier_name: newTier })
          .where('customer_id', '=', customerRow.id)
          .execute();
      });
      console.log(`[CustomerModel] Upgraded ${eventPayload?.firstName} to ${newTier} via Kysely`);
      
      // 3. Emit END event
      if (eventPayload) {
        await this.emitEvent({ ...eventPayload, eventType: 'CUSTOMER_UPDATE_END' });
      }
    } catch (e) {
      console.error('Failed to upgrade tier', e);
      throw e;
    }
  }

  async handleOrderPending(orderEvent: any) {
    let success = false;
    try {
      await db.transaction().execute(async (trx) => {
        const row = await trx.selectFrom('customer').select('id').where('customer_id', '=', orderEvent.customerId).executeTakeFirst();
        if (row) success = true;
      });
      
      if (success) {
        console.log(`[CustomerModel] Validated customer ${orderEvent.customerId} for order ${orderEvent.orderId}`);
      } else {
        console.log(`[CustomerModel] Validation FAILED (Not Found) for customer ${orderEvent.customerId}`);
      }
    } catch (e: any) {
      console.log(`[CustomerModel] Validation failed for customer ${orderEvent.customerId}: ${e.message}`);
    }
    
    const sagaResponse = {
      eventType: success ? 'CUSTOMER_VALIDATED_END' : 'CUSTOMER_INVALID_END',
      orderId: orderEvent.orderId,
      timestamp: new Date().toISOString()
    };
    
    await this.producer.send({
      topic: 'orders-topic',
      messages: [{ key: orderEvent.orderId, value: JSON.stringify(sagaResponse) }]
    });
  }

  private async emitEvent(event: CustomerEvent) {
    await this.producer.send({
      topic: 'customer-topic',
      messages: [{
        key: event.customerId,
        value: JSON.stringify(event)
      }]
    });
    console.log(`[Kafka] Emitted ${event.eventType} for ${event.customerId}`);
  }
}
