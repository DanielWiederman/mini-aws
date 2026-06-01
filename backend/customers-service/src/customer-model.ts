import { db } from './db.js';
import { Producer } from 'kafkajs';
import { CustomerEvent, sendTraced, CreateCustomerCommandPayload } from 'shared-contracts';
import bcrypt from 'bcryptjs';

export class CustomerModel {
  constructor(private producer: Producer) {}

  async createCustomer(payload: CreateCustomerCommandPayload) {
    // 1. Emit START event
    await this.emitEvent({
      eventType: 'CUSTOMER_CREATE_START',
      customerId: payload.customerId,
      firstName: payload.firstName,
      lastName: payload.lastName,
      email: payload.email,
      tier: 'STANDARD',
      role: payload.role || 'CUSTOMER'
    });

    let passwordHash = undefined;
    if (payload.password) {
      passwordHash = await bcrypt.hash(payload.password, 10);
    }

    try {
      await db.transaction().execute(async (trx) => {
        // 2. Perform DB insert
        const newCustomer = await trx.insertInto('customer')
          .values({
            customer_id: payload.customerId,
            first_name: payload.firstName,
            last_name: payload.lastName,
            email: payload.email,
            password_hash: passwordHash,
            role: payload.role || 'CUSTOMER'
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
      console.log(`[CustomerModel] Inserted ${payload.firstName} into DB via Kysely`);
    } catch (e) {
      console.error('Failed to create customer', e);
      throw e;
    }

    // 3. Emit END event
    await this.emitEvent({
      eventType: 'CUSTOMER_CREATE_END',
      customerId: payload.customerId,
      firstName: payload.firstName,
      lastName: payload.lastName,
      email: payload.email,
      tier: 'STANDARD',
      passwordHash: passwordHash,
      role: payload.role || 'CUSTOMER'
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
          passwordHash: customerRow.password_hash ?? undefined,
          role: customerRow.role
        };

        // 1. Emit START event
        await this.emitEvent({ ...(eventPayload as any), eventType: 'CUSTOMER_UPDATE_START' });

        // 2. Perform DB update
        await trx.updateTable('customer_tier_index')
          .set({ tier_name: newTier })
          .where('customer_id', '=', customerRow.id)
          .execute();
      });
      console.log(`[CustomerModel] Upgraded ${(eventPayload as any)?.firstName} to ${newTier} via Kysely`);
      
      // 3. Emit END event
      if (eventPayload) {
        await this.emitEvent({ ...(eventPayload as any), eventType: 'CUSTOMER_UPDATE_END' });
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
    
    await sendTraced(this.producer, 'orders-topic', [
      { key: orderEvent.orderId, value: JSON.stringify(sagaResponse) }
    ]);
  }

  private async emitEvent(event: CustomerEvent) {
    await sendTraced(this.producer, 'customer-topic', [
      { key: event.customerId, value: JSON.stringify(event) }
    ]);
    console.log(`[Kafka] Emitted ${event.eventType} for ${event.customerId}`);
  }
}
