import { Queue, Worker } from 'bullmq';
import { CatalogModel } from './catalog-model.js';
import { db } from './db.js';

export const priceUpdateQueue = new Queue('price-updates-queue', {
  connection: { host: 'localhost', port: 6379 }
});

export function initScheduler(catalogModel: CatalogModel) {
  const worker = new Worker('price-updates-queue', async (job) => {
    const { dbRowId, productId, newPrice } = job.data;
    console.log(`[Scheduler] Processing scheduled price update for product ${productId} to $${newPrice}`);

    try {
      await catalogModel.updatePrice(productId, newPrice);
      
      // Delete backup row from postgres now that it's processed
      await db.deleteFrom('scheduled_price_update')
        .where('id', '=', dbRowId)
        .execute();
      
      console.log(`[Scheduler] Processed update for ${productId} successfully.`);
    } catch (err) {
      console.error(`[Scheduler] Failed to update price for ${productId}`, err);
      throw err;
    }
  }, {
    connection: { host: 'localhost', port: 6379 }
  });

  worker.on('failed', (job, err) => {
    console.error(`[Scheduler] Job ${job?.id} failed with error:`, err.message);
  });
  
  return { worker };
}
