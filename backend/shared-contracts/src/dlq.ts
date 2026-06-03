import { Producer, KafkaMessage } from 'kafkajs';

/**
 * A wrapper to handle poison-pill messages via a Dead Letter Queue (DLQ).
 * 
 * Note: The retryMap is in-process memory. Retries will reset if the service restarts.
 * For a mockup/local simulation, this is acceptable.
 */
export async function withDLQ(
  producer: Producer,
  topic: string,
  partition: number,
  offset: string,
  message: KafkaMessage,
  fn: () => Promise<void>,
  retryMap: Map<string, number>
): Promise<void> {
  const key = `${topic}-${partition}-${offset}`;
  
  try {
    await fn();
    // If successful, clear from retry map
    retryMap.delete(key);
  } catch (error: any) {
    console.error(`❌ [DLQ] Error processing message at ${key}:`, error.message);
    
    const attempts = (retryMap.get(key) || 0) + 1;
    retryMap.set(key, attempts);
    
    if (attempts >= 3) {
      console.warn(`🚨 [DLQ] Message ${key} failed 3 times. Routing to DLQ...`);
      
      try {
        await producer.send({
          topic: `${topic}-dlq`,
          messages: [{
            key: message.key,
            value: message.value,
            headers: {
              ...message.headers,
              originalTopic: topic,
              originalPartition: String(partition),
              originalOffset: String(offset),
              error: error.message
            }
          }]
        });
        
        console.log(`✅ [DLQ] Message ${key} successfully routed to ${topic}-dlq`);
        // Remove from retry map after sending to DLQ so we don't leak memory forever
        retryMap.delete(key);
      } catch (dlqError: any) {
        console.error(`💥 [DLQ] FATAL: Failed to route message ${key} to DLQ!`, dlqError);
        throw dlqError; // Throw so offset isn't committed
      }
    } else {
      console.log(`⏳ [DLQ] Will retry message ${key} (Attempt ${attempts}/3)`);
      throw error; // Throw so kafkajs retries it naturally
    }
  }
}
