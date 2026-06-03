import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import Redis from 'ioredis';

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', credentials: true }
});

const redisSubscriber = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

redisSubscriber.subscribe('orders_pubsub', 'catalog_pubsub', (err) => {
  if (err) console.error('Failed to subscribe to Redis channels', err);
  else console.log('📡 Subscribed to Redis pubsub channels');
});

redisSubscriber.on('message', (channel, message) => {
  try {
    const payload = JSON.parse(message);
    if (channel === 'orders_pubsub') {
      io.emit('orderUpdate', payload);
    } else if (channel === 'catalog_pubsub') {
      io.emit('catalogUpdate', payload);
    }
  } catch (e) {
    console.error('Failed to parse redis message', e);
  }
});

const port = process.env.WS_PORT ? parseInt(process.env.WS_PORT, 10) : 4000;
server.listen(port, () => {
  console.log(`🔌 Websockets Service running on http://localhost:${port}`);
});
