const a = { orderId: 'test_order_1780381606980' };
const b = { orderId: 'order_1780382906283' };
const tsA = parseInt(a.orderId.split('_').pop() || '0', 10);
const tsB = parseInt(b.orderId.split('_').pop() || '0', 10);
console.log(tsA, tsB, tsB - tsA);
