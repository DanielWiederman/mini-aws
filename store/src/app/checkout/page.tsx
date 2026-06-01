'use client';
import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { getCart, clearCart, CartItem } from '@/lib/cart';
import Link from 'next/link';

export default function CheckoutPage() {
  const router = useRouter();
  const [customer, setCustomer] = useState<any>(null);
  const [cart, setCart] = useState<CartItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [orderStatus, setOrderStatus] = useState('');
  const [orderError, setOrderError] = useState('');

  useEffect(() => {
    const currentCart = getCart();
    if (currentCart.length === 0) {
      router.push('/cart');
      return;
    }
    setCart(currentCart);

    // Verify authentication
    fetch('http://localhost:3000/api/customers/me', { credentials: 'include' })
      .then(res => {
        if (!res.ok) throw new Error('Unauthorized');
        return res.json();
      })
      .then(data => {
        setCustomer(data);
        setLoading(false);
      })
      .catch(() => {
        // Redirect to login if not authenticated
        router.push('/login');
      });
  }, [router]);

  const handlePlaceOrder = async () => {
    setOrderStatus('Placing order...');
    setOrderError('');
    
    const orderId = `order_${Date.now()}`;
    const payload = {
      orderId,
      customerId: customer.customerId,
      items: cart.map(item => ({
        productId: item.productId,
        quantity: item.quantity
      }))
    };

    try {
      const res = await fetch('http://localhost:3000/api/orders', {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'Idempotency-Key': `req-${orderId}-create`
        },
        body: JSON.stringify(payload)
      });
      
      if (!res.ok) throw new Error('Failed to place order');
      
      clearCart();
      setOrderStatus('SUCCESS');
    } catch (err: any) {
      setOrderError(err.message);
      setOrderStatus('');
    }
  };

  const handleLogout = async () => {
    await fetch('http://localhost:3000/api/customers/logout', { method: 'POST', credentials: 'include' });
    router.push('/login');
  };

  if (loading) {
    return <div style={{ textAlign: 'center', padding: '4rem' }}>Loading checkout...</div>;
  }

  if (orderStatus === 'SUCCESS') {
    return (
      <div style={{ maxWidth: '600px', margin: '4rem auto', textAlign: 'center' }}>
        <div className="glass-panel" style={{ padding: '3rem' }}>
          <div style={{ fontSize: '4rem', marginBottom: '1rem' }}>🎉</div>
          <h1 style={{ fontSize: '2rem', marginBottom: '1rem', color: 'var(--success)' }}>Order Placed!</h1>
          <p style={{ color: 'var(--text-secondary)', marginBottom: '2rem' }}>
            Thank you for your purchase, {customer.firstName}. Your order is now being processed by our Saga orchestration engine.
          </p>
          <Link href="/" className="btn btn-primary">Continue Shopping</Link>
        </div>
      </div>
    );
  }

  const total = cart.reduce((sum, item) => sum + (Number(item.price || 0) * item.quantity), 0);

  return (
    <div style={{ maxWidth: '800px', margin: '0 auto', padding: '2rem 0' }}>
      <h1 style={{ fontSize: '2.5rem', marginBottom: '2rem' }}>Checkout</h1>
      
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 300px', gap: '2rem' }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '2rem' }}>
          <div className="glass-panel" style={{ padding: '2rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '1.5rem' }}>
              <h2 style={{ fontSize: '1.25rem' }}>Shipping Information</h2>
              <button onClick={handleLogout} className="badge" style={{ border: 'none', cursor: 'pointer' }}>Sign Out</button>
            </div>
            <div style={{ color: 'var(--text-secondary)' }}>
              <p><strong>Name:</strong> {customer.firstName} {customer.lastName}</p>
              <p><strong>Email:</strong> {customer.email}</p>
              <p><strong>Tier:</strong> <span className="badge">{customer.tier}</span></p>
            </div>
          </div>
          
          <div className="glass-panel" style={{ padding: '2rem' }}>
            <h2 style={{ fontSize: '1.25rem', marginBottom: '1.5rem' }}>Order Items</h2>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
              {cart.map(item => (
                <div key={item.productId} style={{ display: 'flex', justifyContent: 'space-between', borderBottom: '1px solid var(--border-color)', paddingBottom: '1rem' }}>
                  <div>
                    <div style={{ fontWeight: 500 }}>{item.title}</div>
                    <div style={{ fontSize: '0.875rem', color: 'var(--text-secondary)' }}>Qty: {item.quantity}</div>
                  </div>
                  <div style={{ fontWeight: 600 }}>${(Number(item.price || 0) * item.quantity).toFixed(2)}</div>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div>
          <div className="glass-panel" style={{ padding: '2rem', position: 'sticky', top: '100px' }}>
            <h2 style={{ fontSize: '1.25rem', marginBottom: '1.5rem' }}>Summary</h2>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '1rem', color: 'var(--text-secondary)' }}>
              <span>Subtotal</span>
              <span>${total.toFixed(2)}</span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '1.5rem', color: 'var(--text-secondary)' }}>
              <span>Tax</span>
              <span>$0.00</span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '2rem', fontSize: '1.25rem', fontWeight: 700, borderTop: '1px solid var(--border-color)', paddingTop: '1rem' }}>
              <span>Total</span>
              <span style={{ color: 'var(--primary)' }}>${total.toFixed(2)}</span>
            </div>
            
            {orderError && (
              <div style={{ padding: '0.75rem', background: 'rgba(239, 68, 68, 0.1)', color: 'var(--danger)', borderRadius: 'var(--radius-md)', marginBottom: '1rem', fontSize: '0.875rem' }}>
                {orderError}
              </div>
            )}
            
            <button 
              className="btn btn-primary" 
              style={{ width: '100%' }} 
              onClick={handlePlaceOrder}
              disabled={!!orderStatus}
            >
              {orderStatus || 'Place Order'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
