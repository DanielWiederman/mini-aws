'use client';
import { addToCart } from '../lib/cart';

interface Product {
  productId: string;
  title: string;
  price: number;
  stockCount: number;
  thumbnail?: string;
  image?: string;
}

export default function ProductCard({ product }: { product: Product }) {
  const isOutOfStock = product.stockCount <= 0;

  return (
    <div className="glass-panel" style={{ padding: '1.5rem', display: 'flex', flexDirection: 'column', gap: '1rem', transition: 'transform 0.2s', height: '100%' }}>
      <div style={{ 
        width: '100%', 
        aspectRatio: '1', 
        borderRadius: 'var(--radius-md)', 
        overflow: 'hidden', 
        background: 'rgba(255, 255, 255, 0.05)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center'
      }}>
        <img 
          src={product.thumbnail || '/aws-mini-default.png'} 
          alt={product.title} 
          style={{ width: '100%', height: '100%', objectFit: 'cover' }} 
        />
      </div>
      <div style={{ flex: 1 }}>
        <h3 style={{ fontSize: '1.25rem', marginBottom: '0.5rem' }}>{product.title}</h3>
        <p style={{ color: 'var(--text-secondary)', fontSize: '0.875rem' }}>ID: {product.productId}</p>
        <div style={{ marginTop: '1rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ fontSize: '1.5rem', fontWeight: 'bold', color: 'var(--primary)' }}>
            ${product.price.toFixed(2)}
          </span>
          <span className="badge" style={{ background: isOutOfStock ? 'rgba(239, 68, 68, 0.1)' : 'rgba(16, 185, 129, 0.1)', color: isOutOfStock ? 'var(--danger)' : 'var(--success)' }}>
            {isOutOfStock ? 'Out of Stock' : `${product.stockCount} in stock`}
          </span>
        </div>
      </div>
      <button 
        className="btn btn-primary" 
        style={{ width: '100%' }}
        disabled={isOutOfStock}
        onClick={() => addToCart(product)}
      >
        {isOutOfStock ? 'Sold Out' : 'Add to Cart'}
      </button>
    </div>
  );
}
