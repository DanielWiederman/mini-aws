'use client';

import React, { useEffect, useState } from 'react';
import { io } from 'socket.io-client';
import ProductCard from '@/components/ProductCard';

export default function CatalogGridClient({ initialCatalog }: { initialCatalog: any[] }) {
  const [products, setProducts] = useState(initialCatalog);

  useEffect(() => {
    // We update initial if it changes from props (e.g., page navigation)
    setProducts(initialCatalog);
  }, [initialCatalog]);

  useEffect(() => {
    const socket = io('http://localhost:4000');
    
    socket.on('catalogUpdate', (event) => {
      setProducts(prev => {
        const index = prev.findIndex(p => p.productId === event.productId);
        if (index >= 0) {
          const newProducts = [...prev];
          newProducts[index] = { ...newProducts[index], ...event };
          return newProducts;
        }
        return prev;
      });
    });

    return () => {
      socket.disconnect();
    };
  }, []);

  if (products.length === 0) {
    return (
      <div style={{ textAlign: 'center', padding: '4rem', color: 'var(--text-secondary)' }}>
        No products found.
      </div>
    );
  }

  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: '2rem' }}>
      {products.map((product: any) => (
        <ProductCard key={product.productId} product={product} />
      ))}
    </div>
  );
}
