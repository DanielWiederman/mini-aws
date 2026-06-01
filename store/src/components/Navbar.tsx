'use client';
import Link from 'next/link';
import { useEffect, useState } from 'react';
import { getCart } from '../lib/cart';
import { fetchCurrentUser, getCurrentUser, User } from '../lib/auth';

export default function Navbar() {
  const [cartCount, setCartCount] = useState(0);
  const [user, setUser] = useState<User | null>(null);
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
    
    const updateCartCount = () => {
      const cart = getCart();
      setCartCount(cart.reduce((acc, item) => acc + item.quantity, 0));
    };
    
    const updateUser = () => {
      setUser(getCurrentUser());
    };

    updateCartCount();
    fetchCurrentUser().then(updateUser);

    window.addEventListener('cart-updated', updateCartCount);
    window.addEventListener('auth-updated', updateUser);
    return () => {
      window.removeEventListener('cart-updated', updateCartCount);
      window.removeEventListener('auth-updated', updateUser);
    };
  }, []);

  return (
    <nav className="navbar">
      <div className="container navbar-content">
        <Link href="/" className="brand">
          Mini-AWS Store
        </Link>
        <div style={{ display: 'flex', gap: '2rem', alignItems: 'center' }}>
          <Link href="/" style={{ fontWeight: 500 }}>Catalog</Link>
          <Link href="/cart" style={{ fontWeight: 500 }}>
            Cart {mounted && cartCount > 0 && <span className="badge">{cartCount}</span>}
          </Link>
          {mounted && user ? (
            <Link href="/account" className="btn btn-primary" style={{ padding: '0.5rem 1rem' }}>
              Welcome {user.firstName}
            </Link>
          ) : (
            <Link href="/login" className="btn btn-outline" style={{ padding: '0.5rem 1rem' }}>
              Login
            </Link>
          )}
        </div>
      </div>
    </nav>
  );
}
