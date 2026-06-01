'use client';
import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { fetchCurrentUser, logout, User } from '../../lib/auth';

export default function AccountPage() {
  const router = useRouter();
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchCurrentUser().then(userData => {
      if (!userData) {
        router.push('/login');
      } else {
        setUser(userData);
      }
      setLoading(false);
    });
  }, [router]);

  const handleLogout = async () => {
    await logout();
    router.push('/');
  };

  if (loading) {
    return <div style={{ textAlign: 'center', padding: '4rem' }}>Loading account details...</div>;
  }

  if (!user) return null;

  return (
    <div className="container" style={{ padding: '4rem 1rem' }}>
      <div className="glass-panel" style={{ maxWidth: '600px', margin: '0 auto', padding: '2rem' }}>
        <h1 style={{ fontSize: '2rem', marginBottom: '1.5rem' }}>My Account</h1>
        
        <div style={{ marginBottom: '2rem' }}>
          <div style={{ marginBottom: '1rem' }}>
            <span style={{ color: 'var(--text-secondary)', fontSize: '0.875rem' }}>Name</span>
            <div style={{ fontSize: '1.25rem', fontWeight: 500 }}>{user.firstName} {user.lastName}</div>
          </div>
          
          <div style={{ marginBottom: '1rem' }}>
            <span style={{ color: 'var(--text-secondary)', fontSize: '0.875rem' }}>Email</span>
            <div style={{ fontSize: '1.25rem' }}>{user.email}</div>
          </div>
          
          <div style={{ marginBottom: '1rem' }}>
            <span style={{ color: 'var(--text-secondary)', fontSize: '0.875rem' }}>Membership Tier</span>
            <div style={{ 
              display: 'inline-block',
              padding: '0.25rem 0.75rem', 
              background: 'rgba(59, 130, 246, 0.1)', 
              color: 'var(--primary)',
              borderRadius: '9999px',
              fontWeight: 600,
              fontSize: '0.875rem',
              marginTop: '0.25rem'
            }}>
              {user.tier}
            </div>
          </div>
        </div>

        <button 
          onClick={handleLogout} 
          className="btn btn-outline" 
          style={{ width: '100%', borderColor: 'var(--danger)', color: 'var(--danger)' }}
        >
          Log Out
        </button>
      </div>
    </div>
  );
}
