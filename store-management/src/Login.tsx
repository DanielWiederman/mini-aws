import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Lock, Store, ChevronRight } from 'lucide-react';

export default function Login() {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const navigate = useNavigate();

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      const res = await fetch('http://localhost:3000/api/customers/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password })
      });
      const data = await res.json();
      
      if (!res.ok) throw new Error(data.error);
      
      if (data.role !== 'ADMIN' && data.role !== 'SUPER_ADMIN') {
        throw new Error('Access denied. Admin role required.');
      }

      localStorage.setItem('store_admin_token', data.token);
      localStorage.setItem('store_admin_role', data.role);
      navigate('/');
    } catch (err: any) {
      setError(err.message);
    }
  };

  return (
    <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px' }}>
      <div className="glass-panel" style={{ width: '100%', maxWidth: '440px', padding: '48px 40px' }}>
        <div style={{ textAlign: 'center', marginBottom: '40px' }}>
          <div style={{ 
            display: 'inline-flex', 
            alignItems: 'center', 
            justifyContent: 'center',
            width: '64px', 
            height: '64px', 
            borderRadius: '16px', 
            background: 'linear-gradient(135deg, rgba(139, 92, 246, 0.2) 0%, rgba(59, 130, 246, 0.2) 100%)',
            border: '1px solid rgba(139, 92, 246, 0.3)',
            marginBottom: '24px',
            boxShadow: 'var(--shadow-glow)'
          }}>
            <Store size={32} color="var(--primary)" />
          </div>
          <h2 style={{ fontSize: '1.75rem', marginBottom: '8px' }}>Store <span className="text-gradient">Management</span></h2>
          <p style={{ color: 'var(--text-muted)' }}>Secure access for administrators</p>
        </div>

        {error && (
          <div style={{ 
            backgroundColor: 'rgba(239, 68, 68, 0.1)', 
            border: '1px solid rgba(239, 68, 68, 0.2)',
            color: 'var(--danger)', 
            padding: '12px 16px',
            borderRadius: '12px',
            marginBottom: '24px', 
            textAlign: 'center',
            fontSize: '0.9rem'
          }}>
            {error}
          </div>
        )}

        <form onSubmit={handleLogin} style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
          <div>
            <input 
              type="email" 
              className="input-field" 
              placeholder="Admin Email Address" 
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
            />
          </div>
          <div>
            <input 
              type="password" 
              className="input-field" 
              placeholder="Password" 
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
            />
          </div>
          <button type="submit" className="btn-primary" style={{ marginTop: '16px' }}>
            Authenticate <ChevronRight size={18} />
          </button>
        </form>
      </div>
    </div>
  );
}
