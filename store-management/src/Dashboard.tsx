import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { LogOut, Edit2, Trash2, Calendar, UserPlus } from 'lucide-react';

export default function Dashboard() {
  const [products, setProducts] = useState<any[]>([]);
  const [role, setRole] = useState(localStorage.getItem('store_admin_role'));
  const token = localStorage.getItem('store_admin_token');
  const navigate = useNavigate();

  // Modals
  const [editingProduct, setEditingProduct] = useState<any>(null);
  const [schedulingPrice, setSchedulingPrice] = useState<any>(null);
  const [showAdminModal, setShowAdminModal] = useState(false);

  useEffect(() => {
    fetchProducts();
  }, []);

  const fetchProducts = async () => {
    const res = await fetch('http://localhost:3000/api/catalog?limit=50');
    const data = await res.json();
    if (data.data) setProducts(data.data);
  };

  const handleLogout = () => {
    localStorage.removeItem('store_admin_token');
    localStorage.removeItem('store_admin_role');
    navigate('/login');
  };

  const deleteProduct = async (id: string) => {
    if (!confirm('Are you sure you want to delete this product?')) return;
    await fetch(`http://localhost:3000/api/catalog/${id}`, {
      method: 'DELETE',
      headers: { 'Authorization': `Bearer ${token}` }
    });
    alert('Product deletion scheduled via event stream!');
    setTimeout(fetchProducts, 1000);
  };

  const updateProduct = async (e: React.FormEvent) => {
    e.preventDefault();
    await fetch(`http://localhost:3000/api/catalog/${editingProduct.productId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
      body: JSON.stringify({
        title: editingProduct.title,
        description: editingProduct.description
      })
    });
    setEditingProduct(null);
    alert('Product update dispatched!');
    setTimeout(fetchProducts, 1000);
  };

  const schedulePrice = async (e: React.FormEvent) => {
    e.preventDefault();
    await fetch(`http://localhost:3000/api/catalog/${schedulingPrice.productId}/price-schedule`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
      body: JSON.stringify({
        newPrice: parseFloat(schedulingPrice.newPrice),
        triggerAt: schedulingPrice.triggerAt
      })
    });
    setSchedulingPrice(null);
    alert('Price update scheduled!');
  };

  const createAdmin = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const formData = new FormData(e.currentTarget);
    const payload = Object.fromEntries(formData.entries());
    
    const res = await fetch('http://localhost:3000/api/admins', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
      body: JSON.stringify(payload)
    });
    
    if (res.ok) {
      alert('Admin created!');
      setShowAdminModal(false);
    } else {
      const data = await res.json();
      alert(`Error: ${data.error}`);
    }
  };

  return (
    <div style={{ padding: '40px', maxWidth: '1200px', margin: '0 auto' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '40px' }}>
        <div>
          <h1 style={{ color: 'var(--primary)', marginBottom: '8px' }}>Admin Dashboard</h1>
          <p style={{ color: 'var(--text-muted)' }}>Role: {role}</p>
        </div>
        <div style={{ display: 'flex', gap: '16px' }}>
          {role === 'SUPER_ADMIN' && (
            <button className="btn-primary" onClick={() => setShowAdminModal(true)} style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <UserPlus size={18} /> Add Admin
            </button>
          )}
          <button className="btn-danger" onClick={handleLogout} style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <LogOut size={18} /> Logout
          </button>
        </div>
      </div>

      <div className="glass-panel">
        <h2 style={{ marginBottom: '24px' }}>Catalog Management</h2>
        <table>
          <thead>
            <tr>
              <th>Image</th>
              <th>Product</th>
              <th>Price</th>
              <th>Stock</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {products.map(p => (
              <tr key={p.productId}>
                <td>
                  <img src={p.thumbnail} alt={p.title} style={{ width: '40px', height: '40px', borderRadius: '8px', objectFit: 'cover' }} />
                </td>
                <td>
                  <div style={{ fontWeight: 600 }}>{p.title}</div>
                  <div style={{ fontSize: '0.875rem', color: 'var(--text-muted)' }}>{p.description || 'No description'}</div>
                </td>
                <td>${p.price.toFixed(2)}</td>
                <td>{p.stockCount} units</td>
                <td>
                  <div style={{ display: 'flex', gap: '8px' }}>
                    <button onClick={() => setEditingProduct(p)} style={{ background: 'transparent', border: 'none', color: 'var(--text)', cursor: 'pointer' }}><Edit2 size={18} /></button>
                    <button onClick={() => setSchedulingPrice({ ...p, newPrice: p.price, triggerAt: '' })} style={{ background: 'transparent', border: 'none', color: 'var(--primary)', cursor: 'pointer' }}><Calendar size={18} /></button>
                    <button onClick={() => deleteProduct(p.productId)} style={{ background: 'transparent', border: 'none', color: 'var(--danger)', cursor: 'pointer' }}><Trash2 size={18} /></button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Edit Product Modal */}
      {editingProduct && (
        <div className="modal-overlay">
          <div className="modal-content glass-panel">
            <h2 style={{ marginBottom: '24px' }}>Edit Product</h2>
            <form onSubmit={updateProduct}>
              <input className="input-field" value={editingProduct.title} onChange={e => setEditingProduct({...editingProduct, title: e.target.value})} placeholder="Title" required />
              <textarea className="input-field" value={editingProduct.description || ''} onChange={e => setEditingProduct({...editingProduct, description: e.target.value})} placeholder="Description" rows={3} />
              <div style={{ display: 'flex', gap: '16px' }}>
                <button type="button" className="btn-danger" style={{ flex: 1 }} onClick={() => setEditingProduct(null)}>Cancel</button>
                <button type="submit" className="btn-primary" style={{ flex: 1 }}>Save Changes</button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Schedule Price Modal */}
      {schedulingPrice && (
        <div className="modal-overlay">
          <div className="modal-content glass-panel">
            <h2 style={{ marginBottom: '24px' }}>Schedule Price Update</h2>
            <p style={{ marginBottom: '16px', color: 'var(--text-muted)' }}>{schedulingPrice.title}</p>
            <form onSubmit={schedulePrice}>
              <input type="number" step="0.01" className="input-field" value={schedulingPrice.newPrice} onChange={e => setSchedulingPrice({...schedulingPrice, newPrice: e.target.value})} placeholder="New Price" required />
              <input type="datetime-local" className="input-field" value={schedulingPrice.triggerAt} onChange={e => setSchedulingPrice({...schedulingPrice, triggerAt: e.target.value})} required />
              <div style={{ display: 'flex', gap: '16px' }}>
                <button type="button" className="btn-danger" style={{ flex: 1 }} onClick={() => setSchedulingPrice(null)}>Cancel</button>
                <button type="submit" className="btn-primary" style={{ flex: 1 }}>Schedule</button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Create Admin Modal */}
      {showAdminModal && (
        <div className="modal-overlay">
          <div className="modal-content glass-panel">
            <h2 style={{ marginBottom: '24px' }}>Create New Admin</h2>
            <form onSubmit={createAdmin}>
              <input name="customerId" className="input-field" placeholder="Admin ID (e.g. admin_2)" required />
              <input name="firstName" className="input-field" placeholder="First Name" required />
              <input name="lastName" className="input-field" placeholder="Last Name" required />
              <input type="email" name="email" className="input-field" placeholder="Email" required />
              <input type="password" name="password" className="input-field" placeholder="Password" required />
              <div style={{ display: 'flex', gap: '16px' }}>
                <button type="button" className="btn-danger" style={{ flex: 1 }} onClick={() => setShowAdminModal(false)}>Cancel</button>
                <button type="submit" className="btn-primary" style={{ flex: 1 }}>Create</button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
