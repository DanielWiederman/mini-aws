import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { LogOut, Edit2, Trash2, Calendar, UserPlus, Package, LayoutDashboard, ShieldCheck } from 'lucide-react';
import DatePicker from 'react-datepicker';
import 'react-datepicker/dist/react-datepicker.css';

export default function Dashboard() {
  const [products, setProducts] = useState<any[]>([]);
  const [role, setRole] = useState(localStorage.getItem('store_admin_role'));
  const token = localStorage.getItem('store_admin_token');
  const navigate = useNavigate();

  // Modals
  const [editingProduct, setEditingProduct] = useState<any>(null);
  const [schedulingPrice, setSchedulingPrice] = useState<any>(null);
  const [showAdminModal, setShowAdminModal] = useState(false);
  const [creatingProduct, setCreatingProduct] = useState<any>(null);

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
    if (!confirm('Are you sure you want to soft delete this product? It will no longer appear in the catalog.')) return;
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
        description: editingProduct.description,
        price: parseFloat(editingProduct.price),
        stockCount: parseInt(editingProduct.stockCount, 10),
        thumbnail: editingProduct.thumbnail,
        image: editingProduct.image
      })
    });
    setEditingProduct(null);
    alert('Product update dispatched!');
    setTimeout(fetchProducts, 1000);
  };

  const handleImageUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const formData = new FormData();
    formData.append('image', file);

    try {
      const res = await fetch('http://localhost:3002/api/assets/upload', {
        method: 'POST',
        body: formData
      });
      const data = await res.json();
      if (res.ok) {
        setEditingProduct({ ...editingProduct, image: data.image, thumbnail: data.thumbnail });
      } else {
        alert('Image upload failed: ' + data.error);
      }
    } catch (err) {
      alert('Failed to connect to assets service');
    }
  };

  const createProduct = async (e: React.FormEvent) => {
    e.preventDefault();
    const payload = {
      productId: creatingProduct.productId,
      title: creatingProduct.title,
      description: creatingProduct.description,
      price: parseFloat(creatingProduct.price),
      stockCount: parseInt(creatingProduct.stockCount, 10),
      thumbnail: creatingProduct.thumbnail,
      image: creatingProduct.image
    };

    const idempotencyKey = `create-${Date.now()}`;
    await fetch(`http://localhost:3000/api/catalog`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}`, 'Idempotency-Key': idempotencyKey },
      body: JSON.stringify(payload)
    });
    setCreatingProduct(null);
    alert('Product creation dispatched!');
    setTimeout(fetchProducts, 1000);
  };

  const handleCreateImageUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const formData = new FormData();
    formData.append('image', file);

    try {
      const res = await fetch('http://localhost:3002/api/assets/upload', {
        method: 'POST',
        body: formData
      });
      const data = await res.json();
      if (res.ok) {
        setCreatingProduct({ ...creatingProduct, image: data.image, thumbnail: data.thumbnail });
      } else {
        alert('Image upload failed: ' + data.error);
      }
    } catch (err) {
      alert('Failed to connect to assets service');
    }
  };

  const schedulePrice = async (e: React.FormEvent) => {
    e.preventDefault();
    await fetch(`http://localhost:3000/api/catalog/${schedulingPrice.productId}/price-schedule`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
      body: JSON.stringify({
        newPrice: parseFloat(schedulingPrice.newPrice),
        triggerAt: schedulingPrice.triggerAt.toISOString()
      })
    });
    setSchedulingPrice(null);
    alert('Price update scheduled in the background queue!');
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
      alert('Admin user creation triggered!');
      setShowAdminModal(false);
    } else {
      const data = await res.json();
      alert(`Error: ${data.error}`);
    }
  };

  return (
    <div style={{ padding: '40px', maxWidth: '1400px', margin: '0 auto' }}>
      
      {/* HEADER */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '40px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
          <div style={{ 
            width: '48px', height: '48px', borderRadius: '12px', 
            background: 'linear-gradient(135deg, rgba(139, 92, 246, 0.2) 0%, rgba(59, 130, 246, 0.2) 100%)',
            border: '1px solid rgba(139, 92, 246, 0.3)',
            display: 'flex', alignItems: 'center', justifyContent: 'center'
          }}>
            <LayoutDashboard size={24} color="var(--primary)" />
          </div>
          <div>
            <h1 style={{ marginBottom: '4px', fontSize: '1.75rem' }}>Store <span className="text-gradient">Management</span></h1>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: 'var(--text-muted)', fontSize: '0.9rem' }}>
              <ShieldCheck size={16} color="var(--accent)" />
              Authenticated as <strong style={{ color: 'var(--text-main)' }}>{role}</strong>
            </div>
          </div>
        </div>
        <div style={{ display: 'flex', gap: '16px' }}>
          <button className="btn-primary" onClick={() => setCreatingProduct({ productId: `prod_${Date.now()}`, title: '', description: '', price: 0, stockCount: 0, image: '', thumbnail: '' })} style={{ background: 'rgba(255, 255, 255, 0.05)', color: 'var(--text-main)', border: '1px solid var(--panel-border)' }}>
            <Package size={18} /> Add Product
          </button>
          {role === 'SUPER_ADMIN' && (
            <button className="btn-primary" onClick={() => setShowAdminModal(true)} style={{ background: 'rgba(255, 255, 255, 0.05)', color: 'var(--text-main)', border: '1px solid var(--panel-border)' }}>
              <UserPlus size={18} /> Provision Admin
            </button>
          )}
          <button className="btn-danger" onClick={handleLogout}>
            <LogOut size={18} /> Disconnect
          </button>
        </div>
      </div>

      {/* CATALOG PANEL */}
      <div className="glass-panel">
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '24px' }}>
          <Package size={24} color="var(--primary)" />
          <h2>Catalog Overview</h2>
        </div>
        <div className="table-container">
          <table>
            <thead>
              <tr>
                <th>Item</th>
                <th>Product Info</th>
                <th>Current Price</th>
                <th>Stock</th>
                <th style={{ textAlign: 'right' }}>Management</th>
              </tr>
            </thead>
            <tbody>
              {products.map(p => (
                <tr key={p.productId}>
                  <td style={{ width: '80px' }}>
                    <div style={{ 
                      width: '48px', height: '48px', borderRadius: '12px', overflow: 'hidden', 
                      border: '1px solid var(--panel-border)', background: 'rgba(0,0,0,0.5)'
                    }}>
                      <img src={p.thumbnail || 'http://localhost:3001/aws-mini-default.png'} alt={p.title} style={{ width: '100%', height: '100%', objectFit: 'cover' }} onError={(e) => { e.currentTarget.src = 'http://localhost:3001/aws-mini-default.png' }} />
                    </div>
                  </td>
                  <td>
                    <div style={{ fontWeight: 600, fontSize: '1.05rem', marginBottom: '4px' }}>{p.title}</div>
                    <div style={{ fontSize: '0.85rem', color: 'var(--text-muted)', maxWidth: '300px', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {p.description || 'No description provided.'}
                    </div>
                    <div style={{ fontSize: '0.75rem', color: 'var(--primary)', marginTop: '4px', fontFamily: 'monospace' }}>ID: {p.productId}</div>
                  </td>
                  <td>
                    <div style={{ fontWeight: 600, fontSize: '1.1rem' }}>${p.price.toFixed(2)}</div>
                  </td>
                  <td>
                    <div style={{ 
                      display: 'inline-block', 
                      padding: '4px 10px', 
                      borderRadius: '20px', 
                      background: p.stockCount > 10 ? 'rgba(34, 197, 94, 0.1)' : 'rgba(239, 68, 68, 0.1)',
                      color: p.stockCount > 10 ? '#4ade80' : '#f87171',
                      fontSize: '0.85rem',
                      fontWeight: 600
                    }}>
                      {p.stockCount} units
                    </div>
                  </td>
                  <td style={{ textAlign: 'right' }}>
                    <div style={{ display: 'flex', gap: '8px', justifyContent: 'flex-end' }}>
                      <button className="action-btn edit" title="Edit Product Details" onClick={() => setEditingProduct(p)}><Edit2 size={18} /></button>
                      <button className="action-btn schedule" title="Schedule Price Change" onClick={() => {
                        setSchedulingPrice({ ...p, newPrice: p.price, triggerAt: new Date() });
                      }}><Calendar size={18} /></button>
                      <button className="action-btn delete" title="Remove Product" onClick={() => deleteProduct(p.productId)}><Trash2 size={18} /></button>
                    </div>
                  </td>
                </tr>
              ))}
              {products.length === 0 && (
                <tr>
                  <td colSpan={5} style={{ textAlign: 'center', padding: '48px', color: 'var(--text-muted)' }}>
                    No products found in the catalog.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Create Product Modal */}
      {creatingProduct && (
        <div className="modal-overlay">
          <div className="modal-content glass-panel" style={{ maxHeight: '90vh', overflowY: 'auto' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '24px' }}>
              <Package size={24} color="var(--primary)" />
              <h2>Create New Product</h2>
            </div>
            <form onSubmit={createProduct}>
              <div style={{ marginBottom: '16px' }}>
                <label style={{ display: 'block', marginBottom: '8px', fontSize: '0.85rem', color: 'var(--text-muted)' }}>Product ID</label>
                <input className="input-field" value={creatingProduct.productId} onChange={e => setCreatingProduct({...creatingProduct, productId: e.target.value})} placeholder="e.g. prod_xyz" required />
              </div>
              <div style={{ marginBottom: '16px' }}>
                <label style={{ display: 'block', marginBottom: '8px', fontSize: '0.85rem', color: 'var(--text-muted)' }}>Product Title</label>
                <input className="input-field" value={creatingProduct.title} onChange={e => setCreatingProduct({...creatingProduct, title: e.target.value})} placeholder="Title" required />
              </div>
              <div style={{ marginBottom: '16px' }}>
                <label style={{ display: 'block', marginBottom: '8px', fontSize: '0.85rem', color: 'var(--text-muted)' }}>Product Description</label>
                <textarea className="input-field" value={creatingProduct.description} onChange={e => setCreatingProduct({...creatingProduct, description: e.target.value})} placeholder="Detailed description..." rows={3} />
              </div>
              <div style={{ display: 'flex', gap: '16px', marginBottom: '16px' }}>
                <div style={{ flex: 1 }}>
                  <label style={{ display: 'block', marginBottom: '8px', fontSize: '0.85rem', color: 'var(--text-muted)' }}>Price ($)</label>
                  <input type="number" step="0.01" className="input-field" value={creatingProduct.price} onChange={e => setCreatingProduct({...creatingProduct, price: e.target.value})} placeholder="Price" required />
                </div>
                <div style={{ flex: 1 }}>
                  <label style={{ display: 'block', marginBottom: '8px', fontSize: '0.85rem', color: 'var(--text-muted)' }}>Initial Stock</label>
                  <input type="number" className="input-field" value={creatingProduct.stockCount} onChange={e => setCreatingProduct({...creatingProduct, stockCount: e.target.value})} placeholder="Stock" required />
                </div>
              </div>
              <div style={{ marginBottom: '24px' }}>
                <label style={{ display: 'block', marginBottom: '8px', fontSize: '0.85rem', color: 'var(--text-muted)' }}>Product Image</label>
                <div style={{ display: 'flex', gap: '16px', alignItems: 'flex-start' }}>
                  <div style={{ width: '80px', height: '80px', borderRadius: '12px', overflow: 'hidden', background: 'rgba(0,0,0,0.5)', border: '1px solid var(--panel-border)', flexShrink: 0 }}>
                    <img 
                      src={creatingProduct.image || creatingProduct.thumbnail || 'http://localhost:3001/aws-mini-default.png'} 
                      alt="Preview" 
                      style={{ width: '100%', height: '100%', objectFit: 'cover' }} 
                      onError={(e) => { e.currentTarget.src = 'http://localhost:3001/aws-mini-default.png' }}
                    />
                  </div>
                  <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: '8px' }}>
                    <div style={{ display: 'flex', gap: '8px' }}>
                      <input 
                        type="url" 
                        className="input-field" 
                        style={{ marginBottom: 0 }}
                        value={creatingProduct.image || ''} 
                        onChange={e => setCreatingProduct({...creatingProduct, image: e.target.value})} 
                        placeholder="Or paste image URL..." 
                      />
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center' }}>
                      <label style={{ 
                        background: 'rgba(255,255,255,0.05)', 
                        padding: '8px 16px', 
                        borderRadius: '8px', 
                        cursor: 'pointer',
                        fontSize: '0.85rem',
                        border: '1px solid var(--panel-border)',
                        color: 'var(--text-main)'
                      }}>
                        Upload File
                        <input type="file" accept="image/*" onChange={handleCreateImageUpload} style={{ display: 'none' }} />
                      </label>
                    </div>
                  </div>
                </div>
              </div>
              <div style={{ display: 'flex', gap: '16px' }}>
                <button type="button" className="btn-danger" style={{ flex: 1, background: 'rgba(255,255,255,0.05)', color: 'white', borderColor: 'transparent' }} onClick={() => setCreatingProduct(null)}>Cancel</button>
                <button type="submit" className="btn-primary" style={{ flex: 1 }}>Create Product</button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Edit Product Modal */}
      {editingProduct && (
        <div className="modal-overlay">
          <div className="modal-content glass-panel">
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '24px' }}>
              <Edit2 size={24} color="var(--accent)" />
              <h2>Edit Product Details</h2>
            </div>
            <form onSubmit={updateProduct}>
              <div style={{ marginBottom: '16px' }}>
                <label style={{ display: 'block', marginBottom: '8px', fontSize: '0.85rem', color: 'var(--text-muted)' }}>Product Title</label>
                <input className="input-field" value={editingProduct.title} onChange={e => setEditingProduct({...editingProduct, title: e.target.value})} placeholder="Title" required />
              </div>
              <div style={{ marginBottom: '24px' }}>
                <label style={{ display: 'block', marginBottom: '8px', fontSize: '0.85rem', color: 'var(--text-muted)' }}>Product Description</label>
                <textarea className="input-field" value={editingProduct.description || ''} onChange={e => setEditingProduct({...editingProduct, description: e.target.value})} placeholder="Detailed description..." rows={4} />
              </div>
              <div style={{ display: 'flex', gap: '16px', marginBottom: '16px' }}>
                <div style={{ flex: 1 }}>
                  <label style={{ display: 'block', marginBottom: '8px', fontSize: '0.85rem', color: 'var(--text-muted)' }}>Price ($)</label>
                  <input type="number" step="0.01" className="input-field" value={editingProduct.price} onChange={e => setEditingProduct({...editingProduct, price: e.target.value})} placeholder="Price" required />
                </div>
                <div style={{ flex: 1 }}>
                  <label style={{ display: 'block', marginBottom: '8px', fontSize: '0.85rem', color: 'var(--text-muted)' }}>Stock Count</label>
                  <input type="number" className="input-field" value={editingProduct.stockCount} onChange={e => setEditingProduct({...editingProduct, stockCount: e.target.value})} placeholder="Stock" required />
                </div>
              </div>
              <div style={{ marginBottom: '24px' }}>
                <label style={{ display: 'block', marginBottom: '8px', fontSize: '0.85rem', color: 'var(--text-muted)' }}>Product Image</label>
                
                <div style={{ display: 'flex', gap: '16px', alignItems: 'flex-start' }}>
                  <div style={{ width: '80px', height: '80px', borderRadius: '12px', overflow: 'hidden', background: 'rgba(0,0,0,0.5)', border: '1px solid var(--panel-border)', flexShrink: 0 }}>
                    <img 
                      src={editingProduct.image || editingProduct.thumbnail || 'http://localhost:3001/aws-mini-default.png'} 
                      alt="Preview" 
                      style={{ width: '100%', height: '100%', objectFit: 'cover' }} 
                      onError={(e) => { e.currentTarget.src = 'http://localhost:3001/aws-mini-default.png' }}
                    />
                  </div>
                  <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: '8px' }}>
                    <div style={{ display: 'flex', gap: '8px' }}>
                      <input 
                        type="url" 
                        className="input-field" 
                        style={{ marginBottom: 0 }}
                        value={editingProduct.image || ''} 
                        onChange={e => setEditingProduct({...editingProduct, image: e.target.value})} 
                        placeholder="Or paste image URL..." 
                      />
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center' }}>
                      <label style={{ 
                        background: 'rgba(255,255,255,0.05)', 
                        padding: '8px 16px', 
                        borderRadius: '8px', 
                        cursor: 'pointer',
                        fontSize: '0.85rem',
                        border: '1px solid var(--panel-border)',
                        color: 'var(--text-main)'
                      }}>
                        Upload File
                        <input type="file" accept="image/*" onChange={handleImageUpload} style={{ display: 'none' }} />
                      </label>
                    </div>
                  </div>
                </div>
              </div>
              <div style={{ display: 'flex', gap: '16px' }}>
                <button type="button" className="btn-danger" style={{ flex: 1, background: 'rgba(255,255,255,0.05)', color: 'white', borderColor: 'transparent' }} onClick={() => setEditingProduct(null)}>Cancel</button>
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
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '24px' }}>
              <Calendar size={24} color="var(--primary)" />
              <h2>Schedule Price Change</h2>
            </div>
            <div style={{ padding: '12px', background: 'rgba(0,0,0,0.3)', borderRadius: '8px', marginBottom: '20px', fontSize: '0.9rem' }}>
              Target: <strong style={{ color: 'var(--text-main)' }}>{schedulingPrice.title}</strong>
            </div>
            <form onSubmit={schedulePrice}>
              <div style={{ marginBottom: '16px' }}>
                <label style={{ display: 'block', marginBottom: '8px', fontSize: '0.85rem', color: 'var(--text-muted)' }}>New Price ($)</label>
                <input type="number" step="0.01" className="input-field" value={schedulingPrice.newPrice} onChange={e => setSchedulingPrice({...schedulingPrice, newPrice: e.target.value})} placeholder="e.g. 19.99" required />
              </div>
              <div style={{ marginBottom: '24px' }}>
                <label style={{ display: 'block', marginBottom: '8px', fontSize: '0.85rem', color: 'var(--text-muted)' }}>Effective Date & Time</label>
                <div style={{ width: '100%' }}>
                  <DatePicker
                    selected={schedulingPrice.triggerAt}
                    onChange={(date: Date | null) => {
                      if (date) setSchedulingPrice({ ...schedulingPrice, triggerAt: date });
                    }}
                    showTimeSelect
                    timeFormat="HH:mm"
                    timeIntervals={15}
                    timeCaption="Time"
                    dateFormat="MMMM d, yyyy h:mm aa"
                    className="input-field"
                    wrapperClassName="date-picker-wrapper"
                    required
                  />
                </div>
              </div>
              <div style={{ display: 'flex', gap: '16px' }}>
                <button type="button" className="btn-danger" style={{ flex: 1, background: 'rgba(255,255,255,0.05)', color: 'white', borderColor: 'transparent' }} onClick={() => setSchedulingPrice(null)}>Cancel</button>
                <button type="submit" className="btn-primary" style={{ flex: 1 }}>Confirm Schedule</button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Create Admin Modal */}
      {showAdminModal && (
        <div className="modal-overlay">
          <div className="modal-content glass-panel">
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '24px' }}>
              <UserPlus size={24} color="var(--primary)" />
              <h2>Provision Administrator</h2>
            </div>
            <form onSubmit={createAdmin}>
              <div style={{ marginBottom: '12px' }}>
                <input name="customerId" className="input-field" placeholder="Admin ID (e.g. admin_2)" required />
              </div>
              <div style={{ display: 'flex', gap: '12px', marginBottom: '12px' }}>
                <input name="firstName" className="input-field" placeholder="First Name" style={{ marginBottom: 0 }} required />
                <input name="lastName" className="input-field" placeholder="Last Name" style={{ marginBottom: 0 }} required />
              </div>
              <div style={{ marginBottom: '12px' }}>
                <input type="email" name="email" className="input-field" placeholder="Official Email" required />
              </div>
              <div style={{ marginBottom: '24px' }}>
                <input type="password" name="password" className="input-field" placeholder="Secure Password" required />
              </div>
              <div style={{ display: 'flex', gap: '16px' }}>
                <button type="button" className="btn-danger" style={{ flex: 1, background: 'rgba(255,255,255,0.05)', color: 'white', borderColor: 'transparent' }} onClick={() => setShowAdminModal(false)}>Cancel</button>
                <button type="submit" className="btn-primary" style={{ flex: 1 }}>Grant Access</button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
