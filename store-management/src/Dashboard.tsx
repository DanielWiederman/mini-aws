import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Package, Plus, Trash2, Edit2, LogOut, Check, X, Calendar, UserPlus, LayoutDashboard, ShieldCheck } from 'lucide-react';
import { io } from 'socket.io-client';
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

  const [activeTab, setActiveTab] = useState<'catalog' | 'orders'>('catalog');
  const [searchQuery, setSearchQuery] = useState('');
  const [orders, setOrders] = useState<any[]>([]);
  const [orderPage, setOrderPage] = useState<number>(1);
  const [orderTotalPages, setOrderTotalPages] = useState<number>(1);
  const [orderFilter, setOrderFilter] = useState<'ALL' | 'COMPLETED' | 'CANCELLED' | 'PENDING'>('ALL');
  
  // Orders Search & Filter States
  const [orderSearchQuery, setOrderSearchQuery] = useState('');
  const [orderStartDate, setOrderStartDate] = useState('');
  const [orderEndDate, setOrderEndDate] = useState('');

  useEffect(() => {
    if (activeTab === 'catalog') {
      fetchProducts();
    } else {
      fetchOrders();
    }
  }, [activeTab, orderSearchQuery, orderStartDate, orderEndDate]);

  useEffect(() => {
    const socket = io('http://localhost:4000');
    
    socket.on('catalogUpdate', (event) => {
      setProducts(prev => {
        const index = prev.findIndex(p => p.productId === event.productId);
        if (index >= 0) {
          const newProducts = [...prev];
          newProducts[index] = { ...newProducts[index], ...event };
          return newProducts;
        } else if (event.eventType === 'CATALOG_UPDATE_END' && !event.isDeleted) {
          // It's a new product, prepend it
          return [event, ...prev];
        }
        return prev;
      });
    });

    socket.on('orderUpdate', (event) => {
      if (activeTab === 'orders') {
        setOrders(prev => {
          const index = prev.findIndex(o => o.orderId === event.orderId);
          if (index >= 0) {
            const newOrders = [...prev];
            newOrders[index] = { ...newOrders[index], ...event };
            return newOrders;
          } else {
            // New order, prepend to top of list
            return [event, ...prev];
          }
        });
      }
    });

    return () => { socket.disconnect(); };
  }, [activeTab, token]);

  const fetchOrders = async (pageToFetch: number = 1) => {
    try {
      const url = new URL('http://localhost:3000/api/orders');
      url.searchParams.append('page', pageToFetch.toString());
      if (orderSearchQuery) url.searchParams.append('q', orderSearchQuery);
      if (orderStartDate) url.searchParams.append('startDate', orderStartDate);
      if (orderEndDate) url.searchParams.append('endDate', orderEndDate);
      
      const res = await fetch(url.toString(), {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      
      if (res.status === 401) {
        handleLogout();
        return;
      }
      
      const data = await res.json();
      
      if (pageToFetch > 1) {
        setOrders(prev => [...prev, ...(data.data || [])]);
      } else {
        setOrders(data.data || []);
      }
      setOrderPage(data.page || 1);
      setOrderTotalPages(data.totalPages || 1);
    } catch (e) {
      console.error(e);
    }
  };

  const fetchProducts = async () => {
    const res = await fetch(`http://localhost:3000/api/catalog?limit=50&q=${encodeURIComponent(searchQuery)}`);
    const data = await res.json();
    if (data.data) setProducts(data.data);
  };

  useEffect(() => {
    if (activeTab === 'catalog') {
      const delayDebounceFn = setTimeout(() => {
        fetchProducts();
      }, 300);
      return () => clearTimeout(delayDebounceFn);
    }
  }, [searchQuery]);

  const handleLogout = () => {
    localStorage.removeItem('store_admin_token');
    localStorage.removeItem('store_admin_role');
    navigate('/login');
  };

  const deleteProduct = async (id: string) => {
    if (!confirm('Are you sure you want to soft delete this product? It will no longer appear in the catalog.')) return;
    const res = await fetch(`http://localhost:3000/api/catalog/${id}`, {
      method: 'DELETE',
      headers: { 'Authorization': `Bearer ${token}` }
    });
    if (res.status === 401) return handleLogout();
    alert('Product deletion scheduled via event stream!');
    setTimeout(fetchProducts, 1000);
  };

  const updateProduct = async (e: React.FormEvent) => {
    e.preventDefault();
    const res = await fetch(`http://localhost:3000/api/catalog/${editingProduct.productId}`, {
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
    if (res.status === 401) return handleLogout();
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
    const res = await fetch(`http://localhost:3000/api/catalog`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}`, 'Idempotency-Key': idempotencyKey },
      body: JSON.stringify(payload)
    });
    if (res.status === 401) return handleLogout();
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
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '20px', marginBottom: '20px' }}>
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
            <h1 style={{ display: 'flex', alignItems: 'center', gap: '12px', fontSize: '1.5rem', marginBottom: '8px' }}>
              Store Management
            </h1>
            <p style={{ color: 'var(--text-secondary)' }}>Logged in as <strong style={{ color: 'var(--primary)' }}>{role}</strong></p>
          </div>
        </div>
        
        <div style={{ display: 'flex', gap: '16px', alignItems: 'center', flexWrap: 'wrap' }}>
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

      <div style={{ display: 'flex', gap: '8px', background: 'var(--panel-bg)', padding: '6px', borderRadius: '12px', border: '1px solid var(--panel-border)', marginBottom: '24px', width: 'fit-content' }}>
        <button 
          onClick={() => setActiveTab('catalog')}
          style={{ 
            padding: '8px 16px', borderRadius: '8px', border: 'none', cursor: 'pointer', fontWeight: 600,
            background: activeTab === 'catalog' ? 'var(--primary)' : 'transparent',
            color: activeTab === 'catalog' ? '#fff' : 'var(--text-muted)',
            transition: 'all 0.2s'
          }}
        >Catalog</button>
        <button 
          onClick={() => setActiveTab('orders')}
          style={{ 
            padding: '8px 16px', borderRadius: '8px', border: 'none', cursor: 'pointer', fontWeight: 600,
            background: activeTab === 'orders' ? 'var(--primary)' : 'transparent',
            color: activeTab === 'orders' ? '#fff' : 'var(--text-muted)',
            transition: 'all 0.2s'
          }}
        >Orders</button>
      </div>

      {activeTab === 'catalog' ? (
        <div className="glass-panel" style={{ overflowX: 'auto', display: 'flex', flexDirection: 'column', gap: '16px' }}>
          
          <div style={{ padding: '0 16px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '16px' }}>
            <input 
              type="text" 
              placeholder="Search products by title..." 
              value={searchQuery}
              onChange={e => setSearchQuery(e.target.value)}
              style={{ 
                flex: 1,
                maxWidth: '500px', 
                padding: '12px 20px', 
                borderRadius: '12px', 
                background: 'rgba(0,0,0,0.3)', 
                border: '1px solid var(--panel-border)',
                color: 'white',
                outline: 'none',
                fontSize: '1rem'
              }} 
            />
            <button className="btn-primary" onClick={() => setCreatingProduct({ productId: `prod_${Date.now()}`, title: '', description: '', price: 0, stockCount: 0, image: '', thumbnail: '' })}>
              <Package size={18} /> Add Product
            </button>
          </div>

          <table style={{ width: '100%', borderCollapse: 'collapse', textAlign: 'left' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid var(--panel-border)' }}>
                <th style={{ padding: '16px', color: 'var(--text-muted)', fontWeight: 600 }}>Image</th>
                <th style={{ padding: '16px', color: 'var(--text-muted)', fontWeight: 600 }}>Product Name</th>
                <th style={{ padding: '16px', color: 'var(--text-muted)', fontWeight: 600 }}>Price</th>
                <th style={{ padding: '16px', color: 'var(--text-muted)', fontWeight: 600 }}>Stock</th>
                <th style={{ padding: '16px', color: 'var(--text-muted)', fontWeight: 600, textAlign: 'right' }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {products.map(p => (
                <tr key={p.productId} style={{ borderBottom: '1px solid rgba(255,255,255,0.02)' }}>
                  <td style={{ padding: '16px' }}>
                    <div style={{ 
                      width: '48px', height: '48px', borderRadius: '12px', overflow: 'hidden', 
                      border: '1px solid var(--panel-border)', background: 'rgba(0,0,0,0.5)'
                    }}>
                      <img src={p.image || p.thumbnail || 'http://localhost:3001/aws-mini-default.png'} alt={p.title} style={{ width: '100%', height: '100%', objectFit: 'cover' }} onError={(e) => { e.currentTarget.src = 'http://localhost:3001/aws-mini-default.png' }} />
                    </div>
                  </td>
                  <td>
                    <div style={{ fontWeight: 600, color: 'var(--text-main)', marginBottom: '4px' }}>{p.title}</div>
                    <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>{p.productId}</div>
                  </td>
                  <td style={{ padding: '16px', fontWeight: 600, color: 'var(--primary)' }}>${Number(p.price || 0).toFixed(2)}</td>
                  <td style={{ padding: '16px' }}>
                    <span className="badge" style={{ background: Number(p.stockCount) > 0 ? 'rgba(16, 185, 129, 0.1)' : 'rgba(239, 68, 68, 0.1)', color: Number(p.stockCount) > 0 ? 'var(--success)' : 'var(--danger)' }}>
                      {p.stockCount} left
                    </span>
                  </td>
                  <td style={{ padding: '16px', textAlign: 'right' }}>
                    <div style={{ display: 'flex', gap: '8px', justifyContent: 'flex-end' }}>
                      <button className="icon-btn" onClick={() => setSchedulingPrice(p)} title="Schedule Price Change">
                        <Calendar size={16} />
                      </button>
                      <button className="icon-btn" onClick={() => setEditingProduct(p)} title="Edit Product">
                        <Edit2 size={16} />
                      </button>
                      {role === 'SUPER_ADMIN' && (
                        <button className="icon-btn btn-danger" onClick={() => deleteProduct(p.productId)} title="Delete Product" style={{ color: 'var(--danger)' }}>
                          <Trash2 size={16} />
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
              {products.length === 0 && (
                <tr>
                  <td colSpan={5} style={{ padding: '32px', textAlign: 'center', color: 'var(--text-muted)' }}>
                    No products found. Add some!
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="glass-panel" style={{ padding: '24px' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '24px' }}>
            <h2 style={{ fontSize: '1.25rem', color: 'var(--text-main)' }}>Order History</h2>
            <div style={{ display: 'flex', gap: '8px' }}>
              {['ALL', 'COMPLETED', 'PENDING', 'CANCELLED'].map(filter => (
                <button
                  key={filter}
                  onClick={() => setOrderFilter(filter as any)}
                  style={{
                    padding: '6px 12px', borderRadius: '6px', border: '1px solid var(--panel-border)', cursor: 'pointer', fontSize: '0.85rem',
                    background: orderFilter === filter ? 'var(--primary)' : 'transparent',
                    color: orderFilter === filter ? '#fff' : 'var(--text-muted)',
                  }}
                >
                  {filter}
                </button>
              ))}
            </div>
          </div>

          {/* Orders Search Bar */}
          <div style={{ display: 'flex', gap: '12px', marginBottom: '24px', flexWrap: 'wrap' }}>
            <input 
              type="text" 
              placeholder="Search by Order ID, Customer, or Items..." 
              value={orderSearchQuery}
              onChange={e => setOrderSearchQuery(e.target.value)}
              style={{ 
                flex: 1, minWidth: '250px', padding: '10px 16px', borderRadius: '8px', 
                background: 'rgba(0,0,0,0.3)', border: '1px solid var(--panel-border)',
                color: 'white', outline: 'none'
              }} 
            />
            <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
              <span style={{ color: 'var(--text-muted)', fontSize: '0.9rem' }}>From:</span>
              <input 
                type="date" 
                value={orderStartDate}
                onChange={e => setOrderStartDate(e.target.value)}
                style={{ 
                  padding: '8px 12px', borderRadius: '8px', background: 'rgba(0,0,0,0.3)', 
                  border: '1px solid var(--panel-border)', color: 'white', outline: 'none'
                }} 
              />
            </div>
            <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
              <span style={{ color: 'var(--text-muted)', fontSize: '0.9rem' }}>To:</span>
              <input 
                type="date" 
                value={orderEndDate}
                onChange={e => setOrderEndDate(e.target.value)}
                style={{ 
                  padding: '8px 12px', borderRadius: '8px', background: 'rgba(0,0,0,0.3)', 
                  border: '1px solid var(--panel-border)', color: 'white', outline: 'none'
                }} 
              />
            </div>
          </div>
          
          <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
            {orders.filter(o => orderFilter === 'ALL' || o.status === orderFilter).length === 0 ? (
              <div style={{ textAlign: 'center', padding: '32px', color: 'var(--text-muted)' }}>No orders match this filter.</div>
            ) : (
              orders.filter(o => orderFilter === 'ALL' || o.status === orderFilter).map(order => (
                <div key={order.orderId} style={{ background: 'rgba(255,255,255,0.02)', border: '1px solid var(--panel-border)', borderRadius: '12px', padding: '16px' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '12px' }}>
                    <div>
                      <h3 style={{ fontSize: '1rem', color: 'var(--text-main)' }}>{order.orderId}</h3>
                      <p style={{ fontSize: '0.85rem', color: 'var(--text-muted)', marginTop: '4px' }}>
                        Customer: <span style={{ color: 'var(--text-main)' }}>{order.customerName}</span> ({order.customerTier})
                      </p>
                    </div>
                    <div style={{ textAlign: 'right' }}>
                      <span className="badge" style={{ 
                        background: order.status === 'COMPLETED' ? 'rgba(16, 185, 129, 0.1)' : order.status === 'CANCELLED' ? 'rgba(239, 68, 68, 0.1)' : 'rgba(245, 158, 11, 0.1)',
                        color: order.status === 'COMPLETED' ? 'var(--success)' : order.status === 'CANCELLED' ? 'var(--danger)' : '#f59e0b'
                      }}>
                        {order.status}
                      </span>
                      <p style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginTop: '6px' }}>{order.processedAt}</p>
                    </div>
                  </div>
                  
                  <div style={{ borderTop: '1px solid rgba(255,255,255,0.05)', paddingTop: '12px', marginTop: '12px' }}>
                    <h4 style={{ fontSize: '0.85rem', color: 'var(--text-secondary)', marginBottom: '8px' }}>Purchased Items</h4>
                    <ul style={{ listStyle: 'none', padding: 0, margin: 0, display: 'flex', flexDirection: 'column', gap: '6px' }}>
                      {order.purchasedItems.map((item: any, idx: number) => (
                        <li key={idx} style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.9rem' }}>
                          <span><span style={{ color: 'var(--text-muted)' }}>{item.qty}x</span> {item.title}</span>
                          <span>${item.totalCost}</span>
                        </li>
                      ))}
                    </ul>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: '12px', paddingTop: '12px', borderTop: '1px dashed rgba(255,255,255,0.1)', fontWeight: 'bold' }}>
                      <span>Total</span>
                      <span style={{ color: 'var(--primary)' }}>${order.invoiceTotal}</span>
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>

            {orderPage < orderTotalPages && (
              <div style={{ textAlign: 'center', marginTop: '16px' }}>
                <button 
                  style={{ padding: '8px 16px', background: 'rgba(255,255,255,0.1)', border: 'none', color: 'white', borderRadius: '8px', cursor: 'pointer' }}
                  onClick={() => fetchOrders(orderPage + 1)}
                >
                  Load More Orders
                </button>
              </div>
            )}
        </div>
      )}

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
