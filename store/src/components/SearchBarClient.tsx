'use client';

import React, { useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';

export default function SearchBarClient() {
  const router = useRouter();
  const searchParams = useSearchParams();
  
  const currentQ = searchParams.get('q') || '';
  const currentSort = searchParams.get('sort') || '';

  const [query, setQuery] = useState(currentQ);
  const [sort, setSort] = useState(currentSort);

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    const params = new URLSearchParams();
    if (query.trim()) params.set('q', query.trim());
    if (sort) params.set('sort', sort);
    router.push(`/?${params.toString()}`);
  };

  return (
    <form onSubmit={handleSearch} style={{ display: 'flex', flexWrap: 'wrap', gap: '1rem', maxWidth: '800px', margin: '0 auto 3rem auto', padding: '0 1rem' }}>
      <input
        type="text"
        placeholder="Search for products..."
        value={query}
        onChange={e => setQuery(e.target.value)}
        style={{
          flex: '1 1 250px',
          padding: '12px 20px',
          borderRadius: '12px',
          border: '1px solid var(--border)',
          background: 'var(--surface)',
          color: 'var(--text-main)',
          fontSize: '1rem',
          outline: 'none'
        }}
      />
      <select 
        value={sort}
        onChange={e => {
          setSort(e.target.value);
          const params = new URLSearchParams();
          if (query.trim()) params.set('q', query.trim());
          if (e.target.value) params.set('sort', e.target.value);
          router.push(`/?${params.toString()}`);
        }}
        style={{
          flex: '1 1 200px',
          padding: '12px 20px',
          borderRadius: '12px',
          border: '1px solid var(--border)',
          background: 'var(--surface)',
          color: 'var(--text-main)',
          fontSize: '1rem',
          cursor: 'pointer',
          outline: 'none'
        }}
      >
        <option value="" style={{ background: '#121212', color: '#ffffff' }}>Sort by Price (Low to High)</option>
        <option value="price_asc" style={{ background: '#121212', color: '#ffffff' }}>Price (Low to High)</option>
        <option value="price_desc" style={{ background: '#121212', color: '#ffffff' }}>Price (High to Low)</option>
      </select>
      <button type="submit" className="btn btn-primary" style={{ flex: '1 1 120px' }}>
        Search
      </button>
    </form>
  );
}
