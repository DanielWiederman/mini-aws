import Link from 'next/link';
import CatalogGridClient from '@/components/CatalogGridClient';
import SearchBarClient from '@/components/SearchBarClient';

async function getCatalog(page: number, limit: number, q?: string, sort?: string) {
  try {
    let url = `http://localhost:3000/api/catalog?page=${page}&limit=${limit}`;
    if (q) url += `&q=${encodeURIComponent(q)}`;
    if (sort) url += `&sort=${encodeURIComponent(sort)}`;
    
    const res = await fetch(url, { 
      cache: 'no-store' // Always fetch latest for this demo
    });
    if (!res.ok) throw new Error('Failed to fetch data');
    return res.json();
  } catch (error) {
    console.error(error);
    return { data: [], total: 0, page: 1, limit: 10, totalPages: 1 };
  }
}

// Next.js 15 requires awaiting searchParams if accessed
export default async function Home({ searchParams }: { searchParams: Promise<{ [key: string]: string | string[] | undefined }> }) {
  const params = await searchParams;
  const page = typeof params.page === 'string' ? parseInt(params.page) : 1;
  const q = typeof params.q === 'string' ? params.q : undefined;
  const sort = typeof params.sort === 'string' ? params.sort : undefined;
  const limit = 8; // Items per page
  
  const catalog = await getCatalog(page, limit, q, sort);

  const getPageLink = (p: number) => {
    let url = `/?page=${p}`;
    if (q) url += `&q=${encodeURIComponent(q)}`;
    if (sort) url += `&sort=${encodeURIComponent(sort)}`;
    return url;
  };

  return (
    <div style={{ padding: '2rem 0' }}>
      <div style={{ textAlign: 'center', marginBottom: '4rem' }}>
        <h1 style={{ fontSize: '3.5rem', marginBottom: '1rem', background: 'linear-gradient(to right, var(--primary), var(--secondary))', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>
          Discover the Extraordinary
        </h1>
        <p style={{ fontSize: '1.25rem', color: 'var(--text-secondary)', maxWidth: '600px', margin: '0 auto' }}>
          Explore our premium catalog of carefully curated products designed just for you.
        </p>
      </div>

      <SearchBarClient />

      <CatalogGridClient initialCatalog={catalog.data} />

      {/* Pagination Controls */}
      {catalog.totalPages > 1 && (
        <div style={{ display: 'flex', justifyContent: 'center', gap: '1rem', marginTop: '4rem' }}>
          {page > 1 && (
            <Link href={getPageLink(page - 1)} className="btn btn-outline">
              Previous
            </Link>
          )}
          <span style={{ display: 'flex', alignItems: 'center', padding: '0 1rem', fontWeight: 600 }}>
            Page {page} of {catalog.totalPages}
          </span>
          {page < catalog.totalPages && (
            <Link href={getPageLink(page + 1)} className="btn btn-outline">
              Next
            </Link>
          )}
        </div>
      )}
    </div>
  );
}
