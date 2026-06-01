export interface User {
  customerId: string;
  firstName: string;
  lastName: string;
  email: string;
  tier: string;
}

let cachedUser: User | null = null;
let fetchPromise: Promise<User | null> | null = null;

export const fetchCurrentUser = async (force = false): Promise<User | null> => {
  if (typeof window === 'undefined') return null;
  if (!force && cachedUser) return cachedUser;
  if (fetchPromise && !force) return fetchPromise;

  fetchPromise = fetch('http://localhost:3000/api/customers/me', { credentials: 'include' })
    .then(async (res) => {
      if (!res.ok) {
        cachedUser = null;
        return null;
      }
      const data = await res.json();
      cachedUser = data;
      return data;
    })
    .catch(() => {
      cachedUser = null;
      return null;
    })
    .finally(() => {
      window.dispatchEvent(new Event('auth-updated'));
    });

  return fetchPromise;
};

export const logout = async () => {
  if (typeof window === 'undefined') return;
  await fetch('http://localhost:3000/api/customers/logout', { method: 'POST', credentials: 'include' });
  cachedUser = null;
  fetchPromise = null;
  window.dispatchEvent(new Event('auth-updated'));
};

export const getCurrentUser = () => cachedUser;
