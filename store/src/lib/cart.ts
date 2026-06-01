export interface CartItem {
  productId: string;
  title: string;
  price: number;
  quantity: number;
}

export const getCart = (): CartItem[] => {
  if (typeof window === 'undefined') return [];
  try {
    const cart = localStorage.getItem('cart');
    return cart ? JSON.parse(cart) : [];
  } catch (err) {
    return [];
  }
};

export const saveCart = (cart: CartItem[]) => {
  if (typeof window === 'undefined') return;
  localStorage.setItem('cart', JSON.stringify(cart));
  // Dispatch custom event to notify components like Navbar
  window.dispatchEvent(new Event('cart-updated'));
};

export const addToCart = (product: { productId: string, title: string, price: number }) => {
  const cart = getCart();
  const existing = cart.find(item => item.productId === product.productId);
  if (existing) {
    existing.quantity += 1;
  } else {
    cart.push({ ...product, quantity: 1 });
  }
  saveCart(cart);
};

export const removeFromCart = (productId: string) => {
  const cart = getCart();
  const newCart = cart.filter(item => item.productId !== productId);
  saveCart(newCart);
};

export const clearCart = () => {
  saveCart([]);
};
