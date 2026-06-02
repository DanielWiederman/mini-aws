const SENSITIVE_KEYS = new Set([
  'password',
  'passwordhash',
  'email',
  'creditcard',
  'token',
  'secret',
  'key',
  'authorization'
]);

export function redactPII(obj: any): any {
  if (obj === null || obj === undefined) {
    return obj;
  }

  if (typeof obj !== 'object') {
    return obj;
  }

  if (Array.isArray(obj)) {
    return obj.map(item => redactPII(item));
  }

  const result: any = {};
  for (const [key, value] of Object.entries(obj)) {
    if (SENSITIVE_KEYS.has(key.toLowerCase())) {
      result[key] = '[REDACTED]';
    } else if (typeof value === 'object' && value !== null) {
      result[key] = redactPII(value);
    } else {
      result[key] = value;
    }
  }
  return result;
}
