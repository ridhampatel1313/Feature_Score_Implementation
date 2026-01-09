import json
import hashlib
from typing import Optional, Any
import os

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# In-memory cache fallback
_memory_cache = {}


class Cache:
    """Cache implementation with Redis support and in-memory fallback"""
    
    def __init__(self):
        self.redis_client = None
        if REDIS_AVAILABLE:
            redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
            try:
                self.redis_client = redis.from_url(redis_url, decode_responses=True)
                # Test connection
                self.redis_client.ping()
            except Exception:
                self.redis_client = None
    
    def _make_key(self, prefix: str, *args) -> str:
        """Generate cache key from prefix and arguments"""
        key_str = f"{prefix}:{':'.join(str(arg) for arg in args)}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def get(self, prefix: str, *args) -> Optional[Any]:
        """Get value from cache"""
        key = self._make_key(prefix, *args)
        
        if self.redis_client:
            try:
                value = self.redis_client.get(key)
                if value:
                    return json.loads(value)
            except Exception:
                pass
        
        # Fallback to in-memory cache
        return _memory_cache.get(key)
    
    def set(self, prefix: str, value: Any, ttl: int = 3600, *args):
        """Set value in cache with TTL (default 1 hour)"""
        key = self._make_key(prefix, *args)
        
        if self.redis_client:
            try:
                self.redis_client.setex(key, ttl, json.dumps(value))
                return
            except Exception:
                pass
        
        # Fallback to in-memory cache (no TTL support)
        _memory_cache[key] = value
    
    def delete(self, prefix: str, *args):
        """Delete value from cache"""
        key = self._make_key(prefix, *args)
        
        if self.redis_client:
            try:
                self.redis_client.delete(key)
            except Exception:
                pass
        
        # Fallback to in-memory cache
        _memory_cache.pop(key, None)
    
    def clear_pattern(self, prefix: str):
        """Clear all keys matching a pattern (for cache invalidation)"""
        if self.redis_client:
            try:
                pattern = f"*{prefix}*"
                keys = self.redis_client.keys(pattern)
                if keys:
                    self.redis_client.delete(*keys)
            except Exception:
                pass
        
        # Fallback: clear all in-memory cache entries matching prefix
        keys_to_delete = [k for k in _memory_cache.keys() if prefix in k]
        for key in keys_to_delete:
            _memory_cache.pop(key, None)


# Global cache instance
cache = Cache()
