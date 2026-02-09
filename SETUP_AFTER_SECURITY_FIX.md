# Setup Instructions After Security Fixes

## ⚠️ IMMEDIATE ACTIONS REQUIRED

### 1. Rotate All Compromised Credentials

**API Keys** (exposed in .env):
- Go to Binance API Management
- Revoke the old key: `61ee7934cffddd6c1689c821e9f710261435c9970ab5bba5`
- Generate new API key and secret
- Update your environment variables

**Redis Password**:
- Change Redis password
- Update `REDIS_PASSWORD` environment variable

**Database Password**:
- Connect to PostgreSQL and change password for `trader` user
- Update `DATABASE_URL` environment variable

### 2. Configure Environment Variables

Create a secure `.env` file (DO NOT commit this to git):

```bash
# Option 1: Create local .env file
cp .env.example .env
# Edit .env with actual values
nano .env
```

Or set environment variables directly:

```bash
export DATABASE_URL="postgresql+psycopg2://trader:NEW_PASSWORD@localhost:5432/market_data"
export REDIS_HOST="your-redis-host"
export REDIS_PORT="6379"
export REDIS_DB="0"
export REDIS_PASSWORD="your-redis-password"
export API_KEY="your-new-binance-api-key"
export API_SECRET="your-new-binance-api-secret"
export SOCKET_ENDPOINT="wss://stream.coindcx.com/"
```

### 3. For Production Deployment

**Using Docker:**
```dockerfile
FROM python:3.10-slim

# ... setup ...

# Use environment variables at runtime
CMD ["python", "-m", "app.main"]
```

**Using Kubernetes Secrets:**
```bash
kubectl create secret generic market-data-secrets \
  --from-literal=DATABASE_URL="..." \
  --from-literal=REDIS_PASSWORD="..." \
  --from-literal=API_KEY="..." \
  --from-literal=API_SECRET="..."
```

**Using AWS Secrets Manager:**
```bash
aws secretsmanager create-secret \
  --name market-data/prod/credentials \
  --secret-string '{
    "DATABASE_URL": "...",
    "REDIS_PASSWORD": "...",
    "API_KEY": "...",
    "API_SECRET": "..."
  }'
```

### 4. Verify Installation

Test that everything works:

```bash
# Install dependencies
poetry install

# Check that config loads correctly
python -c "from app.config import settings; print('Config loaded successfully')"

# Verify database connection
python -c "from app.db import engine; engine.connect(); print('Database connected')"

# Verify Redis connection
python -c "from app.redis_client import redis_client; redis_client.ping(); print('Redis connected')"
```

## Security Checklist

- [ ] All exposed credentials have been rotated
- [ ] .env file is in .gitignore
- [ ] Environment variables are set in deployment environment
- [ ] DATABASE_URL uses strong password (20+ chars, mixed case, numbers, symbols)
- [ ] Redis uses strong password
- [ ] API keys have been regenerated
- [ ] SSL/TLS verification is enabled (verify=True)
- [ ] Application starts without warnings about missing credentials
- [ ] Database connections use proper timeouts
- [ ] Redis uses keepalive settings
- [ ] No `.env` files are committed in git history

## Testing the Security Fixes

### Test 1: Verify No Hardcoded Credentials
```bash
# Should find no credentials in Python files
grep -r "admin1234" app/
grep -r "postgresql.*trader" app/
# Should return nothing
```

### Test 2: Verify Environment Variable Loading
```python
python -c "
import os
os.environ['DATABASE_URL'] = 'test_url'
from app.db import DATABASE_URL
print(f'DATABASE_URL: {DATABASE_URL}')
assert DATABASE_URL == 'test_url', 'Not loading from env'
print('✓ Environment variables working')
"
```

### Test 3: Verify SSL Verification
```python
# Check that requests use verify=True
grep -n "requests.get" app/binance/coins_with_liquidity.py
grep -n "requests.get" app/symbol_filter.py
# Should show verify=True in both
```

### Test 4: Verify JSON Error Handling
```python
# Run with invalid JSON
python -c "
from app.binance.ws.ws_engine import on_message
import json

# Should not crash with invalid JSON
on_message(None, 'invalid json {')
print('✓ JSON error handling working')
"
```

## Monitoring After Fix

### Log for These Warnings
- Missing API credentials warning
- Redis connection failures
- Database connection timeouts
- JSON parsing errors

### Monitor These Metrics
- Database connection pool usage
- Redis connection status
- WebSocket connection stability
- API request success rate

## Next Steps for Hardening

1. **Add Request Signing**
   - Implement HMAC signatures for API calls
   - Validate webhook signatures

2. **Implement Rate Limiting**
   - Add rate limiting to API endpoints
   - Exponential backoff for retries

3. **Add Audit Logging**
   - Log all credential access
   - Track configuration changes

4. **Setup Monitoring**
   - Alert on connection failures
   - Track unusual activity

5. **Regular Audits**
   - Weekly security scans
   - Monthly penetration testing
   - Quarterly dependency updates

## Support

If you encounter issues:

1. Check that all environment variables are set:
   ```bash
   env | grep -E "DATABASE_URL|REDIS|API_"
   ```

2. Test database connection:
   ```bash
   psql "$DATABASE_URL" -c "SELECT NOW();"
   ```

3. Test Redis connection:
   ```bash
   redis-cli -h $REDIS_HOST -p $REDIS_PORT ping
   ```

4. Check application logs for errors related to connections

## References

- [OWASP: Secrets Management](https://owasp.org/www-community/attacks/Sensitive_Data_Exposure)
- [12 Factor App: Config](https://12factor.net/config)
- [Python Security Guide](https://python.readthedocs.io/en/latest/library/security_warnings.html)
