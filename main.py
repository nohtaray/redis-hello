import redis

pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
r = redis.Redis(connection_pool=pool)

r.set('position', 0.1)
pos = float(r.get('position'))

print(pos)
