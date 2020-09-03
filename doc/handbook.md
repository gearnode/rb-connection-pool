# Introduction
This document explain how to use the `rb-connection-pool` library.

# Usage

Create a new connection pool with:
```ruby
$pg = RbConnectionPool.new_pool(size: 10, timeout: 0.5) do
    PG.connect(dbname: 'demo')
end
```

Then use the pool with:
```ruby
$pg.with do |conn|
    conn.exec("SELECT 1;")
end
```

When no objects are available in the pool, the `with` method block until one
becomes available. If no object is available within the timeout, the method
will raise `TimeoutError` exception.

You can change the duration of the timeout with:
```ruby
$pg.with(timeout: 2) do |conn|
    conn.exec("SELECT 1;")
end
```

You can shutdown the pool with:
```ruby
$pg.shutdown do |conn|
    conn.close
end
```
