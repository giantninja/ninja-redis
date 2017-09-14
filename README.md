# redis-sentinel
Wrapper class for phpredis (PECL) based php client, with added support for redis sentinel.

All normal phpredis client methods that aren't overloaded and normally available through PHPs `Redis` obj
should work transparently, while making use of a redis sentinel master/slave setup.

Writes will use the master reported by sentinel, and reads can be set to use
either only the slaves, or randomly picked master/slave instance, or master only.

## examples
Get Redis client set to the current master/slaves as reported via sentinel.

```php
$redis = NinjaRedis::singleton();

$redis->set('string-key1', 'ninja redis!!');

$result = $redis->get('string-key1');
// $result will be "ninja redis!!"

$var = [
    'name' => "giantninja",
    'age'  => "none of you're business",
];

$redis->hmset('hash-key1', $var);

// optionally also set expire/ttl
// $redis->hmset('hash-key1', $var, (60 * 60 * 24)); // 1 day

$result = $redis->hmget('hash-key1');
print_r($result); // prints the $var array

```

