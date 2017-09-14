<?php

require_once 'Logger.php';
require_once 'NinjaSentinel.php';

/**
 * Wrapper for PECL phpredis client adding support for redis sentinel
 *
 * redis sentinel is used to query for up to date master/slaves
 * using phpredis sentinel clients (SentinelClient)
 */
class NinjaRedis extends NinjaSentinel
{
    // our class instance (NinjaRedis) for singleton pattern
    protected static $_instance = null;

    // our redis client obj for pubsub
    protected $_pubsub = null;



    // protected constructor so we have to get object via static singleton() method
    // i.e. $redis = NinjaRedis::singleton();
    protected function __construct()
    {
        // setup and configure our clients based on data from sentinel
        $this->_setup();
    }

    public static function singleton()
    {
        if (self::$_instance == null) {
            self::$_instance = new self;
        }

        return self::$_instance;
    }

    /**
     * Setup redis client for current master
     *
     * Setup our phpredis client object for the current master
     * as resported via redis sentinel servers which monitor master/slave
     * and handle promoting/fallover slave->new master etc...
     */
    public function _setup()
    {
        try {
            // add the connection details for our pool of sentinels
            $this->_addSentinels($this->_config['sentinels']);

            // get the current master and available slaves info from sentinel
            $this->_sentinelConfigureServers();

        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }
    }

    // hash map get by key.
    public function hmget($key, $force_master = false)
    {
        try {
            $mode = (($force_master === true) ? 'master' : 'read');
            if ($redis = $this->_getClient($mode)) {
                if ($res = $redis->hGetAll($key)) {
                    $res['redis_server'] = $redis->getHost();
                    return $res;
                }
            } else {
                throw new Exception("_getClient() failed");
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    // hash map set, for setting assoc array (field => val, field => val) to a key
    // redis handles hash maps of up to 100 field/vals very efficiently
    public function hmset($key, array $val, $expire = 0)
    {
        try {
            if ($redis = $this->_getClient('master')) {
                if (empty($key)) {
                    throw new Exception("key empty/missing");
                }
                if (empty($val)) {
                    throw new Exception("val empty/missing");
                }

                $expire = intval($expire);
                if ($expire) {
                    // use multi/transaction if we're setting an expire as well
                    $multi     = $redis->multi();
                    $res       = $multi->hMSet($key,$val)->expire($key,$expire)->exec();
                    $multi_res = $this->_chkMutiResult($res);

                    if ($multi_res) {
                        return true;
                    } else {
                        $this->_logger(
                            'Ninjaredis_hmset_failed',
                            'redis->multi() failed in Ninjaredis hmset', [
                                'key'    => $key,
                                'val'    => $val,
                                'expire' => $expire,
                                'multi_cmd' => '$multi->hMSet($key,$val)->expire($key,$expire)->exec()',
                                'result_from_redis' => $res
                            ]
                        );
                    }
                } else {

                    $res = $redis->hMSet($key, $val);
                    if ($res) {
                        return true;
                    } else {
                        $this->_logger(
                            'Ninjaredis_hmset_failed',
                            'redis->hMSet() failed in Ninjaredis hmset', [
                                'key'    => $key,
                                'val'    => $val,
                                'expire' => $expire,
                                'result_from_redis' => $res
                            ]
                        );
                    }
                }
            } else {
                throw new Exception("_getClient() failed");
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e, [
                'key'    => $key,
                'val'    => $val,
                'expire' => $expire,
            ]);
        }

        return false;
    }

    // generic get which will check type and call proper get (hmget vs get etc)
    public function get($key, $force_master = false)
    {
        try {
            $mode = (($force_master === true) ? 'master' : 'read');
            if ($redis = $this->_getClient($mode)) {
                // if $key is an array of cache keys, we'll use mGet (multi get)
                if (is_array($key)) {
                    return $redis->mGet($key);
                }

                if ($type = $this->getTypeByKey($key)) {
                    $res = false;

                    switch ($type) {
                        case 'hash':
                            $res = $this->hmget($key, $force_master);
                            break;
                        case 'list':
                            $res = $this->lRangeAll($key, $force_master);
                            break;
                        case 'string':
                        default:
                            $res = $redis->get($key);
                            break;
                    }

                    return $res;
                }
            } else {
                throw new Exception("_getClient() failed");
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    // generic set which will check for arrays and call hmset or set pipeline
    public function set($key, $val, $flag = null, $expire = null)
    {
        try {
            if ($redis = $this->_getClient('master')) {
                if (is_array($val)) {
                    return $this->hmset($key, $val, $expire);
                }

                $expire = intval($expire);
                if ($expire) {
                    // use multi/transaction if we're setting an expire as well
                    $multi     = $redis->multi();
                    $res       = $multi->set($key,$val)->expire($key,$expire)->exec();
                    $multi_res = $this->_chkMutiResult($res);

                    if ($multi_res) {
                        return true;
                    } else {
                        $this->_logger(
                            'Ninjaredis_set_failed',
                            'multi set failed in Ninjaredis', [
                                'key'    => $key,
                                'val'    => $val,
                                'expire' => $expire,
                                'result_from_redis' => $res
                            ]
                        );
                    }
                } else {

                    $res = $redis->set($key, $val);
                    if ($res) {
                        return true;
                    } else {
                        $this->_logger(
                            'Ninjaredis_set_failed',
                            'redis->set() failed in Ninjaredis hmset', [
                                'key'    => $key,
                                'val'    => $val,
                                'expire' => $expire,
                                'result_from_redis' => $res
                            ]
                        );
                    }
                }
            } else {
                throw new Exception("_getClient() failed");
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    // wrapper method to get all elements stored in $key for a redis list
    public function lRangeAll($key, $force_master = false)
    {
        try {
            $mode = (($force_master === true) ? 'master' : 'read');
            if ($redis = $this->_getClient($mode)) {
                // lRange(key, start, end)... 0 to last element (-1)
                if ($res = $redis->lRange($key, 0, -1)) {
                    return $res;
                }
            } else {
                throw new Exception("_getClient() failed");
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    // push $val onto front of stack/list and optionally set expire and trim to $list_max in one transaction/call via multi()
    public function lPushEx($key, $val, $expire, $list_max = null)
    {
        try {
            if ($redis = $this->_getClient('master')) {
                $expire   = intval($expire);
                $list_max = intval($list_max);

                // Redis::PIPELINE block is simply transmitted faster to the server, but without any guarantee of atomicity
                // $multi = $redis->multi(Redis::PIPELINE);

                // Redis::MULTI block of commands runs as a single transaction (default)
                $multi = $redis->multi(Redis::MULTI)
                    ->lPush($key, $val)
                    ->expire($key, $expire);

                // trim the list to the optional $list_max length if provided
                if ($list_max) {
                    $multi->lTrim($key, 0, ($list_max - 1));
                }

                $res       = $multi->exec(); // array of responses for each cmd in multi/transaction
                $multi_res = $this->_chkMutiResult($res);

                if ($multi_res) {
                    return true;
                } else {
                    $this->_logger(
                        'Ninjaredis_lpushex_failed',
                        'multi lpushex failed in Ninjaredis', [
                            'key'      => $key,
                            'val'      => $val,
                            'expire'   => $expire,
                            'list_max' => $list_max,
                            'result_from_redis' => $res
                        ]
                    );
                }
            } else {
                throw new Exception("_getClient() failed");
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    public function exists($key)
    {
        try {
            return $redis->exists($key);
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    public function delete($key)
    {
        try {
            if ($redis = $this->_getClient('master')) {
                return $redis->del($key);
            } else {
                throw new Exception("_getClient() failed");
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    public function isHash($cache_key)
    {
        try {
            if ($type = $this->getTypeByKey($cache_key)) {
                return ($type == 'hash' ? true : false);
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    /**
     * Redis data types returned from type() method
     *
     * Redis::REDIS_STRING - String
     * Redis::REDIS_SET - Set
     * Redis::REDIS_LIST - List
     * Redis::REDIS_ZSET - Sorted set
     * Redis::REDIS_HASH - Hash
     * Redis::REDIS_NOT_FOUND - Not found / other
     */
    public function getTypeByKey($key)
    {
        try {
            if (($redis = $this->_getClient('master')) && $redis->exists($key)) {
                if ($type = $redis->type($key)) {
                    $type_string = null;

                    switch ($type) {
                        case 'hash':
                        case 'Hash':
                        case Redis::REDIS_HASH:
                            $type_string = 'hash';
                            break;
                        case 'string':
                        case 'String':
                        case Redis::REDIS_STRING:
                            $type_string = 'string';
                            break;
                        case 'list':
                        case 'List':
                        case Redis::REDIS_LIST:
                            $type_string = 'list';
                            break;
                        case 'set':
                        case 'Set':
                        case Redis::REDIS_SET:
                            $type_string = 'set';
                            break;
                        default:
                            $type_string = $type;
                            break;
                    }

                    return $type_string;
                }
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    public function getMulti(array $keys, $get_flags = null)
    {
        return $this->get($keys, null, $get_flags);
    }

    public function increment($key, $offset = 1, $init = 0, $expire = 0)
    {
        try {
            if ($redis = $this->_getClient('master')) {
                $incr    = intval($offset);
                $init    = intval($init);
                $expire  = intval($expire);

                // set to the value $init if we have an initial value passed in
                if ($init) {
                    $redis->set($key, $init);
                }

                $inc_res = $redis->incrBy($key, $incr);

                if ($expire) {
                    $exp_res = $redis->expire($key, $expire);
                } else {
                    $exp_res = true;
                }

                return ($inc_res && $exp_res);

            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    // same as set, but if key exists, then operation should fail
    public function add($key, $val, $flag = null, $expire = null)
    {
        try {
            if (($redis = $this->_getClient('master')) && $redis->exists($key)) {
                return $this->set($key, $val, $flag, $expire);
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    // test pubsub client to see if it's connected/good to go before using for api calls
    // since we're setting a specific server for pubsub api calls
    public function chkPubSub()
    {
        try {
            if ($client = $this->_getPubSubClient()) {
                return true;
            } else {
                throw new Exception("_getPubSubClient() failed");
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    // get Redis() client for our current master from details sentinel gave us
    protected function _getPubSubClient()
    {
        // if we have a client already connected, just return it
        if ($this->_chkClientConnected($this->_pubsub)) {
            return $this->_pubsub;
        } else {
            $server = $this->_config['pubsub'];
            $client = new Redis();

            if ($client->connect($server['ip'], $server['port'], $this->_config['timeout'])) {
                $this->_log('NinjaRedis::_getPubSubClient() connect success:', $server);

                $this->_configureRedisClient($client);
                $this->_pubsub = $client;

                return $this->_pubsub;
            } else {
                $msg = "redis connect to pubsub server failed";
                $this->_log_exception(__METHOD__, new RedisException($msg));
            }
        }

        return false;
    }

    // wrapping pubSub() method since prefix messes with it, so use rawCommand instead
    public function pubSub($cmd, $pattern = '')
    {
        $this->_log("NinjaRedis pubSub wrapper called for cmd '{$cmd}' with pattern: '{$pattern}' ");
        try {
            if ($client = $this->_getPubSubClient()) {
                return $client->rawCommand('PUBSUB', $cmd, $pattern);
            } else {
                throw new Exception("_getPubSubClient() failed");
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    // wrapping publish() method since prefix messes with it, so use rawCommand instead
    public function publish($channel, $data)
    {
        $this->_log("NinjaRedis publish wrapper called for channel '{$channel}' with data:", $data);
        try {
            if ($client = $this->_getPubSubClient()) {
                return $client->rawCommand('PUBLISH', $channel, $data);
            } else {
                throw new Exception("_getPubSubClient() failed");
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    // close connections for pubsub Redis() client on class destruction
    public function __destruct()
    {
        try {
            // need to call parent class' __destruct() method if we override here
            parent::__destruct();

            // close pubsub client on destruct if needed
            if ($this->_chkRedisClient($this->_pubsub)) {
                $this->_pubsub->close();
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }
    }

}

