<?php

require_once 'SentinelClient.php';

/**
 * Base class for NinjaRedis to extend with Sentinel functionality
 */
class NinjaSentinel
{
    // redis client configuration options
    protected $_config = [
        'master_only' => false, // false will allow reads from slaves, true will use master for everything
        'master_name' => 'redismaster',
        'password'    => '',
        'database'    => 0,
        'sentinels'   => [],
        'pubsub'      => [],
        'key_prefix'  => 'web:',
        'timeout'     => 2.5, // redis client connect timeout (float in seconds)
        'read_timeout'=> 3.0, // redis client read timeout (float in seconds)

        // redis sentinel instances to query for current master and available slaves (redis instances)
        'sentinels'   => [
            ['host' => '10.0.0.100', 'port' => 26379],
            ['host' => '10.0.0.101', 'port' => 26379],
            ['host' => '10.0.0.102', 'port' => 26379],
        ],

        // separate pubsub redis instance (optional)
        'pubsub'      => ['ip' => '10.0.0.102', 'port' => 6379,]
    ];

    // connection details (ip/port) for available redis instances returned from sentinel
    protected $_servers = [
        'master' => null,
        'slaves' => [],
    ];

    // our connected redis client objs for sentinel, master and slave
    protected $_clients = [
        'sentinel' => null,
        'master'   => null,
        'slave'    => null, // randomly picked slave
    ];

    // phpredis based client objects for each sentinel server instance
    protected $_sentinel_clients = [];

    // which sentinel was used to get host/port of current master
    protected $_from_sentinel = null;

    // to log, or not to log
    protected $_debug_logging = false;



    protected function _addSentinels($sentinels = [])
    {
        $this->_sentinel_clients = [];
        $servers = $sentinels ?: $this->_config['sentinels'];

        foreach ((array) $servers as $sentinel) {
            $this->_addSentinel($sentinel['host'], $sentinel['port']);
        }
    }

    protected function _addSentinel($host, $port)
    {
        $this->_sentinel_clients[] = new SentinelClient([
            'host'    => $host,
            'port'    => $port,
            'auth'    => $this->_config['password'],
        ]);
    }

    // get ip/port info from sentinel for our current master and available slaves
    protected function _sentinelConfigureServers()
    {
        try {
            // get details for current master's ip/host/port etc
            if ($master = $this->_getMasterFromSentinel()) {
                $this->_servers['master'] = $master;
            }

            // add/update server details for our slaves ip/host/port etc
            if (empty($this->_updateSlavesFromSentinel())) {
                throw new RedisException('No valid slaves found');
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }
    }

    // shuffle and loop through our available sentinel clients
    // and return the first client we successfully connect/auth
    protected function _getSentinelClient()
    {
        try {
            // if we're already connected to a sentinel, use that one
            $client = $this->_clients['sentinel'];
            if ( ($client instanceof SentinelClient) && $client->connect() ) {
                return $client;
            }

            // shuffle array of sentinel clients so we don't just try the first one,
            // return connected sentinel client or boolean false
            $sentinels = $this->_sentinel_clients;
            shuffle($sentinels);

            foreach ($sentinels as $sentinel) {
                if (($sentinel instanceof SentinelClient) && $sentinel->connect()) {
                    // add sentinel that returned current master details
                    $this->_from_sentinel = $sentinel->getHost();

                    $this->_clients['sentinel'] = $sentinel;
                    return $this->_clients['sentinel'];
                }
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    // query pool of sentinels and get ip/port of current master
    protected function _getMasterFromSentinel()
    {
        try {
            if ($sentinel = $this->_getSentinelClient()) {
                $master = $sentinel->getMasterAddrByName($this->_config['master_name']);
                $this->_log("master returned from sentinel", $master);

                return $master;
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    // query pool of sentinels and get ips/ports of our available slaves
    protected function _updateSlavesFromSentinel()
    {
        $slaves = [];
        try {
            if ($sentinel = $this->_getSentinelClient()) {
                // get our details for our slaves ip/host/port etc
                if ($servers = $sentinel->slaves($this->_config['master_name'])) {
                    $this->_log("slaves returned from sentinel", $servers);

                    foreach ($servers as $slave) {
                        // only add slaves to array of available slaves if it isn't down
                        if (strpos($slave['flags'], '_down') === false) {
                            $slaves[] = $slave;
                        }
                    }

                    // set class var with our slave server details, if we have valid slaves from sentinel
                    if ($slaves) {
                        $this->_log("updating _servers[slaves] array details after checking flags", $slaves);
                        $this->_servers['slaves'] = $slaves;
                    }
                }
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return $slaves;
    }

    // get Redis() client based on a mode (master,slave,read,write etc)
    protected function _getClient($mode = 'master')
    {
        try {
            switch ($mode) {
                case 'read':
                    // either use master if master_only config flag is set
                    // or 50/50 split read from random slave or master
                    if ($this->_config['master_only']) {
                        return $this->_getMasterClient();
                    } else {
                        return ((mt_rand(1,100) <= 50)
                            ? $this->_getMasterClient()
                            : $this->_getSlaveClient()
                        );
                    }
                case 'slave':
                    return $this->_getSlaveClient();
                    break;
                case 'master':
                case 'write':
                default:
                    return $this->_getMasterClient();
                    break;
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }

        return false;
    }

    // get Redis() client for our current master from details sentinel gave us
    protected function _getMasterClient()
    {
        // if we have a client connected to master, just return that one
        if ($this->_chkClientConnected($this->_clients['master'])) {
            return $this->_clients['master'];
        } else {
            $master = new Redis();
            $server = &$this->_servers['master'];

            if ($master->connect($server['ip'], $server['port'], $this->_config['timeout'])) {
                $this->_log('NinjaSentinel::_getMasterClient() connect success:', $server);

                $this->_configureRedisClient($master);
                $this->_clients['master'] = $master;

                return $this->_clients['master'];
            } else {
                $msg = "NinjaSentinel::_getMasterClient() connect failed";
                throw new RedisException($msg);
            }
        }

        return false;
    }

    // get Redis() client for our current master from details sentinel gave us
    protected function _getSlaveClient()
    {
        // if we have a client connected to master, just return that one
        if ($this->_chkClientConnected($this->_clients['slave'])) {
            return $this->_clients['slave'];
        } else {
            $slaves = ($this->_servers['slaves'] ?: $this->_updateSlavesFromSentinel());

            if (!check_foreach($slaves)) {
                $msg = "NinjaSentinel::_getSlaveClient() no slaves available";
                throw new RedisException($msg);
            }

            // shuffle available slaves and loop through and return first one we connect to successfully
            shuffle($slaves);
            foreach ($slaves as $server) {
                $slave = new Redis();

                if ($slave->connect($server['ip'], $server['port'], $this->_config['timeout'])) {
                    $this->_log('NinjaSentinel::_getSlaveClient() connect success:', $server);

                    $this->_configureRedisClient($slave);
                    $this->_clients['slave'] = $slave;

                    return $this->_clients['slave'];
                } else {
                    // log exception instead of throwing, so we can continue to the next slave in the foreach
                    $msg = "NinjaSentinel::_getSlaveClient() connect failed";
                    $this->_log_exception(__METHOD__, new RedisException($msg));
                }
            }
        }

        return false;
    }

    // pass in a reference to a redis client and configure/auth based on _config settings
    protected function _configureRedisClient(&$client)
    {
        if ($this->_chkRedisClient($client)) {

            if ($this->_config['password']) {
                if ($auth_res = $client->auth($this->_config['password'])) {
                    $this->_log('NinjaSentinel::_configureRedisClient() auth cmd success:', $auth_res);
                } else {
                    throw new RedisException("redis auth on client failed");
                }
            }

            // set custom prefix for all cache keys
            $prefix = $this->_config['key_prefix'];
            if ($client->setOption(Redis::OPT_PREFIX, $prefix)) {
                $this->_log("setting prefix logic passed", $prefix);
            }

            // set read timeout option if config has one set
            if ($this->_config['read_timeout']) {
                if ($client->setOption(Redis::OPT_READ_TIMEOUT, $this->_config['read_timeout'])) {
                    $this->_log("setting read timeout logic passed", $this->_config['read_timeout']);
                }
            }

            // not sure if we should use igbinary serialize/unserialize
            // $serializer = $this->_config['serializer'] ?: false;
            // if ($serializer && $this->_master->setOption(Redis::OPT_SERIALIZER, $serializer)) {
            //     $this->_log("setting serializer logic passed. serializer:", $serializer);
            // }
        }
    }

    protected function _chkRedisClient($obj)
    {
        // phpredis PECL extension (compiled C extension, faster)
        if (!is_null($obj) && is_object($obj) && $obj instanceof Redis) {
            return true;
        }

        return false;
    }

    protected function _chkClientConnected($obj)
    {
        if ($this->_chkRedisClient($obj) && $obj->isConnected() && $obj->getAuth()) {
            return true;
        }

        return false;
    }

    // get the ip/host of the sentinel that returned our connection details for current master
    public function getFromSentinel()
    {
        return $this->_from_sentinel;
    }

    // loop through response array for multi/transactions calls
    // and return false if any of the cmd responses are not truthy
    protected function _chkMutiResult($multi_response)
    {
        if (check_foreach($multi_response)) {
            foreach($multi_response as $cmd_res) {
                if (!$cmd_res) {
                    return false;
                }
            }

            return true;
        }

        return false;
    }

    protected function _logger($type, $msg, $data = [], $trace = null, $base_params = true)
    {
        $input = ['message' => $msg];

        if (!empty($data)) {
            $input['data'] = $data;
        }
        if ($trace) {
            $input['trace'] = $this->_get_simple_trace($trace);
        }

        Logger::log($type, $input, $base_params);
    }

    protected function _log_exception($method, $e, $vars = [])
    {
        $exception_class = get_class($e);
        // $this->_log("{$method} - {$exception_class}: ", $e->getMessage(), $e->getTrace());

        $input = ['method/exception_class' => "{$method} - {$exception_class}"];
        if (!empty($vars)) {
            $input['vars'] = $vars;
        }

        $instance_class = strtolower(get_parent_class($this));
        $this->_logger("{$instance_class}:exception_caught", $e->getMessage(), $input, $e->getTrace());
    }

    protected function _log($msg, $var = null, $trace = null)
    {
        // either change the class var _debug_logging to true or add NINJA_REDIS_DEBUG_LOGGING constant
        if (!(NINJA_REDIS_DEBUG_LOGGING === true || $this->_debug_logging === true)) {
            return;
        }

        error_log("{$msg} - " . print_r($var, true) . PHP_EOL);
        if ($trace) {
            error_log("Trace - " . print_r($this->_get_simple_trace($trace), true) . PHP_EOL);
        }
    }

    // get a useful/shorter stack trace that doesn't blow up logs with every class var etc...
    protected function _get_simple_trace($trace_array) {
        $output = [];
        if (!empty($trace_array) && is_array($trace_array)) {
            foreach ($trace_array as $call) {
                $output[] = [
                    'file'     => $call['file'],
                    'line'     => $call['line'],
                    'function' => $call['function'],
                    'class'    => $call['class'],
                    // 'args'     => $call['args'],
                ];
            }
        }
        return $output;
    }

    // allow calling of methods that are callable via phpredis client
    // without having to overload each method available
    public function __call($name, $args)
    {
        $this->_log("NinjaSentinel __call() triggered for name: {$name} with args:", $args);
        try {
            if ($master = $this->_getClient('master')) {
                if (is_callable([$master, $name])) {
                    $this->_log("NinjaSentinel __call() is_callable passed for name: {$name}");

                    $res = call_user_func_array([$master, $name], $args);
                    $this->_log("NinjaSentinel __call() result for name: {$name}", $res);

                    return $res;
                } else {
                    $msg = "method '{$name}' is not callable.";
                    throw new BadMethodCallException($msg);
                }
            } else {
                $msg = "NinjaSentinel::__call() failed getting master client";
                throw new RedisException($msg);
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }
    }

    // close connections for any Redis() clients on class destruction
    public function __destruct()
    {
        try {
            foreach ($this->_clients as $client) {
                if ($this->_chkRedisClient($client)) {
                    $client->close();
                }
            }
        } catch (Exception $e) {
            $this->_log_exception(__METHOD__, $e);
        }
    }

}

