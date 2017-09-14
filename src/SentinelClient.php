<?php

/**
 * Wrapper for phpredis client connected to a redis sentinel instance
 *
 * Use this class to connect to a redis sentinel server instance
 * and query for details such as the current master or perform other
 * sentinel specific commands like saving configuration
 * or switching masters/slaves/conf files etc...
 */
class SentinelClient
{
    private $_redis;

    private $_host = null;
    private $_port = null;
    private $_auth = null;
    private $_timeout = 1.5; // separate timeout for sentinel clients


    public function __construct($conf = [])
    {
        if ($conf['host']) {
            $this->_host = $conf['host'];
        }
        if ($conf['port']) {
            $this->_port = $conf['port'];
        }
        if ($conf['auth']) {
            $this->_auth = $conf['auth'];
        }
        if ($conf['timeout']) {
            $this->_timeout = $conf['timeout'];
        }

        $this->_redis = new Redis();
    }

    public function __destruct()
    {
        try {
            $this->_redis->close();
        } catch (Exception $e) {
        }
    }

    public function connect()
    {
        try {
            // if we're connected and authorized, we're good to go
            if ($this->_redis->isConnected() && $this->_redis->getAuth()) {
                return true;
            } else {
                if ($this->_redis->connect($this->_host, $this->_port, $this->_timeout)) {
                    // authorize redis client if we've got an auth password set
                    if ($this->_auth) {
                        $this->_redis->auth($this->_auth);
                    }

                    if ($this->_redis->isConnected() && $this->_redis->getAuth()) {
                        return true;
                    }
                } else {
                    // failed/timed out
                }
            }
        } catch (RedisException $e) {
            // log?
        } catch (Exception $e) {
            // log?
        }

        return false;
    }

    public function ping()
    {
        return $this->_redis->ping();
    }

    public function getHost()
    {
        return $this->_host;
    }

    public function masters()
    {
        if ($this->connect()) {
            return $this->_parseRawCmdResponse($this->_redis->rawCommand('SENTINEL', 'masters'));
        }

        return [];
    }

    public function master($master_name)
    {
        if ($this->connect()) {
            return $this->_parseRawCmdResponse($this->_redis->rawCommand('SENTINEL', 'master', $master_name));
        }

        return [];
    }

    public function slaves($master_name)
    {
        if ($this->connect()) {
            return $this->_parseRawCmdResponse($this->_redis->rawCommand('SENTINEL', 'slaves', $master_name));
        }

        return [];
    }

    public function sentinels($master_name)
    {
        if ($this->connect()) {
            return $this->_parseRawCmdResponse($this->_redis->rawCommand('SENTINEL', 'sentinels', $master_name));
        }

        return [];
    }

    // redis sentinel command used to get the current master
    public function getMasterAddrByName($master_name)
    {
        if ($this->connect()) {
            $data = $this->_redis->rawCommand('SENTINEL', 'get-master-addr-by-name', $master_name);

            return [
                'ip'   => $data[0],
                'port' => $data[1],
            ];
        }

        return [];
    }

    public function reset($pattern)
    {
        if ($this->connect()) {
            return $this->_redis->rawCommand('SENTINEL', 'reset', $pattern);
        }

        return [];
    }

    public function failOver($master_name)
    {
        if ($this->connect()) {
            return $this->_redis->rawCommand('SENTINEL', 'failover', $master_name) === 'OK';
        }

        return [];
    }

    public function ckquorum($master_name)
    {
        if ($this->connect()) {
            return $this->checkQuorum($master_name);
        }

        return [];
    }

    public function checkQuorum($master_name)
    {
        if ($this->connect()) {
            return $this->_redis->rawCommand('SENTINEL', 'ckquorum', $master_name);
        }

        return [];
    }

    public function flushConfig()
    {
        if ($this->connect()) {
            return $this->_redis->rawCommand('SENTINEL', 'flushconfig');
        }

        return [];
    }

    public function getLastError()
    {
        return $this->_redis->getLastError();
    }

    public function clearLastError()
    {
        return $this->clearLastError();
    }

    private function _parseRawCmdResponse(array $data)
    {
        $result = [];
        $count  = count($data);

        for ($i = 0; $i < $count;) {
            $record = $data[$i];
            if (is_array($record)) {
                $result[] = $this->_parseRawCmdResponse($record);
                $i++;
            } else {
                $result[$record] = $data[$i + 1];
                $i += 2;
            }
        }

        return $result;
    }

}

