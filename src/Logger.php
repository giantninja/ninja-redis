<?php

// log wrapper class for use with logstash using json formatted log entries
class Logger {

    public static function log($log_type, array $data, $include_base_params = false, $log_file = null) {

        if (empty($log_type) || empty($data)) {
            error_log("Logger::log(): missing log_type or data");
            return;
        }

        if (!is_array($data)) {
            $data = ['message' => $data];
        }

        // include the usual $_SERVER and auth fields if requested (e.g the custom error handler logstash_error_handler)
        if ($include_base_params) {
            $data = array_merge($data, get_logstash_base_params());
        }

        static $server;
        if (!$server) {
            $hostname = trim(strtolower(gethostname()));
            list($server) = explode('.', $hostname);
        }
        $data['server'] = $server;


        $log_entry = 'PHP: ' . json_encode(array_merge(['log_type' => $log_type], $data));

        if ($log_file) {
            // add timestamp to log_entry
            $ts = date('M d H:i:s') . ' ' . $server . ' ';
            error_log($ts . $log_entry . "\n", 3, $log_file);
        } else {
            error_log($log_entry);
        }
    }
}

