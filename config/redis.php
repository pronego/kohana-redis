<?php defined('SYSPATH') or die('No direct script access.');

return array(
    /*
     * Hostname of full connection string, e.g.
     *  tcp://host[:port][/persistence_identifier]
     *  unix:///path/to/redis.sock
     */
    'host' => '127.0.0.1',
    /*
     * Port, ignored if host points to a unix socket
     */
    'port' => 6379,
    /*
     * Connection password
     */
//    'password' => '',
    /*
     * Default database to be selected after connection
     */
//    'database' => 0,
    /*
     * Connection timeout [seconds]
     *
     * default: 2.5 seconds
     */
//    'timeout' => 2.5,
    /*
     * If TRUE, the connection will not be closed on close or end of request until the php process ends.
     *
     * default: FALSE
     */
//    'persistent' => TRUE,
    /*
     * Avoid using of phpredis, use php socket connection instead
     *
     * default: FALSE
     */
//    'force_standalone' => TRUE,
    /*
     * Connection max retries count
     *
     * default: 0
     */
//    'connect_retries' => 0,
    /*
     * Read timeout [ms]
     */
    'read_timeout' => 20,
    /*
     * Configurable default folder containing lua scripts, can be also dynamically passed
     * as a last parameter of Redis_Client::execute
     */
//    'scripts_path' => 'redis/scripts',
);
