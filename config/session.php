<?php defined('SYSPATH') or die('No direct script access.');

return array(
    'redis' => array(
        /*
         * Cookie entry name, under which session id is stored
         */
        'name' => 'session_id',
        /*
         * Session lifetime [seconds]
         * If set to zero, session will expire when the browser closes, however the session
         * won't be deleted from redis. The old sessions can be flushed like this:
         *
         * $namespace = Kohana::$config->load('session')->redis['session_key_namespace'];
         * $last_active_limit = time() - N;    // N is in seconds
         *
         * Redis_Client::instance()->execute("flush_old", array(), array(
         *  $namespace, $last_active_limit
         * ), 'scripts/session');
         *
         * This script should be used with care, because (depending on number of stored
         * sessions) it might block redis for some time.
         */
        'lifetime' => 3600 * 24 * 31,
        /*
         * Load session entries lazily
         *  - if set to FALSE, the whole content of the session is loaded when Session_Redis is
         * instantiated. Only changed variables are written back.
         *  - if TRUE, session variables are loaded on demand. Every write to the session will be
         * sent to redis on session write.
         */
        'lazy' => TRUE,
        /*
         * Namespace of the session keys ($session_key_namespace . $session_id)
         */
        'session_key_namespace' => 's:',
        /*
         * Redis connection config group
         */
        'connection' => 'redis',
    ),
);
