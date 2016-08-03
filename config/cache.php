<?php defined('SYSPATH') or die('No direct script access.');

return array(
    'redis' => array(
        'driver' => 'redis',
        /*
         * Keys namespace ($key_namespace . $cache_key)
         *
         * default: empty
         */
        'key_namespace' => 'k:',
        /*
         * Tags namespace ($tag_namespace.$tag_name)
         *
         * default: empty
         */
        'tag_namespace' => 't:',
        /*
         * boolean|int - compression on/off,
         * in case when compression_lib is gzip, this will be the second parameter to gzcompress (0-9)
         *
         * default: 1
         */
        'compress_data' => 0,
        /*
         * snappy|lzf|gzip
         *
         * default: gzip
         */
        'compression_lib' => 'gzip',
        /*
         * Cache entries larger than compress_threshold [b] will be compressed,
         * if compression is allowed
         *
         * default: 20480
         */
//        'compress_threshold' => 500,
        /*
         * Redis connection config group
         */
        'connection' => 'redis',
    )
);
