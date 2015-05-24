<?php defined('SYSPATH') or die('No direct script access.');

/**
 *
 * @author Mutant Industries ltd. <mutant-industries@hotmail.com>
 */
return array
    (
    'redis' => array(
        'driver' => 'redis',
        'notMatchingTags' => TRUE,
        'compress_tags' => 0,
        'compress_data' => 0,
        'compression_lib' => 'gzip',
//        'compress_threshold' => 20480,
    )
);
