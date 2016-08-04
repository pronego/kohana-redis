<?php defined('SYSPATH') or die('No direct script access.');

/**
 * Redis cache driver with tagging support.
 *
 * @package Redis
 * @author Mutant Industries ltd. <mutant-industries@hotmail.com>
 */
class Kohana_Cache_Redis extends Cache implements Cache_Tagging, Cache_Arithmetic, Cache_GarbageCollect {

    const FIELD_DATA = 'd';
    const FIELD_MTIME = 'm';
    const FIELD_TAGS = 't';
    const COMPRESS_PREFIX = ":\x1f\x8b";

    /**
     * @var Redis_Client
     */
    protected $_client;

    /**
     * @param  array $config
     * @throws Cache_Exception
     */
    public function __construct(array $config)
    {
        parent::__construct($config);

        try
        {
            $this->_client = Redis_Client::instance($config['connection']);
        }
        catch (Redis_Exception $e)
        {
            throw new Cache_Exception('Unable to instantiate redis client: ' . $e->getMessage(), null, 0, $e);
        }

        isset($this->_config['key_namespace']) || $this->_config['key_namespace'] = '';
        isset($this->_config['tag_namespace']) || $this->_config['tag_namespace'] = '';
        isset($this->_config['compress_data']) || $this->_config['compress_data'] = 1;

        if($this->_config['compress_data'])
        {
            isset($this->_config['compress_threshold']) || $this->_config['compress_threshold'] = 20480;

            if ( ! isset($this->_config['compression_lib']))
            {
                if (function_exists('snappy_compress'))
                {
                    $this->_config['compression_lib'] = 'snappy';
                }
                else if (function_exists('lzf_compress'))
                {
                    $this->_config['compression_lib'] = 'lzf';
                }
                else if (function_exists('gzcompress'))
                {
                    $this->_config['compression_lib'] = 'gzip';
                }
                else
                {
                    throw new Cache_Exception('No compression library found.');
                }
            }
        }
    }

    /**
     * Set a value to cache with id and lifetime
     *
     * @param   string $id id of cache entry
     * @param   string $data data to set to cache
     * @param   integer $lifetime lifetime in seconds
     * @return  boolean
     * @throws  Cache_Exception
     */
    public function set($id, $data, $lifetime = 3600)
    {
        return $this->set_with_tags($id, $data, $lifetime);
    }

    /**
     * Retrieve a cached value entry by id.
     *
     * @param   string $id id of cache to entry
     * @param   string $default default value to return if cache miss
     * @return  mixed
     * @throws  Cache_Exception
     */
    public function get($id, $default = NULL)
    {
        try
        {
            $data = $this->_client->hGet($this->_config['key_namespace'] . $id, Cache_Redis::FIELD_DATA);

            return $data === FALSE ? $default : $this->_decode_data($data);
        }
        catch (CredisException $e)
        {
            throw new Cache_Exception('Failed to get redis cached entry "' . $id . '": ' . $e->getMessage(), null, 0, $e);
        }
    }

    /**
     * Set a value based on an id. Optionally add tags.
     *
     * @param   string $id        id
     * @param   mixed $data       data
     * @param   integer $lifetime lifetime [Optional]
     * @param   array $tags       tags [Optional]
     * @return  bool
     * @throws  Cache_Exception
     */
    public function set_with_tags($id, $data, $lifetime = null, array $tags = array())
    {
        $set_script_args = array();

        $set_script_args[] = Cache_Redis::FIELD_DATA;
        $set_script_args[] = $this->_encode_data($data, $this->_config['compress_data']);
        $set_script_args[] = Cache_Redis::FIELD_MTIME;
        $set_script_args[] = time();
        $set_script_args[] = $lifetime;
        $set_script_args[] = Cache_Redis::FIELD_TAGS;
        $set_script_args[] = $this->_config['tag_namespace'];

        foreach ($tags as $one_tag)
        {
            $set_script_args[] = $this->_sanitize_tag($one_tag);
        }

        try
        {
            $this->_client->execute(new Redis_Script_Composite('scripts' . DIRECTORY_SEPARATOR . 'cache', 'set'),
                $this->_config['key_namespace'] . $id, $set_script_args);

            return TRUE;
        }
        catch (Redis_Exception $e)
        {
            throw new Cache_Exception('Failed to set redis cached entry "' . $id . '": ' . $e->getMessage(), null, 0, $e);
        }
    }

    /**
     * Delete a cache entry based on id
     *
     * @param   string $id id to remove from cache
     * @return  bool false when deleting non-existing record
     * @throws  Cache_Exception
     */
    public function delete($id)
    {
        try
        {
            return (bool) $this->_client->execute(new Redis_Script_Composite('scripts' . DIRECTORY_SEPARATOR . 'cache', 'delete'),
                $this->_config['key_namespace'] . $id, array(Cache_Redis::FIELD_TAGS, $this->_config['tag_namespace']));
        }
        catch (Redis_Exception $e)
        {
            throw new Cache_Exception('Failed to delete cached entry "' . $id . '": ' . $e->getMessage(), null, 0, $e);
        }
    }

    /**
     * Find cache entries based on a tag
     *
     * @param   string $tag tag
     * @return  array
     * @throws  Cache_Exception
     */
    public function find($tag)
    {
        try
        {
            $result = $this->_client->execute(new Redis_Script_Composite('scripts' . DIRECTORY_SEPARATOR . 'cache' . DIRECTORY_SEPARATOR . 'tag', 'get'),
                array(), array($this->_sanitize_tag($tag), $this->_config['tag_namespace'], Cache_Redis::FIELD_DATA));
        }
        catch (Redis_Exception $e)
        {
            throw new Cache_Exception('Failed to find cached entries by tag "' . $tag . '": ' . $e->getMessage(), null, 0, $e);
        }

        return array_map(array($this, '_decode_data'), $result);
    }

    /**
     * Delete cache entries based on a tag
     *
     * @param   string $tag tag
     * @throws  Cache_Exception
     */
    public function delete_tag($tag)
    {
        try
        {
            $this->_client->execute(new Redis_Script_Composite('scripts' . DIRECTORY_SEPARATOR . 'cache' . DIRECTORY_SEPARATOR . 'tag', 'delete'),
                array(), array($this->_sanitize_tag($tag), $this->_config['tag_namespace'], Cache_Redis::FIELD_TAGS));
        }
        catch (Redis_Exception $e)
        {
            throw new Cache_Exception('Failed to delete cached entries by tag "' . $tag . '": ' . $e->getMessage(), null, 0, $e);
        }
    }

    /**
     * Increments a given value by the step value supplied.
     *
     * @param   string $id      of cache entry to increment
     * @param   int|float $step value to increment by
     * @return  bool|int New value on success, false when key does not exist / NaN
     * @throws  Cache_Exception
     */
    public function increment($id, $step = 1)
    {
        try
        {
            $result = $this->_client->execute(new Redis_Script_Composite('scripts' . DIRECTORY_SEPARATOR . 'cache', 'increment'),
                $this->_config['key_namespace'] . $id, array($step, Cache_Redis::FIELD_DATA));
        }
        catch (Redis_Exception $e)
        {
            throw new Cache_Exception('Failed to increment entry "' . $id . '" by ' . $step . ': ' . $e->getMessage(), null, 0, $e);
        }

        return $result !== FALSE ? $result + 0 : FALSE;
    }

    /**
     * Decrements a given value by the step value supplied.
     *
     * @param   string    $id of cache entry to decrement
     * @param   int|float $step value to decrement by
     * @return  int|bool       New value on success, false when key does not exist / NaN
     * @throws  Cache_Exception
     */
    public function decrement($id, $step = 1)
    {
        return $this->increment($id, -$step);
    }

    /**
     * Wipe out current redis db. Consider using separate databases for cache and sessions if this method is ever going
     * to be used.
     *
     * @return bool
     * @throws Cache_Exception
     */
    public function delete_all()
    {
        try
        {
            return $this->_client->flushDb();
        }
        catch (CredisException $e)
        {
            throw new Cache_Exception('Failed to execute flushdb: ' . $e->getMessage(), null, 0, $e);
        }
    }

    /**
     * Cleanup of tag sets to contain only existing keys - use only if tagged keys might expire. However expired keys
     * are also removed on every {@link Cache_Redis#find($tag)} call, so this method might be just unnecessary overkill.
     * Anyway this should be used with care due to the fact, that redis is running in a single thread.
     *
     * @throws  Cache_Exception
     */
    public function garbage_collect()
    {
        try
        {
            $this->_client->execute(new Redis_Script_Composite('scripts' . DIRECTORY_SEPARATOR . 'cache', 'garbage_collect'),
                array(), array($this->_config['tag_namespace']));
        }
        catch (Redis_Exception $e)
        {
            throw new Cache_Exception('Failed to run garbage collect script: ' . $e->getMessage(), null, 0, $e);
        }

    }

    // -----------------------------------------------------------------------------------------------------------------
    /**
     * @param   string $data
     * @param   bool|int $level
     * @return  string
     * @throws  Cache_Exception
     */
    protected function _encode_data($data, $level)
    {
        // numbers are not encoded to enable (in|de)crement using hincrbyfloat
        if (is_numeric($data))
        {
            return $data;
        }

        $data = serialize($data);

        if ($level && strlen($data) >= $this->_config['compress_threshold'])
        {
            switch ($this->_config['compression_lib'])
            {
                case 'gzip': $data = gzcompress($data, $level);
                    break;
                case 'snappy': $data = snappy_compress($data);
                    break;
                case 'lzf': $data = lzf_compress($data);
                    break;
            }

            if ( ! $data)
            {
                throw new Cache_Exception("Could not compress cache data.");
            }

            return substr($this->_config['compression_lib'], 0, 2) . Cache_Redis::COMPRESS_PREFIX . $data;
        }

        return $data;
    }

    /**
     * @param   string $data
     * @return  string|int|float
     */
    protected function _decode_data($data)
    {
        if (is_numeric($data))
        {
            return $data + 0;
        }

        if (substr($data, 2, 3) === Cache_Redis::COMPRESS_PREFIX)
        {
            switch (substr($data, 0, 2))
            {
                case 'gz':
                    $data = gzuncompress(substr($data, 5));
                    break;
                case 'sn':
                    $data = snappy_uncompress(substr($data, 5));
                    break;
                case 'lz':
                    $data = lzf_decompress(substr($data, 5));
                    break;
            }
        }

        return unserialize($data);
    }

    /**
     * @param   string $tag
     * @return  string
     */
    protected function _sanitize_tag($tag)
    {
        return str_replace(array(','), '_', $tag);
    }

}
