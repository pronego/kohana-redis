<?php defined('SYSPATH') or die('No direct script access.');

/**
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
     * @param array $config
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

        isset($this->_config['key_prefix']) || $this->_config['key_prefix'] = '';
        isset($this->_config['tag_prefix']) || $this->_config['tag_prefix'] = '';
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
            $data = $this->_client->hGet($this->_config['key_prefix'] . $id, self::FIELD_DATA);

            return $data === FALSE ? $default : $this->_decode_data($data);
        }
        catch (CredisException $e)
        {
            throw new Cache_Exception('Failed to get redis cached entry: ' . $e->getMessage(), null, 0, $e);
        }
    }

    /**
     * Save some datas into a cache record
     *
     * @param  string $data             Datas to cache
     * @param  string $id               Cache id
     * @param  array  $tags             Array of strings, the cache record will be tagged by each string entry
     * @param  int $lifetime            set a specific lifetime for this cache record (null => infinite lifetime)
     * @throws Cache_Exception
     * @return boolean True if no problem
     */
    public function set_with_tags($id, $data, $lifetime = null, array $tags = array())
    {
        $set_script_args = array();

        $set_script_args[] = self::FIELD_DATA;
        /*
         * numbers are not encoded to enable (in|de)crement using hincrby(float)
         */
        $set_script_args[] = $this->_encode_data($data, $this->_config['compress_data']);
        $set_script_args[] = self::FIELD_MTIME;
        $set_script_args[] = time();
        $set_script_args[] = $lifetime;
        $set_script_args[] = self::FIELD_TAGS;
        $set_script_args[] = $this->_config['tag_prefix'];

        foreach ($tags as $one_tag)
        {
            $set_script_args[] = $this->_sanitize_tag($one_tag);
        }

        try
        {
            $this->_client->execute("set", $this->_config['key_prefix'] . $id, $set_script_args,
                'scripts' . DIRECTORY_SEPARATOR . 'cache');

            return TRUE;
        }
        catch (Redis_Exception $e)
        {
            throw new Cache_Exception('Failed to set redis cached entry: ' . $e->getMessage(), null, 0, $e);
        }
    }

    /**
     * Delete a cache entry based on id
     *
     * @param   string $id id to remove from cache
     * @return  boolean
     */
    public function delete($id)
    {
        return (bool) $this->_client->execute("delete", $this->_config['key_prefix'] . $id, array(self::FIELD_TAGS,
            $this->_config['tag_prefix']), 'scripts' . DIRECTORY_SEPARATOR . 'cache');
    }

    /**
     * Find cache entries based on a tag
     *
     * @param   string $tag tag
     * @return  array
     */
    public function find($tag)
    {
        $result = array();

        foreach ($this->_client->execute("get", array(), array($this->_sanitize_tag($tag), $this->_config['tag_prefix'],
            self::FIELD_DATA), 'scripts' . DIRECTORY_SEPARATOR . 'cache' . DIRECTORY_SEPARATOR . 'tag') as $value)
        {
            $result[] = $this->_decode_data($value);
        }

        return $result;
    }

    /**
     * Delete cache entries based on a tag
     *
     * @param   string $tag tag
     */
    public function delete_tag($tag)
    {
        $this->_client->execute("delete", array(), array($this->_sanitize_tag($tag), $this->_config['tag_prefix'],
            self::FIELD_TAGS), 'scripts' . DIRECTORY_SEPARATOR . 'cache' . DIRECTORY_SEPARATOR . 'tag');
    }

    /**
     * Increments a given value by the step value supplied.
     * Useful for shared counters and other persistent integer based
     * tracking.
     *
     * @param   string    $id of cache entry to increment
     * @param   int|float $step value to increment by
     * @return  int|bool       New value on success, false when key does not exist / NaN
     */
    public function increment($id, $step = 1)
    {
        $result = $this->_client->execute("increment", $this->_config['key_prefix'] . $id,
            array($step, self::FIELD_DATA), 'scripts' . DIRECTORY_SEPARATOR . 'cache');

        return $result !== FALSE ? $result + 0 : FALSE;
    }

    /**
     * Decrements a given value by the step value supplied.
     * Useful for shared counters and other persistent integer based
     * tracking.
     *
     * @param   string    $id of cache entry to decrement
     * @param   int|float $step value to decrement by
     * @return  int       New value on success, false when key does not exist / NaN
     */
    public function decrement($id, $step = 1)
    {
        return $this->increment($id, -$step);
    }

    /**
     * Delete all cache entries.
     *
     * This should be used with care, while current redis db will be wiped out. Consider using dedicated databases
     * for caches and sessions.
     *
     * @return boolean True if no problem
     */
    public function delete_all()
    {
        return $this->_client->flushDb();
    }

    /**
     * Cleanup of tag sets to contain only existing keys - use only if keys with tags might expire.
     */
    public function garbage_collect()
    {
        $this->_client->execute("garbage_collect", array(), array($this->_config['tag_prefix']),
            'scripts' . DIRECTORY_SEPARATOR . 'cache');
    }

    // -----------------------------------------------------------------------------------------------------------------
    /**
     * @param string $data
     * @param int $level
     * @throws Cache_Exception
     * @return string
     */
    protected function _encode_data($data, $level)
    {
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

            return substr($this->_config['compression_lib'], 0, 2) . self::COMPRESS_PREFIX . $data;
        }

        return $data;
    }

    /**
     * @param string $data
     * @return string
     */
    protected function _decode_data($data)
    {
        if (is_numeric($data))
        {
            return $data;
        }

        if (substr($data, 2, 3) === self::COMPRESS_PREFIX)
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
     * @param string $tag
     * @return string
     */
    protected function _sanitize_tag($tag)
    {
        return str_replace(array(','), '_', $tag);
    }

}
