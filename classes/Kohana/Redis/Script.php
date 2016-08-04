<?php defined('SYSPATH') or die('No direct script access.');

/**
 * @package Redis
 * @author Mutant Industries ltd. <mutant-industries@hotmail.com>
 */
class Kohana_Redis_Script {

    const SCRIPT_EXT = "lua";
    const SHA1_REDIS_KEY = "__SCRIPT_SHA1_CACHED__";

    /**
     * @var array
     */
    protected static $_sha1_local_cache = array(
        "scripts/cache/set" => "7e0932db420e61eaaad6af2bc364dd1e1897aebc",
        "scripts/cache/increment" => "5439c31b8d154fb2fc039b4755449c892675612d",
        "scripts/cache/delete" => "2fc15db10e9a5e9bdf5aee52c8e54394f31691cd",
        "scripts/cache/garbage_collect" => "6ee79d37a40f078d308c03079d34f6e8fc7488fa",
        "scripts/cache/tag/get" => "7c93148e165a722fcd5a1c0edadf095b80e3d3b1",
        "scripts/cache/tag/delete" => "fe4de4da431483ea7f4771d69f59574bdf87ff46"
    );

    /**
     * @var string
     */
    protected $_path;

    /**
     * @var string
     */
    protected $_script_name;

    /**
     * @var Credis_Client
     */
    protected $_client;

    /**
     * @var bool
     */
    protected $_use_caching;

    /**
     * @param $path
     * @param $script_name
     * @param Credis_Client $client If not set, then the sha1 is expected to be hard-coded to $_sha1_local_cache
     */
    public function __construct($path, $script_name, Credis_Client $client = NULL)
    {
        $this->_path = $path;
        $this->_script_name = $script_name;
        $this->_client = $client;
        $this->_use_caching = Kohana::$caching === TRUE;
    }

    /**
     * @return string
     * @throws Redis_Exception
     */
    public function get_source()
    {
        if (($file = Kohana::find_file($this->_path, $this->_script_name, Redis_Script::SCRIPT_EXT)) === FALSE)
        {
            throw new Redis_Exception('File ' . $this->_path . DIRECTORY_SEPARATOR . $this->_script_name . '.'
                . Redis_Script::SCRIPT_EXT . ' not found in filesystem.');
        }

        return file_get_contents($file);
    }

    /**
     * @return string
     * @throws Redis_Exception
     */
    public function get_sha1()
    {
        return $this->_use_caching ? $this->_get_sha1_cached() : sha1($this->get_source());
    }

    /**
     * @return string
     */
    public function __toString()
    {
        return '[' . get_class($this) . '] ' . $this->_path . DIRECTORY_SEPARATOR . $this->_script_name;
    }

    /**
     * Getting sha1 of a lua script needs to be super-fast, since it precedes every script evaluation in redis,
     * therefore redis itself if preferred for caching those sha1 to some file-based caches. However it still means,
     * that a single "hget" command once precedes each script evaluation.
     *
     * Please note, that the sha1 cache has to be flushed after every update of the scripts, that were already executed:
     *      Redis_Client::instance()->del(Redis_Script::SHA1_REDIS_KEY)
     *
     * The sha1 of all built-in scripts used by cache driver are hard-coded to avoid the unnecessary round-trip.
     *
     * @return string
     * @throws Redis_Exception
     */
    protected function _get_sha1_cached()
    {
        $cache_key = $this->_path . DIRECTORY_SEPARATOR . $this->_script_name;

        if (isset(Redis_Script::$_sha1_local_cache[$cache_key]))
        {
            return Redis_Script::$_sha1_local_cache[$cache_key];
        }

        if ($this->_client === NULL)
        {
            throw new Redis_Exception('Script hash caching not possible, because redis client not set.');
        }

        try
        {
            if ($sha1 = $this->_client->hGet(Redis_Script::SHA1_REDIS_KEY, $cache_key))
            {
                return Redis_Script::$_sha1_local_cache[$cache_key] = $sha1;
            }

            $sha1 = sha1($this->get_source());
            $this->_client->hSet(Redis_Script::SHA1_REDIS_KEY, $cache_key, $sha1);

            return Redis_Script::$_sha1_local_cache[$cache_key] = $sha1;
        }
        catch (CredisException $e)
        {
            throw new Redis_Exception('Error getting cached script hash for ' . $cache_key . '.' . Redis_Script::SCRIPT_EXT
                . ': ' . $e->getMessage(), null, 0, $e);
        }
    }

}
