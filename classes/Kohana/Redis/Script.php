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
        "scripts/cache/set" => "c8a48bf95e7fa1bd68821f08158ff7e2117c8cf2",
        "scripts/cache/increment" => "200d89408672673c23eb9f2ed62f00a3d8dfbb24",
        "scripts/cache/delete" => "9b9c1fbf01748ce312a7452a03c82d776a804c40",
        "scripts/cache/garbage_collect" => "65d170f90fefb210666e99ebe6ef72afe194344e",
        "scripts/cache/tag/get" => "5e5a8519a377604595b056a0133390896dfaffcf",
        "scripts/cache/tag/delete" => "1e948c30397d7ede937d0d9206b28aecf8d0ca88"
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
     * Kohana_Redis_Script constructor.
     *
     * @param $path
     * @param $script_name
     * @param Credis_Client $client
     */
    public function __construct($path, $script_name, Credis_Client $client)
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
