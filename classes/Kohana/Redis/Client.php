<?php defined('SYSPATH') or die('No direct script access.');

/**
 * @package Redis
 * @author Mutant Industries ltd. <mutant-industries@hotmail.com>
 */
class Kohana_Redis_Client extends Credis_Client {

    const SCRIPT_EXT = "lua";

    /**
     * @var Redis_Client[]
     */
    protected static $_instances = array();

    /**
     * @var string
     */
    protected $_scripts_path;

    /**
     * Singleton client per each configuration group
     *
     * @param string $config_group
     * @return Redis_Client
     * @throws Redis_Exception
     */
    public static function instance($config_group = 'redis')
    {
        $config = Kohana::$config->load($config_group);

        $params = array(
            'host' => $config->get('host', '127.0.0.1'),
            'port' => $config->get('port', 6379),
            'password' => $config->get('password'),
            'database' => $config->get('database', 0),
            'timeout' => $config->get('timeout', 2.5),
            'connect_retries' => $config->get('connect_retries', 0),
            'read_timeout' => $config->get('read_timeout', -1),
            'persistent' => $config->get('persistent', FALSE),
            'force_standalone' => $config->get('force_standalone', FALSE),
        );

        if(preg_match('-^unix://.+-', $params['host']))
        {
            $params['port'] = NULL;
        }

        $params_json = json_encode($params);

        if ( ! isset(Redis_Client::$_instances[$params_json]))
        {
            $client = new Redis_Client($params['host'], $params['port'], $params['timeout'],
                $params['persistent'], $params['database'], $params['password']);

            if ($params['force_standalone'])
            {
                $client->forceStandalone();
            }
            if ($params['connect_retries'])
            {
                $client->setMaxConnectRetries($params['connect_retries']);
            }

            $client->setReadTimeout($params['read_timeout']);
            $client->scripts_path($config->get('scripts_path', 'redis/scripts'));

            $instance = Redis_Client::$_instances[$params_json] = $client;
        }
        else
        {
            $instance = Redis_Client::$_instances[$params_json];
        }

        try
        {
            // Always select database on startup in case persistent connection is re-used by other code
            $instance->select((int) $params['database']);
        }
        catch (CredisException $e)
        {
            throw new Redis_Exception('The redis database could not be selected: '.$e->getMessage(), NULL, 0, $e);
        }

        return $instance;
    }

    /**
     * Execute lua script
     *
     * @param string|Redis_Script   $script
     * @param mixed                 $keys
     * @param mixed                 $argv
     * @param string                $path_to_script
     *
     * @return mixed
     * @throws Redis_Exception on script execution error
     */
    public function execute($script, $keys = array(), $argv = array(), $path_to_script = NULL)
    {
        if ($path_to_script === NULL)
        {
            $path_to_script = $this->_scripts_path;
        }

        if ( ! is_array($keys))
        {
            $keys = array($keys);
        }
        if ( ! is_array($argv))
        {
            $argv = array($argv);
        }

        $redis_script = $script instanceof Redis_Script ? $script : $this->_get_script($path_to_script, $script);

        try
        {
            if (($result = $this->evalSha($redis_script->get_sha1(), $keys, $argv)) !== NULL)
            {
                return $result;
            }

            return $this->eval($redis_script->get_source(), $keys, $argv);
        }
        catch (CredisException $e)
        {
            throw new Redis_Exception('Error executing ' . $script . '.' . Redis_Script::SCRIPT_EXT . ': '
                . $e->getMessage(), null, 0, $e);
        }
    }

    public function is_standalone()
    {
        return $this->standalone;
    }

    /**
     * Scripts default path getter / setter
     *
     * @param string $path
     *
     * @return string
     */
    public function scripts_path($path = NULL)
    {
        if ($path === NULL)
        {
            return $this->_scripts_path;
        }

        $this->_scripts_path = $path;
    }

    // -----------------------------------------------------------------------------------------------------------------
    /**
     * Get script source / sha1 provider. There's no internal usage of this method, it's meant to be overridden, if the
     * default provider is insufficient.
     *
     * Please note, that this method is called only in case, when the first parameter of Redis_Client::execute
     * is string (custom implementation of Redis_Script can be passed instead).
     *
     * @param   $path_to_script
     * @param   $script_name
     * @return  Redis_Script
     */
    protected function _get_script($path_to_script, $script_name)
    {
        return new Redis_Script_Composite($path_to_script, $script_name, $this);
    }

}
