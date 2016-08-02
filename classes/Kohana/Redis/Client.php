<?php defined('SYSPATH') or die('No direct script access.');

/**
 * @package Redis
 * @author Mutant Industries ltd. <mutant-industries@hotmail.com>
 */
class Kohana_Redis_Client extends Credis_Client {

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
     * @param string $script_name
     * @param mixed $keys
     * @param mixed $argv
     * @param string $path_to_script
     * @return mixed
     *
     * @throws Kohana_Exception
     * @throws Redis_Exception on script execution error
     */
    public function execute($script_name, $keys = array(), $argv = array(), $path_to_script = NULL)
    {
        if ($path_to_script === NULL)
        {
            $path_to_script = $this->_scripts_path;
        }

        if (($file = Kohana::find_file($path_to_script, $script_name, 'lua')) === FALSE)
        {
            throw new Redis_Exception('Script ' . $path_to_script . DIRECTORY_SEPARATOR . $script_name . ' does not exist.');
        }

        if ( ! is_array($keys))
        {
            $keys = array($keys);
        }
        if ( ! is_array($argv))
        {
            $argv = array($argv);
        }

        $script_content = NULL;

        if (Kohana::$caching === FALSE || ($sha1 = Kohana::cache("file-content-sha1-".$file)) === NULL)
        {
            $sha1 = sha1($script_content = file_get_contents($file));
            Kohana::cache("file-content-sha1-".$file, $sha1);
        }

        try
        {
            if (($result = $this->evalSha($sha1, $keys, $argv)) !== NULL)
            {
                return $result;
            }

            if ($script_content === NULL)
            {
                $script_content = file_get_contents($file);
            }

            return $this->eval($script_content, $keys, $argv);
        }
        catch (CredisException $e)
        {
            throw new Redis_Exception('Error executing '.$script_name.': '.$e->getMessage(), NULL, 0, $e);
        }
    }

    public function is_standalone()
    {
        return $this->standalone;
    }

    /**
     * Scripts default path getter / setter
     *
     * @param null $path
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

}
