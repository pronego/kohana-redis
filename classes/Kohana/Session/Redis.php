<?php defined('SYSPATH') or die('No direct script access.');

/**
 * Redis hash session handler
 *
 * @package Redis
 * @author Mutant Industries ltd. <mutant-industries@hotmail.com>
 */
class Kohana_Session_Redis extends Session {

    /**
     * @var Redis_Client
     */
    protected $_client;

    /**
     * @var string
     */
    protected $_session_key_prefix;

    /**
     * @var String
     */
    protected $_session_id;

    /**
     * @var array
     */
    protected $_changed = array();

    /**
     * @var bool
     */
    protected $_loaded = FALSE;

    /**
     * @var bool
     */
    protected $_lazy;

    /**
     * @param array $config
     * @param string $id
     * @throws Session_Exception
     */
    public function __construct(array $config = NULL, $id = NULL)
    {
        $this->_lazy = $config['lazy'];
        $this->_session_key_prefix = $config['session_key_prefix'];

        try
        {
            $this->_client = Redis_Client::instance($config['connection']);
        }
        catch (Redis_Exception $e)
        {
            throw new Session_Exception('Unable to instantiate redis client: '.$e->getMessage(), NULL, 0, $e);
        }

        parent::__construct($config, $id);
    }

    /**
     * Session object is rendered to a serialized string. If encryption is
     * enabled, the session will be encrypted. If not, the output string will
     * be encoded.
     *
     *     echo $session;
     *
     * @return  string
     * @uses    Encrypt::encode
     */
    public function __toString()
    {
        $this->_loaded || $this->as_array();

        return parent::__toString();
    }

    /**
     * Returns the current session array. The returned array can also be
     * assigned by reference.
     *
     *     // Get a copy of the current session data
     *     $data = $session->as_array();
     *
     *     // Assign by reference for modification
     *     $data =& $session->as_array();
     *
     * @return  array
     */
    public function & as_array()
    {
        if ( ! $this->_loaded)
        {
            $data = $this->_client->hGetAll($this->_session_key_prefix.$this->_session_id);

            $this->_data = Arr::merge(Arr::map('unserialize', $data), $this->_changed);

            $this->_loaded = TRUE;
        }

        return $this->_data;
    }

    /**
     * Get the current session id, if the session supports it.
     *
     *     $id = $session->id();
     *
     * [!!] Not all session types have ids.
     *
     * @return  string
     * @since   3.0.8
     */
    public function id()
    {
        return $this->_session_id;
    }

    /**
     * Get a variable from the session array.
     *
     *     $foo = $session->get('foo');
     *
     * @param   string  $key        variable name
     * @param   mixed   $default    default value to return
     * @return  mixed
     */
    public function get($key, $default = NULL)
    {
        if (array_key_exists($key, $this->_data))
        {
            return $this->_data[$key] === NULL ? $default : $this->_data[$key];
        }
        if ($this->_loaded)
        {
            return $default;
        }
        if ( ! ($value = $this->_client->hGet($this->_session_key_prefix.$this->_session_id, $key)))
        {
            $this->_data[$key] = NULL;

            return $default;
        }

        return $this->_data[$key] = unserialize($value);
    }

    /**
     * Get and delete a variable from the session array.
     *
     *     $bar = $session->get_once('bar');
     *
     * @param   string  $key        variable name
     * @param   mixed   $default    default value to return
     * @return  mixed
     */
    public function get_once($key, $default = NULL)
    {
        $value = $this->get($key, $default);

        $this->_data[$key] = $this->_changed[$key] = NULL;

        return $value;
    }

    /**
     * Set a variable in the session array.
     *
     *     $session->set('foo', 'bar');
     *
     * @param   string  $key    variable name
     * @param   mixed   $value  value
     * @return  $this
     */
    public function set($key, $value)
    {
        if ( ! array_key_exists($key, $this->_data) || $this->_data[$key] !== $value)
        {
            $this->_changed[$key] = $value;
        }

        $this->_data[$key] = $value;

        return $this;
    }

    /**
     * Set a variable by reference.
     *
     *     $session->bind('foo', $foo);
     *
     * @param   string  $key    variable name
     * @param   mixed   $value  referenced value
     * @return  $this
     */
    public function bind($key, & $value)
    {
        $this->_data[$key] = & $value;
        $this->_changed[$key] = & $value;

        return $this;
    }

    /**
     * Removes a variable in the session array.
     *
     *     $session->delete('foo');
     *
     * @param   string  $key,...    variable name
     * @return  $this
     */
    public function delete($key)
    {
        $args = func_get_args();

        foreach ($args as $key)
        {
            $this->set($key, NULL);
        }

        return $this;
    }

    /**
     * Loads the raw session data string and returns it.
     *
     * @param   string $id session id
     * @return  mixed
     */
    protected function _read($id = NULL)
    {
        if ($id || $id = Cookie::get($this->_name))
        {
            if ($this->_client->exists($this->_session_key_prefix.$id))
            {
                // Set the current session id
                $this->_session_id = $id;

                // session found
                return $this->_lazy ? array() : $this->as_array();
            }
        }

        // Create a new session id
        $this->_regenerate();

        return NULL;
    }

    /**
     * Generate a new session id and return it.
     *
     * @return  string
     */
    protected function _regenerate()
    {
        do
        {
            // Create a new session id
            $id = substr_replace(substr_replace(str_replace('.', '', uniqid(NULL, TRUE)), ':', 4, 0), ':', 7, 0);
        }
        while ($this->_client->exists($this->_session_key_prefix.$id));

        return $this->_session_id = $id;
    }

    /**
     * Writes the current session.
     *
     * @return  boolean
     */
    protected function _write()
    {
        $this->_changed['last_active'] = time();

        $this->_client->pipeline()->multi();

        foreach ($this->_changed as $key => $value)
        {
            if ($value === NULL)
            {
                $this->_client->hDel($this->_session_key_prefix.$this->_session_id, $key);
            }
            else
            {
                $this->_client->hSet($this->_session_key_prefix.$this->_session_id, $key, serialize($value));
            }
        }

        if ($this->_lifetime > 0)
        {
            $this->_client->expire($this->_session_key_prefix.$this->_session_id, $this->_lifetime);
        }

        $this->_client->exec();

        // Update the cookie with the new session id
        Cookie::set($this->_name, $this->_session_id, $this->_lifetime);

        return TRUE;
    }

    /**
     * Destroys the current session.
     *
     * @return  boolean
     */
    protected function _destroy()
    {
        $this->_client->del($this->_session_key_prefix.$this->_session_id);

        Cookie::delete($this->_name);

        return TRUE;
    }

    /**
     * Restarts the current session.
     *
     * @return  boolean
     */
    protected function _restart()
    {
        $this->_regenerate();

        return TRUE;
    }

}
