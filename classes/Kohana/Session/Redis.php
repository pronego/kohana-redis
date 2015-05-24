<?php defined('SYSPATH') or die('No direct script access.');

/**
 * Redis hash session handler
 *
 * @package Redis
 * @author Mutant Industries ltd. <mutant-industries@hotmail.com>
 */
class Kohana_Session_Redis extends Session {

    const PREFIX_KEY = 's:k:';
    const LAST_ACTIVE_INDEX = 'last_active';

    /**
     * @var Credis_Client
     */
    protected $_client;

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
    protected $_loaded = false;

    public function __construct(array $config = NULL, $id = NULL)
    {
        try
        {
            $this->_client = Credis_Client::instance();
        }
        catch (Credis_Exception $e)
        {
            throw new Session_Exception('Unable to instantiate redis client', null, 0, $e);
        }

        parent::__construct($config, $id);

        if ( ! $this->_lifetime)
        {
            throw new Session_Exception("Redis session must have lifetime set");
        }
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
            $data = $this->_client->hGetAll(Session_Redis::PREFIX_KEY . $this->_session_id);

            $this->_data = Arr::merge(Arr::map('unserialize', $data), $this->_changed);

            $this->_loaded = true;
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
        if ( ! ($value = unserialize($this->_client->hGet(Session_Redis::PREFIX_KEY . $this->_session_id, $key))))
        {
            $this->_data[$key] = NULL;

            return $default;
        }

        return $this->_data[$key] = $value;
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
     * Sets the last_active timestamp and saves the session.
     *
     *     $session->write();
     *
     * [!!] Any errors that occur during session writing will be logged,
     * but not displayed, because sessions are written after output has
     * been sent.
     *
     * @return  boolean
     * @uses    Kohana::$log
     */
    public function write()
    {
        if ($this->_destroyed)
        {
            return FALSE;
        }

        // Set the last active timestamp
        $this->_data[Session_Redis::LAST_ACTIVE_INDEX] = $this->_changed[Session_Redis::LAST_ACTIVE_INDEX] = time();

        try
        {
            return $this->_write();
        }
        catch (Exception $e)
        {
            // Log & ignore all errors when a write fails
            Kohana::$log->add(Log::ERROR, Kohana_Exception::text($e))->write();

            return FALSE;
        }
    }

    /**
     * Loads the raw session data string and returns it.
     *
     * @param   string $id session id
     * @return  string
     */
    protected function _read($id = NULL)
    {
        if ($id || $id = Cookie::get($this->_name))
        {
            $result = $this->_client->hGet(Session_Redis::PREFIX_KEY . $id, Session_Redis::LAST_ACTIVE_INDEX);

            if ($result)
            {
                // Set the current session id
                $this->_session_id = $id;

                // session found
                return array(Session_Redis::LAST_ACTIVE_INDEX => $result);
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
            $id = str_replace('.', '-', uniqid(NULL, TRUE));
        }
        while ($this->_client->hGet(Session_Redis::PREFIX_KEY . $id, Session_Redis::LAST_ACTIVE_INDEX));

        return $this->_session_id = $id;
    }

    /**
     * Writes the current session.
     *
     * @return  boolean
     */
    protected function _write()
    {
        $this->_client->pipeline()->multi();

        foreach ($this->_changed as $key => $value)
        {
            if ($value === NULL)
            {
                $this->_client->hDel(Session_Redis::PREFIX_KEY . $this->_session_id, $key);
            }
            else
            {
                $this->_client->hSet(Session_Redis::PREFIX_KEY . $this->_session_id, $key, serialize($value));
            }
        }

        $this->_client->exec();

        $this->_client->expire(self::PREFIX_KEY . $this->_session_id, $this->_lifetime);

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
        $this->_client->del(Session_Redis::PREFIX_KEY . $this->_session_id);

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
