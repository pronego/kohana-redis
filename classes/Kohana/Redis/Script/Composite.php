<?php defined('SYSPATH') or die('No direct script access.');

/**
 * Require / require_once support in lua scripts
 *
 * @package Redis
 * @author Mutant Industries ltd. <mutant-industries@hotmail.com>
 */
class Kohana_Redis_Script_Composite extends Redis_Script {

    protected static $_require_pattern = '|^\s*require(?P<once>_once)?\s+"(?P<path>[^"]+)"|m';

    /**
     * @var array
     */
    protected $_already_included;

    /**
     * @var bool
     */
    protected $_once;

    /**
     * @var string
     */
    protected $_source_cached;

    /**
     * @param $path
     * @param $script_name
     * @param Credis_Client $client
     * @param array $already_included
     * @param bool $once
     */
    public function __construct($path, $script_name, Credis_Client $client = NULL, & $already_included = array(), $once = FALSE)
    {
        parent::__construct($path, $script_name, $client);

        $this->_already_included = & $already_included;
        $this->_once = $once;
    }

    /**
     * @return string
     */
    public function get_source() {
        if ($this->_source_cached !== NULL)
        {
            return $this->_source_cached;
        }

        $source = parent::get_source();

        $md5 = md5($source);

        if ($this->_once && array_key_exists($md5, $this->_already_included))
        {
            return '';
        }

        $this->_already_included[$md5] = TRUE;

        return $this->_source_cached = preg_replace_callback(Redis_Script_Composite::$_require_pattern,
            array($this, '_require_replacement'), $source);
    }

    /**
     * @param  array $matches
     * @return string
     */
    protected function _require_replacement($matches)
    {
        $relative = preg_split('|' . DIRECTORY_SEPARATOR . '(?=[^' . DIRECTORY_SEPARATOR . ']+$)|', $matches['path']);

        if (count($relative) == 1)
        {
            $path = $this->_path;
            $script_name = $relative[0];
        }
        else
        {
            $path = preg_replace('|' . DIRECTORY_SEPARATOR . '$|', '', $this->_path) . DIRECTORY_SEPARATOR . $relative[0];
            $script_name = $relative[1];
        }

        $required_script = new Redis_Script_Composite($path, $script_name, $this->_client, $this->_already_included,
            strlen($matches['once']) > 0);

        return $required_script->get_source();
    }

}
