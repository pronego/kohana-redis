<?php defined('SYSPATH') or die('No direct script access.');

/**
 * Compose script containing 'require[_once]' keyword followed by string containing relative path to another script.
 *
 * Use scripts/lua-compose.pl to achieve the same thing via command line and print the result to STDOUT. To evaluate
 * the composite script use the redis-eval-composite.sh. To run redis lua debugger (redis-cli --ldb) the redis-debug.sh
 * helper script can be used. The eval / debug scripts expect the lua-compose.pl to be present in PATH as 'lua-compose'
 * command (without extension). This is the optional installation procedure:
 *
 * cd ${module_root}/scripts
 * sudo cp lua-compose.pl /usr/local/bin/lua-compose
 * sudo cp redis-debug.sh /usr/bin/redis-debug
 * sudo cp redis-eval-composite.sh /usr/bin/redis-eval-composite
 *
 * All these scripts expect the first argument to be existing lua script. The redis-eval-composite.sh and redis-debug.sh
 * will just proxy all subsequent arguments to redis-cli. Please note that regarding the debugger the script doesn't
 * keep track of any changed files and doesn't do any updates to the generated script, so the 'restart' command in
 * redis lua debugger will never update the script source.
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
    public function get_source()
    {
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
        $relative = preg_split('|/(?=[^/]+$)|', $matches['path']);

        if (count($relative) == 1)
        {
            $path = $this->_path;
            $script_name = $relative[0];
        }
        else
        {
            $path = preg_replace('|' . DIRECTORY_SEPARATOR . '$|', '', $this->_path) . DIRECTORY_SEPARATOR .
                (Kohana::$is_windows ? preg_replace('|/|', DIRECTORY_SEPARATOR, $relative[0]) : $relative[0]);
            $script_name = $relative[1];
        }

        $required_script = new Redis_Script_Composite($path, $script_name, $this->_client, $this->_already_included,
            strlen($matches['once']) > 0);

        return $required_script->get_source();
    }

}
