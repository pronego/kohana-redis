<?php defined('SYSPATH') or die('No direct script access.');

/*
  ==New BSD License==

  Copyright (c) 2013, Colin Mollenhour
  All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
 * The name of Colin Mollenhour may not be used to endorse or promote products
  derived from this software without specific prior written permission.
 * The class name must remain as Cm_Cache_Backend_Redis.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY
  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 *
 * @package Redis
 * @author Mutant Industries ltd. <mutant-industries@hotmail.com>
 * Based on Redis adapter for Zend_Cache by Colin Mollenhour (http://colin.mollenhour.com)
 */
class Kohana_Cache_Redis extends Cache implements Cache_Tagging/* , Cache_Arithmetic */, Cache_GarbageCollect {

    const SET_IDS = 'c:ids';
    const SET_TAGS = 'c:tags';
    const PREFIX_KEY = 'c:k:';
    const PREFIX_TAG_IDS = 'c:ti:';
    const FIELD_DATA = 'd';
    const FIELD_MTIME = 'm';
    const FIELD_TAGS = 't';
    const FIELD_INF = 'i';
    const COMPRESS_PREFIX = ":\x1f\x8b";

    /** @var Credis_Client */
    protected $_client;

    /** @var bool */
    protected $_notMatchingTags = TRUE;

    /** @var int */
    protected $_compressTags = 1;

    /** @var int */
    protected $_compressData = 1;

    /** @var int */
    protected $_compressThreshold = 20480;

    /** @var string */
    protected $_compressionLib;

    /**
     * @param array $config
     * @throws Cache_Exception
     * @return \Kohana_Cache_Redis
     */
    public function __construct($config = array())
    {
        try
        {
            $this->_client = Credis_Client::instance();
        }
        catch (Credis_Exception $e)
        {
            throw new Cache_Exception('Unable to instantiate redis client', null, 0, $e);
        }

        if (isset($config['notMatchingTags']))
        {
            $this->_notMatchingTags = (bool) $config['notMatchingTags'];
        }

        if (isset($config['compress_tags']))
        {
            $this->_compressTags = (int) $config['compress_tags'];
        }

        if (isset($config['compress_data']))
        {
            $this->_compressData = (int) $config['compress_data'];
        }

        if (isset($config['compress_threshold']))
        {
            $this->_compressThreshold = (int) $config['compress_threshold'];
        }

        if (isset($config['compression_lib']))
        {
            $this->_compressionLib = $config['compression_lib'];
        }
        else if (function_exists('snappy_compress'))
        {
            $this->_compressionLib = 'snappy';
        }
        else if (function_exists('lzf_compress'))
        {
            $this->_compressionLib = 'lzf';
        }
        else
        {
            $this->_compressionLib = 'gzip';
        }

        $this->_compressPrefix = substr($this->_compressionLib, 0, 2) . self::COMPRESS_PREFIX;
    }

    /**
     * Retrieve a cached value entry by id.
     *
     *     // Retrieve cache entry from default group
     *     $data = Cache::instance()->get('foo');
     *
     *     // Retrieve cache entry from default group and return 'bar' if miss
     *     $data = Cache::instance()->get('foo', 'bar');
     *
     *     // Retrieve cache entry from memcache group
     *     $data = Cache::instance('memcache')->get('foo');
     *
     * @param   string $id id of cache to entry
     * @param   string $default default value to return if cache miss
     * @return  mixed
     * @throws  Cache_Exception
     */
    public function get($id, $default = NULL)
    {
        $data = $this->_client->hGet(self::PREFIX_KEY . $id, self::FIELD_DATA);

        if ($data === NULL)
        {
            return $default;
        }

        return unserialize($this->_decode_data($data));
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
     * Save some datas into a cache record
     *
     * @param  string $data             Datas to cache
     * @param  string $id               Cache id
     * @param  array  $tags             Array of strings, the cache record will be tagged by each string entry
     * @param  bool|int $lifetime If != false, set a specific lifetime for this cache record (null => infinite lifetime)
     * @throws Cache_Exception
     * @return boolean True if no problem
     */
    public function set_with_tags($id, $data, $lifetime = NULL, array $tags = NULL)
    {
        if ( ! is_array($tags))
            $tags = $tags ? array($tags) : array();

        // Get list of tags previously assigned
        $oldTags = $this->_decode_data($this->_client->hGet(self::PREFIX_KEY . $id, self::FIELD_TAGS));
        $oldTags = $oldTags ? explode(',', $oldTags) : array();

        $this->_client->pipeline()->multi();

        // Set the data
        $result = $this->_client->hMSet(self::PREFIX_KEY . $id, array(
            self::FIELD_DATA => $this->_encode_data(serialize($data), $this->_compressData),
            self::FIELD_TAGS => $this->_encode_data(implode(',', $tags), $this->_compressTags),
            self::FIELD_MTIME => time(),
            self::FIELD_INF => $lifetime ? 0 : 1,
        ));

        if ( ! $result)
        {
            throw new Cache_Exception("Could not set cache key $id");
        }

        // Set expiration if specified
        if ($lifetime)
        {
            $this->_client->expire(self::PREFIX_KEY . $id, $lifetime);
        }

        // Process added tags
        if ($tags)
        {
            // Update the list with all the tags
            $this->_client->sAdd(self::SET_TAGS, $tags);

            // Update the id list for each tag
            foreach ($tags as $tag)
            {
                $this->_client->sAdd(self::PREFIX_TAG_IDS . $tag, $id);
            }
        }

        // Process removed tags
        if ($remTags = ($oldTags ? array_diff($oldTags, $tags) : FALSE))
        {
            // Update the id list for each tag
            foreach ($remTags as $tag)
            {
                $this->_client->sRem(self::PREFIX_TAG_IDS . $tag, $id);
            }
        }

        // Update the list with all the ids
        if ($this->_notMatchingTags)
        {
            $this->_client->sAdd(self::SET_IDS, $id);
        }

        $this->_client->exec();

        return TRUE;
    }

    /**
     * Delete cache entries based on a tag
     *
     * @param   string $tag tag
     */
    public function delete_tag($tag)
    {
        $this->_remove_by_matching_any_tags(array($tag));
    }

    /**
     * Find cache entries based on a tag
     *
     * @param   string $tag tag
     * @return  array
     */
    public function find($tag)
    {
        // TODO: Implement find() method or refactor
    }

    /**
     * Delete a cache entry based on id
     *
     *     // Delete 'foo' entry from the default group
     *     Cache::instance()->delete('foo');
     *
     *     // Delete 'foo' entry from the memcache group
     *     Cache::instance('memcache')->delete('foo')
     *
     * @param   string $id id to remove from cache
     * @return  boolean
     */
    public function delete($id)
    {
        // Get list of tags for this id
        $tags = explode(',', $this->_decode_data($this->_client->hGet(self::PREFIX_KEY . $id, self::FIELD_TAGS)));

        $this->_client->pipeline()->multi();

        // Remove data
        $this->_client->del(self::PREFIX_KEY . $id);

        // Remove id from list of all ids
        if ($this->_notMatchingTags)
        {
            $this->_client->sRem(self::SET_IDS, $id);
        }

        // Update the id list for each tag
        foreach ($tags as $tag)
        {
            $this->_client->sRem(self::PREFIX_TAG_IDS . $tag, $id);
        }

        $result = $this->_client->exec();

        return (bool) $result[0];
    }

    /**
     * Increments a given value by the step value supplied.
     * Useful for shared counters and other persistent integer based
     * tracking.
     *
     * @param   string    $id of cache entry to increment
     * @param   int       $step value to increment by
     * @return  integer
     * @return  int
     */
    public function increment($id, $step = 1)
    {
//        return $this->_client->incrBy($id, $step);
    }

    /**
     * Decrements a given value by the step value supplied.
     * Useful for shared counters and other persistent integer based
     * tracking.
     *
     * @param   string    $id of cache entry to decrement
     * @param   int       $step value to decrement by
     * @return  integer
     * @return  int
     */
    public function decrement($id, $step = 1)
    {
//        return $this->_client->decrBy($id, $step);
    }

    /**
     * Delete all cache entries.
     *
     * Beware of using this method when
     * using shared memory cache systems, as it will wipe every
     * entry within the system for all clients.
     *
     * @return boolean True if no problem
     */
    public function delete_all()
    {
        return $this->_client->flushDb();
    }

    /**
     * Garbage collection method that cleans any expired
     * cache entries from the cache.
     *
     * @return void
     */
    public function garbage_collect()
    {
        // Clean up expired keys from tag id set and global id set
        $exists = array();
        $tags = (array) $this->_client->sMembers(self::SET_TAGS);

        foreach ($tags as $tag)
        {
            // Get list of expired ids for each tag
            $tagMembers = $this->_client->sMembers(self::PREFIX_TAG_IDS . $tag);
            $numTagMembers = count($tagMembers);
            $expired = array();
            $numExpired = $numNotExpired = 0;

            if ($numTagMembers)
            {
                while ($id = array_pop($tagMembers))
                {
                    if (!isset($exists[$id]))
                    {
                        $exists[$id] = $this->_client->exists(self::PREFIX_KEY . $id);
                    }
                    if ($exists[$id])
                    {
                        $numNotExpired++;
                    }
                    else
                    {
                        $numExpired++;
                        $expired[] = $id;

                        // Remove incrementally to reduce memory usage
                        if (count($expired) % 100 == 0 && $numNotExpired > 0)
                        {
                            $this->_client->sRem(self::PREFIX_TAG_IDS . $tag, $expired);
                            if ($this->_notMatchingTags)
                            { // Clean up expired ids from ids set
                                $this->_client->sRem(self::SET_IDS, $expired);
                            }
                            $expired = array();
                        }
                    }
                }
                if ( ! count($expired))
                    continue;
            }

            // Remove empty tags or completely expired tags
            if ($numExpired == $numTagMembers)
            {
                $this->_client->del(self::PREFIX_TAG_IDS . $tag);
                $this->_client->sRem(self::SET_TAGS, $tag);
            }
            // Clean up expired ids from tag ids set
            else if (count($expired))
            {
                $this->_client->sRem(self::PREFIX_TAG_IDS . $tag, $expired);
                if ($this->_notMatchingTags)
                { // Clean up expired ids from ids set
                    $this->_client->sRem(self::SET_IDS, $expired);
                }
            }

            unset($expired);
        }

        // Clean up global list of ids for ids with no tag
        if ($this->_notMatchingTags)
        {
            // TODO
        }
    }

    /**
     * Test if a cache is available or not (for the given id)
     *
     * @param  string $id Cache id
     * @return bool|int False if record is not available or "last modified" timestamp of the available cache record
     */
    public function test($id)
    {
        $mtime = $this->_client->hGet(self::PREFIX_KEY . $id, self::FIELD_MTIME);
        return $mtime ? $mtime : FALSE;
    }

    /**
     * Return an array of stored cache ids
     *
     * @return array array of stored cache ids (string)
     */
    public function get_ids()
    {
        if ($this->_notMatchingTags)
        {
            return (array) $this->_client->sMembers(self::SET_IDS);
        }
        else
        {
            $keys = $this->_client->keys(self::PREFIX_KEY . '*');
            $prefixLen = strlen(self::PREFIX_KEY);

            foreach ($keys as $index => $key)
            {
                $keys[$index] = substr($key, $prefixLen);
            }

            return $keys;
        }
    }

    /**
     * Return an array of stored tags
     *
     * @return array array of stored tags (string)
     */
    public function get_tags()
    {
        return (array) $this->_client->sMembers(self::SET_TAGS);
    }

    /**
     * Return an array of stored cache ids which match given tags
     *
     * In case of multiple tags, a logical AND is made between tags
     *
     * @param array $tags array of tags
     * @return array array of matching cache ids (string)
     */
    public function get_ids_matching_tags($tags = array())
    {
        if ($tags)
        {
            return (array) $this->_client->sInter($this->_preprocess_tag_ids($tags));
        }

        return array();
    }

    /**
     * Return an array of stored cache ids which don't match given tags
     *
     * In case of multiple tags, a negated logical AND is made between tags
     *
     * @param array $tags array of tags
     * @throws Cache_Exception
     * @return array array of not matching cache ids (string)
     */
    public function get_ids_not_matching_tags($tags = array())
    {
        if ( ! $this->_notMatchingTags)
        {
            throw new Cache_Exception("notMatchingTags is currently disabled.");
        }
        if ($tags)
        {
            return (array) $this->_client->sDiff(self::SET_IDS, $this->_preprocess_tag_ids($tags));
        }

        return (array) $this->_client->sMembers(self::SET_IDS);
    }

    /**
     * Return an array of stored cache ids which match any given tags
     *
     * In case of multiple tags, a logical OR is made between tags
     *
     * @param array $tags array of tags
     * @return array array of any matching cache ids (string)
     */
    public function get_ids_matching_any_tags($tags = array())
    {
        if ($tags)
        {
            return (array) $this->_client->sUnion($this->_preprocess_tag_ids($tags));
        }

        return array();
    }

    /**
     * Return an array of metadatas for the given cache id
     *
     * The array must include these keys :
     * - expire : the expire timestamp
     * - tags : a string array of tags
     * - mtime : timestamp of last modification time
     *
     * @param string $id cache id
     * @return array array of metadatas (false if the cache id is not found)
     */
    public function get_metadata($id)
    {
        list($tags, $mtime, $inf) = $this->_client->hMGet(self::PREFIX_KEY . $id, array(self::FIELD_TAGS, self::FIELD_MTIME, self::FIELD_INF));

        if ( ! $mtime)
        {
            return FALSE;
        }

        $tags = explode(',', $this->_decode_data($tags));
        $expire = $inf === '1' ? FALSE : time() + $this->_client->ttl(self::PREFIX_KEY . $id);

        return array(
            'expire' => $expire,
            'tags' => $tags,
            'mtime' => $mtime,
        );
    }

    /**
     * Give (if possible) an extra lifetime to the given cache id
     *
     * @param string $id cache id
     * @param int $extraLifetime
     * @return boolean true if ok
     */
    public function touch($id, $extraLifetime)
    {
        list($inf) = $this->_client->hGet(self::PREFIX_KEY . $id, self::FIELD_INF);

        if ($inf === '0')
        {
            $expireAt = time() + $this->_client->ttl(self::PREFIX_KEY . $id) + $extraLifetime;

            return (bool) $this->_client->expireAt(self::PREFIX_KEY . $id, $expireAt);
        }

        return false;
    }

    /**
     * @param array $tags
     */
    protected function _remove_by_not_matching_tags($tags)
    {
        $ids = $this->get_ids_not_matching_tags($tags);

        if ($ids)
        {
            $this->_client->pipeline()->multi();

            // Remove data
            $this->_client->del($this->_preprocess_ids($ids));

            // Remove ids from list of all ids
            if ($this->_notMatchingTags)
            {
                $this->_client->sRem(self::SET_IDS, $ids);
            }

            $this->_client->exec();
        }
    }

    /**
     * @param array $tags
     */
    protected function _remove_by_matching_tags($tags)
    {
        $ids = $this->get_ids_matching_tags($tags);
        if ($ids)
        {
            $this->_client->pipeline()->multi();

            // Remove data
            $this->_client->del($this->_preprocess_ids($ids));

            // Remove ids from list of all ids
            if ($this->_notMatchingTags)
            {
                $this->_client->sRem(self::SET_IDS, $ids);
            }

            $this->_client->exec();
        }
    }

    /**
     * @param array $tags
     */
    protected function _remove_by_matching_any_tags($tags)
    {
        $ids = $this->get_ids_matching_any_tags($tags);

        $this->_client->pipeline()->multi();

        if ($ids)
        {
            // Remove data
            $this->_client->del($this->_preprocess_ids($ids));

            // Remove ids from list of all ids
            if ($this->_notMatchingTags)
            {
                $this->_client->sRem(self::SET_IDS, $ids);
            }
        }

        // Remove tag id lists
        $this->_client->del($this->_preprocess_tag_ids($tags));

        // Remove tags from list of tags
        $this->_client->sRem(self::SET_TAGS, $tags);

        $this->_client->exec();
    }

    /**
     * @param string $data
     * @param int $level
     * @throws Cache_Exception
     * @return string
     */
    protected function _encode_data($data, $level)
    {
        if ($level && strlen($data) >= $this->_compressThreshold)
        {
            switch ($this->_compressionLib)
            {
                case 'snappy': $data = snappy_compress($data);
                    break;
                case 'lzf': $data = lzf_compress($data);
                    break;
                case 'gzip': $data = gzcompress($data, $level);
                    break;
            }
            if ( ! $data)
            {
                throw new Cache_Exception("Could not compress cache data.");
            }

            return $this->_compressPrefix . $data;
        }

        return $data;
    }

    /**
     * @param bool|string $data
     * @return string
     */
    protected function _decode_data($data)
    {
        if (substr($data, 2, 3) == self::COMPRESS_PREFIX)
        {
            switch (substr($data, 0, 2))
            {
                case 'sn': return snappy_uncompress(substr($data, 5));
                case 'lz': return lzf_decompress(substr($data, 5));
                case 'gz': case 'zc': return gzuncompress(substr($data, 5));
            }
        }

        return $data;
    }

    /**
     * @param $item
     * @param $index
     * @param $prefix
     */
    protected function _preprocess(&$item, $index, $prefix)
    {
        $item = $prefix . $item;
    }

    /**
     * @param $ids
     * @return array
     */
    protected function _preprocess_ids($ids)
    {
        array_walk($ids, array($this, '_preprocess'), self::PREFIX_KEY);
        return $ids;
    }

    /**
     * @param $tags
     * @return array
     */
    protected function _preprocess_tag_ids($tags)
    {
        array_walk($tags, array($this, '_preprocess'), self::PREFIX_TAG_IDS);
        return $tags;
    }

}
