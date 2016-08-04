require_once "_common"
require_once "../functions/is_empty"
require_once "../functions/string/split"

-- Set cache record {@field_data = @data, @field_mtime = @mtime} to a @key, set it's expiration time (@ttl) if not empty.
-- If tags are specified, they are stored to @field_tags (without namespace, separated by ","), the @key is stored to
-- sets @tag_namespace .. @tag, tags (without namespace) are also stored to @tag_namespace .. "__TAG_INDEX__".
-- If the record already exists and belongs to some tags, only difference in tags is applied.
--
-- parameters:
-- - KEYS[1] key
-- - ARGV[1] field_data
-- - ARGV[2] data
-- - ARGV[3] field_mtime
-- - ARGV[4] mtime
-- - ARGV[5] ttl
-- - ARGV[6] field_tags
-- - ARGV[7] tag_namespace
-- - ... followed by any number of tags
-- -----------------------------------------------------------------------------

local key = KEYS[1]
local field_data = ARGV[1]
local data = ARGV[2]
local field_mtime = ARGV[3]
local mtime = tonumber(ARGV[4])
local ttl = ARGV[5]
local field_tags = ARGV[6]
local tag_namespace = ARGV[7] or ''

local tags = {}
local tags_str = {}

for i = 8, #ARGV do
    tags[ARGV[i]] = true -- just for hash-table based lookup
    table.insert(tags_str, ARGV[i])
end

assert(not is_empty(key), "key cannot be empty")
assert(not is_empty(field_data), "field_data cannot be empty")
assert(not is_empty(data), "data cannot be empty")
assert(not is_empty(field_mtime), "field_mtime cannot be empty")
assert(not is_empty(mtime) and mtime > 0, "mtime must be a positive number")

if not is_empty(ttl) then
    ttl = tonumber(ttl)

    assert(not is_empty(ttl) and ttl > 0, "if set, ttl must be a positive number")
end

assert(not is_empty(field_tags), "field_tags cannot be empty")

-- -----------------------------------------------------------------------------

local old_tags_raw = redis.call("hget", key, field_tags)
local old_tags = {}

if old_tags_raw then
    for _, tag in pairs(old_tags_raw:split(tags_delimiter)) do
        old_tags[tag] = true
    end
end

local hmset_args = { key, field_data, data, field_mtime, mtime }

if table.getn(tags_str) > 0 then
    table.insert(hmset_args, field_tags)
    table.insert(hmset_args, table.concat(tags_str, tags_delimiter))
end

redis.call("hmset", unpack(hmset_args))

if is_empty(ttl) then
    -- remove expiration time of a key - just in case when the key already exists and it's expiration is set
    redis.call("persist", key)
else
    redis.call("expire", key, ttl)
end

for tag, _ in pairs(tags) do
    if not old_tags[tag] then
        redis.call("sadd", tag_namespace .. tag, key)
        redis.call("sadd", tag_namespace .. tag_index_key, tag)
    end
end

for tag, _ in pairs(old_tags) do
    if not tags[tag] then
        if redis.call("srem", tag_namespace .. tag, key) == 1 and redis.call("exists", tag_namespace .. tag) == 0 then
            redis.call("srem", tag_namespace .. tag_index_key, tag)
        end
    end
end

return redis.status_reply("OK")
