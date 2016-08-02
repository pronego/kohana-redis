--
-- parameters:
-- - KEYS[1] key
-- - ARGV[1] field_data
-- - ARGV[2] data
-- - ARGV[3] field_mtime
-- - ARGV[4] mtime
-- - ARGV[5] ttl
-- - ARGV[6] field_tags
-- - ARGV[7] tag_prefix
-- - ... followed by tags
-- -----------------------------------------------------------------------------

local function is_empty(s)
    return s == nil or s == ""
end

function string:split(sep)
    local sep, fields = sep or ":", {}
    local pattern = string.format("([^%s]+)", sep)
    self:gsub(pattern, function(c) fields[#fields + 1] = c end)

    return fields
end

-- -----------------------------------------------------------------------------

local key = KEYS[1]
local field_data = ARGV[1]
local data = ARGV[2]
local field_mtime = ARGV[3]
local mtime = tonumber(ARGV[4])
local ttl = ARGV[5]
local field_tags = ARGV[6]
local tag_prefix = ARGV[7] or ''

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

local tags_delimiter = ","
local tag_index_key = "__TAG_INDEX__"

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

if not is_empty(ttl) then
    redis.call("expire", key, ttl)
else
    -- remove expiration time of a key
    redis.call("persist", key)
end

for tag, _ in pairs(tags) do
    if not old_tags[tag] then
        redis.call("sadd", tag_prefix .. tag, key)
        redis.call("sadd", tag_prefix .. tag_index_key, tag)
    end
end

for tag, _ in pairs(old_tags) do
    if not tags[tag] then
        if redis.call("srem", tag_prefix .. tag, key) == 1 and redis.call("exists", tag_prefix .. tag) == 0 then
            redis.call("srem", tag_prefix .. tag_index_key, tag)
        end
    end
end

return redis.status_reply("OK")
