require_once "_common"
require_once "../functions/is_empty"
require_once "../functions/string/split"

-- Delete @key, delete if from all tags in @tag_namespace it belongs to.
-- Tags, that will no longer exist after this operation, shall be also removed from @tag_namespace .. "__TAG_INDEX__".
--
-- parameters:
-- - KEYS[1] key
-- - ARGV[1] field_tags
-- - ARGV[2] tag_namespace

-- return int 0, if @key doesn't exist, 1 otherwise
-- -----------------------------------------------------------------------------

local key = KEYS[1]
local field_tags = ARGV[1]
local tag_namespace = ARGV[2] or ''

assert(not is_empty(key), "key cannot be empty")
assert(not is_empty(field_tags), "field_tags cannot be empty")

-- -----------------------------------------------------------------------------

local tags = redis.call("hget", key, field_tags)

if tags then
    for _, tag in pairs(tags:split(tags_delimiter)) do
        if redis.call("srem", tag_namespace .. tag, key) == 1 and redis.call("exists", tag_namespace .. tag) == 0 then
            redis.call("srem", tag_namespace .. tag_index_key, tag)
        end
    end
end

return redis.call("del", key)
