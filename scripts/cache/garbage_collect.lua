-- Run the cleanup of tag sets for a given @tag_namespace. All keys, that no longer exist, shall be removed from tag sets.
-- Tags, that will no longer exist after this operation, shall be also removed from @tag_namespace .. "__TAG_INDEX__".
--
-- - please note, that it only makes sense to run this script, when caching tagged items with limited lifetime.
--
-- parameters:
-- - ARGV[1] tag_namespace
-- -----------------------------------------------------------------------------

local tag_namespace = ARGV[1] or ''

-- -----------------------------------------------------------------------------

local tag_index_key = "__TAG_INDEX__"

for _, tag in pairs(redis.call("smembers", tag_namespace .. tag_index_key)) do
    local size = redis.call("scard", tag_namespace .. tag)
    local deleted = 0

    for _, key in pairs(redis.call("smembers", tag_namespace .. tag)) do
        if redis.call("exists", key) == 0 then
            redis.call("srem", tag_namespace .. tag, key)

            deleted = deleted + 1
        end
    end

    if size > 0 and size - deleted == 0 then
        redis.call("srem", tag_namespace .. tag_index_key, tag)
    end
end

return redis.status_reply("OK")
