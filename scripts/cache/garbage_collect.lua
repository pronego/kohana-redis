--
-- parameters:
-- - ARGV[1] tag_prefix
-- -----------------------------------------------------------------------------

local tag_prefix = ARGV[1]

-- -----------------------------------------------------------------------------

local tag_index_key = "__TAG_INDEX__"

for _, tag in pairs(redis.call("smembers", tag_prefix .. tag_index_key)) do
    local size = redis.call("scard", tag_prefix .. tag)
    local deleted = 0

    for _, key in pairs(redis.call("smembers", tag_prefix .. tag)) do
        if redis.call("exists", key) == 0 then
            redis.call("srem", tag_prefix .. tag, key)

            deleted = deleted + 1
        end
    end

    if size > 0 and size - deleted == 0 then
        redis.call("srem", tag_prefix .. tag_index_key, tag)
    end
end

return redis.status_reply("OK")
