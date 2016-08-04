require_once "../functions/is_empty"

-- Increment existing @field_data by @step in hashmap given by @key.
--
-- parameters:
-- - KEYS[1] key
-- - ARGV[1] step - positive / negative integer / float
-- - ARGV[2] field_data

-- return string|nil new value on success, nil if key doesn't exist / is not a hashmap / @field_data doesn't contain number
-- -----------------------------------------------------------------------------

local key = KEYS[1]
local step = ARGV[1]
local field_data = ARGV[2]

assert(not is_empty(key), "key cannot be empty")
assert(not is_empty(step), "step cannot be empty")
assert(not is_empty(field_data), "field_data cannot be empty")

step = tonumber(step)

assert(not is_empty(step), "step must be number")

-- -----------------------------------------------------------------------------

-- only increment existing values
if redis.call("exists", key) == 1 then
    local result = redis.pcall("hincrbyfloat", key, field_data, step)

    if type(result) == "string" then
        return result
    end
end
