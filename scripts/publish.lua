-- Atomically dedup, sequence, store and broadcast one event.
--
-- KEYS (all share the {topic} hash tag, so one cluster slot):
--   1 seq counter   2 ver counter   3 log stream   4 state hash
--   5 latest hash   6 dedup key for this eventId
-- ARGV:
--   1 "1" if reliable (status/SOS), "0" for location
--   2 event JSON object without seq/ver
--   3 stream MAXLEN (approximate)
--   4 dedup TTL in milliseconds
--   5 hash field: "cabId|kind" for state, "cabId" for latest
--   6 pub/sub channel
-- Returns {pos, dup}: pos is the Seq (reliable) or Ver (location).

local old = redis.call('GET', KEYS[6])
if old then
  return {tonumber(old), 1}
end

local reliable = ARGV[1] == '1'
local pos, field
if reliable then
  pos = redis.call('INCR', KEYS[1])
  field = '"seq":'
else
  pos = redis.call('INCR', KEYS[2])
  field = '"ver":'
end

-- Put the position into the JSON without decoding it, so the payload bytes
-- are stored exactly as sent.
local body = ARGV[2]
if string.sub(body, 2, 2) == '}' then
  body = '{' .. field .. pos .. '}'
else
  body = '{' .. field .. pos .. ',' .. string.sub(body, 2)
end

if reliable then
  redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[3], pos .. '-0', 'ev', body)
  redis.call('HSET', KEYS[4], ARGV[5], body)
else
  redis.call('HSET', KEYS[5], ARGV[5], body)
end

redis.call('SET', KEYS[6], pos, 'PX', ARGV[4])
redis.call('PUBLISH', ARGV[6], body)
return {pos, 0}
