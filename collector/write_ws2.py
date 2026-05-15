target = '/root/bingx-collector/ws_collector.py'
content = open(target).read().replace(
    'FLUSH_INTERVAL = 2.0',
    'FLUSH_INTERVAL = 5.0'
).replace(
    'if candle["is_closed"] and _agg_queue is not None:',
    'logger.info(f"TICK {candle[\'symbol\']} c={candle[\'close\']}")\n    if candle["is_closed"] and _agg_queue is not None:'
)
open(target, 'w').write(content)
print("Done")
