from pyesgf.search import SearchConnection

conn = SearchConnection('http://esgf-data.dkrz.de/esg-search', distrib=True)
ctx = conn.new_context(project='CMIP5', query='humidity')
ctx.hit_count
ctx = conn.new_context(project='CMIP5', model='HadCM3', experiment='decadal2000', time_frequency='day')
ctx = ctx.constrain(realm='ocean', ensemble='r1i2p1')
ctx.hit_count

result = ctx.search()[0]
print ctx.connection.get_shard_list().keys()
agg_ctx = result.aggregation_context()
agg = agg_ctx.search()[0]
print agg.opendap_url
