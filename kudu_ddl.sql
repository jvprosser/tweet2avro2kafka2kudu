
USE DEFAULT;
CREATE TABLE STATUS_TWEETS (
  ID_STR          STRING,
  TWEET_TS        BIGINT,
  TWEET_ID        BIGINT,
  FOLLOWERS_COUNT INT,
  STATUSES_COUNT  INT,
  FRIENDS_COUNT   INT,
  TEXT            STRING,
  SCREEN_NAME     STRING,
  PRIMARY KEY(ID_STR,tweet_ts)
)
DISTRIBUTE BY HASH (id_str) INTO 3 BUCKETS
STORED AS KUDU
TBLPROPERTIES(
  'kudu.master_addresses' = 'ip-172-31-6-171.us-west-2.compute.internal:7051'
);

# 
# CREATE TABLE KTEST (
#   TWEET_ID        BIGINT,
#   FOLLOWERS_COUNT INT,
#   STATUSES_COUNT  INT,
#   ID_STR          STRING,
#   FRIENDS_COUNT   INT,
#   TEXT            STRING,
#   TWEET_TS        BIGINT,
#   SCREEN_NAME     STRING
# )
# STORED AS PARQUET
# 
# op = table.new_insert({
# 'ID_STR'          :  data.id_str          ,
# 'TWEET_TS'        :  data.tweet_ts        ,
# 'TWEET_ID'        :  data.tweet_id	  ,
# 'FOLLOWERS_COUNT' :  data.followers_count ,
# 'STATUSES_COUNT'  :  data.statuses_count  ,	  
# 'FRIENDS_COUNT'   :  data.friends_count	  ,
# 'TEXT'            :  data.text		  ,
# 'SCREEN_NAME'     :  data.screen_name	  })
# session.apply(op)
# 
# 
# 
# # Insert a row
# op = table.new_insert({'key': 1, 'ts_val': datetime.utcnow()})
# 
# 
# # Upsert a row
# op = table.new_upsert({'key': 2, 'ts_val': "2016-01-01T00:00:00.000000"})
# session.apply(op)
# 
# # Updating a row
# op = table.new_update({'key': 1, 'ts_val': ("2017-01-01", "%Y-%m-%d")})
# session.apply(op)
# 
# # Delete a row
# op = table.new_delete({'key': 2})
# session.apply(op)
# 
# # Flush write operations, if failures occur, capture print them.
# try:
#     session.flush()
# except kudu.KuduBadStatus as e:
#     print(session.get_pending_errors())
# 
# 
#               'ID_STR'          :  data.id_str          ,
#               'TWEET_TS'        :  data.tweet_ts        ,
#               'TWEET_ID'        :  data.tweet_id        ,
#               'FOLLOWERS_COUNT' :  data.followers_count ,
#               'STATUSES_COUNT'  :  data.statuses_count  ,
#               'FRIENDS_COUNT'   :  data.friends_count   ,
#               'TEXT'            :  data.text            ,
#               'SCREEN_NAME'     :  data.screen_name     })
