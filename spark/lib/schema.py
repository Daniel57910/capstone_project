'''define rows for each rdd structure'''
schema = {}

schema['artist_schema']         = ['artist_id', 'artist_name', 'artist_latitude', 'artist_longitude', 'artist_location']
schema['song_schema']           = ['song_id',  'title', 'year', 'duration', 'artist_id']
schema['timestamp_schema']      = ['year', 'month', 'day', 'minute', 'second', 'hour', 'weekday', 'ts']
schema['app_user_schema']       = ['firstName', 'gender', 'lastName', 'level', 'location', 'userId', 'ts']

schema['artist_and_song_join']  = ['artist_id', 'song_id', 'artist_name', 'title']
schema['songplay_schema']       = ['ts', 'userId', 'level', 'artist', 'song', 'sessionId', 'location', 'userAgent', 'page']

