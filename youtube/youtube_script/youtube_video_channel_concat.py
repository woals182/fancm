import pandas as pd

path = '파일 위치'

df1 = pd.read_csv(f'{path}/youtube_name.csv')
df2 = pd.read_csv(f'{path}/youtube_video_id.csv')
df3 = pd.read_csv(f'{path}/youtube_channel_id.csv')

df1_ = df1.drop('Unnamed: 0',axis=1)
df2_ = df2.drop('Unnamed: 0',axis=1)
df3_ = df3.drop('Unnamed: 0',axis=1)

print(df1_.head(5))
print(df2_.head(5))

merge_df = pd.merge(df1_, df2_, on='channel_name', how='inner')
merge_df.drop_duplicates(subset='channel_name', inplace=True)

print(merge_df.head(5))

merge_df_ = pd.merge(merge_df, df3_, on='channel_name', how='inner')
merge_df_.drop_duplicates(subset='channel_name', inplace=True)

merge_df_.to_csv(f'{path}/youtube_name_video_id.csv',encoding='utf-8')
