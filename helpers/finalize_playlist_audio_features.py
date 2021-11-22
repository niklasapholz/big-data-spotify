import pandas as pd
import sys

def finalize_audio_features(raw_file_path: str, category: str)->None:
  df = pd.read_csv(raw_file_path, sep='\t')
  df.drop(['uri', 'track_href', 'analysis_url', 'type'], axis=1, inplace=True)
  df['category'] = category
  df.to_csv('./audio_features/final/' + category + '.csv', sep='\t', index=False)


if __name__ == '__main__':
  if len(sys.argv) == 3:
    raw_file_path = sys.argv[1]
    category = sys.argv[2]
    finalize_audio_features(raw_file_path, category)
  else:
    print('Please provide a filepath to a raw audio_features file!')