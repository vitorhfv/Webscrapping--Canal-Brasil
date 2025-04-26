import pandas as pd

## tratamento

df = pd.read_csv('data_raw/dataraw.csv')

df['datetime'] = pd.to_datetime(df['data']
                                            + ' '
                                            + df['horario'], errors='coerce')

df['tem_imdb'] = df['genero_ano'].str.contains('/', regex=True)

df['duracao'] = df['datetime'].shift(-1) - df['datetime']

df['duracao_min'] = df['duracao'].dt.total_seconds() / 60

df = df[
        ['titulo', 'datetime', 'duracao_min',
         'genero_ano', 'sinopse', 'tem_imdb']
        ]

df.to_csv('/data/data_tratada.csv', index = False)