import pandas as pd
import io
import bz2

from lifelines import KaplanMeierFitter

import matplotlib.pyplot as plt

df = pd.read_csv('joined.csv.bz2', sep=',', compression='bz2', low_memory=False)

# strip ' months' in column 'term'
df['term'] = df['term'].map(lambda x: int(x.strip(' months')))

# prepare column 'T' for training survival model
df['T'] = df['firstMissed'] / df['term']
df.loc[df['loan_status']=='Fully Paid', 'T']=1

# column 'E' seems to be column 'censored'

T = df['T']
E = ~df['censored']

kmf = KaplanMeierFitter()

ix = df['grade'] == 'A'
kmf.fit(T[ix], E[ix], label='Grade: A')
ax = kmf.plot()

ix = df['grade'] == 'B'
kmf.fit(T[ix], E[ix], label='Grade: B')
kmf.plot(ax=ax)

ix = df['grade'] == 'C'
kmf.fit(T[ix], E[ix], label='Grade: C')
kmf.plot(ax=ax)

ix = df['grade'] == 'D'
kmf.fit(T[ix], E[ix], label='Grade: D')
kmf.plot(ax=ax)

ix = df['grade'] == 'E'
kmf.fit(T[ix], E[ix], label='Grade: E')
kmf.plot(ax=ax)

ix = df['grade'] == 'F'
kmf.fit(T[ix], E[ix], label='Grade: F')
kmf.plot(ax=ax)

ix = df['grade'] == 'G'
kmf.fit(T[ix], E[ix], label='Grade: G')
kmf.plot(ax=ax)

plt.show()
