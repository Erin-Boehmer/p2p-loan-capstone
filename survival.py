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
kmf.fit(T, event_observed=E) # more succiently, kmf.fit(T,E)


kmf.survival_function_
kmf.median_
kmf.plot()
plt.show()


